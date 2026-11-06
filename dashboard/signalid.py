#!/usr/bin/env python3
"""
signalid.py - multi-protocol carrier classifier for SDR Looking Glass.

Given a frequency, captures IQ from the bladeRF (caller must ensure the device is
free, i.e. OP25 stopped), then identifies the signal:
  - POCSAG / FLEX paging  -> decoded via multimon-ng (returns the actual messages)
  - analog FM voice       -> wide occupied BW, voiceband energy
  - narrowband digital     -> 2-level (FSK/data) vs 4-level (C4FM/DMR/NXDN-like)
  - empty / weak
P25 and DMR identity are confirmed separately by the OP25 decode-probes in app.py;
this module covers everything else (and gives a spectral hint for the digital ones).
"""
import os
import re
import subprocess
import tempfile
import numpy as np
try:
    from scipy import signal as sps
    HAVE_SCIPY = True
except Exception:
    HAVE_SCIPY = False
try:
    import pocsag                            # built-in POCSAG decoder (no multimon-ng needed)
except Exception:
    pocsag = None

# bladeRF-cli: native Windows uses radioconda's exe; WSL/Linux uses PATH.
BLADE = os.environ.get("BLADERF_CLI") or (
    os.path.expanduser(r"~\radioconda\Library\bin\bladeRF-cli.exe") if os.name == "nt" else "bladeRF-cli")
# Per-process temp paths so concurrent processes never collide on the same scratch file.
_TMP = os.environ.get("OP25_TMP") or os.path.join(tempfile.gettempdir(), "op25lg")
os.makedirs(_TMP, exist_ok=True)
_PID = os.getpid()
CAP = os.path.join(_TMP, "sigid-%d.sc16" % _PID)
RAW = os.path.join(_TMP, "sigid-%d.raw" % _PID)

RTL = (os.environ.get("SDR_KIND") or "").lower() == "rtl"

def capture_rtl(freq_hz, rate, secs, path=CAP):
    """rtl_sdr (cu8) on the pinned dongle (SDR_ARGS rtl=<serial|index>). 225k-300k / 900k-3.2M are the only
    valid RTL rates, so capture at an integer multiple >= 900k and decimate back to `rate`."""
    m = max(1, -(-900000 // rate)); fs = rate * m
    dev = (os.environ.get("SDR_ARGS") or "rtl=0").split("rtl=")[-1].split(",")[0] or "0"
    gain = os.environ.get("SDR_RTL_GAIN") or "40"
    try:
        subprocess.run(["rtl_sdr", "-d", dev, "-f", str(int(freq_hz)), "-s", str(fs), "-g", gain,
                        "-n", str(int(fs * secs)), path], timeout=int(secs) + 15,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        raw = np.fromfile(path, dtype=np.uint8)
    except Exception:
        return None
    if raw.size < 4000:
        return None
    iq = (raw[0::2].astype(np.float32) - 127.5 + 1j * (raw[1::2].astype(np.float32) - 127.5)) / 127.5
    if m > 1:                                                  # FIR decimate (block average if no scipy)
        iq = sps.decimate(iq, m, ftype="fir") if HAVE_SCIPY else iq[: iq.size // m * m].reshape(-1, m).mean(axis=1)
    return iq.astype(np.complex64)

def capture(freq_hz, rate, secs, gain=50, path=CAP):
    if RTL:
        return capture_rtl(freq_hz, rate, secs, path)
    n = int(rate * secs)
    bw = max(1500000, int(rate * 0.8))     # bladeRF analog LPF minimum is 1.5 MHz
    cmd = ("set frequency rx %d; set samplerate rx %d; set bandwidth rx %d; set gain rx %d; "
           "rx config file=%s format=bin n=%d; rx start; rx wait") % (freq_hz, rate, bw, gain, path, n)
    try:
        subprocess.run([BLADE, "-e", cmd], timeout=int(secs) + 18,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        return None
    try:
        raw = np.fromfile(path, dtype=np.int16)
    except Exception:
        return None
    if raw.size < 2000:
        return None
    return (raw[0::2].astype(np.float32) + 1j * raw[1::2].astype(np.float32)) / 2048.0

def _measure(iqc, fs, nfft=4096):
    """iqc must be CENTERED on the carrier. Returns dict with windowed PSD-based
    metrics: occupied 99% bandwidth (noise-subtracted), peak, noise floor, snr."""
    nseg = len(iqc) // nfft
    if nseg < 1:
        return {"bw": 0.0, "pk": -120.0, "nf": -120.0, "snr": 0.0}
    win = np.hanning(nfft)
    acc = np.zeros(nfft)
    for i in range(nseg):
        acc += np.abs(np.fft.fftshift(np.fft.fft(iqc[i*nfft:(i+1)*nfft] * win))) ** 2
    psd = acc / nseg
    pdb = 10 * np.log10(psd + 1e-12)
    f = np.fft.fftshift(np.fft.fftfreq(nfft, 1.0 / fs))
    skirt = (np.abs(f) > 80000) & (np.abs(f) < 200000)         # clean noise (Hanning kills leakage)
    nf = float(np.median(pdb[skirt])) if skirt.any() else float(np.median(pdb))
    inwin = np.abs(f) < 30000
    pk = float(pdb[inwin].max())
    # occupied bandwidth = 99% of noise-subtracted power within +/-30 kHz
    fwin = f[inwin]
    lin = np.clip(psd[inwin] - 10 ** (nf / 10), 0.0, None)
    order = np.argsort(fwin); fo = fwin[order]; lo = lin[order]
    tot = lo.sum()
    if tot <= 0:
        occ = 0.0
    else:
        cum = np.cumsum(lo) / tot
        i_lo = int(np.searchsorted(cum, 0.005)); i_hi = int(np.searchsorted(cum, 0.995))
        i_lo = min(i_lo, len(fo) - 1); i_hi = min(i_hi, len(fo) - 1)
        occ = float(fo[i_hi] - fo[i_lo])
    return {"bw": occ, "pk": pk, "nf": nf, "snr": round(pk - nf, 1)}

def _channelize(iq, fs, off_hz, bw=16000, out_rate=50000):
    n = np.arange(iq.size)
    x = iq * np.exp(-1j * 2 * np.pi * off_hz / fs * n)        # carrier (at +off) -> DC
    if HAVE_SCIPY:
        taps = sps.firwin(151, (bw / 2) / (fs / 2))
        x = sps.lfilter(taps, 1.0, x)
    dec = max(1, int(fs // out_rate))
    return x[::dec], fs / dec

def _find_carrier(iqc, fs, search=6000, nfft=8192):
    """Dominant carrier offset (Hz) within +/-search of DC, else 0. iqc must already be
    centered on the tuned frequency. Corrects the SDR's residual frequency error (the
    bladeRF runs a few kHz off at 460 MHz) so bursty paging lands inside the channel filter."""
    nseg = iqc.size // nfft
    if nseg < 1:
        return 0.0
    win = np.hanning(nfft)
    acc = np.zeros(nfft)
    for i in range(nseg):
        acc += np.abs(np.fft.fftshift(np.fft.fft(iqc[i*nfft:(i+1)*nfft] * win))) ** 2
    f = np.fft.fftshift(np.fft.fftfreq(nfft, 1.0 / fs))
    far = np.abs(f) > 60000
    nf = float(np.median(acc[far])) if far.any() else float(np.median(acc))
    band = np.abs(f) < search
    pk = int(np.argmax(np.where(band, acc, 0.0)))
    if acc[pk] < nf * 4.0:              # nothing clearly (~6 dB) above noise -> no carrier
        return 0.0
    return float(f[pk])

def _fm_demod(x, fs):
    d = x[1:] * np.conj(x[:-1])
    return (np.angle(d) * (fs / (2 * np.pi))).astype(np.float32)

def _to_22050(audio, fs):
    if HAVE_SCIPY:
        g = np.gcd(22050, int(fs)); a = sps.resample_poly(audio, 22050 // g, int(fs) // g)
    else:
        idx = (np.arange(int(len(audio) * 22050 / fs)) * fs / 22050).astype(int)
        a = audio[np.clip(idx, 0, len(audio) - 1)]
    a = a - np.mean(a); mx = np.max(np.abs(a)) or 1.0
    return (a / mx * 20000).astype(np.int16)

def _max_letter_run(c):
    run = best = 0
    for ch in c:
        if ch.isalpha():
            run += 1
            best = max(best, run)
        else:
            run = 0
    return best

def _plausible(m):
    """True if a decoded page looks like real content (vs marginal-SNR garbage that still
    passed CRC). Tone pages always pass; numeric needs mostly digits; alpha needs an actual
    run of letters (real dispatch has words) and a mostly-readable character mix."""
    c = (m.get("content") or "")
    t = m.get("type")
    if t == "tone":
        return True
    if not c:
        return False
    if t == "numeric":
        digits = sum(ch.isdigit() for ch in c)
        return len(c) >= 3 and digits >= 3 and digits / len(c) >= 0.7
    letters = sum(ch.isalpha() for ch in c)
    readable = sum(1 for ch in c if ch.isalnum() or ch in " .,:/-#()&'@")
    return (len(c) >= 4 and letters >= 3 and _max_letter_run(c) >= 3
            and readable / len(c) >= 0.7)


def _fmt_msg(m, baud):
    if m["type"] == "tone":
        return "POCSAG%s: Address: %d  Function: %d  Tone Only" % (baud, m["address"], m["function"])
    label = "Alpha" if m["type"] == "alpha" else "Numeric"
    return "POCSAG%s: Address: %d  Function: %d  %s: %s" % (baud, m["address"], m["function"], label, m["content"])


def decode_pocsag_structured(aud, fsd):
    """Decode POCSAG from FM-demod audio (auto-baud). Returns (list of message dicts
    each with address/function/type/content/baud, locked_baud)."""
    if pocsag is None:
        return [], None
    r = pocsag.decode_audio(aud, fsd)
    baud = r.get("locked_baud")
    return [dict(m, baud=baud) for m in r["messages"]], baud


def decode_pocsag_audio(aud, fsd):
    """multimon-ng-style strings, so existing string callers stay unchanged."""
    msgs, baud = decode_pocsag_structured(aud, fsd)
    return [_fmt_msg(m, baud or "?") for m in msgs]


def run_multimon(audio_i16, demods=("POCSAG512", "POCSAG1200", "POCSAG2400", "FLEX")):
    audio_i16.tofile(RAW)
    args = ["multimon-ng", "-t", "raw", "-q"]
    for d in demods: args += ["-a", d]
    args += [RAW]
    try:
        out = subprocess.run(args, capture_output=True, text=True, timeout=40).stdout
    except Exception:
        return []
    msgs = []
    for ln in out.splitlines():
        ln = ln.strip()
        if ln.startswith("POCSAG") or ln.startswith("FLEX"):
            msgs.append(ln)
    return msgs

def _levels(aud, dev=4500):
    """Rough modulation hint from the FM-discriminator histogram."""
    sel = aud[np.abs(aud) < dev * 1.6]
    if sel.size < 2000: return "weak", 0
    h, edges = np.histogram(sel, bins=60, range=(-dev * 1.6, dev * 1.6))
    h = h.astype(float)
    if h.max() < 1: return "weak", 0
    hs = np.convolve(h, np.ones(3) / 3, "same")
    thr = hs.max() * 0.30
    peaks = 0; i = 1
    while i < len(hs) - 1:
        if hs[i] > thr and hs[i] >= hs[i-1] and hs[i] > hs[i+1]:
            peaks += 1; i += 3
        else:
            i += 1
    return ("2-level" if peaks <= 2 else ("4-level" if peaks <= 4 else "multi")), peaks

def classify(freq_mhz, secs=3.0):
    fs = 600000; off = 120000
    iq = capture(int(freq_mhz * 1e6) - off, fs, secs)
    if iq is None or iq.size < fs // 2:
        return {"type": "NO-SIG", "detail": "no samples"}
    n = np.arange(iq.size)
    iqc = iq * np.exp(-1j * 2 * np.pi * off / fs * n)         # center the carrier first
    m = _measure(iqc, fs)
    bw, snr = m["bw"], m["snr"]
    if snr < 12:
        return {"type": "WEAK", "detail": "snr %.0f dB" % snr, "snr": snr, "bw_khz": round(bw / 1000, 1)}
    x, fsd = _channelize(iqc, fs, 0, bw=16000, out_rate=50000)   # already centered
    aud = _fm_demod(x, fsd)
    msgs = decode_pocsag_audio(aud, fsd) or run_multimon(_to_22050(aud, fsd))
    if msgs:
        proto = "FLEX" if any(m.startswith("FLEX") for m in msgs) else "POCSAG"
        return {"type": proto, "detail": " | ".join(msgs[:6]), "count": len(msgs),
                "bw_khz": round(bw / 1000, 1), "snr": snr}
    lvl, peaks = _levels(aud)
    bk = bw / 1000.0
    if bk < 1.0:
        guess = "UNMODULATED CARRIER (CW/idle)"               # all power at DC: dead key / STL / control tone idle
    elif bk >= 16.0:
        guess = "ANALOG/WIDE-FM (voice)"
    elif lvl == "4-level" and bk <= 7.5:
        guess = "4-FSK NARROW (NXDN 6.25k?)"
    elif lvl == "4-level":
        guess = "4-FSK 12.5k (P25 C4FM / DMR?)"
    elif lvl == "2-level" and bk <= 4.0:
        guess = "2-FSK NARROW (data/MDC/idle?)"
    elif lvl == "2-level":
        guess = "2-FSK (data / paging?)"
    else:
        guess = "DIGITAL ~%.0fk" % bk
    return {"type": guess, "detail": "bw~%.1fkHz lvl=%s snr=%ddB" % (bk, lvl, snr),
            "bw_khz": round(bk, 1), "snr": snr, "levels": lvl}

_MM = re.compile(r"^(POCSAG(\d+)):\s+Address:\s*(\d+)\s+Function:\s*(\d+)(?:\s+(Alpha|Numeric):\s?(.*))?$")

def pocsag_stream_rtl(freq_mhz):
    """RTL path: rtl_fm | multimon-ng (DSP in C; the numpy chain needs ~25 s per 12 s window on a CM4).
    -E dc removes the discriminator DC from the RTL's ~2 kHz carrier offset; -F 9 = better downsample FIR;
    -f auto = alpha vs numeric per page (multimon's default forced numeric -> garbage text).
    Returns (rtl_fm, multimon) Popens; read multimon's stdout lines, parse with parse_mm()."""
    dev = (os.environ.get("SDR_ARGS") or "rtl=0").split("rtl=")[-1].split(",")[0] or "0"
    fm = ["rtl_fm", "-d", dev, "-M", "fm", "-f", str(int(freq_mhz * 1e6)), "-s", "22050", "-E", "dc", "-F", "9",
          "-g", os.environ.get("SDR_RTL_GAIN") or "40", "-p", os.environ.get("SDR_PPM") or "0", "-"]
    mm = ["multimon-ng", "-t", "raw", "-q", "-a", "POCSAG512", "-a", "POCSAG1200", "-a", "POCSAG2400", "-a", "FLEX",
          "-f", "auto", "-"]
    p1 = subprocess.Popen(fm, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    p2 = subprocess.Popen(mm, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
    p1.stdout.close()
    return p1, p2

def parse_mm(ln):
    """One multimon-ng line -> POCSAG message dict, a FLEX line string, or None."""
    ln = ln.rstrip()
    m = _MM.match(ln)
    if m:
        c = re.sub(r"(<(NUL|EOT|ETX|ETB)>)+\s*$", "", (m.group(6) or "")).strip()   # trailing fill characters
        return {"address": int(m.group(3)), "function": int(m.group(4)), "type": (m.group(5) or "tone").lower(),
                "content": c, "baud": int(m.group(2))}
    return ln if ln.startswith("FLEX") else None

def decode_pocsag_rtl(freq_mhz, secs=12.0):
    """One fixed window through pocsag_stream_rtl (the Scanner's one-shot POCSAG probe; the pager streams)."""
    try:
        p1, p2 = pocsag_stream_rtl(freq_mhz)
        try: p1.wait(timeout=secs)
        except subprocess.TimeoutExpired: p1.terminate(); p1.wait(timeout=5)
        out = p2.communicate(timeout=10)[0]
    except Exception as e:
        return {"ok": False, "msgs": [], "messages": [], "detail": str(e)}
    msgs, messages = [], []
    for ln in out.splitlines():
        r = parse_mm(ln)
        if isinstance(r, dict):
            messages.append(r)
            if _plausible(r): msgs.append(_fmt_msg(r, r["baud"]))
        elif r:
            msgs.append(r)
    return {"ok": bool(msgs or messages), "msgs": msgs, "messages": messages, "count": len(msgs), "carrier_hz": 0}

def decode_pocsag(freq_mhz, secs=12.0):
    """Dedicated longer capture to catch intermittent paging traffic and decode it.
    Uses the built-in POCSAG decoder; falls back to multimon-ng for FLEX if installed."""
    if RTL:
        return decode_pocsag_rtl(freq_mhz, secs)
    fs = 600000; off = 120000
    iq = capture(int(freq_mhz * 1e6) - off, fs, secs)
    if iq is None:
        return {"ok": False, "msgs": [], "messages": [], "detail": "no samples"}
    n = np.arange(iq.size)
    iqc = iq * np.exp(-1j * 2 * np.pi * off / fs * n)        # tuned carrier -> DC
    carrier = _find_carrier(iqc, fs)                          # correct the SDR's frequency error
    x, fsd = _channelize(iq, fs, off + carrier, bw=16000, out_rate=50000)
    aud = _fm_demod(x, fsd)
    messages, baud = decode_pocsag_structured(aud, fsd)
    msgs = [_fmt_msg(m, baud or "?") for m in messages if _plausible(m)]   # display: drop junk
    if not msgs and not messages:
        msgs = [m for m in run_multimon(_to_22050(aud, fsd)) if m.startswith("FLEX")]
    return {"ok": bool(msgs or messages), "msgs": msgs, "messages": messages,
            "count": len(msgs), "carrier_hz": round(carrier)}

if __name__ == "__main__":
    import sys
    f = float(sys.argv[1]); secs = float(sys.argv[2]) if len(sys.argv) > 2 else 4.0
    print(classify(f, secs))
