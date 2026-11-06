#!/usr/bin/env python3
"""
SDR Looking Glass - backend.

Manages OP25 multi_rx as a child process (so the browser can switch systems,
edit SDR settings, etc.), parses its decoded events, maintains live state, and
serves the dashboard + SSE feed. Run as root (sudo) so it can open the bladeRF.

Storage (next to this script):
  lookingglass.db - SQLite store: systems registry + talkgroup/radio tags (see db.py)
  legacy systems.json / tags.json / rids.json are imported once then archived
"""
import os, re, json, time, threading, queue, collections, subprocess, signal, atexit, sys, tempfile, struct
import urllib.request
import numpy as np
import db                                 # SQLite flat-file store (systems / tags / rids)
try:
    import signalid                       # multi-protocol carrier classifier (POCSAG/FLEX/analog/spectral)
except Exception:
    signalid = None

HERE = os.path.dirname(os.path.abspath(__file__))

# ---- platform: native Windows (radioconda OP25, no WSL) vs WSL/Linux ----
IS_WIN = (os.name == "nt")
TMP = os.environ.get("OP25_TMP") or os.path.join(tempfile.gettempdir(), "op25lg")
os.makedirs(TMP, exist_ok=True)
if IS_WIN:
    APPS = os.environ.get("OP25_APPS", os.path.expanduser(r"~\op25-win\op25\gr-op25_repeater\apps"))
    OP25_PY = os.environ.get("OP25_PYTHON", sys.executable)        # radioconda python (has gnuradio)
else:
    APPS = os.environ.get("OP25_APPS", os.path.expanduser("~%s/op25/op25/gr-op25_repeater/apps" % os.environ.get("SUDO_USER", "")))
    OP25_PY = os.environ.get("OP25_PYTHON", "python3")
BLADE_CLI = os.environ.get("BLADERF_CLI") or (
    os.path.expanduser(r"~\radioconda\Library\bin\bladeRF-cli.exe") if IS_WIN else "bladeRF-cli")
# ---- SDR device profile: SDR_KIND=bladerf (default, x40 on WSL/Windows) | rtl (RTL2832U, e.g. uConsole AIO V2).
# Systems store their own sdr dict; dev_sdr() swaps in this profile's device keys when a stored
# system was saved for the other device kind, so one systems DB works on both rigs.
SDR_KIND = (os.environ.get("SDR_KIND") or "bladerf").lower()
DEV = {"bladerf": {"args": "bladerf=0", "gains": "lna:6,rxvga1:25,rxvga2:20", "offset": -250000, "rate": 1000000,
                   "ppm": 0.8, "wb_rate": 6000000, "ppm_max": 25.0, "can_tx": True},
       # R820T has no TCXO: ppm is per-dongle and large (uConsole unit ~+120 rough) -> SDR_PPM env, wide clamp.
       # Keep the -250 kHz offset: the RTL has a DC spike at center too. 2.4 MS/s is the clean max (0 lost).
       # SDR_ARGS pins the dongle by serial (rtl=<serial>) so a second USB RTL can't steal index 0.
       "rtl":     {"args": os.environ.get("SDR_ARGS") or "rtl=0", "gains": "LNA:40", "offset": -250000, "rate": 1000000,
                   "ppm": float(os.environ.get("SDR_PPM") or 0.0), "wb_rate": 2400000, "ppm_max": 200.0, "can_tx": False},
       }[SDR_KIND]
DEV_KEYS = ("args", "gains", "offset", "rate", "ppm", "wb_rate")

def sdr_defaults(**kw):
    d = {k: DEV[k] for k in ("args", "gains", "offset", "rate", "ppm")}; d.update(kw); return d

def dev_sdr(sdr):
    """sdr dict for OP25 on *this* device: stored values if saved for this device kind, else profile defaults."""
    sdr = dict(sdr or {})
    if not str(sdr.get("args", "")).startswith(DEV["args"].split("=")[0]):
        sdr.update({k: DEV[k] for k in DEV_KEYS})
    for k in DEV_KEYS: sdr.setdefault(k, DEV[k])
    if SDR_KIND == "rtl" and os.environ.get("SDR_ARGS"): sdr["args"] = DEV["args"]   # pinned dongle beats a stored rtl=0
    return sdr
RTL_SDR = os.environ.get("RTL_SDR_BIN") or "rtl_sdr"
# ---- user crypt keys (testing your own AES/DES/ADP keys). Kept OUT of the DB/export/code dir, owner-only,
# in OP25's own crypt_keys JSON format so the file is handed to multi_rx as-is. Key material is never sent back.
KEYS_PATH = os.environ.get("LG_KEYS") or os.path.expanduser("~/.config/sdr-looking-glass/crypt_keys.json")
CRYPT_ALGS = {0x84: ("AES-256", 32), 0x81: ("DES-OFB", 8), 0xAA: ("ADP/RC4", 5)}   # what OP25 can decrypt

def keys_load():
    try:
        with open(KEYS_PATH) as f: return json.load(f)
    except Exception: return {}

def keys_save(d):
    os.makedirs(os.path.dirname(KEYS_PATH), mode=0o700, exist_ok=True)
    tmp = KEYS_PATH + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f: json.dump(d, f, indent=1)
    os.replace(tmp, KEYS_PATH)

def keys_file(): return KEYS_PATH if keys_load() else ""
SYSTEMS_PATH = os.path.join(HERE, "systems.json")
TAGS_PATH = os.path.join(HERE, "tags.json")
RIDS_PATH = os.path.join(HERE, "rids.json")

from flask import Flask, Response, request, jsonify, send_from_directory
app = Flask(__name__)

# ----------------------------------------------------------------- persistence
def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)

DB_PATH = os.path.join(HERE, "lookingglass.db")
db.init_db(DB_PATH)
db.migrate_from_json(SYSTEMS_PATH, TAGS_PATH, RIDS_PATH)   # one-time, idempotent; archives the JSONs
SYSTEMS = db.load_all()                    # {"active":.., "systems":{..}} - same shape as the old JSON
TAGS = db.load_tags()                      # {"talkgroups":{..}, "radios":{..}}
RIDS = db.load_rids()                      # {rid: alias}

def _read_version():
    for p in (os.path.join(HERE, "..", "VERSION"), os.path.join(HERE, "VERSION")):
        try:
            with open(p) as f:
                v = f.read().strip()
                if v: return v
        except Exception:
            pass
    return "0.0.0"
__version__ = _read_version()
WATCH = set()                              # watched RIDs (str)
LISTEN = []                                # talkgroups OP25 should follow (whitelist); empty = follow all
CC_ONLY = [False]                          # True = stay on control channel, never follow voice (capture ALL CC data)
try:                                       # restore pinned talkgroups so the lock survives restarts / travels with the app
    LISTEN[:] = [int(t) for t in (db.get_meta("pinned_tgs", []) or []) if str(t).isdigit()]
except Exception:
    pass
try:                                       # restore watched RIDs (coverage-troubleshooting list)
    WATCH.update(str(r) for r in (db.get_meta("watched_rids", []) or []) if str(r).isdigit())
except Exception:
    pass
try:                                       # restore CC-only (data-focus) mode preference
    CC_ONLY[0] = bool(db.get_meta("cc_only", False))
except Exception:
    pass

def tg_tag(tgid): return TAGS["talkgroups"].get(str(tgid), "")
def rid_alias(rid): return RIDS.get(str(rid), "") or TAGS["radios"].get(str(rid), "")

# ----------------------------------------------------------------- live state
LOCK = threading.Lock()
SYSLOCK = threading.Lock()                 # guards SYSTEMS read-modify-write + db.sync_all
                                           # (scanner thread vs Flask request threads); separate
                                           # from LOCK so slow SQLite I/O never stalls the decoder
STATE = {}
EVENTS = collections.deque(maxlen=2000)
WATCH_EVENTS = collections.deque(maxlen=400)
WEV = {}                                   # rid -> that watched radio's own events (as target or source); kept until cleared
WKEEP = {}                                 # rid -> last-known watch card values; survive OP25 restarts / system switches until cleared
RESET_GEN = [0]                            # bumped by reset_state() so watch counters can carry across resets
RAW = collections.deque(maxlen=600)        # every decoded TSBK/MBT/TDMA line (CC firehose)
OPCOUNTS = {}                              # opcode -> count
SIGQUAL = {}                               # rid -> {rid, rf, ber, count, last, ber_max, rf_min, hist} from pwr_ctl_sig_qual
VOICEQ = {}                                # rid -> voice-frame error rate as WE receive that talker (traffic channel)
DIAGLOG = {"on": False, "file": None, "rows": 0, "error": None}      # radio diagnostics CSV log
WSESS = {"on": False, "start": None, "rids": set(), "events": [], "sq": {}, "vq": {}}   # watch session -> report
SUBS = []

# ---- live RF tuning diagnostics, polled from OP25's own http terminal (:8081) ----
# OP25 computes the demod carrier offset (FLL, in Hz) and current device ppm and serves
# them via the http terminal already enabled in build_cfg(). We poll it for a calibration
# readout: the site reference is GPS-disciplined, so the residual error is essentially our
# TCXO error -> a direct ppm correction. No OP25 patch needed.
OP25_TERM_PORT = 8081
DIAG = {"error_hz": None, "ppm": None, "freq": None, "ts": None, "locked": False,
        "samples": collections.deque(maxlen=40),
        "level_db": None, "level_hist": collections.deque(maxlen=240)}   # channel RSSI (dBFS) from patched OP25, 1/s
REG_RV = {0: "accept", 1: "fail", 2: "deny", 3: "refused"}

def _diag_to_hz(f):
    if f is None: return None
    if isinstance(f, (int, float)):
        f = float(f); return f if f > 1e6 else f * 1e6          # accept Hz or MHz
    m = re.search(r"\d+(?:\.\d+)?", str(f))
    if not m: return None
    v = float(m.group(0)); return v if v > 1e6 else v * 1e6

def _op25_term(cmds, port=OP25_TERM_PORT, timeout=1.5):
    """POST a command list to OP25's http terminal; return the decoded JSON response list."""
    body = json.dumps(cmds).encode()
    req = urllib.request.Request("http://127.0.0.1:%d/" % port, data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode() or "[]")

def diag_median():
    xs = sorted(x for x in list(DIAG["samples"]) if x is not None)   # list() copy is atomic vs the poll thread
    return xs[len(xs) // 2] if xs else None

def suggested_ppm():
    """ppm that would null the measured carrier offset against the GPS-locked control channel.
    delta_ppm = error_Hz / center_Hz * 1e6, subtracted from the ppm in force when measured.
    Sign confirmed empirically against the bladeRF: with '+' the error diverged (drove further
    negative each calibrate), so OP25's tune convention needs '-' here to converge toward 0."""
    err = diag_median(); freq = DIAG.get("freq"); ppm = DIAG.get("ppm")
    if err is None or not freq: return None
    base = float(ppm) if ppm is not None else 0.0
    return round(base - (err / float(freq)) * 1e6, 3)

def diag_poll_loop():
    n = 0
    while True:
        time.sleep(1)                       # 1 s for a responsive RSSI meter; cal samples still every 3rd poll
        n += 1
        if not OP25["running"]:
            with LOCK: DIAG["locked"] = False
            continue
        try:
            resp = _op25_term([{"command": "update", "arg1": 0, "arg2": 0}])
        except Exception:
            continue
        cu = next((m for m in resp if isinstance(m, dict) and m.get("json_type") == "channel_update"), None)
        if not cu: continue
        ids = cu.get("channels") or []
        if not ids: continue
        c = cu.get(str(ids[0])) or cu.get(ids[0]) or {}
        err = c.get("error")
        with LOCK:
            DIAG["error_hz"] = err
            DIAG["ppm"] = c.get("ppm")
            DIAG["freq"] = _diag_to_hz(c.get("freq"))
            DIAG["ts"] = time.strftime("%H:%M:%S")
            DIAG["locked"] = err is not None
            if err is not None and n % 3 == 0: DIAG["samples"].append(err)
            lv = c.get("level")             # None unless OP25 has the lg-rssi patch (clockwork-orange/tools)
            DIAG["level_db"] = lv
            if lv is not None: DIAG["level_hist"].append((round(time.time(), 1), lv))

def diag_reset():
    with LOCK:
        DIAG.update({"error_hz": None, "ppm": None, "freq": None, "ts": None, "locked": False, "level_db": None})
        DIAG["level_hist"].clear()
        DIAG["samples"].clear()

# ---- automatic closed-loop ppm calibration against the GPS-locked control channel ----
# Sidesteps the sign-convention guessing: nudge ppm, re-measure, learn the actual response
# (slope = Hz per ppm) from two points, then Newton-step to null the error; iterate and keep
# the best ppm seen. Each step re-tunes OP25, so it runs in a background thread (tens of seconds).
CAL_TARGET_HZ = 30                          # "good enough" |error| to stop at
CAL_PPM_MAX = DEV["ppm_max"]                # refuse corrections beyond this — bladeRF TCXO: a few ppm; RTL: ~100+
CAL = {"running": False, "phase": "idle", "iter": 0, "log": [], "best": None, "result": None}

def _cal_measure(min_samples=5, timeout=30):
    """Wait for OP25 to re-lock the CC and a stable median error to form, then return it (Hz)."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        time.sleep(2)
        if not OP25["running"]:
            continue
        with LOCK:
            n = len(DIAG["samples"]); locked = DIAG["locked"]
        if locked and n >= min_samples:
            return diag_median()
    return diag_median()                    # best effort if it never fully settled

def _cal_set_ppm(sid, p):
    p = round(max(-CAL_PPM_MAX, min(CAL_PPM_MAX, float(p))), 3)   # clamp to a sane TCXO range
    with SYSLOCK:
        SYSTEMS["systems"][sid]["sdr"]["ppm"] = p
        db.sync_all(SYSTEMS)
    diag_reset(); op25_start()
    return p

def _cal_decoding(min_tsbks=4, timeout=14):
    """True once OP25 is actually decoding the CC again (TSBKs flowing / NAC seen) — i.e. the
    applied ppm didn't break sync. Counters reset on op25_start, so we just watch them climb.
    This is the real calibration guard: a low FLL error doesn't guarantee the system decodes."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        time.sleep(1)
        if not OP25["running"]:
            continue
        with LOCK:
            tsbks = STATE["counters"].get("tsbks", 0)
            nac = STATE["system"].get("nac")
        if nac or tsbks >= min_tsbks:
            return True
    return False

def auto_calibrate(max_iter=6):
    CAL.update({"running": True, "phase": "measuring", "iter": 0, "log": [], "best": None, "result": None})
    try:
        sid = SYSTEMS.get("active")
        if not sid or sid not in SYSTEMS["systems"]:
            CAL["phase"] = "error: no active system"; return
        def cur_ppm():
            with SYSLOCK: return round(float(SYSTEMS["systems"][sid]["sdr"].get("ppm", 0.0) or 0.0), 3)
        meas = []
        def record(pp, ee):
            meas.append((pp, ee)); CAL["log"] = (CAL["log"] + [{"ppm": pp, "err": ee}])[-12:]
            if ee is not None and (CAL["best"] is None or abs(ee) < abs(CAL["best"][1])):
                CAL["best"] = (pp, ee)
        freq = DIAG.get("freq")
        record(cur_ppm(), _cal_measure())                  # baseline at current ppm
        base_err = meas[0][1]
        if base_err is None or not freq:
            CAL["phase"] = "error: no CC lock"; return
        if abs(base_err) <= CAL_TARGET_HZ:
            CAL["result"] = {"ppm": meas[0][0], "err": base_err}; CAL["phase"] = "done"; return
        step = max(abs(base_err) / float(freq) * 1e6, 0.05)  # gain-1 guess for the first nudge
        for it in range(1, max_iter + 1):
            CAL["iter"] = it
            pa, ea = meas[-1]
            if len(meas) >= 2 and meas[-2][1] is not None and ea is not None and meas[-2][0] != pa:
                pp, ep = meas[-2]                          # learn response, Newton-step to null ea
                slope = (ea - ep) / (pa - pp)
                if abs(slope) < 1e-9:
                    CAL["phase"] = "error: ppm has no effect"; break
                p_next = pa - ea / slope
            else:
                p_next = pa + step                         # first move: arbitrary probe; direction self-corrects next
            CAL["phase"] = "tuning (iter %d)" % it
            applied = _cal_set_ppm(sid, p_next)
            CAL["phase"] = "checking (iter %d)" % it
            e_next = _cal_measure()
            record(applied, e_next)
            if e_next is None:
                CAL["phase"] = "lost lock — reverting to best"; break
            if abs(e_next) <= CAL_TARGET_HZ:
                break
        best = CAL["best"]
        if best is None:
            CAL["phase"] = "error: no valid measurement"; return
        start_ppm = meas[0][0]                              # ppm we began with (the CC was decoding)
        if cur_ppm() != best[0]:                            # settle on the best ppm found
            _cal_set_ppm(sid, best[0]); _cal_measure()
        # GUARD: a low FLL error doesn't guarantee the system decodes. Confirm TSBKs flow on
        # the chosen ppm; if not, revert to the known-good ppm we started from.
        CAL["phase"] = "verifying decode..."
        if not _cal_decoding():
            _cal_set_ppm(sid, start_ppm)
            CAL["result"] = {"ppm": start_ppm, "err": meas[0][1], "reverted": True, "rejected": best[0]}
            CAL["phase"] = "reverted: %.3f ppm broke decode, kept %.3f" % (best[0], start_ppm)
            return
        CAL["result"] = {"ppm": best[0], "err": best[1]}; CAL["phase"] = "done"
    except Exception as e:
        CAL["phase"] = "error: %s" % e
    finally:
        CAL["running"] = False

def cal_status():
    return {"running": CAL["running"], "phase": CAL["phase"], "iter": CAL["iter"],
            "best": CAL["best"], "result": CAL["result"], "log": list(CAL["log"]),
            "target_hz": CAL_TARGET_HZ}
POSITIONS = {}                             # rid(int) -> latest GPS position record (for the map)
POS_TRAILS = collections.defaultdict(lambda: collections.deque(maxlen=24))   # rid -> recent points

ALGID = {0x80: "CLEAR", 0x81: "DES-OFB", 0x83: "DES-XL", 0x84: "AES-256",
         0x85: "AES-128", 0x9f: "CLEAR", 0xaa: "ADP/RC4", 0xa0: "DES"}

# CC Viewer (/cc): every control-channel broadcast kept structured - RFSS/network status, adjacent sites,
# IDEN band plan, secondary CCs, services, time, per-opcode last message. OP25 prints ids in hex except
# the TSBK forms of stid (decimal); RFSS/Site are shown decimal, SysID/WACN hex.
CCX = {}
CC_PIN = {"sid": None, "freq": None}          # run a system on one chosen CC first (CC Viewer CC picker / seek)
TEMP_SYS = {}                                  # System Hunter finds live here, unsaved, until the user presses Save
RUN_TEMP = [None]                              # TEMP_SYS id OP25 is running instead of the saved active system
VT_SEEN = {}                                   # payload -> last time (the CC repeats each TSBK; one event per burst)
XLOG = {"counts": {}, "last": {}, "recent": collections.deque(maxlen=200), "since": time.time()}   # OP25 receiver/log events (Diagnostics tab)
def cc_reset():
    XLOG["counts"].clear(); XLOG["last"].clear(); XLOG["recent"].clear(); XLOG["since"] = time.time()
    CCX.clear()
    CCX.update({"msgs": {}, "adj": {}, "iden": {}, "sccb": {}, "rfss": None, "net": None, "srv": None,
                "time": None, "sync": None, "pparm": None, "since": time.time(),
                "mload": None, "m05": None, "shutdown": None, "vendor": {}, "sndcp": None})
    VT_SEEN.clear()
cc_reset()

def _chf(v):
    """'1234(859.262500)' or '859.2625' -> '859.26250' (MHz) or None."""
    m = re.search(r"\(([\d.]+)\)", v or "") or re.match(r"^(\d{2,4}\.\d+)$", v or "")
    try: return "%.5f" % float(m.group(1)) if m and float(m.group(1)) > 0 else None
    except Exception: return None

def cc_ingest(op, rest, ts):
    """Caller holds LOCK. op = 'tsbk(0x3c) adj_sts_bcst', rest = OP25's 'key: value ...' text."""
    kv = {k: v.rstrip(":,") for k, v in re.findall(r"([A-Za-z][\w/-]*): (\S+)", rest)}
    t = ts.split()[-1].split(".")[0]; kind = op.split("(")[0]; name = op.split()[-1]
    m = CCX["msgs"].setdefault(op, {"op": op, "count": 0, "first": t})
    m["count"] += 1; m["last"] = t; m["text"] = rest[:200]
    H = lambda k: int(kv[k], 16) if kv.get(k) else None
    st = (lambda k: int(kv[k]) if kv.get(k) else None) if kind == "tsbk" else H      # stid: %d in TSBKs, %x elsewhere
    sy = lambda: ("%03X" % H("syid")) if kv.get("syid") else (("%03X" % H("sys")) if kv.get("sys") else None)
    try:
        if name == "adj_sts_bcst":
            if "rfid" not in kv: return                                   # the 'base freq/step' follow-up line
            a = {"sysid": sy() or STATE["system"].get("sysid"), "rfss": H("rfid"), "site": st("stid"),
                 "freq": _chf(kv.get("ch1") or kv.get("ch") or kv.get("f1") or ""), "via": kind, "last": t}
            if kv.get("wacn"): a["wacn"] = "0x" + kv["wacn"]
            if "V" in kv: a.update(flags={"C": kv.get("C") == "1", "F": kv.get("F") == "1", "V": kv.get("V") == "1", "A": kv.get("A") == "1"})
            key = "%s-%s-%s" % (a["sysid"], a["rfss"], a["site"])
            old = CCX["adj"].get(key, {}); a["count"] = old.get("count", 0) + 1; a["first"] = old.get("first", t)
            if not a.get("flags") and old.get("flags"): a["flags"] = old["flags"]
            CCX["adj"][key] = a
        elif name == "rfss_sts_bcst":
            CCX["rfss"] = {"sysid": sy(), "rfss": H("rfid"), "site": st("stid"), "net_active": kv.get("A") == "1" if "A" in kv else None,
                           "freq": _chf(kv.get("ch1") or kv.get("ch") or ""), "via": kind, "last": t}
        elif name == "net_sts_bcst":
            CCX["net"] = {"wacn": "0x" + kv["wacn"] if kv.get("wacn") else None, "sysid": sy(),
                          "lra": ("0x%02X" % H("lra")) if kv.get("lra") else None,
                          "freq": _chf(kv.get("ch1") or kv.get("ch") or ""), "via": kind, "last": t}
        elif name in ("sccb", "sccb_exp"):
            for fk in ("ch1", "ch2", "freq-t"):
                f = _chf(kv.get(fk, ""))
                if f: CCX["sccb"][f] = {"freq": f, "rfss": H("rfid"), "site": st("stid"), "via": op, "last": t}
        elif name.startswith("iden_up"):
            i = kv.get("id"); tdma = "tdma" in name
            if i is None: return
            base = kv.get("freq") or kv.get("base_f"); off = kv.get("toff") or kv.get("offset"); sp = kv.get("spac") or kv.get("spacing")
            sl = re.search(r"slots/carrier:? (\d+)", rest)
            bw = None
            if kv.get("bw"):
                b = int(kv["bw"], 16); bw = {4: 6.25, 5: 12.5}.get(b) if name == "iden_up_vu" else round(b * 0.125, 3)
            CCX["iden"][str(int(i))] = {"id": int(i), "type": "TDMA" if tdma else "FDMA", "base": "%.5f" % float(base),
                "spacing_khz": round(float(sp), 4), "tx_offset_mhz": round(float(off), 4), "bw_khz": bw,
                "slots": int(sl.group(1)) if sl else (1 if not tdma else None), "via": op, "last": t,
                "band": "VHF/UHF" if name == "iden_up_vu" else ""}
        elif name == "sys_srv_bcst":
            CCX["srv"] = {"avail": "0x" + kv.get("avail", ""), "supp": "0x" + kv.get("supp", ""), "prio": kv.get("prio"), "last": t}
        elif name == "time_date_ann":
            CCX["time"] = {"utc_offset_min": kv.get("utc_offset_min"), "vl": kv.get("vl"), "last": t}
        elif name == "tdma_sync_bcst":
            tm = re.search(r"time: (\S+ \S+)", rest)
            CCX["sync"] = {"time": tm.group(1) if tm else None, "lto_min": kv.get("lto_min"), "us": kv.get("us"), "last": t}
        elif name == "p_parm_bcst":
            CCX["pparm"] = {"algid": "0x" + kv.get("algid", ""), "last": t}
        elif name == "sndcp_data_ch":
            ch = [int(kv[k], 16) for k in ("ch1", "ch2") if kv.get(k)]
            if len(ch) < 2: return
            dn = "none" if ch[0] == 0xffff else _chs(ch[0] >> 12, ch[0] & 0xfff)
            upv = None if ch[1] in (0xffff, ch[0]) or (ch[1] & 0xfff) == 0xfff else _chs(ch[1] >> 12, ch[1] & 0xfff)
            up = ", uplink " + upv if upv else ""
            if "dso" not in kv:
                d = "Data channel announcement: packet data (SNDCP) %s." % ("is not offered here right now" if dn == "none" else "is on " + dn + up)
            else:
                aa, ra, dso, rsv, dac = int(kv.get("aa", 0)), int(kv.get("ra", 0)), H("dso"), H("rsv"), H("dac")
                acc = ("open (autonomous) + on request" if aa and ra else "open (autonomous): radios may send without asking" if aa
                       else "on request: radios must ask for the channel first" if ra else "no access flags set")
                p = CCX["sndcp"] or {"rsv_seen": [], "chans": []}
                if rsv not in p["rsv_seen"]: p["rsv_seen"].append(rsv)
                if dn != "none" and dn not in p["chans"]: p["chans"] = sorted(p["chans"] + [dn])[-12:]   # sites rotate through several
                p.update(down=dn, up=upv, aa=aa, ra=ra, dso=dso, dso_txt=_dso(dso), rsv=rsv, dac=dac, last=t)
                CCX["sndcp"] = p
                d = ("Data channel announcement: packet data (SNDCP) %s. Access: %s; service options %s; data access code %04X%s." %
                     ("is not offered here right now" if dn == "none" else "is on " + dn + up, acc, _dso(dso), dac,
                      "" if dac != 0xFFFF else " (FFFF = any radio)") +
                     (" Reserved bits = 0x%02X (normally 0; something new is in them)." % rsv if rsv else "") +
                     " The SNDCP version itself (V1/V2/V3) is not sent here: radios agree on it when they open a data session on the data channel.")
            m["dec"] = d; return d
        elif name == "sn_data_chn_gnt":
            ta = int(kv["ta"]) if kv.get("ta", "").isdigit() else 0
            ch = int(kv.get("ch1", "0").split("(")[0], 16)
            d = "Data channel grant: %s was given packet-data channel %s (%s)." % (_R(ta), _chs(ch >> 12, ch & 0xfff), _dso(H("opts") or 0))
            m["dec"] = d; return d
        elif name == "unhandled" and kind == "tsbk":
            hm = re.search(r"0x([0-9a-f]{24})", rest)
            if hm:
                d = vendor_tsbk(hm.group(1), ts, t)
                if d: m["dec"] = d
                return d
    except Exception:
        pass

# ---- TSBKs OP25 prints as "unhandled": standard opcodes it skips + Motorola (MFID 0x90) ones.
# Bit layouts from SDRTrunk (DSheirer/sdrtrunk tsbk/standard/osp + tsbk/motorola/osp); bit 0 = MSB of the opcode octet.
SVC = {0x00: "group call", 0x02: "group call", 0x03: "group call", 0x04: "unit-to-unit call", 0x05: "unit-to-unit answer", 0x08: "phone call",
       0x14: "data channel", 0x18: "status update", 0x1c: "message", 0x1f: "call alert", 0x20: "acknowledge", 0x24: "extended function",
       0x28: "talkgroup affiliation", 0x2b: "location registration", 0x2c: "unit registration", 0x2f: "de-registration", 0x3e: "encryption"}
DENY_RC = {0x10: "requesting radio not valid", 0x11: "requesting radio not authorized for this service", 0x20: "target radio not valid",
           0x21: "target radio not authorized", 0x2F: "target radio refused the call", 0x30: "target talkgroup not valid",
           0x31: "talkgroup not authorized", 0x40: "invalid dialing", 0x41: "phone number not authorized", 0x42: "PSTN not valid",
           0x50: "call timeout", 0x51: "landline ended the call", 0x52: "radio ended the call", 0x5F: "call preempted",
           0x60: "site access denied", 0x67: "PTT collide", 0x77: "PTT bonk (talk permit refused)", 0xF0: "call options not valid for this service",
           0xF1: "encryption option not valid", 0xF2: "duplex option not valid", 0xF3: "circuit/packet mode not valid",
           0xFF: "system does not support this service"}
QUEUE_RC = {0x10: "requesting radio busy with another service", 0x20: "target radio busy", 0x2F: "target radio queued the call",
            0x30: "talkgroup already in a call", 0x40: "no free voice channels", 0x41: "no telephone resources", 0x42: "no data resources",
            0x50: "a higher-priority service is active"}
XFN = {0x0000: "radio check", 0x007D: "radio detach", 0x007E: "radio UN-INHIBIT (re-enabled)", 0x007F: "radio INHIBIT (stunned / disabled)",
       0x0080: "radio check answered", 0x00FD: "radio detach acknowledged", 0x00FE: "un-inhibit acknowledged", 0x00FF: "inhibit acknowledged",
       0x0200: "regroup: create supergroup", 0x0201: "regroup: cancel supergroup", 0x0280: "regroup create acknowledged", 0x0281: "regroup cancel acknowledged"}

def _b(h, st, n): return (int(h, 16) >> (len(h) * 4 - st - n)) & ((1 << n) - 1)
def _rc(tbl, v): return tbl.get(v) or ("user/system-defined reason 0x%02X" % v if 0x61 <= v <= 0xEF or v >= 0x80 else "reason 0x%02X" % v)
def _R(r): a = rid_alias(r) if r else ""; return ("radio %d%s" % (r, " “%s”" % a if a else "")) if r else "an unidentified radio"
def _T(g): a = tg_tag(g); return "talkgroup %d%s" % (g, " “%s”" % a if a else "")

def _chan_mhz(band, ch):
    i = CCX["iden"].get(str(band))
    if not i: return None
    n = ch // (i.get("slots") or 1) if i["type"] == "TDMA" else ch
    return "%.5f" % (float(i["base"]) + n * i["spacing_khz"] / 1000.0)

def _chs(band, ch):
    f = _chan_mhz(band, ch); return ("%s MHz" % f) if f else "channel %d-%d" % (band, ch)

def _dso(o):                                       # SNDCP data service options octet (SDRTrunk ServiceOptions/DataServiceOptions)
    return "NSAPI %d, %s, %s-duplex, %s mode%s" % (o & 0x0F, "encrypted" if o & 0x40 else "clear", "full" if o & 0x20 else "half",
                                                   "packet" if o & 0x10 else "circuit", ", EMERGENCY" if o & 0x80 else "")

def vendor_tsbk(h, ts, t):
    """Decode one 'unhandled' TSBK (24 hex digits). Returns a plain-English sentence; emits Activity/Watch events. Caller holds LOCK."""
    op = _b(h, 2, 6); mf = _b(h, 8, 8); now = time.time()
    fresh = now - VT_SEEN.get(h[2:], 0) > 4; VT_SEEN[h[2:]] = now
    if len(VT_SEEN) > 500: VT_SEEN.clear()
    v = CCX["vendor"].setdefault("%02X/%02X" % (mf, op), {"count": 0}); v["count"] += 1; v["last"] = t
    ev = (lambda *a, **k: emit(*a, **k)) if fresh else (lambda *a, **k: None)
    tgt, src = _b(h, 56, 24), _b(h, 32, 24)                 # the common target/source slots (swapped in some messages)
    if mf in (0, 1):
        if op in (0x27, 0x21):
            svc, rc = _b(h, 18, 6), _b(h, 24, 8); deny = op == 0x27
            why = _rc(DENY_RC if deny else QUEUE_RC, rc); what = SVC.get(svc, "service 0x%02X" % svc)
            touch_rid(tgt, ts, "denied" if deny else "queued")
            ev("DENY" if deny else "QUEUED", ts, rid=tgt, kind2=what, reason=why, granted=False if deny else None)
            return ("Request DENIED: %s asked for a %s and the system refused: %s." if deny else
                    "Request QUEUED (system busy): %s asked for a %s and must wait: %s.") % (_R(tgt), what, why)
        if op == 0x24:
            fn, arg = _b(h, 16, 16), _b(h, 32, 24); nm = XFN.get(fn, "extended function 0x%04X" % fn)
            touch_rid(tgt, ts, nm); ev("XFN", ts, rid=tgt, kind2=nm, src=arg or None)
            return "Remote command to %s: %s%s." % (_R(tgt), nm, (" (from %s)" % _R(arg)) if arg and fn < 0x200 else "") +                    (" The radio is shut off from the system until un-inhibited." if fn == 0x007F else "")
        if op == 0x1f:
            ev("ALERT", ts, rid=src, src=tgt, kind2="call alert")
            return "Call alert (page): %s is paging %s." % (_R(tgt), _R(src))
        if op in (0x04, 0x06):
            f = _chan_mhz(_b(h, 16, 4), _b(h, 20, 12))
            if op == 0x04: touch_rid(tgt, ts, "uu call"); ev("GRANT", ts, rid=tgt, src=src, kind2="unit-to-unit (private) call", freq=f)
            return "%s: %s is calling %s privately on %s." % ("Private call start" if op == 0x04 else "Private call still going",
                                                               _R(tgt), _R(src), _chs(_b(h, 16, 4), _b(h, 20, 12)))
        if op == 0x05:
            ev("ALERT", ts, rid=src, src=tgt, kind2="private call request")
            return "Private call request: %s wants to call %s; the system is asking the target to answer." % (_R(tgt), _R(src))
        if op in (0x08, 0x09):
            a, tm = _b(h, 56, 24), _b(h, 40, 16)
            if op == 0x08: ev("GRANT", ts, rid=a, kind2="telephone interconnect call", freq=_chan_mhz(_b(h, 24, 4), _b(h, 28, 12)))
            return "Phone call %s: %s on %s (call timer %.1f s)." % ("start" if op == 0x08 else "still going", _R(a), _chs(_b(h, 24, 4), _b(h, 28, 12)), tm * 0.1)
        if op == 0x0a:
            return "Phone call coming in for %s; the system is asking it to answer." % _R(tgt)
        if op == 0x18:
            us, ss = _b(h, 16, 8), _b(h, 24, 8); ev("STATUS", ts, rid=tgt, src=src, kind2="status %d / user status %d" % (us, ss))
            return "Status message: %s sent status %d (user status %d) to %s." % (_R(tgt), us, ss, _R(src))
        if op == 0x1a:
            return "Status query: %s is asking %s for its status." % (_R(tgt), _R(src))
        if op == 0x1c:
            msg = _b(h, 16, 16); ev("STATUS", ts, rid=tgt, src=src, kind2="short message %d" % msg)
            return "Short message %d from %s to %s." % (msg, _R(tgt), _R(src))
        if op == 0x1d:
            ev("XFN", ts, rid=tgt, src=src, kind2="radio unit MONITOR (remote listen)")
            return "Radio unit monitor: %s ordered %s to transmit so dispatch can listen (remote monitor)." % (_R(src), _R(tgt))
        if op == 0x2d:
            ev("REG", ts, rid=src, kind2="registration command")
            return "Registration command: the system told %s to register (log on) again." % _R(src)
        if op == 0x2e:
            return "Authentication challenge: the system asked %s to prove its identity (link-layer authentication)." % _R(tgt)
        if op in (0x36, 0x37):
            return "Roaming address %s for %s (its list of other systems it may roam to)." % ("command" if op == 0x36 else "update", _R(tgt))
        if op == 0x15:
            return "Data page: %s is being paged for a packet-data session (%s, access code %04X)." % (_R(tgt), _dso(_b(h, 16, 8)), _b(h, 40, 16))
        return None
    if mf == 0x90:
        if op == 0x09:                                         # SDRTrunk "ChannelLoading": 12-bit field steps by 20 (octet + 2-bit counter)
            lv = _b(h, 16, 12) // 20; ml = CCX["mload"] or {"min": lv, "max": lv, "hist": []}
            ml.update(level=lv, min=min(ml["min"], lv), max=max(ml["max"], lv), last=t); ml["hist"].append([round(now), lv]); del ml["hist"][:-300]
            CCX["mload"] = ml
            return ("Motorola site loading (vendor-specific, meaning not published): level %d. On this system it idles around %d and has peaked at %d; "
                    "it climbs when calls are being set up, so read it as a site-busy meter." % (lv, ml["min"], ml["max"]))
        if op == 0x05:
            p = h[4:20]; old = CCX["m05"]
            if not old or old["payload"] != p:
                CCX["m05"] = {"payload": p, "since": t, "changes": (old["changes"] + 1) if old else 0, "prev": old["payload"] if old else None}
            m5 = CCX["m05"]
            return ("Motorola traffic-channel ID (vendor-specific, layout not published): %s. Unchanged since %s%s. "
                    "Community analysis has seen one octet change when the site announces a planned control-channel shutdown." %
                    (" ".join(p[i:i + 2] for i in range(0, 16, 2)), m5["since"], (", %d change(s) this run" % m5["changes"]) if m5["changes"] else ""))
        if op == 0x0e:
            CCX["shutdown"] = t; ev("XFN", ts, kind2="CONTROL CHANNEL PLANNED SHUTDOWN")
            return "Planned control-channel shutdown (Motorola): this site is about to take this CC down; radios will move to another CC."
        if op == 0x0a:
            ga, sa = _b(h, 40, 16), _b(h, 56, 24); touch_rid(sa, ts, "emergency alarm", ga)
            if fresh: STATE["counters"]["emergencies"] += 1
            ev("EMERG", ts, rid=sa, tg=ga, emergency=True, kind2="emergency ALARM button")
            return "🚨 EMERGENCY ALARM (Motorola): %s pressed its emergency button on %s." % (_R(sa), _T(ga))
        if op in (0x06, 0x07):
            svc, rc = _b(h, 18, 6), _b(h, 24, 8); deny = op == 0x07; what = SVC.get(svc, "Motorola service 0x%02X" % svc)
            why = ({0: "encrypted request on a clear supergroup", 1: "clear request on an encrypted supergroup"}.get(rc, "Motorola reason 0x%02X" % rc)) if deny and _b(h, 16, 1) else _rc(DENY_RC if deny else QUEUE_RC, rc)
            touch_rid(tgt, ts, "denied" if deny else "queued")
            ev("DENY" if deny else "QUEUED", ts, rid=tgt, kind2=what, reason=why, granted=False if deny else None)
            return "Motorola %s: %s, %s: %s." % ("DENY" if deny else "QUEUED (busy)", _R(tgt), what, why)
        if op == 0x08:
            return "Motorola acknowledge: the system confirmed %s's request to %s." % (_R(src), _R(tgt))
        if op == 0x04:
            cl, od, arg = _b(h, 16, 8), _b(h, 24, 8), _b(h, 32, 24)
            nm = {(2, 0): "regroup: create supergroup %d" % arg, (2, 1): "regroup: cancel supergroup"}.get((cl, od), "extended function class %d operand %d" % (cl, od))
            ev("XFN", ts, rid=tgt, kind2=nm)
            return "Motorola remote command to %s: %s." % (_R(tgt), nm)
        if op == 0x01:
            gs = [g for g in dict.fromkeys([_b(h, 32, 16), _b(h, 48, 16), _b(h, 64, 16)]) if g]
            return "Motorola patch removed: %s taken out of supergroup %d." % (", ".join(_T(g) for g in gs), _b(h, 16, 16))
        if op == 0x16:
            dl, ul = (_b(h, 32, 4), _b(h, 36, 12)), (_b(h, 48, 4), _b(h, 52, 12))
            if dl[1] in (0, 4095): return "Motorola TDMA data channel: this site has no dedicated TDMA data channel."
            return "Motorola TDMA data channel: packet data on %s%s." % (_chs(*dl), "" if ul[1] == 4095 else " (radios send on %s)" % _chs(*ul))
        if op == 0x0f:
            return "Motorola opcode 15: purpose unknown (SDRTrunk doesn't decode it either)."
        return None
    return None

def cc_snapshot():
    """Caller holds LOCK."""
    return {"rfss": CCX["rfss"], "net": CCX["net"], "srv": CCX["srv"], "time": CCX["time"], "sync": CCX["sync"],
            "pparm": CCX["pparm"], "since": CCX["since"], "mload": CCX["mload"] and {k: v for k, v in CCX["mload"].items() if k != "hist"},
            "mload_hist": (CCX["mload"] or {}).get("hist", [])[-120:], "m05": CCX["m05"], "shutdown": CCX["shutdown"], "vendor": CCX["vendor"], "sndcp": CCX["sndcp"],
            "adj": sorted(CCX["adj"].values(), key=lambda a: (str(a.get("sysid")), a.get("rfss") or 0, a.get("site") or 0)),
            "iden": [CCX["iden"][k] for k in sorted(CCX["iden"], key=int)],
            "sccb": sorted(CCX["sccb"].values(), key=lambda x: x["freq"]),
            "msgs": sorted(CCX["msgs"].values(), key=lambda m: m["op"].split()[-1])}

def reset_state():
    with LOCK:
        STATE.clear()
        STATE.update({
            "system": {"nac": None, "wacn": None, "sysid": None, "rfss": None,
                       "site": None, "cc": None, "secondary_cc": [], "last_update": None},
            "bandplan": {}, "adjacent": {}, "talkgroups": {}, "radios": {},
            "cur_voice_tg": None, "chantg": {},
            "datachans": {}, "unhandled_ops": {}, "pdu_samples": [],
            "datastats": {"grants": 0, "pages": 0, "anns": 0, "last_target": None,
                          "pdus": 0, "pdu_payload": 0, "pdu_clear": 0, "pdu_enc": 0},
            "dmr_cc": None, "dmr_variant": None, "netid": None, "siteid": None,
            "counters": {"grants": 0, "emergencies": 0, "encrypted": 0,
                         "affiliations": 0, "registrations": 0, "tsbks": 0,
                         "cc_timeouts": 0, "sync_losses": 0}})
        EVENTS.clear(); RAW.clear(); OPCOUNTS.clear(); SIGQUAL.clear(); VOICEQ.clear()
        RESET_GEN[0] += 1
        cc_reset()
reset_state()

def publish(ev):
    EVENTS.append(ev)
    if ev.get("watch"):
        WATCH_EVENTS.append(ev)
        for k in {str(ev.get("rid")), str(ev.get("src"))} & WATCH:
            WEV.setdefault(k, collections.deque(maxlen=200)).append(ev)
        if WSESS["on"]: wsess_add(ev)
    if DIAGLOG["on"] and ev.get("rid") not in (None, 0, ""):
        diag_row(ev["kind"].lower(), rid=ev.get("rid"), tg=ev.get("tg"), freq=ev.get("freq"), encrypted=ev.get("encrypted"),
                 emergency=ev.get("emergency"), detail=" ".join("%s=%s" % (k, ev[k]) for k in ("kind2", "granted", "reason") if k in ev))
    for q in list(SUBS):
        try: q.put_nowait(ev)
        except queue.Full:
            try: SUBS.remove(q)
            except ValueError: pass

def touch_tg(tgid, **kw):
    tg = STATE["talkgroups"].setdefault(str(tgid), {
        "tgid": tgid, "count": 0, "last": None, "last_rid": None,
        "emergency": False, "encrypted": False, "algid": None, "keyid": None, "last_freq": None})
    tg["count"] += 1; tg["last"] = kw.get("ts")
    for k in ("last_rid", "emergency", "encrypted", "last_freq"):
        if kw.get(k) is not None: tg[k] = F5(kw[k]) if k == "last_freq" else kw[k]
    return tg

def touch_rid(rid, ts, action, tg=None):
    if rid in (0, None): return
    r = STATE["radios"].setdefault(str(rid), {
        "rid": rid, "count": 0, "last": None, "last_tg": None, "last_action": None})
    r["count"] += 1; r["last"] = ts; r["last_action"] = action
    if tg is not None: r["last_tg"] = tg
    # Stamp the site this radio was last seen on (one control channel == one site), so a
    # watched radio's registrations/grants carry the RFSS/site/system for coverage work.
    sysd = STATE["system"]
    if sysd.get("site") is not None:
        r["site"] = sysd.get("site"); r["rfss"] = sysd.get("rfss")
        r["sysid"] = sysd.get("sysid"); r["nac"] = sysd.get("nac"); r["site_cc"] = sysd.get("cc")
    return r

def bump_regfail(rid, ts, kind, rv):
    """A subscriber's registration/affiliation was not accepted (rv/gav != 0): a trouble signal
    (not provisioned, wrong system, failing radio, or fringe coverage). Tally it on the radio."""
    if rid in (0, None): return
    if str(rid) not in STATE["radios"]: touch_rid(rid, ts, kind)
    r = STATE["radios"][str(rid)]
    r["reg_fails"] = r.get("reg_fails", 0) + 1
    r["last_reg_fail"] = REG_RV.get(rv, "rv%d" % rv)
    emit("REGFAIL", ts, rid=rid, reason=r["last_reg_fail"], kind2=kind)

# ------------------------------------------------ P25 data-channel (SNDCP) intel
# Friendly names for control opcodes OP25's tk_p25.py prints as "unhandled".
OSP_NAMES = {
    0x05: "Unit-to-unit answer request", 0x09: "Telephone interconnect grant update",
    0x14: "SNDCP data channel grant", 0x15: "SNDCP data page request",
    0x16: "SNDCP data channel announce", 0x18: "Status update", 0x1a: "Status query",
    0x1c: "Message update", 0x1d: "Radio unit monitor", 0x1f: "Call alert",
    0x20: "Acknowledge response (FNE)", 0x21: "Queued response", 0x24: "Extended function",
    0x27: "Deny response", 0x2d: "Unit registration command", 0x34: "Iden update VHF/UHF",
    0x36: "Time & date announce"}

# Documented Motorola (MFID 0x90) trunking opcodes. 0x05/0x09 and most others are
# Motorola-internal and NOT publicly documented, so we label those honestly rather
# than inventing a meaning (verified vs SDRTrunk's opcode list + RadioReference).
MOT_OSP = {
    0x00: "Motorola group regroup ADD (patch/supergroup)",
    0x01: "Motorola group regroup DELETE (patch)",
    0x02: "Motorola patch-group voice grant",
    0x03: "Motorola patch-group grant update",
    0x0a: "Motorola emergency alarm",
}

def opname(op, mfid):
    if mfid == 0x90:
        return MOT_OSP.get(op, "Motorola proprietary — undocumented (0x%02x)" % op)
    if mfid not in (0x00, 0x01):
        return "vendor MFID 0x%02x (0x%02x)" % (mfid, op)
    return OSP_NAMES.get(op, "standard opcode 0x%02x" % op)

def chan_to_freq(chan):
    """P25 16-bit channel id -> downlink MHz via the tracked band plan, or None.
    Channel = (iden<<12)|chan_no; freq = base + spacing*(chan_no/slots)."""
    try:
        c = int(chan, 16) if isinstance(chan, str) else int(chan)
    except Exception:
        return None
    if c in (0, 0xffff):
        return None
    iden = (c >> 12) & 0xf; ch = c & 0xfff
    bp = STATE["bandplan"].get(str(iden))
    if not bp:
        return None
    try:
        base = float(bp["base"]); spac = float(bp["spacing"])    # MHz, kHz
    except Exception:
        return None
    slots = 1
    if bp.get("type") == "TDMA":
        try: slots = max(1, int(bp.get("slots") or 1))
        except Exception: slots = 1
    return round(base + (spac / 1000.0) * (ch // slots), 6)

def touch_datachan(chan, ts, kind="ann", target=None):
    if chan in (0, 0xffff, None):
        return None
    key = "%04x" % chan
    dc = STATE["datachans"].setdefault(key, {
        "chan": key, "iden": (chan >> 12) & 0xf, "freq": None, "count": 0,
        "grants": 0, "pages": 0, "anns": 0, "last": None, "last_target": None, "targets": []})
    dc["count"] += 1; dc["last"] = ts
    f = chan_to_freq(chan)
    if f: dc["freq"] = f
    if kind == "grant": dc["grants"] += 1
    elif kind == "page": dc["pages"] += 1
    else: dc["anns"] += 1
    if target:
        dc["last_target"] = target
        if target not in dc["targets"]:
            dc["targets"] = (dc["targets"] + [target])[-12:]
    return dc

# ----------------------------------------------- GPS / LRRP positions (map feed)
def record_position(rid, lat, lon, tg=None, ts=None, alias=None, heading=None, speed=None, src="lrrp"):
    """Store + broadcast a radio GPS fix. Rides the same SSE bus as everything else
    (kind=POSITION), so the map updates live with no extra socket."""
    try:
        lat = float(lat); lon = float(lon)
    except Exception:
        return None
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0): return None
    if lat == 0.0 and lon == 0.0: return None          # null island = no-fix
    ts = ts or time.strftime("%H:%M:%S")
    rec = {"rid": rid, "alias": alias or rid_alias(rid), "tg": tg,
           "tgtag": tg_tag(tg) if tg is not None else "",
           "lat": round(lat, 6), "lon": round(lon, 6), "ts": ts,
           "heading": heading, "speed": speed, "src": src}
    with LOCK:
        POSITIONS[rid] = rec
        POS_TRAILS[rid].append({"lat": rec["lat"], "lon": rec["lon"], "ts": ts})
    publish({"kind": "POSITION", **rec})
    return rec

def parse_lrrp(payload):
    """Extract {lat,lon} from a reassembled LRRP UDP payload (bytes), or None.
    LRRP encodes a position report as element tag 0x66 followed by latitude int32 BE
    (scale 90/2**31) then longitude int32 BE (scale 180/2**31) — the lat/lon use
    DIFFERENT scale factors. Verified vs 66 3F7EBBBB 07CB0755 -> 44.64496N 10.95902E."""
    try:
        i = payload.find(b"\x66")
        if i < 0 or i + 9 > len(payload):
            return None
        lat_int = struct.unpack(">i", payload[i + 1:i + 5])[0]
        lon_int = struct.unpack(">i", payload[i + 5:i + 9])[0]
        lat = lat_int * 90.0 / 2147483648.0
        lon = lon_int * 180.0 / 2147483648.0
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return None
        return {"lat": lat, "lon": lon}
    except Exception:
        return None

# Demo position generator — proves the whole map pipeline with no RF (toggle via /api/sim).
SIM = {"on": False, "thread": None}
def _sim_loop():
    import random
    fleet = [(7700001, "SIM MEDIC-1", 3201, 26.122, -80.143),
             (7700002, "SIM ENGINE-12", 3205, 26.190, -80.110),
             (7700003, "SIM PD-44", 3110, 26.071, -80.207),
             (7700004, "SIM K9-7", 3110, 26.150, -80.250)]
    pos = {r[0]: [r[3], r[4]] for r in fleet}
    while SIM["on"]:
        for rid, alias, tg, _b, _c in fleet:
            p = pos[rid]
            p[0] = max(25.90, min(26.45, p[0] + (random.random() - 0.5) * 0.006))
            p[1] = max(-80.45, min(-79.95, p[1] + (random.random() - 0.5) * 0.006))
            record_position(rid, p[0], p[1], tg=tg, alias=alias, src="sim")
        time.sleep(2.5)
    SIM["thread"] = None

def analyze_pdu(hexs, sap, ts):
    """A non-MBT packet-data PDU (dumped by our patched OP25 process_PDU). Decide clear vs
    encrypted by scanning for a clear IPv4/UDP header; if it carries LRRP, extract + plot the fix."""
    try:
        b = bytes.fromhex(hexs or "")
    except Exception:
        return
    rid = ((b[2] << 16) | (b[3] << 8) | b[4]) if len(b) >= 5 else None   # logical link id (radio) from header
    clear = False; gps = None
    for i in range(0, max(0, len(b) - 10)):
        if b[i] == 0x45 and i + 9 < len(b) and b[i + 9] == 0x11:         # IPv4 (IHL 5) carrying UDP (proto 17)
            clear = True
            gps = parse_lrrp(b[i + 28:]) if i + 28 < len(b) else None    # skip IP(20)+UDP(8) headers
            break
    payload = len(b) > 12                                                # has data blocks beyond the 12B header
    with LOCK:
        ds = STATE["datastats"]
        ds["pdus"] = ds.get("pdus", 0) + 1
        if payload: ds["pdu_payload"] = ds.get("pdu_payload", 0) + 1
        if clear: ds["pdu_clear"] = ds.get("pdu_clear", 0) + 1
        elif payload: ds["pdu_enc"] = ds.get("pdu_enc", 0) + 1           # data payload present but not clear IP
        STATE["pdu_samples"] = (STATE.get("pdu_samples", []) + [{
            "ts": ts, "sap": sap, "clear": clear, "rid": rid, "payload": payload,
            "gps": gps, "hex": (hexs or "")[:96]}])[-20:]
    if gps and rid:                                                      # clear LRRP fix -> plot it
        record_position(rid, gps["lat"], gps["lon"], src="lrrp", ts=ts)

# ----------------------------------------------------------------- parser
TS = r"^(?P<ts>\d\d/\d\d/\d\d \d\d:\d\d:\d\d\.\d+) \[\d+\] "
PATS = []
def P(name, regex): PATS.append((name, re.compile(TS + regex)))
P("nac",      r"NAC 0x(?P<nac>[0-9a-f]+) TSBK:")
P("grant",    r"tsbk\(0x00\) grp_v_ch_grant: opts: 0x(?P<opts>[0-9a-f]+) freq: (?P<freq>\S+) ga: (?P<ga>\d+) sa: (?P<sa>\d+)")
P("grant_up", r"tsbk\(0x02\) grp_v_ch_grant_up: ch1: (?P<f1>\S+) ga1: (?P<ga1>\d+) ch2: (?P<f2>\S+) ga2: (?P<ga2>\d+)")
P("grant_exp",r"tsbk\(0x03\) grp_v_ch_grant_up_exp: opts: 0x(?P<opts>[0-9a-f]+) freq-t: (?P<freq>\S+) freq-r: \S+ ga: (?P<ga>\d+)")
P("tgrant",   r"tdma\(0x(?:40|c0)\) grp_v_ch_grant: opts: 0x(?P<opts>[0-9a-f]+) (?:ch|freq-t): (?P<freq>\S+)(?: freq-r: \S+)? ga: (?P<ga>\d+) sa: (?P<sa>\d+)")
P("aff",      r"tsbk\(0x28\) grp_aff_rsp: mfid: 0x[0-9a-f]+ gav: (?P<gav>\d+) aga: (?P<aga>\d+) ga: (?P<ga>\d+) ta: (?P<ta>\d+)")
P("aff_mbt",  r"mbt\(0x28\) grp_aff_rsp: mfrid: 0x[0-9a-f]+ wacn: 0x[0-9a-f]+ syid: 0x[0-9a-f]+ lg: \d+ gav: (?P<gav>\d+) aga: (?P<aga>\d+) ga: (?P<ga>\d+) ta: (?P<ta>\d+)")
P("loc_reg",  r"tsbk\(0x2b\) loc_reg_rsp: mfid: 0x[0-9a-f]+ rv: (?P<rv>\d+) ga: (?P<ga>\d+) rfid: 0x[0-9a-f]+ stid: 0x[0-9a-f]+ ta: (?P<ta>\d+)")
P("u_reg",    r"tsbk\(0x2c\) u_reg_rsp: mfid: 0x[0-9a-f]+ rv: (?P<rv>\d+) syid: 0x[0-9a-f]+ sid: (?P<sid>\d+) sa: (?P<sa>\d+)")
P("u_reg_mbt",r"mbt\(0x2c\) u_reg_rsp: mfid: 0x[0-9a-f]+ rv: (?P<rv>\d+) wacn: 0x[0-9a-f]+ syid: 0x[0-9a-f]+ sid: (?P<sid>\d+) sa: (?P<sa>\d+)")
P("dereg",    r"tsbk\(0x2f\) u_de_reg_ack: mfid: 0x[0-9a-f]+ wacn: 0x[0-9a-f]+ syid: 0x[0-9a-f]+ sid: (?P<sid>\d+)")
P("rfss",     r"tsbk\(0x3a\) rfss_sts_bcst: syid: (?P<syid>[0-9a-f]+) rfid: (?P<rfid>[0-9a-f]+) stid: (?P<stid>\d+) A: \d+ ch1: \S*?\((?P<cc>[\d.]+)\)")
P("net",      r"tsbk\(0x3b\) net_sts_bcst: wacn: (?P<wacn>[0-9a-f]+) syid: (?P<syid>[0-9a-f]+)")
P("adj",      r"tsbk\(0x3c\) adj_sts_bcst: rfid: (?P<rfid>[0-9a-f]+) stid: (?P<stid>\d+).*?ch1: \S*?\((?P<f>[^)]+)\)")
P("iden",     r"tsbk\(0x3d\) iden_up id: (?P<id>\d+) toff: (?P<toff>\S+) spac: (?P<spac>\S+) freq: (?P<freq>[\d.]+) bw: (?P<bw>\d+)")
P("iden_tdma",r"tsbk\(0x33\) iden_up_tdma: id: (?P<id>\d+) freq: (?P<freq>[\d.]+) toff: (?P<toff>\S+) spac: (?P<spac>\S+) slots/carrier: (?P<slots>\d+)")
P("sccb",     r"tsbk\(0x39\) sccb: rfid: (?P<rfid>\d+) stid: (?P<stid>\d+) ch1: \S*?\((?P<f1>[\d.]+)\) ch2: \S*?\((?P<f2>[\d.]+)\)")
P("sngnt",    r"tsbk\(0x14\) sn_data_chn_gnt: opts: 0x(?P<o>[0-9a-f]+) ch1: (?P<c1>[0-9a-f]+).*? ta: (?P<ta>\d+)")
P("sndcp",    r"tsbk\(0x16\) sndcp_data_ch:? ch1: (?P<c1>[0-9a-f]+) ch2: (?P<c2>[0-9a-f]+)")
P("unhandled",r"(?P<utype>tsbk|mbt)\(0x(?P<uop>[0-9a-f]+)\) unhandled:?\s+(?:0x)?(?P<uhex>[0-9a-f]+)")
P("verr",     r"(?:AMBE|IMBE)\b.*?err_rate (?P<er>[\d.]+)")
P("cclost",   r"control channel timeout, freq\((?P<f>[\d.]+)\)")
P("synclost", r"rx_sync::sync_timeout")
P("keyload",  r"op25_crypt_algs::key: loaded key keyId:(?P<keyid>[0-9a-f]+), algId:(?P<algid>[0-9a-f]+)")
P("crypt",    r"encrypt info: tg=(?P<tg>\d+), algid=0x(?P<algid>[0-9a-f]+), keyid=0x(?P<keyid>[0-9a-f]+)")
P("vupd",     r"voice update:\s+tg\((?P<tg>\d+)\)")
P("vfts",     r"VF ts ph2: tgid: (?P<tg>\d+)")
P("settg",    r"set tgid=(?P<tg>\d+)")
P("ess",      r"BURST\(\d\) TDMA slot ID=\d+ ESS: algid=(?P<algid>[0-9a-f]+), keyid=(?P<keyid>[0-9a-f]+)")
P("sigq",     r"tdma\(0x30\) pwr_ctl_sig_qual:? ta: (?P<ta>\d+) rf: 0x(?P<rf>[0-9a-f]+):? ber: 0x(?P<ber>[0-9a-f]+)")
# DMR (OP25 dmr_slot.cc C++ stderr + tk_trbo.py). Specific lines first, generic color-code catch-all last.
P("dmr_vlc",  r"Slot\(\d+\), CC\((?P<cc>[0-9a-f]+)\), (?:VOICE|TERM) LC .*?DSTADDR\((?P<dst>[0-9a-f]+)\), SRCADDR\((?P<src>[0-9a-f]+)\)")
P("dmr_pi",   r"Slot\(\d+\), CC\((?P<cc>[0-9a-f]+)\), PI HEADER: ALGID\((?P<algid>[0-9a-f]+)\), KEYID\((?P<keyid>[0-9a-f]+)\)")
P("dmr_cpg",  r"Slot\(\d+\), CC\((?P<cc>[0-9a-f]+)\),.*CONNECT PLUS GRANT srcAddr\((?P<src>[0-9a-f]+)\), grpAddr\((?P<grp>[0-9a-f]+)\), LCN\((?P<lcn>[0-9a-f]+)\), TS\((?P<slot>\d+)\)")
P("dmr_cpnb", r"Slot\(\d+\), CC\((?P<cc>[0-9a-f]+)\),.*CONNECT PLUS NB1")
P("dmr_vu",   r"Voice update:  tg\((?P<tg>\d+)\), freq\((?P<freq>[\d.]+)\), slot\((?P<slot>\d+)\), lcn\((?P<lcn>\d+)\)")
P("dmr_slot", r"Slot\((?P<slot>\d+)\), CC\((?P<cc>[0-9a-f]+)\)")
GENERIC = re.compile(TS + r"(?P<op>(?:tsbk|mbt|tdma)\(0x[0-9a-f]+\) [a-z0-9_]+)\b[:\s]*(?P<rest>.*)")
# which decoder channel ([N]) is currently on which call -> maps ws audio port (9000+N) to a TG
VCH = re.compile(r"\[(?P<ch>\d+)\] voice update:\s+tg\((?P<tg>\d+)\), rid\((?P<rid>\d+)\), freq\((?P<freq>[\d.]+)\)")
PDU_RE = re.compile(TS + r"NAC 0x[0-9a-f]+ PDU_DATA: fmt=(?P<fmt>[0-9a-f]+) sap=(?P<sap>[0-9a-f]+) blks=(?P<blks>\d+) bytes=(?P<hex>[0-9a-f]*)")

def F5(v):
    """Frequencies everywhere as MHz with exactly 5 decimals (OP25 prints 6; 6.25 kHz channels need 5)."""
    try: return "%.5f" % float(v) if v not in (None, "") and 100 <= float(v) < 10000 else v
    except Exception: return v

def emit(kind, ts, **fields):
    if fields.get("freq") is not None: fields["freq"] = F5(fields["freq"])
    ev = {"kind": kind, "ts": (ts.split()[-1].split(".")[0] if ts else ""), **fields}
    if ("rid" in fields and str(fields["rid"]) in WATCH) or ("src" in fields and str(fields["src"]) in WATCH):
        ev["watch"] = True
    if "tg" in fields and fields.get("tg") is not None:
        ev["tgtag"] = tg_tag(fields["tg"])
    if fields.get("rid"):
        ev["ridalias"] = rid_alias(fields["rid"])
    publish(ev)

def handle(name, m):
    g = m.groupdict(); ts = g.get("ts"); sysd = STATE["system"]; sysd["last_update"] = ts
    if name == "nac":
        sysd["nac"] = "0x" + g["nac"]; STATE["counters"]["tsbks"] += 1
    elif name in ("grant", "grant_exp", "tgrant"):
        opts = int(g["opts"], 16); emerg = bool(opts & 0x80); enc = bool(opts & 0x40)
        ga = int(g["ga"]); sa = int(g.get("sa") or 0)
        touch_tg(ga, ts=ts, last_rid=sa or None, emergency=emerg, encrypted=enc, last_freq=g.get("freq"))
        if sa: touch_rid(sa, ts, "grant", ga)
        STATE["counters"]["grants"] += 1
        if emerg: STATE["counters"]["emergencies"] += 1
        if enc: STATE["counters"]["encrypted"] += 1
        emit("GRANT", ts, tg=ga, rid=sa, freq=g.get("freq"), emergency=emerg, encrypted=enc, prio=opts & 0x07)
    elif name == "grant_up":
        for fk, gk in (("f1", "ga1"), ("f2", "ga2")):
            ga = int(g[gk])
            if ga == 0: continue
            known = STATE["talkgroups"].get(str(ga), {})
            enc = known.get("encrypted", False); em = known.get("emergency", False)
            touch_tg(ga, ts=ts, last_freq=g.get(fk)); STATE["counters"]["grants"] += 1
            emit("GRANT", ts, tg=ga, rid=0, freq=g.get(fk), emergency=em, encrypted=enc, prio=0)
    elif name in ("aff", "aff_mbt"):
        ta = int(g["ta"]); ga = int(g["ga"]); gav = int(g.get("gav") or 0)
        touch_rid(ta, ts, "affiliate", ga); touch_tg(ga, ts=ts, last_rid=ta)
        STATE["counters"]["affiliations"] += 1
        if gav != 0: bump_regfail(ta, ts, "grp_aff", gav)
        emit("AFFIL", ts, rid=ta, tg=ga, granted=(gav == 0))
    elif name == "loc_reg":
        ta = int(g["ta"]); ga = int(g["ga"]); rv = int(g["rv"])
        touch_rid(ta, ts, "loc_reg", ga); STATE["counters"]["registrations"] += 1
        if rv != 0: bump_regfail(ta, ts, "loc_reg", rv)
        emit("REG", ts, rid=ta, tg=ga, kind2="loc_reg", granted=(rv == 0))
    elif name in ("u_reg", "u_reg_mbt"):
        sa = int(g["sa"]); rv = int(g["rv"])
        touch_rid(sa, ts, "register"); STATE["counters"]["registrations"] += 1
        if rv != 0: bump_regfail(sa, ts, "u_reg", rv)
        emit("REG", ts, rid=sa, kind2="u_reg", granted=(rv == 0))
    elif name == "dereg":
        sid = int(g["sid"]); touch_rid(sid, ts, "de_register"); emit("DEREG", ts, rid=sid)
    elif name == "rfss":
        sysd["sysid"] = "%03X" % int(g["syid"], 16); sysd["rfss"] = int(g["rfid"], 16); sysd["site"] = int(g["stid"]); sysd["cc"] = F5(g["cc"])
    elif name == "net":
        sysd["wacn"] = "0x" + g["wacn"]; sysd["sysid"] = "%03X" % int(g["syid"], 16)
    elif name == "adj":
        STATE["adjacent"][f"{g['rfid']}-{g['stid']}"] = {"rfid": g["rfid"], "stid": int(g["stid"]), "freq": F5(g["f"]), "last": ts}
    elif name == "sccb":
        for fk in ("f1", "f2"):
            f = F5(g[fk])
            if f and f not in sysd["secondary_cc"]: sysd["secondary_cc"].append(f)
    elif name in ("iden", "iden_tdma"):
        STATE["bandplan"][g["id"]] = {"base": F5(g["freq"]), "spacing": g["spac"], "offset": g["toff"],
                                      "type": "TDMA" if name == "iden_tdma" else "FDMA", "slots": g.get("slots")}
    elif name == "cclost":
        STATE["counters"]["cc_timeouts"] = STATE["counters"].get("cc_timeouts", 0) + 1
    elif name == "synclost":
        STATE["counters"]["sync_losses"] = STATE["counters"].get("sync_losses", 0) + 1
    elif name == "keyload":
        kl = STATE.setdefault("keys_loaded", [])
        k = "0x%04x" % int(g["keyid"], 16)
        if k not in kl: kl.append(k)
    elif name == "crypt":
        tg = int(g["tg"]); alg = int(g["algid"], 16); t = touch_tg(tg, ts=ts)
        t["encrypted"] = (alg not in (0x80, 0x9f)); t["algid"] = ALGID.get(alg, f"0x{alg:x}"); t["keyid"] = "0x" + g["keyid"]
        if t["encrypted"]: emit("CRYPT", ts, tg=tg, algid=t["algid"], keyid=t["keyid"])

    elif name in ("vupd", "vfts", "settg"):
        STATE["cur_voice_tg"] = int(g["tg"])

    elif name == "sigq":
        ta = int(g["ta"]); rf = int(g["rf"], 16); ber = int(g["ber"], 16)
        q = SIGQUAL.setdefault(str(ta), {"rid": ta, "count": 0, "ber_max": 0, "rf_min": 15, "hist": []})
        q["rf"] = rf; q["ber"] = ber; q["count"] += 1; q["last"] = ts
        q["ber_max"] = max(q.get("ber_max", 0), ber)         # worst uplink BER the tower saw from this radio
        q["rf_min"] = min(q.get("rf_min", 15), rf)           # weakest the tower heard it
        h = q.setdefault("hist", []); h.append(ber); del h[:-24]
        diag_row("uplink_sq", rid=ta, rf=rf, ber=ber)
        if WSESS["on"] and str(ta) in WATCH: WSESS["sq"].setdefault(str(ta), []).append((rf, ber))

    elif name == "verr":                        # per-call voice error rate -> credit the talker on that channel
        cm = re.match(r"\S+ \S+ \[(\d+)\]", m.string); c = STATE.get("chantg", {}).get(cm.group(1)) if cm else None
        if c and c.get("rid"):
            er = float(g["er"]); rid = c["rid"]; now = time.time()
            v = VOICEQ.setdefault(str(rid), {"er_max": 0.0, "calls": 0, "_logged": 0})
            if v.get("tg") != c["tg"] or now - v.get("_seen", 0) > 5: v["calls"] += 1   # new call
            v.update(er=er, er_max=max(v["er_max"], er), tg=c["tg"], last=ts, _seen=now)
            if now - v["_logged"] >= 1:                                                 # 1 row/s/radio is plenty
                v["_logged"] = now; diag_row("voice_err", rid=rid, tg=c["tg"], freq=c.get("freq"), voice_err=er)
                if WSESS["on"] and str(rid) in WATCH: WSESS["vq"].setdefault(str(rid), []).append(er)

    elif name == "sngnt":                       # 0x14 SNDCP data channel GRANT as printed by OP25
        ch = int(g["c1"], 16); tgt = int(g["ta"])
        touch_datachan(ch, ts, kind="grant", target=tgt or None); STATE["datastats"]["grants"] += 1
        emit("DATA", ts, dtype="data grant", chan="%04x" % ch, freq=chan_to_freq(ch), rid=(tgt or 0))
    elif name == "sndcp":                       # 0x16 standing data-channel announcement
        for ck in ("c1", "c2"):
            try: ch = int(g[ck], 16)
            except Exception: continue
            if ch in (0, 0xffff): continue
            touch_datachan(ch, ts, kind="ann"); STATE["datastats"]["anns"] += 1
    elif name == "unhandled":                   # decode SNDCP grant/page hex; tally the rest
        utype = g.get("utype") or "tsbk"; hexs = g["uhex"]; op = int(g["uop"], 16)
        mfid = int(hexs[2:4], 16) if (utype == "tsbk" and len(hexs) >= 4) else 0
        nm = opname(op, mfid) if utype == "tsbk" else ("MBT opcode 0x%02x" % op)
        key = "%s/0x%02x/%02x" % (utype, op, mfid)
        rec = STATE["unhandled_ops"].setdefault(key, {
            "op": "0x%02x" % op, "mfid": "0x%02x" % mfid, "name": nm,
            "count": 0, "last": None})
        rec["count"] += 1; rec["last"] = ts
        if utype == "tsbk" and mfid == 0x00 and op in (0x14, 0x15) and len(hexs) >= 20:
            h = hexs[:24]
            if len(h) % 2: h = h[:-1]
            try: b = bytes.fromhex(h)
            except Exception: b = b""
            if len(b) >= 10:
                ch = (b[3] << 8) | b[4]; tgt = (b[7] << 16) | (b[8] << 8) | b[9]
                if op == 0x14:                  # SNDCP data channel GRANT (ch + target radio)
                    touch_datachan(ch, ts, kind="grant", target=tgt or None)
                    STATE["datastats"]["grants"] += 1
                    emit("DATA", ts, dtype="data grant", chan="%04x" % ch,
                         freq=chan_to_freq(ch), rid=(tgt or 0))
                else:                           # SNDCP data PAGE request (target radio)
                    STATE["datastats"]["pages"] += 1
                    if tgt: STATE["datastats"]["last_target"] = tgt
                    emit("DATA", ts, dtype="data page", rid=(tgt or 0))

    elif name == "dmr_vlc":
        sysd["dmr_cc"] = int(g["cc"], 16)
        tg = int(g["dst"], 16); sa = int(g["src"], 16)
        touch_tg(tg, ts=ts, last_rid=sa or None)
        if sa: touch_rid(sa, ts, "voice", tg)
        STATE["counters"]["grants"] += 1
        emit("GRANT", ts, tg=tg, rid=sa, freq=None, emergency=False, encrypted=False)
    elif name == "dmr_pi":
        sysd["dmr_cc"] = int(g["cc"], 16); alg = int(g["algid"], 16)
        if alg:
            STATE["counters"]["encrypted"] += 1
            emit("CRYPT", ts, tg=STATE.get("cur_voice_tg"), algid="DMR-0x%02x" % alg, keyid="0x" + g["keyid"])
    elif name == "dmr_cpg":
        sysd["dmr_cc"] = int(g["cc"], 16); sysd["dmr_variant"] = "connect_plus"
        tg = int(g["grp"], 16); sa = int(g["src"], 16)
        touch_tg(tg, ts=ts, last_rid=sa or None)
        if sa: touch_rid(sa, ts, "grant", tg)
        STATE["counters"]["grants"] += 1
        emit("GRANT", ts, tg=tg, rid=sa, freq=None, emergency=False, encrypted=False)
    elif name == "dmr_cpnb":
        sysd["dmr_cc"] = int(g["cc"], 16); sysd["dmr_variant"] = "connect_plus"
    elif name == "dmr_vu":
        tg = int(g["tg"])
        touch_tg(tg, ts=ts, last_freq=g.get("freq")); STATE["counters"]["grants"] += 1
        emit("GRANT", ts, tg=tg, rid=0, freq=g.get("freq"), emergency=False, encrypted=False)
    elif name == "dmr_slot":
        sysd["dmr_cc"] = int(g["cc"], 16)

    elif name == "ess":
        tg = STATE.get("cur_voice_tg")
        if tg is None:
            return
        alg = int(g["algid"], 16); enc = alg not in (0x80, 0x9f)
        algname = ALGID.get(alg, f"0x{alg:x}") if enc else "CLEAR"
        t = STATE["talkgroups"].get(str(tg))
        if t is None:
            t = STATE["talkgroups"].setdefault(str(tg), {"tgid": tg, "count": 0, "last": ts,
                "last_rid": None, "emergency": False, "encrypted": False, "algid": None, "keyid": None, "last_freq": None})
        changed = (t["encrypted"] != enc) or (t["algid"] != algname)
        t["encrypted"] = enc; t["algid"] = algname; t["keyid"] = "0x" + g["keyid"]
        if enc and changed:
            emit("CRYPT", ts, tg=tg, algid=algname, keyid="0x" + g["keyid"])

def parse_line(line):
    gm = GENERIC.search(line)
    if gm:
        with LOCK:
            op = gm.group("op")
            OPCOUNTS[op] = OPCOUNTS.get(op, 0) + 1
            RAW.append({"ts": gm.group("ts").split()[-1].split(".")[0], "op": op,
                        "text": (gm.group("rest") or "")[:100]})
            d = cc_ingest(op, gm.group("rest") or "", gm.group("ts"))
            if d: RAW[-1]["dec"] = d
    vm = VCH.search(line)
    if vm:
        with LOCK:
            STATE.setdefault("chantg", {})[vm.group("ch")] = {
                "tg": int(vm.group("tg")), "rid": int(vm.group("rid")), "freq": F5(vm.group("freq"))}
    pdm = PDU_RE.search(line)
    if pdm:                                    # patched-OP25 packet-data dump (data-channel probe / LRRP)
        analyze_pdu(pdm.group("hex"), int(pdm.group("sap"), 16), pdm.group("ts"))
        return
    for name, pat in PATS:
        m = pat.search(line)
        if m:
            with LOCK:
                try: handle(name, m)
                except Exception: pass
            return

# ----------------------------------------------------------------- OP25 manager
OP25 = {"proc": None, "running": False, "error": None}

def build_cfg(sid):
    s = SYSTEMS["systems"].get(sid) or TEMP_SYS[sid]; sdr = dev_sdr(s["sdr"])
    wl = ""
    if CC_ONLY[0]:
        # whitelist a TGID that never transmits -> OP25 never follows voice -> stays on the CC
        wl = os.path.join(TMP, "op25_wlist.tsv")
        with open(wl, "w") as f:
            f.write("1\n")
    elif LISTEN:
        wl = os.path.join(TMP, "op25_wlist.tsv")
        with open(wl, "w") as f:
            f.write("\n".join(str(t) for t in LISTEN) + "\n")
    sysname = s["name"]; mod = s.get("modulation", "cqpsk")
    is_dmr = str(s.get("type") or "").upper().startswith("DMR")
    if is_dmr:                          # Motorola TRBO (Connect Plus / Capacity Plus)
        ccs_hz = [int(round(float(c) * 1e6)) for c in s["control_channels"]]
        trunk = {"module": "tk_trbo.py",
                 "chans": [{"lcn": i + 1, "frequency": f, "cc": 0} for i, f in enumerate(ccs_hz)]}
        dmod, dfilt = "fsk4", "rrc"
    else:
        trunk = {"module": "tk_p25.py", "chans": [{"nac": "0x0", "wacn": "0x0", "sysid": "0x0",
                 "sysname": sysname, "control_channel_list": ",".join(
                     ([CC_PIN["freq"]] if CC_PIN["sid"] == sid and CC_PIN["freq"] else []) +
                     [c for c in s["control_channels"] if not (CC_PIN["sid"] == sid and _parse_freq(c) == _parse_freq(CC_PIN["freq"]))]),
                 "whitelist": wl, "blacklist": "", "tgid_tags_file": "", "rid_tags_file": "",
                 "tdma_cc": False, "crypt_behavior": 0}]}
        dmod, dfilt = mod, "rc"
    term = {"module": "terminal.py", "terminal_type": "http:0.0.0.0:8081",
            "curses_plot_interval": 0.1, "http_plot_interval": 1.0,
            "http_plot_directory": "../www/images", "tuning_step_large": 1200,
            "tuning_step_small": 100, "default_channel": sysname}

    def ch(i, port):
        return {"name": f"{sysname}-ch{i}", "device": "sdr0", "trunking_sysname": sysname,
                "demod_type": dmod, "destination": f"ws://0.0.0.0:{port}", "excess_bw": 0.2,
                "filter_type": dfilt, "if_rate": 24000, "plot": "", "symbol_rate": 4800,
                "enable_analog": "off", "blacklist": "", "whitelist": "", "crypt_keys": keys_file()}

    if sdr.get("wideband"):
        # WIDEBAND multi-channel: one wide capture, N decoders (1 control + voice) off the same
        # stream via DDC — decodes the CC and voice simultaneously (no retuning, no missed CC data).
        ccs = [float(c) for c in s["control_channels"]]
        # LPF now opened (bandwidth=rate), so 6 MSPS covers CC + voice. Bias center ~1 MHz below the
        # CC mean since 700/800 voice channels usually sit below the control channels.
        rate = int(sdr.get("wb_rate", DEV["wb_rate"]))
        center = int(sdr.get("center") or round((sum(ccs) / len(ccs)) * 1e6 - min(1_000_000, rate // 4)))  # narrow RTL capture: smaller bias
        nvoice = max(1, int(sdr.get("voice_channels", 2)))
        cc0 = int(ccs[0] * 1e6)
        chans = [ch(i, 9000 + i) for i in range(1 + nvoice)]
        for c in chans:
            c["frequency"] = cc0   # trunking SM reassigns: one stays on CC, others follow voice
        devices = [{"args": sdr["args"], "gains": sdr["gains"], "name": "sdr0", "frequency": center,
                    "offset": 0, "ppm": float(sdr.get("ppm", 0.0)), "rate": rate,
                    "bandwidth": int(sdr.get("bandwidth", rate)),   # open the analog LPF to the full capture width
                    "usable_bw_pct": float(sdr.get("usable_bw_pct", 0.92)), "tunable": False}]
    else:
        c0 = ch(0, 9000); c0["name"] = sysname
        chans = [c0]
        devices = [{"args": sdr["args"], "gains": sdr["gains"], "name": "sdr0",
                    "offset": int(sdr.get("offset", -250000)), "ppm": float(sdr.get("ppm", 0.0)),
                    "rate": int(sdr.get("rate", 1000000)), "usable_bw_pct": 0.85, "tunable": True}]

    cfg = {"channels": chans, "devices": devices, "trunking": trunk, "terminal": term}
    path = os.path.join(TMP, "op25_active.json")
    save_json(path, cfg)
    return path

def op25_stop():
    p = OP25["proc"]
    if p and p.poll() is None:
        try:
            p.terminate(); p.wait(timeout=5)
        except Exception:
            try: p.kill()
            except Exception: pass
    OP25["proc"] = None; OP25["running"] = False

def _reader(p):
    try:
        lf = open(os.path.join(TMP, "op25_raw.log"), "w", buffering=1)
    except Exception:
        lf = None
    try:
        for line in p.stdout:
            if lf:
                try: lf.write(line)
                except Exception: pass
            parse_line(line)
            if "(0x" not in line[:60]: xlog_line(line)
    except Exception:
        pass
    finally:
        if lf:
            try: lf.close()
            except Exception: pass

def op25_start():
    op25_stop(); reset_state(); diag_reset()
    sid = RUN_TEMP[0] if RUN_TEMP[0] in TEMP_SYS else SYSTEMS.get("active")
    if not sid or (sid not in SYSTEMS["systems"] and sid not in TEMP_SYS):
        OP25["error"] = "no active system"; return
    try:
        cfg = build_cfg(sid)
        p = subprocess.Popen([OP25_PY, "multi_rx.py", "-v", "10", "-c", cfg],
                             cwd=APPS, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             text=True, bufsize=1)
        OP25["proc"] = p; OP25["running"] = True; OP25["error"] = None; OP25["since"] = time.time()
        threading.Thread(target=_reader, args=(p,), daemon=True).start()
    except Exception as e:
        OP25["error"] = str(e); OP25["running"] = False

# ----------------------------------------------------------------- SCAN engine
SURVEY_RATE = 3_000_000 if SDR_KIND == "bladerf" else 2_400_000
SURVEY_BW   = 2_800_000
SURVEY_USABLE = 2_500_000 if SDR_KIND == "bladerf" else 2_000_000   # Hz kept per chunk (RTL: drop the rolled-off edges)
PROBE_NAC_TIMEOUT = 8.0
PROBE_IDENT_DWELL = 6.0
IQ_PATH = os.path.join(TMP, "scan_chunk.sc16")
BANDS = {
  "700":  {"start": 769.0, "stop": 775.0, "raster": 0.0125, "label": "700 MHz public safety"},
  "800f": {"start": 851.0, "stop": 862.0, "raster": 0.0125, "label": "800 MHz (NPSPAC/public safety)"},
  "800":  {"start": 851.0, "stop": 869.0, "raster": 0.0125, "label": "800 MHz (full)"},
  "uhf":  {"start": 450.0, "stop": 470.0, "raster": 0.0125, "label": "UHF 450-470"},
  "fed":  {"start": 406.0, "stop": 420.0, "raster": 0.0125, "label": "Federal 406-420"},
}
SCAN = {"running": False, "cancel": False, "phase": "idle", "band": None, "dir": None,
        "mode": "p25",                                       # "p25" = P25/DMR systems only; "discover" = classify everything
        "progress": {"chunk": 0, "chunks": 0, "cand": 0, "cands": 0},
        "candidates": [], "found": [], "skip": set(), "table": {},
        "saved_active": None, "was_running": False, "thread": None, "error": None}

def _parse_freq(s):
    try:
        m = re.search(r"\d+\.\d+", str(s)); return float(m.group(0)) if m else None
    except Exception: return None

def _snap(mhz, raster=0.0125):
    return round(round(float(mhz) / raster) * raster, 5)

def _scan_pub(kind, **f):
    publish({"kind": kind, "ts": time.strftime("%H:%M:%S"), **f})

def _read_fft(path, rate, nfft=4096):
    try:
        raw = np.fromfile(path, dtype=np.int16 if SDR_KIND == "bladerf" else np.uint8)   # sc16 vs rtl_sdr cu8
    except Exception:
        return None
    if raw.size < nfft * 4: return None
    raw = raw.astype(np.float32) if SDR_KIND == "bladerf" else (raw.astype(np.float32) - 127.5) * 16.0
    iq = (raw[0::2] + 1j * raw[1::2]) / 2048.0
    nseg = len(iq) // nfft
    if nseg < 1: return None
    win = np.hanning(nfft); acc = np.zeros(nfft)
    for i in range(nseg):
        acc += np.abs(np.fft.fftshift(np.fft.fft(iq[i*nfft:(i+1)*nfft] * win))) ** 2
    acc /= nseg
    pdb = 10 * np.log10(acc + 1e-12)
    freqs = np.fft.fftshift(np.fft.fftfreq(nfft, 1.0 / rate))
    return freqs, pdb

def _detect_carriers(fc_hz, c1, c2, mode="p25"):
    if not c1 or not c2: return []
    f, p1 = c1; _, p2 = c2
    nf = float(np.median(p1)); thr = nf + 10.0
    binhz = float(f[1] - f[0]); usable = SURVEY_USABLE / 2.0
    if mode == "discover":
        mask = ((p1 > thr) | (p2 > thr)) & (np.abs(f) < usable)   # present in EITHER capture (catch bursty)
        wmin, wmax = 700, 45000                                   # narrow/idle carriers through wide analog
    else:
        mask = (p1 > thr) & (p2 > thr) & (np.abs(f) < usable)     # continuous carrier in BOTH captures
        wmin, wmax = 4000, 18000                                  # ~6.25-12.5 kHz (P25-shaped only)
    out = []; i = 0; n = len(mask)
    while i < n:
        if mask[i]:
            j = i
            while j < n and mask[j]: j += 1
            width = (j - i) * binhz
            if wmin <= width <= wmax:
                k = i + int(np.argmax(p1[i:j])); off = float(f[k])
                if 4000 < abs(off) < usable - 50000:        # drop chunk-edge + DC-spike (center) artifacts
                    out.append({"freq": _snap((fc_hz + off) / 1e6, 0.00625), "power_db": round(float(p1[k] - nf), 1),
                                "width_khz": round(width / 1000.0, 1), "probed": False, "present": None})
            i = j
        else:
            i += 1
    return out

def _survey_chunk(fc_hz, mode="p25"):
    gain = 52 if mode == "discover" else 40          # discover needs more sensitivity for weak/bursty analog+data
    if SDR_KIND == "rtl":
        return _survey_chunk_rtl(fc_hz, mode, 49.6 if mode == "discover" else 40.2)
    cmd = ("set frequency rx %d; set samplerate rx %d; set bandwidth rx %d; set gain rx %d; "
           "rx config file=%s format=bin n=1500000; rx start; rx wait" % (fc_hz, SURVEY_RATE, SURVEY_BW, gain, IQ_PATH))
    try:
        subprocess.run([BLADE_CLI, "-e", cmd], timeout=15,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        c1 = _read_fft(IQ_PATH, SURVEY_RATE)
        time.sleep(1.5)
        subprocess.run([BLADE_CLI, "-e", cmd], timeout=15,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        c2 = _read_fft(IQ_PATH, SURVEY_RATE)
        return _detect_carriers(fc_hz, c1, c2, mode)
    except Exception:
        return []

def _survey_chunk_rtl(fc_hz, mode, gain):
    cmd = [RTL_SDR, "-d", DEV["args"].split("=", 1)[1], "-f", str(fc_hz), "-s", str(SURVEY_RATE), "-g", str(gain),
           "-p", str(int(round(DEV["ppm"]))), "-n", "1500000", IQ_PATH]
    try:
        subprocess.run(cmd, timeout=15, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        c1 = _read_fft(IQ_PATH, SURVEY_RATE)
        time.sleep(1.5)
        subprocess.run(cmd, timeout=15, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        c2 = _read_fft(IQ_PATH, SURVEY_RATE)
        return _detect_carriers(fc_hz, c1, c2, mode)
    except Exception:
        return []

def _build_probe_cfg(freq_mhz, sdr):
    sdr = dev_sdr(sdr)
    with open(os.path.join(TMP, "op25_probe_wl.tsv"), "w") as f:
        f.write("1\n")                                       # CC-only: never retune off the candidate
    chan = {"name": "probe", "device": "sdr0", "trunking_sysname": "probe", "demod_type": "cqpsk",
            "destination": "ws://0.0.0.0:9100", "excess_bw": 0.2, "filter_type": "rc", "if_rate": 24000,
            "plot": "", "symbol_rate": 4800, "enable_analog": "off", "blacklist": "",
            "whitelist": os.path.join(TMP, "op25_probe_wl.tsv"), "crypt_keys": ""}
    devices = [{"args": sdr["args"], "gains": sdr["gains"],
                "name": "sdr0", "offset": -250000, "ppm": float(sdr["ppm"]),
                "rate": 1000000, "usable_bw_pct": 0.85, "tunable": True}]
    trunk = {"module": "tk_p25.py", "chans": [{"nac": "0x0", "wacn": "0x0", "sysid": "0x0", "sysname": "probe",
             "control_channel_list": "%.5f" % freq_mhz, "whitelist": os.path.join(TMP, "op25_probe_wl.tsv"), "blacklist": "",
             "tgid_tags_file": "", "rid_tags_file": "", "tdma_cc": False, "crypt_behavior": 0}]}
    term = {"module": "terminal.py", "terminal_type": "http:0.0.0.0:8082", "curses_plot_interval": 0.1,
            "http_plot_interval": 1.0, "http_plot_directory": "../www/images",
            "tuning_step_large": 1200, "tuning_step_small": 100, "default_channel": "probe"}
    save_json(os.path.join(TMP, "op25_probe.json"), {"channels": [chan], "devices": devices, "trunking": trunk, "terminal": term})
    return os.path.join(TMP, "op25_probe.json")

def _probe_one(freq_mhz, sdr):
    reset_state()
    res = {"freq": round(freq_mhz, 5), "present": False}
    try:
        p = subprocess.Popen([OP25_PY, "multi_rx.py", "-v", "10", "-c", _build_probe_cfg(freq_mhz, sdr)],
                             cwd=APPS, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    except Exception as e:
        res["error"] = str(e); return res
    rt = threading.Thread(target=lambda: [parse_line(l) for l in p.stdout], daemon=True); rt.start()
    t0 = time.time(); nac_at = None
    while True:
        time.sleep(0.25)
        with LOCK:
            sd = STATE["system"]; nac = sd["nac"]
            have = all([sd["wacn"], sd["sysid"] is not None, sd["rfss"] is not None]) and bool(STATE["bandplan"])
        now = time.time()
        if nac and nac_at is None: nac_at = now; res["present"] = True
        if nac_at is None and now - t0 > PROBE_NAC_TIMEOUT: break
        if nac_at and (have or now - nac_at > PROBE_IDENT_DWELL): break
        if p.poll() is not None: break
    if p.poll() is None:
        try: p.terminate(); p.wait(timeout=5)
        except Exception:
            try: p.kill()
            except Exception: pass
    rt.join(timeout=2)
    if res["present"]:
        with LOCK:
            sd = STATE["system"]
            phase2 = any(b.get("type") == "TDMA" for b in STATE["bandplan"].values())
            res.update({"nac": sd["nac"], "wacn": sd["wacn"], "sysid": sd["sysid"], "rfss": sd["rfss"],
                        "site": sd["site"], "cc": sd["cc"], "secondary_cc": list(sd["secondary_cc"]),
                        "neighbors": [v["freq"] for v in STATE["adjacent"].values()],
                        "adjacent": [{"rfss": v.get("rfid"), "site": v.get("stid"), "freq": v.get("freq")}
                                     for v in STATE["adjacent"].values()],
                        "phase2": phase2, "modulation": "cqpsk"})
    return res

PROBE_DMR_TIMEOUT = 11.0
def _build_dmr_probe_cfg(freq_mhz, sdr):
    sdr = dev_sdr(sdr)
    chan = {"name": "dprobe", "device": "sdr0", "trunking_sysname": "dprobe", "demod_type": "fsk4",
            "filter_type": "rrc", "excess_bw": 0.2, "symbol_rate": 4800, "if_rate": 24000, "plot": "",
            "destination": "ws://0.0.0.0:9101", "enable_analog": "off", "blacklist": "", "whitelist": "", "crypt_keys": ""}
    devices = [{"args": sdr["args"], "gains": sdr["gains"],
                "name": "sdr0", "offset": 0, "ppm": float(sdr["ppm"]),
                "rate": 1000000, "usable_bw_pct": 0.85, "tunable": True}]
    trunk = {"module": "tk_trbo.py", "chans": [{"lcn": 1, "frequency": int(round(freq_mhz * 1e6)), "cc": 0}]}
    term = {"module": "terminal.py", "terminal_type": "http:0.0.0.0:8083", "curses_plot_interval": 0.1,
            "http_plot_interval": 1.0, "http_plot_directory": "../www/images", "tuning_step_large": 1200,
            "tuning_step_small": 100, "default_channel": "dprobe"}
    save_json(os.path.join(TMP, "op25_dmr_probe.json"), {"channels": [chan], "devices": devices, "trunking": trunk, "terminal": term})
    return os.path.join(TMP, "op25_dmr_probe.json")

def _probe_dmr_one(freq_mhz, sdr):
    reset_state()
    res = {"freq": round(freq_mhz, 5), "present": False}
    try:
        p = subprocess.Popen([OP25_PY, "multi_rx.py", "-v", "10", "-c", _build_dmr_probe_cfg(freq_mhz, sdr)],
                             cwd=APPS, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    except Exception as e:
        res["error"] = str(e); return res
    rt = threading.Thread(target=lambda: [parse_line(l) for l in p.stdout], daemon=True); rt.start()
    t0 = time.time(); seen_at = None
    while True:
        time.sleep(0.25)
        with LOCK:
            have = STATE["system"].get("dmr_cc") is not None or bool(STATE["talkgroups"])
        now = time.time()
        if have and seen_at is None: seen_at = now; res["present"] = True
        if seen_at is None and now - t0 > PROBE_DMR_TIMEOUT: break
        if seen_at and now - seen_at > 3.0: break
        if p.poll() is not None: break
    if p.poll() is None:
        try: p.terminate(); p.wait(timeout=5)
        except Exception:
            try: p.kill()
            except Exception: pass
    rt.join(timeout=2)
    if res["present"]:
        with LOCK:
            sd = STATE["system"]
            res.update({"type": "DMR", "color_code": sd.get("dmr_cc"),
                        "variant": sd.get("dmr_variant") or "conventional",
                        "netid": sd.get("netid"), "siteid": sd.get("siteid"), "cc": "%.5f" % freq_mhz})
    return res

def _seed_table_from_systems():
    SCAN["table"].clear(); SCAN["skip"].clear()
    with SYSLOCK:                          # snapshot under the lock; UI may be editing SYSTEMS
        items = list(SYSTEMS["systems"].items())
    for k, v in items:
        key = "%s|%s" % (str(v.get("wacn") or "").lower(), v.get("sysid"))
        ccs = set()
        for f in v.get("control_channels", []):
            ff = _parse_freq(f)
            if ff: ccs.add(round(ff, 4)); SCAN["skip"].add(_snap(ff))
        SCAN["table"][key] = {"id": k, "name": v.get("name"), "wacn": v.get("wacn"),
                              "sysid": v.get("sysid"), "ccs": ccs}

def _merge_sites(dst, new):
    """Merge site dicts {rfss, site, freq} keyed by (rfss, site), keeping the first freq seen."""
    seen = {(str(s.get("rfss")), str(s.get("site"))) for s in dst}
    for s in new:
        k = (str(s.get("rfss")), str(s.get("site")))
        if k not in seen and s.get("freq"):
            dst.append(s); seen.add(k)

def _ingest_or_merge(res):
    key = "%s|%s" % (res.get("wacn"), res.get("sysid"))
    cc = _parse_freq(res.get("cc")) or res["freq"]
    cc_mhz = "%.5f" % cc
    # every control channel of this system: primary + secondary + adjacent-site CCs (same WACN/SysID,
    # so they're valid CCs for it) — capturing them all makes dedupe robust and shows the multi-site layout.
    extra = list(res.get("secondary_cc", [])) + list(res.get("neighbors", []))
    allccs = list(dict.fromkeys([cc_mhz] + ["%.5f" % _parse_freq(x) for x in extra if _parse_freq(x)]))
    sites = [{"rfss": res.get("rfss"), "site": res.get("site"), "freq": cc_mhz}]
    for a in res.get("adjacent", []):
        f = _parse_freq(a.get("freq"))
        if f: sites.append({"rfss": a.get("rfss"), "site": a.get("site"), "freq": "%.5f" % f})
    rw = str(res.get("wacn") or "").lower(); rsy = str(res.get("sysid"))
    with SYSLOCK:                          # serialize this scanner-thread write vs UI edits/reads
        sid = next((k for k, v in SYSTEMS["systems"].items()
                    if str(v.get("wacn") or "").lower() == rw and str(v.get("sysid")) == rsy), None)
        if sid:
            s = SYSTEMS["systems"][sid]
            for f in allccs:
                if f not in s["control_channels"]: s["control_channels"].append(f)
            _merge_sites(s.setdefault("sites", []), sites)
            if res.get("rfss") is not None: s["rfss"] = res.get("rfss")
            if res.get("site") is not None: s["site"] = res.get("site")
            merged = True
        else:
            sid = ("nac%s-sys%s" % (str(res.get("nac")), str(res.get("sysid")))).replace("0x", "").replace("None", "x").lower()
            SYSTEMS["systems"][sid] = {"name": "NAC %s / SYS %s" % (res.get("nac"), res.get("sysid")),
                "county": "", "type": "P25 Phase II" if res.get("phase2") else "P25", "nac": res.get("nac"),
                "wacn": res.get("wacn"), "sysid": res.get("sysid"), "control_channels": allccs,
                "rfss": res.get("rfss"), "site": res.get("site"), "sites": sites,
                "modulation": "cqpsk", "phase2": res.get("phase2", False),
                "sdr": sdr_defaults(offset=-250000)}
            merged = False
        db.sync_all(SYSTEMS)
    t = SCAN["table"].setdefault(key, {"id": sid, "name": SYSTEMS["systems"][sid]["name"],
                                       "wacn": res.get("wacn"), "sysid": res.get("sysid"), "ccs": set()})
    for f in allccs: t["ccs"].add(round(float(f), 4))
    rec = {"id": sid, "merged": merged, "name": SYSTEMS["systems"][sid]["name"], "nac": res.get("nac"),
           "wacn": res.get("wacn"), "sysid": res.get("sysid"), "cc": cc_mhz, "rfss": res.get("rfss"),
           "site": res.get("site"), "phase2": res.get("phase2"), "freq": res["freq"]}
    return rec

def _ingest_or_merge_dmr(res):
    """Add/merge a discovered DMR trunked system. Dedupe by NetID|SiteID (Connect Plus) when
    available, else by a shared control-channel frequency; default name from NetID/Site or freq."""
    cc = _parse_freq(res.get("cc")) or res["freq"]
    cc_mhz = "%.5f" % cc
    netid = res.get("netid"); siteid = res.get("siteid")
    variant = res.get("variant") or "conventional"
    is_dmr = lambda v: str(v.get("type") or "").upper().startswith("DMR")
    with SYSLOCK:
        sid = None
        if netid is not None and siteid is not None:
            sid = next((k for k, v in SYSTEMS["systems"].items()
                        if is_dmr(v) and str(v.get("netid")) == str(netid) and str(v.get("siteid")) == str(siteid)), None)
        if sid is None:
            sid = next((k for k, v in SYSTEMS["systems"].items()
                        if is_dmr(v) and cc_mhz in (v.get("control_channels") or [])), None)
        if sid:
            s = SYSTEMS["systems"][sid]
            if cc_mhz not in s["control_channels"]: s["control_channels"].append(cc_mhz)
            if netid is not None: s["netid"] = netid
            if siteid is not None: s["siteid"] = siteid
            merged = True
        else:
            if netid is not None and siteid is not None:
                sid = ("dmr-net%s-site%s" % (netid, siteid)).lower()
                name = "DMR %s NetID %s Site %s" % (variant, netid, siteid)
            else:
                sid = ("dmr-%s" % cc_mhz.replace(".", "")).lower()
                name = "DMR %s %s MHz" % (variant, cc_mhz)
            SYSTEMS["systems"][sid] = {"name": name, "county": "", "type": "DMR (%s)" % variant,
                "nac": "", "wacn": "", "sysid": "", "netid": netid, "siteid": siteid,
                "color_code": res.get("color_code"), "control_channels": [cc_mhz],
                "modulation": "fsk4", "phase2": False,
                "sdr": sdr_defaults(offset=0)}
            merged = False
        db.sync_all(SYSTEMS)
    return {"id": sid, "merged": merged, "name": SYSTEMS["systems"][sid]["name"], "type": "DMR",
            "netid": netid, "siteid": siteid, "cc": cc_mhz, "color_code": res.get("color_code"), "freq": res["freq"]}

def _collapse_skip(res):
    for f in list(res.get("secondary_cc", [])) + list(res.get("neighbors", [])) + [res.get("cc")]:
        ff = _parse_freq(f)
        if ff: SCAN["skip"].add(_snap(ff))

def _classify_known(cand):
    """Annotate a candidate that sits on a known control channel from the registry."""
    snapf = _snap(cand["freq"])
    for v in SCAN["table"].values():
        if any(abs(snapf - c) < 0.006 for c in v.get("ccs", set())):
            cand["present"] = "known"; cand["class"] = "P25 (known)"; cand["detail"] = v.get("name") or ""
            return True
    return False

def _survey_and_probe_band(band, sdr, mode="p25"):
    bd = BANDS[band]; SCAN["band"] = band
    step = SURVEY_USABLE / 1e6
    centers = []
    fc = bd["start"] + step / 2
    while fc - step / 2 < bd["stop"]:          # cover the full band incl. the top edge
        centers.append(round(fc, 4)); fc += step
    SCAN["progress"] = {"chunk": 0, "chunks": len(centers), "cand": 0, "cands": 0}
    SCAN["phase"] = "survey"; cands = []
    for idx, c in enumerate(centers):
        if SCAN["cancel"]: return
        SCAN["progress"]["chunk"] = idx + 1
        _scan_pub("SCAN", phase="survey", band=band, chunk=idx + 1, chunks=len(centers), center=c)
        found = _survey_chunk(int(c * 1e6), mode)
        for fc2 in found:
            if not any(abs(fc2["freq"] - e["freq"]) < 0.006 for e in cands):
                cands.append(fc2)
    cands.sort(key=lambda x: -x["power_db"])
    SCAN["candidates"] = cands
    SCAN["progress"]["cands"] = len(cands); SCAN["phase"] = "probe"
    paging_band = bd["start"] >= 929.0 or (462.0 <= bd["start"] <= 466.0)
    for idx, cand in enumerate(cands):
        if SCAN["cancel"]: return
        SCAN["progress"]["cand"] = idx + 1
        if _classify_known(cand) or _snap(cand["freq"]) in SCAN["skip"]:
            cand["present"] = "known"
            if not cand.get("class"): cand["class"] = "P25 (known)"
            continue
        _scan_pub("PROBE", freq=cand["freq"], power=cand["power_db"])
        cand["probed"] = True
        # discover mode: classify first; only run the heavy OP25 P25/DMR probes on 4-FSK ~12.5k carriers
        spec = None
        if mode == "discover" and signalid:
            try: spec = signalid.classify(cand["freq"], 3.0)
            except Exception as e: spec = {"type": "ERR", "detail": str(e)}
            cand["class"] = spec.get("type"); cand["detail"] = spec.get("detail", "")
            cand["present"] = spec.get("type") not in ("WEAK", "NO-SIG", "ERR")
            cand["bw_khz"] = spec.get("bw_khz")
            if spec.get("type") in ("POCSAG", "FLEX"):
                cand["msgs"] = spec.get("detail")
                _scan_pub("PAGING", freq=cand["freq"], proto=spec.get("type"), sample=spec.get("detail", "")[:80])
            digital_like = "4-FSK" in (spec.get("type") or "") or "12.5" in (spec.get("type") or "")
            if not digital_like:
                continue                                     # analog / paging / data — spectral class is the answer
        # 1) P25
        res = _probe_one(cand["freq"], sdr)
        if res.get("present") and res.get("wacn"):
            rec = _ingest_or_merge(res); _collapse_skip(res)
            cand["present"] = True; cand["class"] = "P25"; cand["detail"] = rec["name"]
            SCAN["found"].append(rec)
            _scan_pub("FOUND", **{k: rec.get(k) for k in ("name", "nac", "wacn", "sysid", "cc", "merged")})
            continue
        # 2) DMR
        dres = _probe_dmr_one(cand["freq"], sdr)
        if dres.get("present"):
            drec = _ingest_or_merge_dmr(dres)
            SCAN["found"].append(drec); SCAN["skip"].add(_snap(cand["freq"]))
            cand["present"] = True; cand["class"] = "DMR"
            cand["detail"] = "%s%s" % (drec["name"], " (merged)" if drec["merged"] else "")
            _scan_pub("FOUND", **{k: drec.get(k) for k in ("name", "netid", "siteid", "cc", "merged", "type")})
            continue
        # 3) everything else: POCSAG/FLEX (decoded) or analog/spectral  (p25 mode, or digital_like that failed both probes)
        if signalid and spec is None:
            secs = 18.0 if paging_band else 3.0
            try: spec = (signalid.decode_pocsag(cand["freq"], secs) if paging_band else None) or signalid.classify(cand["freq"], 3.0)
            except Exception as e: spec = {"type": "ERR", "detail": str(e)}
        if spec:
            if spec.get("ok"):                               # decode_pocsag hit
                cand["class"] = "PAGING"; cand["detail"] = " | ".join(spec.get("msgs", [])[:4]); cand["present"] = True
                _scan_pub("PAGING", freq=cand["freq"], proto="POCSAG", sample=cand["detail"][:80])
            else:
                cand["class"] = spec.get("type", "?"); cand["detail"] = spec.get("detail", "")
                cand["present"] = spec.get("type") not in ("WEAK", "NO-SIG", "ERR")
                if spec.get("type") in ("POCSAG", "FLEX"):
                    cand["msgs"] = spec.get("detail")
                    _scan_pub("PAGING", freq=cand["freq"], proto=spec.get("type"), sample=spec.get("detail", "")[:80])
        else:
            cand["present"] = False; cand["class"] = "?"

def _next_known(fnow, direction, cur_key):
    pool = sorted({(round(f, 4), k) for k, v in SCAN["table"].items() for f in v.get("ccs", set())})
    if direction == "up":
        cand = [(f, k) for f, k in pool if f > fnow + 0.001 and k != cur_key]
    else:
        cand = [(f, k) for f, k in pool if f < fnow - 0.001 and k != cur_key][::-1]
    return cand[0] if cand else None

def run_scan(bands, direction=None, fnow=None, mode="p25"):
    if SCAN["running"]: return
    SCAN.update({"running": True, "cancel": False, "phase": "init", "found": [], "candidates": [],
                 "mode": mode, "error": None, "saved_active": SYSTEMS.get("active"), "was_running": OP25["running"]})
    _seed_table_from_systems()
    sdr = (SYSTEMS["systems"].get(SCAN["saved_active"], {}).get("sdr")
           or sdr_defaults())
    op25_stop(); time.sleep(1)
    try:
        if direction:
            nk = _next_known(fnow or 0, direction, None)
            if nk:
                res = _probe_one(nk[0], sdr)
                if res.get("present") and res.get("wacn"):
                    rec = _ingest_or_merge(res); SCAN["found"].append(rec)
                    _scan_pub("FOUND", **{k: rec.get(k) for k in ("name", "nac", "wacn", "sysid", "cc", "merged")})
            else:
                band = next((b for b, d in BANDS.items() if d["start"] <= (fnow or 0) <= d["stop"]), "700")
                _survey_and_probe_band(band, sdr, mode)
        else:
            for b in bands:
                if SCAN["cancel"]: break
                _survey_and_probe_band(b, sdr, mode)
        SCAN["phase"] = "cancelled" if SCAN["cancel"] else "done"
    except Exception as e:
        SCAN["phase"] = "error"; SCAN["error"] = str(e)
    finally:
        reset_state()
        if SCAN["was_running"] and SCAN["saved_active"]:
            op25_start()
        SCAN["running"] = False

# ----------------------------------------------------------------- tag import
def import_tags(text, typ, id_col=None, name_col=None):
    """Import id->name pairs from CSV/TSV/pasted text. Handles a Radio Reference talkgroup
    export (Decimal,Hex,Alpha Tag,Description,Tag,Category) out of the box: quoted commas are
    parsed correctly, the header row auto-maps the id + name columns, and short hex-looking
    tags are no longer dropped."""
    import csv, io
    dst = TAGS["talkgroups"] if typ == "tg" else (RIDS if typ == "rid" else TAGS["radios"])
    added = 0
    sample = text[:4000]
    if "," in sample and sample.count(",") >= sample.count("\t"):
        rows = list(csv.reader(io.StringIO(text)))                    # RR CSV — quoted fields safe
    elif "\t" in sample:
        rows = list(csv.reader(io.StringIO(text), delimiter="\t"))
    else:
        rows = [re.split(r"\s{2,}", ln) for ln in text.splitlines()]  # 2+ space columns
    rows = [[c.strip() for c in r] for r in rows if any(c.strip() for c in r)]
    if not rows:
        return 0
    hdr = [c.lower() for c in rows[0]]
    is_header = not rows[0][0].isdigit()
    if is_header:
        if id_col is None:
            id_col = next((i for i, h in enumerate(hdr) if h in ("decimal", "dec", "tgid", "talkgroup", "rid", "radio id", "id")), id_col)
        if name_col is None:
            name_col = next((i for i, h in enumerate(hdr) if h in ("alpha tag", "alpha", "name", "tag", "label", "description")), name_col)
        rows = rows[1:]
    for parts in rows:
        if id_col is not None and id_col < len(parts) and parts[id_col].isdigit():
            ident = parts[id_col]
            name = parts[name_col] if (name_col is not None and name_col < len(parts)) else ""
        else:
            didx = next((i for i, c in enumerate(parts) if c.isdigit()), None)
            if didx is None: continue
            ident = parts[didx]; rest = [c for j, c in enumerate(parts) if j != didx]
            cand = [c for c in rest if len(c) > 1 and not c.isdigit() and not re.fullmatch(r"[0-9a-fA-F]{1,5}", c)]
            name = cand[0] if cand else ""
        if name:
            dst[ident] = name; added += 1
    if added:
        db.bulk_set("talkgroups", TAGS["talkgroups"]); db.bulk_set("radios", TAGS["radios"])
        db.bulk_set("rids", RIDS)
    return added

# ------------------------------------------------ data-channel packet-data probe
PROBE_DATA = {"running": False, "freq": None, "result": None}

def _build_data_probe_cfg(freq_mhz, sdr, mod="cqpsk"):
    sdr = dev_sdr(sdr)
    wl = os.path.join(TMP, "op25_dprobe_wl.tsv")
    with open(wl, "w") as f: f.write("1\n")              # camp on the data freq; never retune
    chan = {"name": "dataprobe", "device": "sdr0", "trunking_sysname": "dataprobe", "demod_type": mod,
            "destination": "ws://0.0.0.0:9102", "excess_bw": 0.2, "filter_type": "rc", "if_rate": 24000,
            "plot": "", "symbol_rate": 4800, "enable_analog": "off", "blacklist": "",
            "whitelist": wl, "crypt_keys": ""}
    devices = [{"args": sdr["args"], "gains": sdr["gains"],
                "name": "sdr0", "offset": int(sdr.get("offset", DEV["offset"])), "ppm": float(sdr["ppm"]),
                "rate": 1000000, "usable_bw_pct": 0.85, "tunable": True}]
    trunk = {"module": "tk_p25.py", "chans": [{"nac": "0x0", "wacn": "0x0", "sysid": "0x0", "sysname": "dataprobe",
             "control_channel_list": "%.5f" % freq_mhz, "whitelist": wl, "blacklist": "",
             "tgid_tags_file": "", "rid_tags_file": "", "tdma_cc": False, "crypt_behavior": 0}]}
    term = {"module": "terminal.py", "terminal_type": "http:0.0.0.0:8084", "curses_plot_interval": 0.1,
            "http_plot_interval": 1.0, "http_plot_directory": "../www/images",
            "tuning_step_large": 1200, "tuning_step_small": 100, "default_channel": "dataprobe"}
    path = os.path.join(TMP, "op25_dataprobe.json")
    save_json(path, {"channels": [chan], "devices": devices, "trunking": trunk, "terminal": term})
    return path

def _run_data_probe(freq_mhz, secs=75):
    """Pause trunking, camp OP25 on the data channel, and watch the patched PDU dump to learn
    whether the packet payload is clear (and carries LRRP GPS) or encrypted. Resumes after."""
    sid = SYSTEMS.get("active"); s = SYSTEMS["systems"].get(sid, {}) if sid else {}
    sdr = s.get("sdr", {}); mod = s.get("modulation", "cqpsk")
    PROBE_DATA.update({"running": True, "freq": round(freq_mhz, 5), "result": None})
    _scan_pub("PROBE", phase="start", freq=round(freq_mhz, 5))
    op25_stop(); time.sleep(0.6); reset_state()
    try:
        p = subprocess.Popen([OP25_PY, "multi_rx.py", "-v", "10", "-c", _build_data_probe_cfg(freq_mhz, sdr, mod)],
                             cwd=APPS, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    except Exception as e:
        PROBE_DATA.update({"running": False, "result": {"error": str(e), "verdict": "error"}})
        _scan_pub("PROBE", phase="done", verdict="error", freq=round(freq_mhz, 5))
        op25_start(); return
    rt = threading.Thread(target=lambda: [parse_line(l) for l in p.stdout], daemon=True); rt.start()
    t0 = time.time()
    while time.time() - t0 < secs:
        time.sleep(1.0)
        with LOCK:
            ds = dict(STATE["datastats"]); ng = sum(1 for x in STATE.get("pdu_samples", []) if x.get("gps"))
        _scan_pub("PROBE", phase="run", freq=round(freq_mhz, 5), elapsed=int(time.time() - t0),
                  pdus=ds.get("pdus", 0), clear=ds.get("pdu_clear", 0), enc=ds.get("pdu_enc", 0), gps=ng)
        if p.poll() is not None: break
    if p.poll() is None:
        try: p.terminate(); p.wait(timeout=5)
        except Exception:
            try: p.kill()
            except Exception: pass
    rt.join(timeout=2)
    with LOCK:
        ds = dict(STATE["datastats"]); samp = list(STATE.get("pdu_samples", []))
    ngps = sum(1 for x in samp if x.get("gps"))
    verdict = ("gps" if ngps else
               ("clear" if ds.get("pdu_clear", 0) else
                ("encrypted" if ds.get("pdu_enc", 0) else
                 ("signaling" if ds.get("pdus", 0) > 0 else "idle"))))
    result = {"freq": round(freq_mhz, 5), "pdus": ds.get("pdus", 0), "payload": ds.get("pdu_payload", 0),
              "clear": ds.get("pdu_clear", 0), "encrypted": ds.get("pdu_enc", 0),
              "gps_fixes": ngps, "verdict": verdict, "samples": samp[-8:]}
    PROBE_DATA.update({"running": False, "result": result})
    _scan_pub("PROBE", phase="done", **{k: result[k] for k in
              ("freq", "pdus", "payload", "clear", "encrypted", "gps_fixes", "verdict")})
    op25_start()                                          # resume normal trunking
    return result

# ----------------------------------------------------------------- routes
@app.route("/")
def index():
    resp = send_from_directory(HERE, "index.html")
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp

def watch_diag(rid):
    """One consolidated diagnostic bundle for a watched radio: where it's registered
    (site/RFSS/system), how the site hears it (uplink RF level + BER, plus worst-case
    and a short trend), reg failures, and its recent event timeline. Caller holds LOCK."""
    s = str(rid)
    r = STATE["radios"].get(s, {})
    q = SIGQUAL.get(s, {})
    tg = r.get("last_tg")
    live = {
        "last": r.get("last"), "last_action": r.get("last_action"),
        "tg": tg, "tgtag": (tg_tag(tg) if tg is not None else None),
        "site": r.get("site"), "rfss": r.get("rfss"), "sysid": r.get("sysid"),
        "nac": r.get("nac"), "site_cc": r.get("site_cc"), "last_reg_fail": r.get("last_reg_fail"),
        # uplink signal quality as measured by the SITE (P25 power-control sig-qual):
        "rf": q.get("rf"), "ber": q.get("ber"), "sq_last": q.get("last"), "hist": q.get("hist") or None,
    }
    k = WKEEP.setdefault(s, {"gen": RESET_GEN[0], "base": {}, "seen": {}})
    if k["gen"] != RESET_GEN[0]:                     # OP25 restarted / system switched: bank the old counts
        for c, v in k["seen"].items(): k["base"][c] = k["base"].get(c, 0) + v
        k["seen"] = {}; k["gen"] = RESET_GEN[0]
    for f, v in live.items():
        if v is not None: k[f] = v                   # live value wins; otherwise keep the last one we saw
    if q.get("rf_min") is not None: k["rf_min"] = min(q["rf_min"], k.get("rf_min", q["rf_min"]))
    if q.get("ber_max") is not None: k["ber_max"] = max(q["ber_max"], k.get("ber_max", q["ber_max"]))
    for c, v in (("count", r.get("count", 0)), ("reg_fails", r.get("reg_fails", 0)), ("sq_count", q.get("count", 0))):
        k["seen"][c] = v
    out = {f: k.get(f) for f in live}
    out.update({c: k["base"].get(c, 0) + k["seen"].get(c, 0) for c in ("count", "reg_fails", "sq_count")})
    out.update(rid=rid, alias=rid_alias(rid), tgtag=k.get("tgtag") or "", hist=k.get("hist") or [],
               rf_min=k.get("rf_min"), ber_max=k.get("ber_max"), events=list(WEV.get(s, []))[-24:])
    return out


@app.route("/api/state")
def api_state():
    with SYSLOCK:                          # consistent SYSTEMS snapshot (scanner thread may be writing)
        _active = SYSTEMS.get("active")
        _sys_summary = {k: {"name": v["name"], "county": v.get("county"), "type": v.get("type")}
                        for k, v in SYSTEMS["systems"].items()}
        _active_sdr = SYSTEMS["systems"].get(_active, {}).get("sdr", {})
    with LOCK:
        tgs = []
        for tg in STATE["talkgroups"].values():
            d = dict(tg); d["tag"] = tg_tag(tg["tgid"]); tgs.append(d)
        rids = []
        for r in STATE["radios"].values():
            d = dict(r); d["alias"] = rid_alias(r["rid"]); rids.append(d)
        tgs.sort(key=lambda x: x["count"], reverse=True)
        rids.sort(key=lambda x: x["last"] or "", reverse=True)
        return jsonify({
            "system": STATE["system"], "counters": STATE["counters"],
            "bandplan": STATE["bandplan"], "adjacent": STATE["adjacent"],
            "talkgroups": tgs[:80], "radios": rids[:80],
            "events": list(EVENTS)[-200:], "watch_events": list(WATCH_EVENTS)[-100:],
            "watch_diag": [watch_diag(int(x)) for x in sorted(WATCH) if str(x).isdigit()],
            "active": _active,
            "systems": _sys_summary,
            "op25": {"running": OP25["running"], "error": OP25["error"]},
            "sdr": _active_sdr,
            "watch": sorted(WATCH), "listen": list(LISTEN), "cc_only": CC_ONLY[0],
            "keys_loaded": STATE.get("keys_loaded", []),
            "cc": list(RAW)[-250:],
            "opcodes": dict(sorted(OPCOUNTS.items(), key=lambda x: -x[1])[:40]),
            "sigqual": sorted([{**q, "alias": rid_alias(q["rid"])} for q in SIGQUAL.values()],
                              key=lambda x: (-x.get("ber", 0), -x.get("count", 0)))[:25],
            "diag": {**{k: DIAG[k] for k in ("error_hz", "ppm", "freq", "ts", "locked", "level_db")},
                     "median_hz": diag_median(), "suggested_ppm": suggested_ppm(),
                     "n": len(DIAG["samples"]), "term_port": OP25_TERM_PORT, "cal": cal_status()},
            "chantg": STATE.get("chantg", {}),
            "datachans": sorted(STATE["datachans"].values(), key=lambda d: (d.get("freq") or 0)),
            "datastats": STATE["datastats"],
            "pdu_samples": list(STATE.get("pdu_samples", []))[-12:],
            "probe": {"running": PROBE_DATA["running"], "freq": PROBE_DATA["freq"], "result": PROBE_DATA["result"]},
            "unhandled": sorted(STATE["unhandled_ops"].values(), key=lambda x: -x["count"])[:30],
            "scanning": {"running": SCAN["running"], "phase": SCAN["phase"], "band": SCAN["band"],
                         "progress": SCAN["progress"], "found": len(SCAN["found"])}})

@app.route("/api/scan/start", methods=["POST"])
def api_scan_start():
    if SCAN["running"]: return jsonify({"ok": False, "err": "scanning"}), 409
    body = request.get_json(force=True)
    bands = body.get("bands") or ["700", "800f"]
    if "all" in bands: bands = ["700", "800", "uhf", "fed"]
    bands = [b for b in bands if b in BANDS]
    mode = "discover" if body.get("mode") == "discover" else "p25"
    SCAN["thread"] = threading.Thread(target=run_scan, args=(bands,), kwargs={"mode": mode}, daemon=True); SCAN["thread"].start()
    return jsonify({"ok": True, "bands": bands, "mode": mode})

@app.route("/api/scan/next", methods=["POST"])
def api_scan_next():
    if SCAN["running"]: return jsonify({"ok": False, "err": "scanning"}), 409
    d = request.get_json(force=True); direction = d.get("dir", "up")
    fnow = d.get("from")
    if fnow in (None, ""): fnow = _parse_freq(STATE["system"].get("cc")) or 0
    fnow = float(fnow)
    SCAN["thread"] = threading.Thread(target=run_scan, args=([],), kwargs={"direction": direction, "fnow": fnow}, daemon=True)
    SCAN["thread"].start()
    return jsonify({"ok": True, "dir": direction, "from": fnow})

@app.route("/api/scan/stop", methods=["POST"])
def api_scan_stop():
    SCAN["cancel"] = True
    return jsonify({"ok": True})

@app.route("/api/scan/status")
def api_scan_status():
    return jsonify({"running": SCAN["running"], "phase": SCAN["phase"], "band": SCAN["band"],
                    "dir": SCAN["dir"], "mode": SCAN["mode"], "progress": SCAN["progress"], "error": SCAN["error"],
                    "candidates": SCAN["candidates"][:80], "found": SCAN["found"],
                    "bands": {k: v["label"] for k, v in BANDS.items()}})

@app.route("/api/scan/ingest", methods=["POST"])
def api_scan_ingest():
    if SCAN["running"]: return jsonify({"ok": False, "err": "scanning"}), 409
    d = request.get_json(force=True); freq = _parse_freq(d.get("freq")) or float(d.get("freq") or 0)
    if not freq: return jsonify({"ok": False, "err": "no freq"}), 400
    def _job():
        SCAN.update({"running": True, "cancel": False, "phase": "probe", "found": [],
                     "saved_active": SYSTEMS.get("active"), "was_running": OP25["running"]})
        sdr = (SYSTEMS["systems"].get(SCAN["saved_active"], {}).get("sdr")
               or sdr_defaults())
        op25_stop(); time.sleep(1)
        try:
            res = _probe_one(freq, sdr)
            if res.get("present") and res.get("wacn"):
                rec = _ingest_or_merge(res); SCAN["found"].append(rec)
                _scan_pub("FOUND", **{k: rec.get(k) for k in ("name", "nac", "wacn", "sysid", "cc", "merged")})
            SCAN["phase"] = "done"
        except Exception as e:
            SCAN["phase"] = "error"; SCAN["error"] = str(e)
        finally:
            reset_state()
            if SCAN["was_running"] and SCAN["saved_active"]: op25_start()
            SCAN["running"] = False
    threading.Thread(target=_job, daemon=True).start()
    return jsonify({"ok": True, "freq": freq})

@app.route("/api/systems", methods=["GET", "POST"])
def api_systems():
    if request.method == "GET":
        with SYSLOCK:
            return jsonify(SYSTEMS)
    d = request.get_json(force=True)
    sid = d.get("id") or (re.sub(r"\W+", "-", (d.get("name", "sys")).lower()).strip("-")[:80] or "sys")
    with SYSLOCK:
        SYSTEMS["systems"][sid] = {
            "name": d.get("name", sid), "county": d.get("county", ""), "type": d.get("type", "P25"),
            "nac": d.get("nac", "0x0"), "wacn": d.get("wacn", ""), "sysid": d.get("sysid", ""),
            "control_channels": [c.strip() for c in (d.get("control_channels") or []) if c.strip()],
            "modulation": d.get("modulation", "cqpsk"),
            "sdr": d.get("sdr", sdr_defaults(ppm=DEV["ppm"] if SDR_KIND == "rtl" else 0.0))}
        db.sync_all(SYSTEMS)
    return jsonify({"ok": True, "id": sid})

@app.route("/api/systems/delete", methods=["POST"])
def api_sys_del():
    sid = request.get_json(force=True).get("id")
    with SYSLOCK:
        SYSTEMS["systems"].pop(sid, None); db.sync_all(SYSTEMS)
    return jsonify({"ok": True})

@app.route("/api/systems/export")
def api_systems_export():
    """Download the whole systems registry as portable JSON (share / back up / move machines)."""
    with SYSLOCK:
        data = json.dumps({"active": SYSTEMS.get("active"), "systems": SYSTEMS["systems"]}, indent=2)
    return app.response_class(data, mimetype="application/json",
                              headers={"Content-Disposition": "attachment; filename=systems.json"})

@app.route("/api/systems/import", methods=["POST"])
def api_systems_import():
    """Merge a systems JSON export into the registry (dedupe by WACN|SysID, else by id)."""
    d = request.get_json(force=True) or {}
    incoming = d.get("systems") if isinstance(d.get("systems"), dict) else d
    if not isinstance(incoming, dict):
        return jsonify({"ok": False, "err": "expected a {\"systems\": {...}} object"}), 400
    added = merged = 0
    with SYSLOCK:
        for rid, rec in incoming.items():
            if not isinstance(rec, dict) or "control_channels" not in rec:
                continue
            rw = str(rec.get("wacn") or "").lower(); rsy = str(rec.get("sysid"))
            existing = next((k for k, v in SYSTEMS["systems"].items()
                             if rw and str(v.get("wacn") or "").lower() == rw and str(v.get("sysid")) == rsy), None)
            if existing:
                s = SYSTEMS["systems"][existing]
                for f in rec.get("control_channels", []):
                    if f not in s["control_channels"]: s["control_channels"].append(f)
                if rec.get("sites"): _merge_sites(s.setdefault("sites", []), rec["sites"])
                merged += 1
            else:
                key = rid if rid and rid not in SYSTEMS["systems"] else ("imp-%s" % (rid or len(SYSTEMS["systems"])))
                SYSTEMS["systems"][key] = rec; added += 1
        db.sync_all(SYSTEMS)
    return jsonify({"ok": True, "added": added, "merged": merged})

@app.route("/api/switch", methods=["POST"])
def api_switch():
    d = request.get_json(force=True); sid = d.get("id")
    if sid in TEMP_SYS: RUN_TEMP[0] = sid       # listen to an unsaved hunter find
    else:
        with SYSLOCK:
            if sid not in SYSTEMS["systems"]:
                return jsonify({"ok": False, "err": "unknown system"}), 400
            SYSTEMS["active"] = sid; db.sync_all(SYSTEMS)
        RUN_TEMP[0] = None
    LISTEN.clear()
    CC_PIN.update(sid=sid if d.get("cc") else None, freq=("%.5f" % _parse_freq(d["cc"])) if _parse_freq(d.get("cc")) else None)
    if "cc_only" in d:
        CC_ONLY[0] = bool(d["cc_only"]); db.set_meta("cc_only", CC_ONLY[0])
    op25_start()
    return jsonify({"ok": True})

@app.route("/api/sdr", methods=["POST"])
def api_sdr():
    d = request.get_json(force=True); sid = SYSTEMS.get("active")
    with SYSLOCK:
        if sid not in SYSTEMS["systems"]:
            return jsonify({"ok": False}), 400
        sdr = SYSTEMS["systems"][sid]["sdr"]
        for k in ("gains", "ppm", "offset", "rate", "args", "center", "wb_rate", "voice_channels", "usable_bw_pct"):
            if k in d and d[k] != "": sdr[k] = d[k]
        if "wideband" in d:
            sdr["wideband"] = bool(d["wideband"])
        db.sync_all(SYSTEMS)
    op25_start()
    return jsonify({"ok": True, "sdr": sdr})

@app.route("/api/diag")
def api_diag():
    with LOCK:
        return jsonify({
            "error_hz": DIAG["error_hz"], "median_hz": diag_median(), "ppm": DIAG["ppm"],
            "freq": DIAG["freq"], "ts": DIAG["ts"], "locked": DIAG["locked"],
            "n": len(DIAG["samples"]), "samples": list(DIAG["samples"])[-24:],
            "suggested_ppm": suggested_ppm(), "running": OP25["running"], "cal": cal_status()})

@app.route("/api/calibrate", methods=["POST"])
def api_calibrate():
    """Apply a ppm correction to the active system and re-tune. With no body, uses the
    GPS-CC-derived suggestion; pass {"ppm": <value>} to set an explicit value."""
    d = request.get_json(force=True) or {}
    new = d.get("ppm")
    if new in (None, ""):
        new = suggested_ppm()
    if new is None:
        return jsonify({"ok": False, "err": "no CC lock yet — wait for a freq-error reading"}), 409
    try:
        new = float(new)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "err": "bad ppm"}), 400
    if abs(new) > CAL_PPM_MAX:
        return jsonify({"ok": False, "err": "refusing ppm %.3f — beyond +/-%g, almost certainly a bad calibration" % (new, CAL_PPM_MAX)}), 400
    sid = SYSTEMS.get("active")
    with SYSLOCK:
        if sid not in SYSTEMS["systems"]:
            return jsonify({"ok": False, "err": "no active system"}), 400
        prev = round(float(SYSTEMS["systems"][sid]["sdr"].get("ppm", 0.0) or 0.0), 3)
        SYSTEMS["systems"][sid]["sdr"]["ppm"] = new
        db.sync_all(SYSTEMS)
    diag_reset()              # the old measurement is stale once we re-tune; start clean
    op25_start()

    def _verify():            # GUARD: revert if the new ppm broke the CC decode
        if prev != new and not _cal_decoding():
            _cal_set_ppm(sid, prev)
            CAL["result"] = {"ppm": prev, "err": None, "reverted": True, "rejected": new}
            CAL["phase"] = "reverted: %.3f ppm broke decode, kept %.3f" % (new, prev)
    threading.Thread(target=_verify, daemon=True).start()
    return jsonify({"ok": True, "ppm": new})

@app.route("/api/calibrate/auto", methods=["GET", "POST"])
def api_calibrate_auto():
    """POST starts the closed-loop auto-calibration in a background thread; GET returns status.
    It re-tunes OP25 several times (tens of seconds) — watch cal.phase / cal.log."""
    if request.method == "GET":
        return jsonify(cal_status())
    if CAL["running"]:
        return jsonify({"ok": False, "err": "already calibrating"}), 409
    if SCAN["running"] or PROBE_DATA["running"]:
        return jsonify({"ok": False, "err": "scanner/probe busy"}), 409
    if not OP25["running"]:
        return jsonify({"ok": False, "err": "receiver not running"}), 409
    threading.Thread(target=auto_calibrate, daemon=True).start()
    return jsonify({"ok": True})

POCSAG_MON = {"running": False, "freq": 460.6125, "secs": 0, "result": None}

@app.route("/api/pocsag", methods=["POST"])
def api_pocsag():
    """On-demand POCSAG decode on the bladeRF: stop OP25, capture N seconds at the
    given frequency, decode with the built-in decoder, then resume trunking."""
    if signalid is None:
        return jsonify({"ok": False, "err": "signalid module unavailable"}), 503
    if POCSAG_MON["running"]:
        return jsonify({"ok": False, "err": "already decoding"}), 409
    if SCAN["running"] or PROBE_DATA["running"] or CAL["running"] or PAGER["enabled"]:
        return jsonify({"ok": False, "err": "scanner/probe/calibration/pager busy"}), 409
    d = request.get_json(force=True) or {}
    try:
        freq = float(d.get("freq") or 460.6125)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "err": "bad freq"}), 400
    secs = max(4, min(60, int(d.get("secs") or 15)))

    def _job():
        POCSAG_MON.update({"running": True, "freq": freq, "secs": secs, "result": None})
        was = OP25["running"]; saved = SYSTEMS.get("active")
        op25_stop(); time.sleep(1)
        try:
            res = signalid.decode_pocsag(freq, secs=secs)
        except Exception as e:
            res = {"ok": False, "msgs": [], "detail": str(e)}
        res["ts"] = time.strftime("%H:%M:%S")
        POCSAG_MON["result"] = res
        for m in res.get("msgs", []):
            emit("PAGE", res["ts"], text=m, freq=freq)
        if was and saved:
            op25_start()
        POCSAG_MON["running"] = False

    threading.Thread(target=_job, daemon=True).start()
    return jsonify({"ok": True, "freq": freq, "secs": secs})

@app.route("/api/pocsag/status")
def api_pocsag_status():
    return jsonify({k: POCSAG_MON[k] for k in ("running", "freq", "secs", "result")})

# ---- Virtual pager: continuously watch the paging channel for monitored capcodes ----
# Enabling pauses trunking (the bladeRF does one thing at a time); the loop repeatedly
# captures + decodes POCSAG and raises an alert whenever a decoded RIC matches a capcode.
PAGER = {"enabled": False, "freq": 460.6125, "interval": 12, "capcodes": [],
         "pages": collections.deque(maxlen=200), "alerts": collections.deque(maxlen=120),
         "alert_seq": 0, "seen": 0, "last": None, "status": "idle",
         "thread": None, "was_op25": False, "saved_sys": None}

def _pager_load():
    cfg = db.get_meta("pager", {}) or {}
    try: PAGER["freq"] = float(cfg.get("freq", 460.6125))
    except (TypeError, ValueError): pass
    try: PAGER["interval"] = max(5, min(40, int(cfg.get("interval", 12))))
    except (TypeError, ValueError): pass
    cc = []
    for c in cfg.get("capcodes", []) or []:
        try: cc.append({"code": int(c["code"]), "label": str(c.get("label", "")).strip()[:60]})
        except (TypeError, ValueError, KeyError): continue
    PAGER["capcodes"] = cc

def _pager_save():
    db.set_meta("pager", {"freq": PAGER["freq"], "interval": PAGER["interval"], "capcodes": PAGER["capcodes"]})

def _pager_page(m, ts):
    codes = {c["code"]: c.get("label", "") for c in PAGER["capcodes"]}
    rec = {"ts": ts, "code": m.get("address"), "type": m.get("type"),
           "content": m.get("content", ""), "function": m.get("function"), "baud": m.get("baud")}
    matched = m.get("address") in codes
    if matched or signalid._plausible(m):               # capcode match always shown; else drop junk
        PAGER["pages"].appendleft(rec); PAGER["seen"] += 1
    if matched:
        PAGER["alerts"].appendleft(dict(rec, label=codes[m["address"]]))
        PAGER["alert_seq"] += 1

def _pager_kill():
    for p in PAGER.get("procs") or ():
        try: p.terminate(); p.wait(timeout=3)            # wait = reap (no <defunct> rtl_fm)
        except Exception:
            try: p.kill()
            except Exception: pass

def pager_stream():
    """RTL: one continuous rtl_fm | multimon-ng (no capture windows, so no gaps between them). Disable or a
    freq change kills the pipeline (_pager_kill) to unblock the read; the loop restarts it on the new freq."""
    while PAGER["enabled"]:
        f = PAGER["freq"]
        PAGER["status"] = "listening %.5f MHz (continuous)" % f
        try:
            p1, p2 = signalid.pocsag_stream_rtl(f); PAGER["procs"] = (p1, p2)
            for ln in p2.stdout:
                r = signalid.parse_mm(ln)
                if isinstance(r, dict):
                    _pager_page(r, time.strftime("%H:%M:%S")); PAGER["last"] = time.strftime("%H:%M:%S")
                if not PAGER["enabled"] or PAGER["freq"] != f: break
        except Exception as e:
            PAGER["status"] = "error: %s" % e
        _pager_kill(); PAGER["procs"] = None
        if PAGER["enabled"]: time.sleep(1)                # rtl_fm died or retune: let the dongle settle
    PAGER["status"] = "stopped"
    PAGER["thread"] = None

def pager_loop():
    if getattr(signalid, "RTL", False):
        return pager_stream()
    while PAGER["enabled"]:
        PAGER["status"] = "listening %.5f MHz (%ds windows)" % (PAGER["freq"], PAGER["interval"])
        try:
            res = signalid.decode_pocsag(PAGER["freq"], secs=PAGER["interval"])
        except Exception as e:
            PAGER["status"] = "error: %s" % e
            time.sleep(2)
            if not PAGER["enabled"]: break
            continue
        ts = time.strftime("%H:%M:%S")
        for m in res.get("messages", []):
            _pager_page(m, ts)
        PAGER["last"] = ts
        if not PAGER["enabled"]: break
    PAGER["status"] = "stopped"
    PAGER["thread"] = None

def pager_set_enabled(on):
    if on and not PAGER["enabled"]:
        PAGER["was_op25"] = OP25["running"]; PAGER["saved_sys"] = SYSTEMS.get("active")
        op25_stop(); time.sleep(1)
        PAGER["enabled"] = True
        PAGER["thread"] = threading.Thread(target=pager_loop, daemon=True); PAGER["thread"].start()
    elif not on and PAGER["enabled"]:
        PAGER["enabled"] = False; _pager_kill()
        t = PAGER["thread"]; was = PAGER["was_op25"]; sid = PAGER["saved_sys"]
        def _resume():
            if t: t.join(timeout=PAGER["interval"] + 25)     # let the in-flight bladeRF capture finish
            if was and sid: op25_start()
        threading.Thread(target=_resume, daemon=True).start()

_pager_load()

@app.route("/api/pager")
def api_pager_get():
    return jsonify({"enabled": PAGER["enabled"], "freq": PAGER["freq"], "interval": PAGER["interval"],
                    "capcodes": PAGER["capcodes"], "status": PAGER["status"], "alert_seq": PAGER["alert_seq"],
                    "seen": PAGER["seen"], "last": PAGER["last"],
                    "alerts": list(PAGER["alerts"])[:60], "pages": list(PAGER["pages"])})   # all 200: "hide tone-only" filters client-side

@app.route("/api/pager/config", methods=["POST"])
def api_pager_config():
    d = request.get_json(force=True) or {}
    if "freq" in d:
        try:
            f = float(d["freq"])
            if f != PAGER["freq"]: PAGER["freq"] = f; _pager_kill()   # streaming pager retunes
        except (TypeError, ValueError): pass
    if "interval" in d:
        try: PAGER["interval"] = max(5, min(40, int(d["interval"])))
        except (TypeError, ValueError): pass
    if isinstance(d.get("capcodes"), list):
        cc = []
        for c in d["capcodes"]:
            try: code = int(c["code"])
            except (TypeError, ValueError, KeyError): continue
            cc.append({"code": code, "label": str(c.get("label", "")).strip()[:60]})
        PAGER["capcodes"] = cc
    _pager_save()
    return jsonify({"ok": True, "freq": PAGER["freq"], "interval": PAGER["interval"], "capcodes": PAGER["capcodes"]})

@app.route("/api/pager/enable", methods=["POST"])
def api_pager_enable():
    on = bool((request.get_json(force=True) or {}).get("on"))
    if on and not PAGER["enabled"]:
        if signalid is None:
            return jsonify({"ok": False, "err": "signalid module unavailable"}), 503
        if SCAN["running"] or PROBE_DATA["running"] or CAL["running"] or POCSAG_MON["running"]:
            return jsonify({"ok": False, "err": "scanner/probe/calibration busy"}), 409
    pager_set_enabled(on)
    return jsonify({"ok": True, "enabled": PAGER["enabled"]})

# ---- P25 site emulator (configurable control channel) -------------------------------
# Validate = build CC symbols + decode them through a stock OP25 (no SDR) to confirm what
# a receiver will see. Transmit = dv_tx -> bladeRF (Faraday cage / dummy load ONLY; pauses
# trunking, gated behind an explicit cage confirmation).
EMU_DIR = os.path.normpath(os.path.join(HERE, "..", "emulator"))
sys.path.insert(0, EMU_DIR)
try:
    import p25_cc as emu_p25
    import validate as emu_validate
except Exception:
    emu_p25 = None; emu_validate = None

EMU_FIELDS = ("nac", "wacn", "system_id", "rfss", "site_id", "cc_freq", "vc_freqs",
              "spacing_hz", "bw_hz", "tx_offset_hz")
EMU = {"config": dict(emu_p25.DEFAULTS) if emu_p25 else {}, "validating": False,
       "result": None, "tx": False, "tx_proc": None, "status": "idle",
       "tx_gains": "", "tx_if_rate": 1000000, "was_op25": False, "saved_sys": None}

_EMU_NET = re.compile(r"net_sts_bcst: wacn: ([0-9a-f]+) syid: ([0-9a-f]+)")
_EMU_RFSS = re.compile(r"rfss_sts_bcst: syid: ([0-9a-f]+) rfid: (\d+) stid: (\d+)")
_EMU_IDEN = re.compile(r"iden_up id: (\d+).*?freq: ([\d.]+)")
_EMU_NAC = re.compile(r"NAC 0x([0-9a-f]+) TSBK")

def _emu_load():
    if not emu_p25: return
    saved = db.get_meta("emulator", {}) or {}
    cfg = dict(emu_p25.DEFAULTS)
    for k in EMU_FIELDS:
        if k in saved: cfg[k] = saved[k]
    EMU["config"] = cfg
    EMU["tx_gains"] = saved.get("tx_gains", "")
    EMU["tx_if_rate"] = int(saved.get("tx_if_rate", 1000000))

def _emu_save():
    d = {k: EMU["config"].get(k) for k in EMU_FIELDS}
    d["tx_gains"] = EMU["tx_gains"]; d["tx_if_rate"] = EMU["tx_if_rate"]
    db.set_meta("emulator", d)

_emu_load()

def emu_validate_run():
    EMU.update({"validating": True, "result": None, "status": "building CC symbols..."})
    try:
        cfgpath, c = emu_validate.build_symbols(EMU["config"], seconds=6.0)
        EMU["status"] = "decoding through OP25 (no RF)..."
        runner = os.path.join(EMU_DIR, "run_op25.py")
        p = subprocess.Popen([OP25_PY, runner, cfgpath, APPS],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        lines = []; t0 = time.time()
        try:
            for line in p.stdout:
                lines.append(line)
                if time.time() - t0 > 18: break
        except Exception:
            pass
        try: p.terminate()
        except Exception: pass
        text = "".join(lines)
        res = {"sync": "sync_established" in text, "ts": time.strftime("%H:%M:%S")}
        m = _EMU_NAC.search(text);  res["nac"] = m.group(1) if m else None
        m = _EMU_NET.search(text)
        res["wacn"] = m.group(1) if m else None
        res["system_id"] = m.group(2) if m else None
        m = _EMU_RFSS.search(text)
        res["rfss"] = int(m.group(2)) if m else None
        res["site_id"] = int(m.group(3)) if m else None
        m = _EMU_IDEN.search(text); res["iden_freq"] = m.group(2) if m else None
        res["decoded"] = bool(res["nac"] and res["wacn"])
        EMU["result"] = res
        EMU["status"] = "validated OK" if res["decoded"] else "decode failed (see config)"
    except Exception as e:
        EMU["result"] = {"decoded": False, "error": str(e)}
        EMU["status"] = "error: %s" % e
    finally:
        EMU["validating"] = False

def emu_tx_set(on):
    if on and not EMU["tx"] and not DEV["can_tx"]:
        EMU["status"] = "TX needs the bladeRF; this rig (SDR_KIND=%s) is receive-only" % SDR_KIND; return
    if on and not EMU["tx"]:
        syms, c = emu_p25.generate(EMU["config"], seconds=4.0)
        symfile = os.path.join(TMP, "emu_tx.bin")
        emu_p25.write_symbol_file(syms, symfile)
        EMU["was_op25"] = OP25["running"]; EMU["saved_sys"] = SYSTEMS.get("active")
        op25_stop(); time.sleep(1)
        freq = int(c["cc_freq"])
        cmd = [OP25_PY, "tx/dv_tx.py", "-p", "p25", "-t", symfile, "-r",
               "-a", "bladerf=0", "-Q", str(freq), "-i", str(int(EMU["tx_if_rate"])), "-q", "0"]
        if EMU["tx_gains"]:
            cmd += ["-N", EMU["tx_gains"]]
        try:
            p = subprocess.Popen(cmd, cwd=APPS, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            EMU["tx_proc"] = p; EMU["tx"] = True
            EMU["status"] = "TRANSMITTING %.5f MHz — CAGE ONLY" % (freq / 1e6)
        except Exception as e:
            EMU["status"] = "tx start failed: %s" % e
            if EMU["was_op25"] and EMU["saved_sys"]: op25_start()
    elif not on and EMU["tx"]:
        p = EMU["tx_proc"]
        if p and p.poll() is None:
            try: p.terminate(); p.wait(timeout=5)
            except Exception:
                try: p.kill()
                except Exception: pass
        EMU["tx_proc"] = None; EMU["tx"] = False
        EMU["status"] = "tx stopped"
        if EMU["was_op25"] and EMU["saved_sys"]: op25_start()

@app.route("/api/emulator")
def api_emulator():
    return jsonify({"available": emu_p25 is not None, "config": EMU["config"],
                    "tx_gains": EMU["tx_gains"], "tx_if_rate": EMU["tx_if_rate"],
                    "validating": EMU["validating"], "result": EMU["result"],
                    "tx": EMU["tx"], "status": EMU["status"]})

@app.route("/api/emulator/config", methods=["POST"])
def api_emulator_config():
    if not emu_p25: return jsonify({"ok": False, "err": "emulator module unavailable"}), 503
    d = request.get_json(force=True) or {}
    for k in ("nac", "wacn", "system_id", "rfss", "site_id", "cc_freq", "spacing_hz", "bw_hz", "tx_offset_hz"):
        if k in d:
            try: EMU["config"][k] = int(d[k])
            except (TypeError, ValueError): pass
    if "vc_freqs" in d and isinstance(d["vc_freqs"], list):
        EMU["config"]["vc_freqs"] = [int(x) for x in d["vc_freqs"] if str(x).strip()]
    if "tx_gains" in d: EMU["tx_gains"] = str(d["tx_gains"]).strip()[:80]
    if "tx_if_rate" in d:
        try: EMU["tx_if_rate"] = max(400000, min(4000000, int(d["tx_if_rate"])))
        except (TypeError, ValueError): pass
    _emu_save()
    return jsonify({"ok": True, "config": EMU["config"]})

@app.route("/api/emulator/validate", methods=["POST"])
def api_emulator_validate():
    if not emu_validate: return jsonify({"ok": False, "err": "emulator module unavailable"}), 503
    if EMU["validating"]: return jsonify({"ok": False, "err": "already validating"}), 409
    if EMU["tx"]: return jsonify({"ok": False, "err": "stop transmit first"}), 409
    threading.Thread(target=emu_validate_run, daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/emulator/tx", methods=["POST"])
def api_emulator_tx():
    if not emu_p25: return jsonify({"ok": False, "err": "emulator module unavailable"}), 503
    d = request.get_json(force=True) or {}
    on = bool(d.get("on"))
    if on:
        if not d.get("confirm_cage"):
            return jsonify({"ok": False, "err": "cage confirmation required"}), 400
        if SCAN["running"] or PROBE_DATA["running"] or CAL["running"] or PAGER["enabled"]:
            return jsonify({"ok": False, "err": "scanner/probe/calibration/pager busy"}), 409
    emu_tx_set(on)
    return jsonify({"ok": True, "tx": EMU["tx"], "status": EMU["status"]})

@app.route("/api/control", methods=["POST"])
def api_control():
    a = request.get_json(force=True).get("action")
    if a in ("start", "restart") and (PAGER["enabled"] or EMU["tx"]):
        return jsonify({"ok": False, "err": "pager/emulator owns the bladeRF — disable it first"}), 409
    if a == "stop": op25_stop()
    elif a in ("start", "restart"): op25_start()
    return jsonify({"ok": True, "running": OP25["running"]})

@app.route("/api/tag", methods=["POST"])
def api_tag():
    d = request.get_json(force=True); t = d.get("type"); ident = str(d.get("id")); name = (d.get("name") or "").strip()
    if t == "tg":
        if name: TAGS["talkgroups"][ident] = name
        else: TAGS["talkgroups"].pop(ident, None)
        db.set_talkgroup(ident, name)
    elif t == "rid":
        if name: RIDS[ident] = name
        else: RIDS.pop(ident, None)
        db.set_rid(ident, name)
    else:
        return jsonify({"ok": False}), 400
    return jsonify({"ok": True})

@app.route("/api/import", methods=["POST"])
def api_import():
    d = request.get_json(force=True)
    ic = d.get("id_col"); nc = d.get("name_col")
    added = import_tags(d.get("text", ""), d.get("type", "tg"),
                        int(ic) if ic not in (None, "") else None,
                        int(nc) if nc not in (None, "") else None)
    return jsonify({"ok": True, "added": added})

@app.route("/api/watch", methods=["POST"])
def api_watch():
    d = request.get_json(force=True); rid = str(d.get("rid", "")).strip()
    if not rid.isdigit(): return jsonify({"ok": False}), 400
    if d.get("on", True): WATCH.add(rid)
    else: WATCH.discard(rid); WKEEP.pop(rid, None); WEV.pop(rid, None)
    db.set_meta("watched_rids", sorted(WATCH))   # persist watch list across restarts
    return jsonify({"ok": True, "watch": sorted(WATCH)})

@app.route("/api/watch/clear", methods=["POST"])
def api_watch_clear():
    """Clear the kept watch-card data (one radio, or all) without un-watching."""
    rid = str((request.get_json(force=True, silent=True) or {}).get("rid", "")).strip()
    with LOCK:
        for k in ([rid] if rid else list(WKEEP) + list(WEV)):
            WKEEP.pop(k, None); WEV.pop(k, None)
            if k in STATE["radios"]: STATE["radios"][k].update(count=0, reg_fails=0)
            SIGQUAL.pop(k, None)
    return jsonify({"ok": True})

# ---- radio diagnostics log + watch-session reports -------------------------------------------------
import csv as _csv, html as _html
REPORT_DIR = os.environ.get("LG_REPORT_DIR") or os.path.expanduser("~/lg-reports")
DIAG_COLS = ["time", "kind", "rid", "alias", "tg", "tgtag", "site", "rfss", "nac", "freq", "rf", "ber", "voice_err",
             "encrypted", "emergency", "lat", "lon", "detail"]

def _here():
    L = (globals().get("SURV") or {}).get("last") or {}      # latest GPS sample from the survey sampler
    return L.get("lat"), L.get("lon")

def diag_row(kind, **f):
    if not DIAGLOG["on"] or not DIAGLOG["file"]: return
    lat, lon = _here(); sd = STATE["system"]
    row = {"time": time.strftime("%Y-%m-%d %H:%M:%S"), "kind": kind, "site": sd.get("site"), "rfss": sd.get("rfss"),
           "nac": sd.get("nac"), "lat": lat, "lon": lon, **f}
    if row.get("rid") not in (None, ""): row["alias"] = rid_alias(row["rid"])
    if row.get("tg") not in (None, ""): row["tgtag"] = tg_tag(row["tg"])
    try:
        new = not os.path.exists(DIAGLOG["file"])
        with open(DIAGLOG["file"], "a", newline="") as fh:
            w = _csv.DictWriter(fh, DIAG_COLS, extrasaction="ignore")
            if new: w.writeheader()
            w.writerow(row)
        DIAGLOG["rows"] += 1
    except Exception as e:
        DIAGLOG.update(on=False, error=str(e))

def wsess_add(ev):
    lat, lon = _here(); sd = STATE["system"]
    WSESS["rids"].add(str(ev.get("rid")))
    WSESS["events"].append({**ev, "wall": time.strftime("%Y-%m-%d %H:%M:%S"), "site": sd.get("site"),
                            "rfss": sd.get("rfss"), "nac": sd.get("nac"), "lat": lat, "lon": lon})

def radio_table():
    """Every radio seen: control-channel activity + uplink sig-qual (site-measured) + our voice error rate."""
    rows = []
    for rid in set(STATE["radios"]) | set(SIGQUAL) | set(VOICEQ):
        r = STATE["radios"].get(rid, {}); q = SIGQUAL.get(rid, {}); v = VOICEQ.get(rid, {})
        tg = r.get("last_tg", v.get("tg"))
        rows.append({"rid": int(rid), "alias": rid_alias(rid), "last": r.get("last") or q.get("last") or v.get("last"),
                     "last_action": r.get("last_action"), "count": r.get("count", 0), "last_tg": tg,
                     "tgtag": tg_tag(tg) if tg is not None else "", "site": r.get("site"), "rfss": r.get("rfss"),
                     "rf": q.get("rf"), "rf_min": q.get("rf_min") if q else None, "ber": q.get("ber"),
                     "ber_max": q.get("ber_max") if q else None, "sq_count": q.get("count", 0),
                     "voice_err": v.get("er"), "voice_err_max": v.get("er_max") if v else None, "voice_calls": v.get("calls", 0),
                     "watched": rid in WATCH})
    rows.sort(key=lambda x: x["last"] or "", reverse=True)
    return rows

def _stat(xs, f=max):
    return f(xs) if xs else None

def wsess_report():
    """Build the watch-session report (HTML + CSV) and return the HTML file name."""
    os.makedirs(REPORT_DIR, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S"); base = os.path.join(REPORT_DIR, "watch-" + stamp)
    evs = list(WSESS["events"]); end = time.strftime("%Y-%m-%d %H:%M:%S")
    with SYSLOCK: sname = SYSTEMS["systems"].get(SYSTEMS.get("active"), {}).get("name", "")
    cols = ["wall", "kind", "rid", "ridalias", "tg", "tgtag", "freq", "encrypted", "emergency", "granted", "kind2", "reason", "site", "rfss", "nac", "lat", "lon"]
    with open(base + ".csv", "w", newline="") as fh:
        w = _csv.DictWriter(fh, cols, extrasaction="ignore"); w.writeheader(); [w.writerow(e) for e in evs]
    E = _html.escape; out = []
    out.append("<!doctype html><html><head><meta charset='utf-8'><title>Watch report %s</title><style>"
               "body{font:13px/1.4 system-ui,sans-serif;margin:24px;color:#111}h1{font-size:20px;margin:0 0 4px}h2{font-size:16px;margin:22px 0 6px;border-bottom:2px solid #333}"
               "table{border-collapse:collapse;margin:6px 0 10px;width:100%%}td,th{border:1px solid #bbb;padding:3px 6px;text-align:left;vertical-align:top}th{background:#eee}"
               ".k{color:#555;width:220px}.sum td{width:auto}.mut{color:#777}@media print{h2{page-break-before:auto}}</style></head><body>" % stamp)
    out.append("<h1>Radio watch report</h1><div class='mut'>System: <b>%s</b> &middot; %s &rarr; %s &middot; %d events &middot; generated by SDR Looking Glass</div>"
               % (E(sname), E(WSESS["start"] or ""), E(end), len(evs)))
    watched = sorted(set(str(x) for x in WATCH) | WSESS["rids"], key=lambda x: int(x) if x.isdigit() else 0)
    out.append("<h2>Summary</h2><table class='sum'><tr><th>RID</th><th>Alias</th><th>Heard</th><th>First</th><th>Last</th><th>Grants</th><th>Talkgroups</th>"
               "<th>Sites</th><th>Emerg</th><th>Enc calls</th><th>Uplink RF (min)</th><th>Uplink BER (max)</th><th>Voice err (max)</th></tr>")
    per = {}
    for rid in watched:
        re_ = [e for e in evs if str(e.get("rid")) == rid]
        grants = [e for e in re_ if e["kind"] == "GRANT"]
        tgs = collections.Counter(e.get("tg") for e in re_ if e.get("tg") is not None)
        sites = sorted({"%s-%s" % (e.get("rfss"), e.get("site")) for e in re_ if e.get("site") is not None})
        sq = WSESS["sq"].get(rid, []); vq = WSESS["vq"].get(rid, [])
        per[rid] = dict(evs=re_, grants=grants, tgs=tgs, sites=sites, sq=sq, vq=vq)
        out.append("<tr><td><b>%s</b></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%s</td><td>%s</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
            rid, E(rid_alias(rid)), "yes" if re_ else "<span class='mut'>not heard</span>",
            E(re_[0]["wall"][11:]) if re_ else "", E(re_[-1]["wall"][11:]) if re_ else "", len(grants),
            ", ".join("%s &times;%d" % (E("%s %s" % (t, tg_tag(t)) if tg_tag(t) else str(t)), n) for t, n in tgs.most_common(6)),
            E(", ".join(sites)), sum(1 for e in grants if e.get("emergency")), sum(1 for e in grants if e.get("encrypted")),
            _stat([x[0] for x in sq], min) if sq else "&mdash;", _stat([x[1] for x in sq]) if sq else "&mdash;",
            "%.4f" % max(vq) if vq else "&mdash;"))
    out.append("</table><div class='mut'>Uplink RF/BER are the codes the <i>site</i> reports for the radio (P25 Phase 2 power-control signal quality; "
               "higher RF = stronger, higher BER = worse). Voice err is the vocoder error rate as this receiver decoded that radio's calls.</div>")
    for rid in watched:
        p = per[rid]
        out.append("<h2>%s %s</h2>" % (rid, E(rid_alias(rid))))
        if not p["evs"]:
            out.append("<div class='mut'>Not heard during this session.</div>"); continue
        kinds = collections.Counter(e["kind"] for e in p["evs"])
        out.append("<table><tr><td class='k'>Activity</td><td>%s</td></tr>" % E(", ".join("%s %d" % kv for kv in kinds.most_common())))
        out.append("<tr><td class='k'>Talkgroups</td><td>%s</td></tr>" % E(", ".join("%s %s (%d)" % (t, tg_tag(t), n) for t, n in p["tgs"].most_common())))
        out.append("<tr><td class='k'>Sites (RFSS-site)</td><td>%s</td></tr>" % E(", ".join(p["sites"]) or "-"))
        if p["sq"]:
            out.append("<tr><td class='k'>Uplink sig-qual (site-measured)</td><td>%d samples &middot; RF last %s / min %s &middot; BER last %s / max %s</td></tr>"
                       % (len(p["sq"]), p["sq"][-1][0], min(x[0] for x in p["sq"]), p["sq"][-1][1], max(x[1] for x in p["sq"])))
        if p["vq"]:
            out.append("<tr><td class='k'>Voice error rate (as received)</td><td>%d samples &middot; avg %.4f / max %.4f</td></tr>"
                       % (len(p["vq"]), sum(p["vq"]) / len(p["vq"]), max(p["vq"])))
        out.append("</table><table><tr><th>Time</th><th>Event</th><th>TG</th><th>Freq</th><th>Flags</th><th>Site</th><th>Position</th></tr>")
        for e in p["evs"][-300:]:
            flags = " ".join(x for x in (("EMERG" if e.get("emergency") else ""), ("ENC" if e.get("encrypted") else ""),
                                         ("denied" if e.get("granted") is False else ""), e.get("kind2") or "", e.get("reason") or "") if x)
            pos = "%.5f, %.5f" % (e["lat"], e["lon"]) if e.get("lat") is not None else ""
            out.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                E(e["wall"][11:]), E(e["kind"]), E("%s %s" % (e.get("tg") or "", e.get("tgtag") or "")), E(str(e.get("freq") or "")),
                E(flags), E("%s-%s" % (e.get("rfss"), e.get("site")) if e.get("site") is not None else ""), E(pos)))
        out.append("</table>")
    out.append("</body></html>")
    with open(base + ".html", "w", encoding="utf-8") as fh: fh.write("".join(out))
    return os.path.basename(base + ".html")

def _reports():
    os.makedirs(REPORT_DIR, exist_ok=True)
    fs = sorted((f for f in os.listdir(REPORT_DIR) if f.endswith((".html", ".csv"))), reverse=True)[:30]
    return [{"name": f, "kb": round(os.path.getsize(os.path.join(REPORT_DIR, f)) / 1024, 1)} for f in fs]

@app.route("/api/radiodiag")
def api_radiodiag():
    with LOCK: rows = radio_table()
    return jsonify({"radios": rows[:300], "n": len(rows), "reports": _reports(), "dir": REPORT_DIR,
                    "diaglog": {"on": DIAGLOG["on"], "file": os.path.basename(DIAGLOG["file"] or ""), "rows": DIAGLOG["rows"], "error": DIAGLOG["error"]},
                    "wsess": {"on": WSESS["on"], "start": WSESS["start"], "events": len(WSESS["events"]), "rids": sorted(str(x) for x in WATCH)}})

@app.route("/api/radiodiag.csv")
def api_radiodiag_csv():
    with LOCK: rows = radio_table()
    buf = __import__("io").StringIO(); w = _csv.DictWriter(buf, list(rows[0].keys()) if rows else ["rid"]); w.writeheader(); [w.writerow(r) for r in rows]
    return app.response_class(buf.getvalue(), mimetype="text/csv",
                              headers={"Content-Disposition": "attachment; filename=radios-%s.csv" % time.strftime("%Y%m%d-%H%M%S")})

@app.route("/api/diaglog/<act>", methods=["POST"])
def api_diaglog(act):
    if act == "start":
        os.makedirs(REPORT_DIR, exist_ok=True)
        DIAGLOG.update(on=True, rows=0, error=None, file=os.path.join(REPORT_DIR, "diag-%s.csv" % time.strftime("%Y%m%d-%H%M%S")))
    else:
        DIAGLOG["on"] = False
    return jsonify({"ok": True, "on": DIAGLOG["on"], "file": os.path.basename(DIAGLOG["file"] or "")})

@app.route("/api/watchsess/<act>", methods=["POST"])
def api_watchsess(act):
    if act == "start":
        if not WATCH: return jsonify({"ok": False, "err": "add at least one radio ID to Watch first"}), 400
        WSESS.update(on=True, start=time.strftime("%Y-%m-%d %H:%M:%S"), rids=set(), events=[], sq={}, vq={})
        return jsonify({"ok": True, "msg": "watch session started for %d radio(s)" % len(WATCH)})
    if not WSESS["on"]: return jsonify({"ok": False, "err": "no watch session running"}), 400
    WSESS["on"] = False
    with LOCK: name = wsess_report()
    return jsonify({"ok": True, "report": name, "msg": "report saved: " + name})

@app.route("/api/report/<name>")
def api_report(name):
    name = os.path.basename(name)
    if not name.endswith((".html", ".csv")): return jsonify({"err": "not found"}), 404
    return send_from_directory(REPORT_DIR, name, as_attachment=name.endswith(".csv"))

# ---- drive survey: GPS-tagged control-channel health, sampled every 2 s (always, for the live tiles);
# recorded to CSV only while a survey is running. KML export colours points by decode rate.
SURVEY_DIR = os.environ.get("LG_SURVEY_DIR") or os.path.expanduser("~/lg-survey")
SURVEY_EVERY = 2.0
SURV_COLS = ["time", "lat", "lon", "alt_m", "gps_fix", "sats_used", "sats", "system", "wacn", "sysid", "rfss", "site",
             "nac", "cc_mhz", "tsbk_per_s", "grants", "cc_timeouts", "sync_losses", "freq_err_hz", "ppm", "decoding", "cc_only", "rssi_dbfs"]
SURV = {"on": False, "label": "", "file": None, "rows": 0, "recent": collections.deque(maxlen=40), "last": None}

def gps_poll(timeout=1.5):
    """One quick gpsd read (TPV + SKY); {} when gpsd/GPS isn't there."""
    import socket
    v = {}
    try:
        with socket.create_connection(("127.0.0.1", 2947), timeout=1.0) as s:
            s.sendall(b'?WATCH={"enable":true,"json":true}\n'); s.settimeout(timeout); buf = b""; t0 = time.time()
            while time.time() - t0 < timeout and not ("lat" in v and "sats" in v):
                try: chunk = s.recv(4096)
                except socket.timeout: break
                if not chunk: break
                buf += chunk; *lines, buf = buf.split(b"\n")
                for ln in lines:
                    try: m = json.loads(ln)
                    except Exception: continue
                    if m.get("class") == "TPV":
                        v["mode"] = max(v.get("mode", 0), m.get("mode", 0)); v.update({k: m[k] for k in ("lat", "lon", "alt") if k in m})
                    elif m.get("class") == "SKY" and "satellites" in m:
                        v["sats"] = len(m["satellites"]); v["used"] = sum(1 for x in m["satellites"] if x.get("used"))
    except Exception:
        pass
    return v

def _survey_loop():
    prev = None
    while True:
        t = time.time()
        with LOCK: c = dict(STATE["counters"]); sd = dict(STATE["system"])
        g = gps_poll(); SURV["gpsd"] = bool(g)
        if prev:
            dt = max(t - prev[0], 0.1); d = lambda k: max(0, c.get(k, 0) - prev[1].get(k, 0))   # counters reset on OP25 restart
            fix = g.get("mode", 0) >= 2; rate = round(d("tsbks") / dt, 1)
            with SYSLOCK: sname = SYSTEMS["systems"].get(SYSTEMS.get("active"), {}).get("name", "")
            row = {"time": time.strftime("%Y-%m-%d %H:%M:%S"), "lat": round(g["lat"], 6) if fix and "lat" in g else None,
                   "lon": round(g["lon"], 6) if fix and "lon" in g else None, "alt_m": round(g["alt"]) if fix and "alt" in g else None,
                   "gps_fix": {0: "none", 1: "none", 2: "2D", 3: "3D"}.get(g.get("mode", 0), "none"), "sats_used": g.get("used"),
                   "sats": g.get("sats"), "system": sname, "wacn": sd.get("wacn"), "sysid": sd.get("sysid"), "rfss": sd.get("rfss"),
                   "site": sd.get("site"), "nac": sd.get("nac"), "cc_mhz": sd.get("cc"), "tsbk_per_s": rate, "grants": d("grants"),
                   "cc_timeouts": d("cc_timeouts"), "sync_losses": d("sync_losses"), "freq_err_hz": DIAG.get("error_hz"),
                   "ppm": DIAG.get("ppm"), "decoding": 1 if rate > 0 else 0,
                   "cc_only": int(bool(CC_ONLY[0])), "rssi_dbfs": DIAG.get("level_db")}   # 0 = receiver also leaves the CC to follow voice -> TSBK/s dips are expected
            SURV["last"] = row
            if SURV["on"] and SURV["file"]:
                new = not os.path.exists(SURV["file"])
                try:
                    with open(SURV["file"], "a", newline="") as f:
                        w = __import__("csv").DictWriter(f, SURV_COLS)
                        if new: w.writeheader()
                        w.writerow(row)
                    SURV["rows"] += 1; SURV["recent"].append(row)
                except Exception as e:
                    SURV["on"] = False; SURV["error"] = str(e)
        prev = (t, c)
        time.sleep(max(0.2, SURVEY_EVERY - (time.time() - t)))

threading.Thread(target=_survey_loop, daemon=True).start()

def survey_kml(csv_path):
    import csv, html
    rows = [r for r in csv.DictReader(open(csv_path)) if r.get("lat") and r.get("lon")]
    col = lambda r: "good" if float(r["tsbk_per_s"] or 0) >= 20 else "fair" if float(r["tsbk_per_s"] or 0) >= 5 else "poor" if float(r["tsbk_per_s"] or 0) > 0 else "none"
    styles = {"good": "ff00c000", "fair": "ff00ffff", "poor": "ff0080ff", "none": "ff0000ff"}   # KML colours are aabbggrr
    out = ['<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://www.opengis.net/kml/2.2"><Document><name>%s</name>' % html.escape(os.path.basename(csv_path))]
    out += ['<Style id="%s"><IconStyle><color>%s</color><scale>0.6</scale><Icon><href>http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png</href></Icon></IconStyle></Style>' % kv for kv in styles.items()]
    for r in rows:
        desc = "".join("<b>%s</b>: %s<br/>" % (k, html.escape(str(r.get(k, "")))) for k in SURV_COLS)
        out.append('<Placemark><name>%s/s</name><styleUrl>#%s</styleUrl><description><![CDATA[%s]]></description><Point><coordinates>%s,%s,0</coordinates></Point></Placemark>'
                   % (r["tsbk_per_s"], col(r), desc, r["lon"], r["lat"]))
    out.append("</Document></kml>")
    return "".join(out)

@app.route("/api/rssi")
def api_rssi():
    """Fast poll for the antenna/RSSI meter: channel power of OP25's channel 0 (dBFS, relative at fixed gain)."""
    with LOCK:
        h = list(DIAG["level_hist"]); lv = DIAG["level_db"]; f = DIAG.get("freq")
    with SYSLOCK: gains = (SYSTEMS["systems"].get(SYSTEMS.get("active"), {}).get("sdr") or {}).get("gains")
    return jsonify({"level": lv, "freq": f, "hist": h[-120:], "cc_only": CC_ONLY[0], "gains": gains,
                    "running": OP25["running"], "supported": bool(h) or lv is not None})

@app.route("/api/survey")
def api_survey():
    os.makedirs(SURVEY_DIR, exist_ok=True)
    files = sorted((f for f in os.listdir(SURVEY_DIR) if f.endswith(".csv")), reverse=True)[:15]
    return jsonify({"on": SURV["on"], "label": SURV["label"], "file": os.path.basename(SURV["file"] or ""), "rows": SURV["rows"],
                    "error": SURV.get("error"), "last": SURV["last"], "recent": list(SURV["recent"])[-15:][::-1],
                    "files": [{"name": f, "kb": round(os.path.getsize(os.path.join(SURVEY_DIR, f)) / 1024, 1)} for f in files],
                    "dir": SURVEY_DIR})

@app.route("/api/survey/start", methods=["POST"])
def api_survey_start():
    label = re.sub(r"[^\w.-]+", "_", str((request.get_json(force=True) or {}).get("label", "")).strip())[:40]
    os.makedirs(SURVEY_DIR, exist_ok=True)
    SURV.update(on=True, label=label, rows=0, error=None, recent=collections.deque(maxlen=40),
                file=os.path.join(SURVEY_DIR, time.strftime("%Y%m%d-%H%M%S") + ("-" + label if label else "") + ".csv"))
    return jsonify({"ok": True, "file": os.path.basename(SURV["file"])})

@app.route("/api/survey/stop", methods=["POST"])
def api_survey_stop():
    SURV["on"] = False
    return jsonify({"ok": True, "rows": SURV["rows"]})

@app.route("/api/survey/file/<name>")
def api_survey_file(name):
    name = os.path.basename(name)
    if name.endswith(".kml"):
        src = os.path.join(SURVEY_DIR, name[:-4] + ".csv")
        if not os.path.isfile(src): return jsonify({"err": "not found"}), 404
        return app.response_class(survey_kml(src), mimetype="application/vnd.google-earth.kml+xml",
                                  headers={"Content-Disposition": "attachment; filename=" + name})
    if not name.endswith(".csv"): return jsonify({"err": "not found"}), 404
    return send_from_directory(SURVEY_DIR, name, as_attachment=True)

@app.route("/api/keys", methods=["GET", "POST"])
def api_keys():
    """List stored keys (metadata only) or add/replace one: {keyid hex, algid, key hex, label}."""
    if request.method == "GET":
        out = [{"keyid": k, "algid": v.get("algid"), "alg": CRYPT_ALGS.get(int(v.get("algid", "0"), 16), ("?",))[0],
                "label": v.get("label", ""), "tail": "".join("%02x" % int(b, 16) for b in v.get("key", [])[-2:])}
               for k, v in sorted(keys_load().items())]
        return jsonify({"keys": out, "loaded": STATE.get("keys_loaded", []), "path": KEYS_PATH})
    d = request.get_json(force=True) or {}
    try:
        kid = int(str(d.get("keyid", "")).lower().replace("0x", "").strip(), 16); alg = int(str(d.get("algid", "")), 16)
    except ValueError:
        return jsonify({"ok": False, "err": "key ID must be hex (e.g. 1A2B)"}), 400
    if not 0 < kid <= 0xFFFF: return jsonify({"ok": False, "err": "key ID must be 1-FFFF"}), 400
    if alg not in CRYPT_ALGS: return jsonify({"ok": False, "err": "unsupported algorithm"}), 400
    hx = re.sub(r"[\s:,-]|0x", "", str(d.get("key", "")).lower())
    name, nbytes = CRYPT_ALGS[alg]
    if not re.fullmatch(r"[0-9a-f]+", hx or "-") or len(hx) != nbytes * 2:
        return jsonify({"ok": False, "err": "%s key must be %d hex chars (got %d)" % (name, nbytes * 2, len(hx))}), 400
    keys = keys_load()
    keys["0x%04x" % kid] = {"algid": "0x%02x" % alg, "key": ["0x" + hx[i:i + 2] for i in range(0, len(hx), 2)],
                            "label": str(d.get("label", ""))[:40]}
    keys_save(keys); op25_start()                      # multi_rx only reads crypt_keys at start
    return jsonify({"ok": True, "msg": "saved %s key 0x%04x, decoder reloading" % (name, kid)})

@app.route("/api/keys/delete", methods=["POST"])
def api_keys_delete():
    k = str((request.get_json(force=True) or {}).get("keyid", "")).lower()
    keys = keys_load()
    if keys.pop(k, None) is None: return jsonify({"ok": False, "err": "no such key"}), 404
    keys_save(keys); op25_start()
    return jsonify({"ok": True})

@app.route("/api/listen", methods=["POST"])
def api_listen():
    d = request.get_json(force=True)
    tgids = []
    for t in (d.get("tgids") or []):
        if str(t).isdigit() and int(t) not in tgids:
            tgids.append(int(t))
    LISTEN[:] = tgids
    if tgids:
        CC_ONLY[0] = False   # pinning talkgroups implies we want to follow voice
    db.set_meta("pinned_tgs", tgids)   # persist the lock across restarts
    op25_start()   # rebuild config with whitelist and re-acquire
    return jsonify({"ok": True, "listen": list(LISTEN)})

@app.route("/api/ccmode", methods=["POST"])
def api_ccmode():
    CC_ONLY[0] = bool(request.get_json(force=True).get("on"))
    db.set_meta("cc_only", CC_ONLY[0])   # remember the data-focus preference across restarts
    op25_start()   # rebuild config: CC-only stays glued to the control channel (no voice follow)
    return jsonify({"ok": True, "cc_only": CC_ONLY[0]})

@app.route("/api/version")
def api_version():
    return jsonify({"version": __version__})

@app.route("/api/shutdown", methods=["POST"])
def api_shutdown():
    """Graceful stop requested by the launcher when its window closes: kill OP25,
    then exit this process. The launcher still pkills as a belt-and-suspenders fallback."""
    op25_stop()
    def _bye():
        time.sleep(0.3); os._exit(0)
    threading.Thread(target=_bye, daemon=True).start()
    return jsonify({"ok": True})

@app.route("/vendor/<path:fn>")
def vendor(fn):
    return send_from_directory(os.path.join(HERE, "vendor"), fn)

@app.route("/api/positions")
def api_positions():
    with LOCK:
        return jsonify({"positions": list(POSITIONS.values()),
                        "trails": {str(r): list(d) for r, d in POS_TRAILS.items()}})

@app.route("/api/position", methods=["POST"])
def api_position():
    """Ingest a GPS fix from any external decoder (SDRTrunk/DSD bridge) or a manual push."""
    d = request.get_json(force=True) or {}
    try:
        rid = int(d["rid"]); lat = float(d["lat"]); lon = float(d["lon"])
    except Exception:
        return jsonify({"ok": False, "err": "need rid,lat,lon"}), 400
    rec = record_position(rid, lat, lon, tg=d.get("tg"), ts=d.get("ts"),
                          alias=d.get("alias"), heading=d.get("heading"),
                          speed=d.get("speed"), src=d.get("src", "ingest"))
    return jsonify({"ok": bool(rec), "position": rec})

@app.route("/api/sim", methods=["POST"])
def api_sim():
    SIM["on"] = bool(request.get_json(force=True).get("on"))
    if SIM["on"] and (SIM["thread"] is None or not SIM["thread"].is_alive()):
        SIM["thread"] = threading.Thread(target=_sim_loop, daemon=True); SIM["thread"].start()
    return jsonify({"ok": True, "sim": SIM["on"]})

@app.route("/api/probe/data", methods=["POST"])
def api_probe_data():
    if PROBE_DATA["running"]: return jsonify({"ok": False, "err": "probe already running"}), 409
    if SCAN["running"]: return jsonify({"ok": False, "err": "scanner running"}), 409
    d = request.get_json(force=True) or {}
    freq = d.get("freq")
    if not freq:                                 # default: the busiest known data channel
        with LOCK:
            dcs = sorted(STATE["datachans"].values(),
                         key=lambda x: -((x.get("grants") or 0) + (x.get("anns") or 0)))
        cand = next((c for c in dcs if c.get("freq")), None)
        if not cand: return jsonify({"ok": False, "err": "no data channel discovered yet"}), 400
        freq = cand["freq"]
    secs = max(20, min(180, int(d.get("secs", 75))))
    threading.Thread(target=_run_data_probe, args=(float(freq), secs), daemon=True).start()
    return jsonify({"ok": True, "freq": float(freq), "secs": secs})

@app.route("/api/probe/data/status")
def api_probe_data_status():
    return jsonify(PROBE_DATA)

# ---- CC Viewer: a uConsole-sized control-channel viewer (cc.html) on this same backend/radio ----------
# ---- Expert diagnostics (CC Viewer): OP25 receiver lines -> plain English, plus host health. First matching rule wins.
XRULES = [(k, sev, re.compile(rx, re.I), txt) for k, sev, rx, txt in [
    ("cc_timeout", "crit", r"control channel timeout", "Control channel lost: no CC data for several seconds, so OP25 moved on to the next CC in the list"),
    ("sync_timeout", "warn", r"rx_sync::sync_timeout", "Frame sync lost: the demodulator stopped recognising P25 frames (weak signal, fade or interference)"),
    ("nid_err", "warn", r"load_nid\(\) error check failed", "Corrupt frame header (NID) thrown away: too many bit errors on that frame"),
    ("usb", "crit", r"usb_claim_interface|No supported devices|Failed to open rtlsdr|Failed to submit transfer|LIBUSB_ERROR|read_async", "The RTL-SDR could not be opened or a USB transfer failed"),
    ("overflow", "crit", r"^[aOU]+(?=\d|\s*$)|overflow|underrun", "Sample overflow: the CPU fell behind the SDR and samples were dropped"),
    ("no_rx", "info", r"has no idle receivers|needs control channel receiver", "No spare receiver to watch the CC: the only receiver was off following a voice call"),
    ("rs_errs", "info", r"rs_errs=[1-9]", "TDMA burst needed Reed-Solomon error correction (bit errors fixed, signal not perfect)"),
    ("sync_reset", "info", r"rx_sync::sync_reset", "Receiver restarted its frame search (normal after a retune)"),
    ("sync_ok", "ok", r"rx_sync::sync_established", "Locked onto a P25 signal"),
    ("tune", "info", r"Tuning to frequency", "Receiver retuned to a new frequency"),
    ("enc", "info", r"encrypt info:", "Encryption parameters seen on a voice call"),
    ("key", "ok", r"loaded key", "An encryption key from your Keys list was loaded into OP25"),
    ("voice", "info", r"voice update:", "Receiver followed a voice call"),
    # OP25's audio websocket logs "[error] handle_read_frame error: ... End of File" whenever a browser tab closes or reloads
    ("ws_close", "info", r"websocketpp|handle_read_frame", "An audio client (browser tab) disconnected from OP25"),
    ("sw_err", "crit", r"Traceback|Exception|\bError\b|error:", "OP25 reported a software error"),
]]

def xlog_line(line):
    for k, sev, rx, txt in XRULES:
        if rx.search(line):
            ts = time.strftime("%H:%M:%S"); XLOG["counts"][k] = XLOG["counts"].get(k, 0) + 1
            XLOG["last"][k] = ts
            if sev != "info" or k in ("tune", "sync_reset", "no_rx"):
                XLOG["recent"].append({"ts": ts, "key": k, "sev": sev, "text": line.strip()[:160]})
            return

XRULE_TXT = {k: (sev, txt) for k, sev, _, txt in XRULES}
HOST = {"t": 0, "v": {}}

def _sh(cmd):
    try: return subprocess.run(cmd, capture_output=True, text=True, timeout=3).stdout.strip()
    except Exception: return ""

def host_info():
    """Pi/receiver health for the Diagnostics tab, refreshed at most every 5 s."""
    if time.time() - HOST["t"] < 5: return HOST["v"]
    v = {}
    try: v["load"] = [round(x, 2) for x in os.getloadavg()]; v["cpus"] = os.cpu_count()
    except Exception: pass
    try: v["temp_c"] = round(int(open("/sys/class/thermal/thermal_zone0/temp").read()) / 1000, 1)
    except Exception: pass
    t = _sh(["vcgencmd", "get_throttled"])
    if "=" in t:
        try: v["throttled"] = int(t.split("=")[1], 16)
        except Exception: pass
    try:
        mi = dict((l.split(":")[0], int(l.split()[1])) for l in open("/proc/meminfo") if l.split()[1].isdigit())
        v["mem_avail_mb"] = mi.get("MemAvailable", 0) // 1024; v["mem_total_mb"] = mi.get("MemTotal", 0) // 1024
    except Exception: pass
    try:
        st = os.statvfs(os.path.expanduser("~")); v["disk_free_gb"] = round(st.f_bavail * st.f_frsize / 1e9, 1)
    except Exception: pass
    try: v["uptime_s"] = int(float(open("/proc/uptime").read().split()[0]))
    except Exception: pass
    sdr = None
    try:
        import glob
        sdr = any(open(p).read().strip() == "2838" for p in glob.glob("/sys/bus/usb/devices/*/idProduct"))
    except Exception: pass
    v["sdr_usb"] = sdr
    v["ntp_synced"] = {"yes": True, "no": False}.get(_sh(["timedatectl", "show", "-p", "NTPSynchronized", "--value"]))
    v["rtc"] = os.path.exists("/dev/rtc0")
    v["clock"] = time.time()
    g = SURV.get("last") or {}
    v["gps"] = {"gpsd": SURV.get("gpsd"), "fix": g.get("gps_fix"), "sats": g.get("sats"), "used": g.get("sats_used"), "lat": g.get("lat"), "lon": g.get("lon")}
    p = OP25.get("proc")
    v["op25_alive"] = bool(p and p.poll() is None); v["op25_exit"] = p.poll() if p else None
    v["op25_since"] = OP25.get("since")
    if v["op25_alive"]:
        try:
            f = open("/proc/%d/stat" % p.pid).read().split(); clk = os.sysconf("SC_CLK_TCK")
            cpu = (int(f[13]) + int(f[14])) / clk; prev = HOST.get("cpu")
            if prev and prev[0] == p.pid: v["op25_cpu"] = round(100 * (cpu - prev[1]) / max(0.1, time.time() - prev[2]))
            HOST["cpu"] = (p.pid, cpu, time.time())
        except Exception: pass
    HOST.update(t=time.time(), v=v)
    return v

@app.route("/cc")
def cc_page():
    resp = send_from_directory(HERE, "cc.html")
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp

CC_RATE = collections.deque(maxlen=20)
SEEK = {"start": None, "dir": None, "band": None, "center": None, "freq": None, "probed": 0, "msg": "", "result": None}
CC_KINDS = ("GRANT", "AFFIL", "REG", "DEREG", "REGFAIL", "CRYPT", "DATA", "DENY", "QUEUED", "XFN", "ALERT", "EMERG", "STATUS")

def _tsec(ts):
    try: h, m, x = str(ts).split()[-1].split(".")[0].split(":"); return int(h) * 3600 + int(m) * 60 + int(x)
    except Exception: return None

def ev_group(evs, gap=4):
    """Collapse the CC's repeats for the Activity view: a call's grant + its ~1/s grant updates (same TG + MHz, no gap > 4 s,
    same talker) become one row, as do repeated affiliation/registration responses. Adds n / first / last."""
    out, open_ = [], {}
    for e in evs:
        t = _tsec(e.get("ts")); g0 = e.get("kind") == "GRANT"
        k = ("G", e.get("tg"), e.get("freq")) if g0 else (e.get("kind"), e.get("rid"), e.get("src"), e.get("tg"), e.get("reason"),
                                                         e.get("kind2"), e.get("dtype"), e.get("granted"))
        g = open_.get(k)
        if (g is not None and t is not None and 0 <= t - g["_t"] <= gap
                and not (g0 and e.get("rid") and g.get("rid") and e["rid"] != g["rid"])):
            g["n"] += 1; g["_t"] = t; g["last"] = e.get("ts")
            if e.get("rid") and not g.get("rid"): g["rid"] = e["rid"]; g["ridalias"] = e.get("ridalias", "")
            for f in ("watch", "emergency", "encrypted"):
                if e.get(f): g[f] = True
        else:
            g = dict(e, n=1, first=e.get("ts"), last=e.get("ts"), _t=t if t is not None else -99); open_[k] = g; out.append(g)
    for g in out: g.pop("_t", None)
    return out

@app.route("/api/cc")
def api_cc():
    with SYSLOCK:
        active = RUN_TEMP[0] if RUN_TEMP[0] in TEMP_SYS else SYSTEMS.get("active")
        systems = {k: {"name": v["name"], "type": v.get("type"), "ccs": list(v.get("control_channels") or [])[:20],
                       "wacn": v.get("wacn"), "sysid": v.get("sysid"), "nac": v.get("nac"), "temp": k in TEMP_SYS}
                   for k, v in list(SYSTEMS["systems"].items()) + list(TEMP_SYS.items())}
    now = time.time()
    with LOCK:
        c = dict(STATE["counters"]); CC_RATE.append((now, c.get("tsbks", 0)))
        while len(CC_RATE) > 2 and now - CC_RATE[0][0] > 6: CC_RATE.popleft()
        dt = CC_RATE[-1][0] - CC_RATE[0][0]
        rate = round((CC_RATE[-1][1] - CC_RATE[0][1]) / dt, 1) if dt > 1.5 and CC_RATE[-1][1] >= CC_RATE[0][1] else None
        evs = [e for e in EVENTS if e.get("kind") in CC_KINDS]
        evs = evs[-150:] if request.args.get("all") else ev_group(evs)[-150:]
        out = {"system": dict(STATE["system"]), "counters": c, "tsbk_per_s": rate, "cc": cc_snapshot(),
               "bandplan": STATE["bandplan"], "events": evs, "firehose": list(RAW)[-150:],
               "opcodes": dict(sorted(OPCOUNTS.items(), key=lambda x: -x[1])[:60]),
               "radios": radio_table()[:80],
               "talkgroups": sorted(({**t, "tag": tg_tag(t["tgid"])} for t in STATE["talkgroups"].values()),
                                    key=lambda x: x.get("last") or "", reverse=True)[:60],
               "watch": sorted(WATCH), "watch_diag": [watch_diag(int(x)) for x in sorted(WATCH) if str(x).isdigit()],
               "keys_loaded": STATE.get("keys_loaded", [])}
    out.update({"active": active, "systems": systems, "op25": {"running": OP25["running"], "error": OP25["error"]},
                "cc_only": CC_ONLY[0], "pin": CC_PIN["freq"] if CC_PIN["sid"] == active else None,
                "diag": {k: DIAG.get(k) for k in ("error_hz", "ppm", "freq", "locked", "level_db")},
                "wsess": {"on": WSESS["on"], "start": WSESS["start"], "events": len(WSESS["events"])},
                "reports": [r for r in _reports() if r["name"].startswith("watch-")][:12],
                "scan": {"running": SCAN["running"], "phase": SCAN["phase"], "mode": SCAN["mode"], "error": SCAN["error"],
                         "seek": {k: v for k, v in SEEK.items() if k != "result"}},
                "seek_result": SEEK["result"], "hunt": HUNT["found"], "bands": {k: v["label"] for k, v in BANDS.items()},
                "host": host_info(),
                "xlog": {"counts": XLOG["counts"], "last": XLOG["last"], "since": XLOG["since"], "recent": list(XLOG["recent"])[-80:],
                         "rules": XRULE_TXT}})
    return jsonify(out)

def V0(x): return "?" if x is None else x
HUNT = {"found": [], "n": 0}

def _temp_from_res(res, f0, sdr):
    """Park a probe result as an unsaved system (TEMP_SYS) and return its find record. Nothing touches the DB."""
    HUNT["n"] += 1; tid = "_f%d" % HUNT["n"]
    res = dict(res, cc="%.5f" % f0)
    with SYSLOCK:
        match = next((v["name"] for v in SYSTEMS["systems"].values() if str(v.get("wacn") or "").lower() == str(res.get("wacn")).lower()
                      and str(v.get("sysid")) == str(res.get("sysid"))), None)
    name = "NAC %s / SYS %s / site %s-%s" % (res.get("nac"), res.get("sysid"), V0(res.get("rfss")), V0(res.get("site")))
    ccs = list(dict.fromkeys(["%.5f" % f0] + ["%.5f" % x for x in (_parse_freq(y) for y in res.get("secondary_cc", [])) if x]))
    TEMP_SYS[tid] = {"name": "(unsaved) " + name, "type": "P25 Phase II" if res.get("phase2") else "P25", "nac": res.get("nac"),
                     "wacn": res.get("wacn"), "sysid": res.get("sysid"), "control_channels": ccs, "modulation": "cqpsk",
                     "phase2": res.get("phase2", False), "sdr": sdr, "_res": res}
    return {"id": tid, "name": name, "nac": res.get("nac"), "wacn": res.get("wacn"), "sysid": res.get("sysid"), "rfss": res.get("rfss"),
            "site": res.get("site"), "phase2": res.get("phase2"), "freq": f0, "cc": "%.5f" % f0, "match": match, "saved": False,
            "time": time.strftime("%H:%M:%S")}

def _find_mark(tid, **kw):
    for f in HUNT["found"] + ([SEEK["result"]] if SEEK["result"] else []):
        if f.get("id") == tid: f.update(kw)

def cc_seek(direction, fnow, band=None, mode="system", hunt=False):
    """System Hunter. Survey outward from fnow in one direction and probe carriers nearest-first.
    seek: stop at the first P25 CC of a different system (mode "site": any other site) and run it, UNSAVED.
    hunt: keep going to the band edge and list every distinct system/site found (each unsaved).
    Saving is always the user's call (/api/cc/save)."""
    saved = SYSTEMS.get("active"); was = OP25["running"]
    with LOCK:
        sd = STATE["system"]; cur = {"wacn": str(sd.get("wacn") or "").lower(), "sysid": str(sd.get("sysid") or ""),
                                     "nac": str(sd.get("nac") or "").lower(), "rfss": sd.get("rfss"), "site": sd.get("site")}
    with SYSLOCK:
        s = SYSTEMS["systems"].get(saved) or {}; sdr = s.get("sdr") or sdr_defaults()
        run = TEMP_SYS.get(RUN_TEMP[0]) if RUN_TEMP[0] else None
        ref = run or s
        if not cur["wacn"]:                     # not decoding when pressed: identity from the selected system
            cur.update(wacn=str(ref.get("wacn") or "").lower(), sysid=str(ref.get("sysid") or ""), nac=str(ref.get("nac") or "").lower())
        same = lambda v: cur["wacn"] and str(v.get("wacn") or "").lower() == cur["wacn"] and str(v.get("sysid") or "") == cur["sysid"]
        pool = ([v for v in SYSTEMS["systems"].values() if same(v)] if mode == "system" else [s]) + ([run] if run else [])
        mine = [f for v in pool for f in (_parse_freq(x) for x in v.get("control_channels", [])) if f] if not hunt else []
    def key(r):
        w, y = str(r.get("wacn") or "").lower(), str(r.get("sysid") or "")
        if mode == "system": return (w, y)
        return (w, y, str(r.get("nac") or "").lower(), r.get("rfss"), r.get("site"))
    seen = set() if hunt else {key(cur)} if cur["wacn"] else set()
    if band not in BANDS:
        inb = [(d["stop"] - d["start"], b) for b, d in BANDS.items() if d["start"] <= fnow <= d["stop"]]
        band = max(inb)[1] if inb else "800"
    bd = BANDS[band]
    if hunt or not (bd["start"] <= fnow <= bd["stop"]): fnow = bd["start"] if direction == "up" else bd["stop"]
    for k in [k for k in TEMP_SYS if k != RUN_TEMP[0]]: del TEMP_SYS[k]          # drop old unsaved finds
    HUNT["found"] = [f for f in HUNT["found"] if f["id"] == RUN_TEMP[0]] if not hunt else []
    SCAN.update({"running": True, "cancel": False, "phase": "hunt" if hunt else "seek", "dir": direction, "band": band, "found": [],
                 "candidates": [], "mode": "seek", "error": None, "saved_active": saved, "was_running": was})
    SEEK.update(start=round(fnow, 5), dir=direction, band=band, mode=mode, hunt=hunt, center=None, freq=None, probed=0, skipped=0,
                found=0, msg="starting", result=None if not hunt else SEEK.get("result"))
    op25_stop(); time.sleep(1)
    half = SURVEY_USABLE / 2e6; step = (SURVEY_USABLE - 200000) / 1e6     # overlap: the detector drops chunk edges
    sign = 1 if direction == "up" else -1; tried = []; hit = None
    try:
        c = fnow + sign * (half - 0.06)
        while not SCAN["cancel"] and bd["start"] - half < c < bd["stop"] + half:
            SEEK.update(center=round(c, 5), msg="surveying %.5f MHz" % c)
            cands = [x for x in _survey_chunk(int(c * 1e6)) if sign * (x["freq"] - fnow) > 0.006
                     and bd["start"] <= x["freq"] <= bd["stop"] and not any(abs(x["freq"] - f) < 0.006 for f in mine + tried)]
            cands.sort(key=lambda x: abs(x["freq"] - fnow))
            for cand in cands:
                if SCAN["cancel"] or any(abs(cand["freq"] - f) < 0.006 for f in mine): continue
                tried.append(cand["freq"]); SEEK.update(freq=cand["freq"], probed=SEEK["probed"] + 1, msg="probing %.5f MHz" % cand["freq"])
                res = _probe_one(cand["freq"], sdr)
                if not (res.get("present") and res.get("wacn")): continue
                adv = [f for f in (_parse_freq(x) for x in list(res.get("secondary_cc", [])) + list(res.get("neighbors", []))) if f]
                if key(res) in seen:                    # same system/site again: skip it and every CC it advertises
                    SEEK["skipped"] += 1; mine += adv; continue
                seen.add(key(res))
                if mode == "system": mine += adv        # its other sites are the same system
                with LOCK: snap = cc_snapshot()
                rcc = _parse_freq(res.get("cc"))        # the CC's own frequency beats the FFT peak (off-raster = poor decode)
                f0 = rcc if rcc and abs(rcc - cand["freq"]) < 0.02 else cand["freq"]
                rec = _temp_from_res(res, f0, sdr)
                rec.update(power_db=cand.get("power_db"), adjacent=res.get("adjacent"), secondary_cc=res.get("secondary_cc"), ccx=snap)
                if hunt:
                    HUNT["found"].append(rec); SEEK["found"] += 1
                    continue
                hit = rec; break
            if hit: break
            c += sign * step
        if not hunt: SEEK["result"] = hit
        n = len(HUNT["found"]) if hunt else (1 if hit else 0)
        SCAN["phase"] = "cancelled" if SCAN["cancel"] else ("found" if n else "none")
        SEEK["msg"] = ("hunt %s: %d system%s found" % ("stopped" if SCAN["cancel"] else "done", n, "" if n == 1 else "s")) if hunt else (
            ("found %s at %.5f MHz" % (hit["name"], hit["freq"])) if hit else
            ("cancelled" if SCAN["cancel"] else "no other %s %s to the band edge" % (mode, "above" if sign > 0 else "below")))
    except Exception as e:
        SCAN["phase"] = "error"; SCAN["error"] = str(e); SEEK["msg"] = "error: %s" % e
    finally:
        reset_state()
        if hit:                                 # run the find unsaved; the saved active system is left alone
            RUN_TEMP[0] = hit["id"]; LISTEN.clear(); CC_PIN.update(sid=hit["id"], freq="%.5f" % hit["freq"]); CC_ONLY[0] = True
            op25_start()
        elif was:
            if RUN_TEMP[0] not in TEMP_SYS: RUN_TEMP[0] = None
            if RUN_TEMP[0] or saved: op25_start()
        SCAN["running"] = False

@app.route("/api/cc/seek", methods=["POST"])
def api_cc_seek():
    if SCAN["running"]: return jsonify({"ok": False, "err": "a scan is already running"}), 409
    d = request.get_json(force=True); direction = "down" if d.get("dir") == "down" else "up"
    fnow = _parse_freq(d.get("from")) or _parse_freq(STATE["system"].get("cc")) or 0.0
    mode = "site" if d.get("mode") == "site" else "system"
    threading.Thread(target=cc_seek, args=(direction, fnow, d.get("band"), mode, bool(d.get("hunt"))), daemon=True).start()
    return jsonify({"ok": True, "dir": direction, "from": fnow, "hunt": bool(d.get("hunt"))})

@app.route("/api/cc/save", methods=["POST"])
def api_cc_save():
    """Store an unsaved find: merges into the system with the same WACN/SysID (adds the site/CCs), else a new system."""
    tid = (request.get_json(force=True, silent=True) or {}).get("id") or RUN_TEMP[0]
    t = TEMP_SYS.get(tid)
    if not t: return jsonify({"ok": False, "err": "nothing unsaved to store"}), 400
    rec = _ingest_or_merge(dict(t["_res"]))
    if RUN_TEMP[0] == tid:                      # keep decoding, now as the saved system
        with SYSLOCK: SYSTEMS["active"] = rec["id"]; db.sync_all(SYSTEMS)
        CC_PIN.update(sid=rec["id"]); RUN_TEMP[0] = None
    del TEMP_SYS[tid]
    _find_mark(tid, saved=True, saved_as=rec["name"], saved_id=rec["id"], merged=rec["merged"])
    return jsonify({"ok": True, "id": rec["id"], "name": rec["name"], "merged": rec["merged"],
                    "msg": ("site added to " if rec["merged"] else "saved as new system ") + rec["name"]})

@app.route("/api/cc/discard", methods=["POST"])
def api_cc_discard():
    tid = (request.get_json(force=True, silent=True) or {}).get("id") or RUN_TEMP[0]
    TEMP_SYS.pop(tid, None); _find_mark(tid, discarded=True)
    if RUN_TEMP[0] == tid:                      # was listening to it: back to the saved active system
        RUN_TEMP[0] = None; op25_start()
    return jsonify({"ok": True})

@app.route("/api/uicmd", methods=["POST"])
def api_uicmd():
    """Hardware-button hook (uConsole X/Y/A/B via labwc keybinds -> co-lg-cmd): tell open dashboards to
    toggle a UI control. Straight to the SSE subscribers (not EVENTS/the feed). Localhost only."""
    if request.remote_addr not in ("127.0.0.1", "::1"): return jsonify({"ok": False}), 403
    cmd = str((request.get_json(force=True, silent=True) or {}).get("cmd", ""))
    if cmd not in ("uc", "audio", "boost", "cc"): return jsonify({"ok": False, "err": "unknown cmd"}), 400
    for q in list(SUBS):
        try: q.put_nowait({"kind": "UICMD", "cmd": cmd})
        except queue.Full: pass
    return jsonify({"ok": True, "clients": len(SUBS)})

@app.route("/stream")
def stream():
    q = queue.Queue(maxsize=400); SUBS.append(q)
    def gen():
        yield "retry: 2000\n\n"
        try:
            while True:
                try: yield "data: " + json.dumps(q.get(timeout=15)) + "\n\n"
                except queue.Empty: yield ": keepalive\n\n"
        finally:
            try: SUBS.remove(q)
            except ValueError: pass
    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

def _graceful(*_a):
    try: op25_stop()
    except Exception: pass

atexit.register(_graceful)
try:
    signal.signal(signal.SIGTERM, lambda *_a: (_graceful(), os._exit(0)))
except Exception:
    pass

if __name__ == "__main__":
    threading.Thread(target=diag_poll_loop, daemon=True).start()
    op25_start()
    app.run(host="0.0.0.0", port=8080, threaded=True, debug=False)
