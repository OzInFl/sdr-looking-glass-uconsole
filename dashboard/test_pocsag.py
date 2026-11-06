#!/usr/bin/env python3
"""Round-trip self-test for pocsag.py: encode -> synth FM audio -> decode.
Run from the dashboard/ dir:  python test_pocsag.py   (exit 0 = all pass)
Validates timing recovery, auto-baud, BCH, and alpha/numeric decode with no SDR."""
import sys
import numpy as np
import pocsag as P

SYNC = 0x7CD215D8
IDLE = 0x7A89C197


def bch_encode(d21):
    cw = (d21 << 11) & 0xFFFFFFFF
    bits = cw >> 1
    for i in range(30, 9, -1):
        if bits & (1 << i):
            bits ^= P.BCH_POLY << (i - 10)
    cw |= (bits & 0x3FF) << 1
    if P._parity(cw):
        cw |= 1
    return cw & 0xFFFFFFFF


def msg_cw(bits20):
    val = 0
    for k in range(20):
        val |= (bits20[k] if k < len(bits20) else 0) << (19 - k)
    return bch_encode((1 << 20) | val)


def addr_cw(ric, func):
    return bch_encode((((ric >> 3) & 0x3FFFF) << 2) | (func & 3))


def encode_alpha(text):
    bits = []
    for ch in text:
        c = ord(ch) & 0x7F
        bits += [(c >> b) & 1 for b in range(7)]
    bits += [(0x04 >> b) & 1 for b in range(7)]           # EOT
    return [msg_cw(bits[i:i + 20]) for i in range(0, len(bits), 20)]


def encode_numeric(digits):
    bmap = {c: i for i, c in enumerate("0123456789*U -)(")}
    bits = []
    for ch in digits:
        v = bmap.get(ch, 0xC)
        bits += [(v >> b) & 1 for b in range(4)]           # LSB first
    return [msg_cw(bits[i:i + 20]) for i in range(0, len(bits), 20)]


def build_bits(ric, func, msg_cws):
    start = (ric & 7) * 2
    seq = [addr_cw(ric, func)] + list(msg_cws)
    slots = [IDLE] * start + seq
    while len(slots) % 16 != 0:
        slots.append(IDLE)
    if (start + len(seq) - 1) % 16 == 15:
        slots += [IDLE] * 16
    stream = [1, 0] * 288
    for b in range(len(slots) // 16):
        stream += [(SYNC >> (31 - i)) & 1 for i in range(32)]
        for s in range(16):
            cw = slots[b * 16 + s]
            stream += [(cw >> (31 - i)) & 1 for i in range(32)]
    return stream


def synth_audio(bits, fs, baud, noise=0.0, dc=0.0, seed=1):
    spb = fs / baud
    rng = np.random.default_rng(seed)
    a = np.zeros(int(len(bits) * spb) + int(fs * 0.05))
    idx = 0
    for i, b in enumerate(bits):
        cnt = int(round((i + 1) * spb)) - int(round(i * spb))
        a[idx:idx + cnt] = 1.0 if b else -1.0
        idx += cnt
    a = a[:idx]
    if noise:
        a = a + rng.normal(0, noise, a.shape)
    return a + dc


FS = 50000
CASES = [
    (1234567, "EMS CARDIAC ARREST 1200 N UNIVERSITY DR", 3, 512),
    (1222222, "9545550142", 0, 1200),
    (1456789, "FIRE STRUCTURE 3400 W BROWARD BLVD", 3, 2400),
]


def run(noise, dc, label):
    p = 0
    for ric, msg, func, baud in CASES:
        cws = encode_numeric(msg) if func == 0 else encode_alpha(msg)
        audio = synth_audio(build_bits(ric, func, cws), FS, baud, noise=noise, dc=dc)
        r = P.decode_audio(audio, FS)
        hit = next((m for m in r["messages"] if m["address"] == ric), None)
        got = hit["content"] if hit else "(none)"
        ok = hit and got.replace(" ", "") == msg.replace(" ", "") and r["locked_baud"] == baud
        p += 1 if ok else 0
        print("  %s baud=%d lock=%s ric=%d got=%r" % ("PASS" if ok else "FAIL", baud, r["locked_baud"], ric, got))
    print("  %s: %d/%d" % (label, p, len(CASES)))
    return p


if __name__ == "__main__":
    print("clean:");        c = run(0.0, 0.0, "clean")
    print("noisy+offset:"); n = run(0.08, 0.20, "noisy")
    sys.exit(0 if c == len(CASES) and n == len(CASES) else 1)
