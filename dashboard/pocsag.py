#!/usr/bin/env python3
"""
pocsag.py - self-contained POCSAG pager decoder (no multimon-ng dependency).

Ported from the validated rf-toolkit decoder (modulator->demod->decoder self-test,
6/6 clean + noisy). Input is FM-demodulated audio (float samples) at a known rate;
it runs 512/1200/2400 baud in parallel and returns whichever locks.

POCSAG (ITU-R M.584): 2-FSK, codeword = 1 flag + 20 data + 10 BCH parity + 1 even
parity (BCH(31,21) + parity over 32 bits). Sync 0x7CD215D8, idle 0x7A89C197.
Address = 18-bit field + 3 low bits from the frame index. Alpha = 7-bit ASCII LSB
first; numeric = 4-bit BCD LSB first.
"""

SYNC_WORD = 0x7CD215D8
IDLE_WORD = 0x7A89C197
BCH_POLY = 0x769
BCD_TABLE = "0123456789*U #-."


def _syndrome(cw):
    bits = cw >> 1
    for i in range(30, 9, -1):
        if bits & (1 << i):
            bits ^= BCH_POLY << (i - 10)
    return bits & 0x3FF


def _parity(cw):
    p = 0
    while cw:
        p ^= cw & 1
        cw >>= 1
    return p


def _crc_ok(cw):
    return _syndrome(cw) == 0 and _parity(cw) == 0


# syndrome -> single-bit position (bits 31..1 are BCH-protected)
_SYN_TABLE = {}
for _i in range(1, 32):
    _s = _syndrome(1 << _i)
    if _s:
        _SYN_TABLE[_s] = _i


def _correct(cw):
    """Conservatively correct at most a single bit error; return corrected codeword or None.
    Deliberately NO 2-bit brute-force: on a marginal/noisy channel that manufactures
    CRC-valid garbage (wrong capcodes + junk text). Dropping uncertain codewords gives far
    cleaner output; strong pages have 0-1 errors and decode fine."""
    syn = _syndrome(cw)
    par = _parity(cw)
    if syn == 0 and par == 0:
        return cw
    if syn == 0 and par != 0:
        return (cw ^ 1) & 0xFFFFFFFF                     # parity bit only
    if par != 0 and syn in _SYN_TABLE:                   # single BCH bit error (odd parity confirms)
        fixed = (cw ^ (1 << _SYN_TABLE[syn])) & 0xFFFFFFFF
        if _crc_ok(fixed):
            return fixed
    return None                                          # 2+ errors -> drop, don't guess


def _hamming(a, b):
    return bin((a ^ b) & 0xFFFFFFFF).count("1")


class _Decoder:
    """Single-baud streaming decoder. Feed FM-demod audio via process()."""

    def __init__(self, sample_rate, baud):
        self.sample_rate = float(sample_rate)
        self.baud = baud
        self.messages = []
        self.syncs = 0
        self.corrected = 0
        self.crc_errors = 0
        self._dc = 0.0
        self._baud_phase = 0.0
        self._prev = 0.0
        self._sync_reg = 0
        self._synced = False
        self._preamble = 0
        self._bits_since_batch = 0
        self._batch = []
        self._inverted = False
        self._cur_addr = 0
        self._cur_func = 0
        self._msg_bits = []
        self._in_msg = False

    # --- front end: full-rate zero-crossing timing recovery ---
    def process(self, audio):
        import math
        spb = self.sample_rate / self.baud
        dc_alpha = 1.0 - math.exp(-2 * math.pi * 15 / self.sample_rate)
        prev = self._prev
        phase = self._baud_phase
        dc = self._dc
        for x in audio:
            dc += dc_alpha * (x - dc)
            s = x - dc
            phase += 1.0
            if (s > 0) != (prev > 0):
                phase -= (phase - spb / 2) * 0.3
            prev = s
            if phase >= spb:
                phase -= spb
                self._feed(1 if s > 0 else 0)
        self._prev = prev
        self._baud_phase = phase
        self._dc = dc

    def _feed(self, bit):
        self._sync_reg = ((self._sync_reg << 1) | bit) & 0xFFFFFFFF
        if not self._synced:
            self._bits_since_batch += 1
            last2 = self._sync_reg & 3
            self._preamble = self._preamble + 1 if last2 in (1, 2) else 0
            d_norm = _hamming(self._sync_reg, SYNC_WORD)
            d_inv = _hamming(self._sync_reg, SYNC_WORD ^ 0xFFFFFFFF)
            best = min(d_norm, d_inv)
            if best <= 2:
                if self._preamble > 10 or best == 0 or self._bits_since_batch < 50:
                    self._synced = True
                    self.syncs += 1
                    self._batch = []
                    self._inverted = d_inv < d_norm
                self._preamble = 0
        else:
            self._batch.append(bit ^ 1 if self._inverted else bit)
            if len(self._batch) >= 512:
                self._process_batch(self._batch)
                self._batch = []
                self._synced = False
                self._bits_since_batch = 0

    def _process_batch(self, bits):
        cws = []
        for i in range(16):
            cw = 0
            for j in range(32):
                cw = ((cw << 1) | (bits[i * 32 + j] if i * 32 + j < len(bits) else 0)) & 0xFFFFFFFF
            cws.append(cw)
        for frame in range(8):
            for slot in range(2):
                cw = cws[frame * 2 + slot]
                if _hamming(cw, IDLE_WORD) <= 2:
                    if self._in_msg:
                        self._flush()
                    continue
                if not _crc_ok(cw):
                    fixed = _correct(cw)
                    if fixed is None:
                        self.crc_errors += 1
                        continue
                    cw = fixed
                    self.corrected += 1
                if ((cw >> 31) & 1) == 0:                # address codeword
                    if self._in_msg:
                        self._flush()
                    self._cur_addr = (((cw >> 13) & 0x3FFFF) << 3) | frame
                    self._cur_func = (cw >> 11) & 3
                    self._msg_bits = []
                    self._in_msg = True
                elif self._in_msg:                       # message codeword (20 data bits)
                    for b in range(30, 10, -1):
                        self._msg_bits.append((cw >> b) & 1)

    def _flush(self):
        self._in_msg = False
        bits = self._msg_bits
        func = self._cur_func
        if not bits and func == 0:
            self.messages.append({"address": self._cur_addr, "function": func, "type": "tone", "content": ""})
            return
        # alpha: 7-bit ASCII, LSB first
        alpha = []
        for i in range(0, len(bits) - 6, 7):
            ch = 0
            for b in range(7):
                ch |= bits[i + b] << b
            if 0x20 <= ch < 0x7F:
                alpha.append(chr(ch))
            elif ch in (0x0A, 0x0D):
                alpha.append(" ")
        alpha = "".join(alpha).strip()
        # numeric: 4-bit BCD, LSB first
        num = []
        for i in range(0, len(bits) - 3, 4):
            v = bits[i] | (bits[i + 1] << 1) | (bits[i + 2] << 2) | (bits[i + 3] << 3)
            num.append(BCD_TABLE[v] if v < len(BCD_TABLE) else "?")
        numeric = "".join(num).rstrip("U ").strip()

        if func == 3:
            content, typ = alpha, "alpha"
        elif not alpha and not numeric:
            return
        else:
            ag = sum(1 for c in alpha if c.isalnum() or c in " .,:/-()#<")
            ar = ag / len(alpha) if alpha else 0.0
            nd = sum(1 for c in numeric if c.isdigit())
            nr = nd / len(numeric) if numeric else 0.0
            num_clean = nr > 0.7 and len(numeric) <= 30
            alpha_clean = ar > 0.65 and len(alpha) >= 3
            if func == 0 and (num_clean or not alpha_clean):
                content, typ = numeric, "numeric"
            elif alpha_clean:
                content, typ = alpha, "alpha"
            elif num_clean:
                content, typ = numeric, "numeric"
            elif ar >= nr:
                content, typ = alpha, "alpha"
            else:
                content, typ = numeric, "numeric"
        if content:
            self.messages.append({"address": self._cur_addr, "function": func, "type": typ, "content": content})


def decode_audio(audio, fs, bauds=(512, 1200, 2400)):
    """Decode FM-demod audio (iterable of floats) at sample rate fs. Returns
    {messages, locked_baud, syncs, corrected} for whichever baud locked best."""
    import numpy as np
    a = np.asarray(audio, dtype=np.float64)
    best = None
    for baud in bauds:
        d = _Decoder(fs, baud)
        d.process(a)
        score = d.syncs * 2 + len(d.messages) * 5
        if best is None or score > best[0]:
            best = (score, d)
    d = best[1] if best else None
    if not d or best[0] == 0:
        return {"messages": [], "locked_baud": None, "syncs": 0, "corrected": 0}
    # de-dup identical address+content
    seen = set()
    msgs = []
    for m in d.messages:
        key = (m["address"], m["content"])
        if key in seen:
            continue
        seen.add(key)
        msgs.append(m)
    return {"messages": msgs, "locked_baud": d.baud, "syncs": d.syncs, "corrected": d.corrected}
