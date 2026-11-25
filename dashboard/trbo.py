"""
TRBO Monitor - MOTOTRBO / DMR traffic monitor and usage logger for SDR Looking Glass.

Runs OP25 multi_rx with ONE wide, fixed-tuned RTL capture per "window" and a DMR decoder per
repeater frequency inside it, so every channel and both timeslots are watched at the same time
(no hopping, no missed calls). Works for:
  - conventional MOTOTRBO / DMR Tier II repeaters (each frequency = a repeater, 2 timeslots)
  - Capacity Max / DMR Tier III control channels: TV/PV/data grants from the CSBKs (one grant per
    PTT), plus the voice on any traffic channel that falls in a monitored window
  - Capacity Plus / Connect Plus traffic channels (voice identity comes from the embedded LC)

What gets logged (trbo.db next to this file, SQLite):
  calls      every transmission: channel, slot, talkgroup, radio, start/end, encrypted, emergency
  grants     Capacity Max / Tier III channel grants (PTT counts per talkgroup / radio)
  data_msgs  packet data: LRRP (GPS), ARS (radio registration), TMS (text messages), other IP
  positions  GPS fixes (LRRP and the in-band GPS LC)
  coverage   per channel per minute: seconds monitored, seconds the repeater was keyed, data seconds
Utilization is always airtime / seconds actually monitored, so a rotating single-RTL capture
still gives honest percentages.

Receive only. Nothing here transmits.
"""
import os, re, json, time, sqlite3, threading, collections, struct, datetime, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("TRBO_DB") or os.path.join(HERE, "trbo.db")
AUDIO_PORT0 = 9200                          # channel i streams decoded audio on ws://host:9200+i
HANG = 1.2                                  # s without voice on a slot = that transmission ended
KEYED_GAP = 1.0                             # s between frames still counted as "repeater keyed"
BURST = 0.06                                # one DMR burst (frame) = 60 ms of one slot
GRANT_DEDUPE = 3.0                          # Cap Max sends each grant twice; collapse repeats

H = {}                                      # hooks from app.py: publish, record_position, op25_start, op25_stop, dev, tmp
LOCK = threading.RLock()
DBL = threading.Lock()

# --------------------------------------------------------------------------------- database
SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS sites (id TEXT PRIMARY KEY, name TEXT, kind TEXT, cfg TEXT, created REAL);
CREATE TABLE IF NOT EXISTS calls (id INTEGER PRIMARY KEY, site TEXT, ch INTEGER, freq TEXT, slot INTEGER,
  start REAL, end REAL, dur REAL, tg INTEGER, src INTEGER, ctype TEXT, enc INTEGER, emerg INTEGER,
  algid TEXT, keyid TEXT, term INTEGER, frames INTEGER, how TEXT);
CREATE INDEX IF NOT EXISTS calls_start ON calls(site, start);
CREATE TABLE IF NOT EXISTS grants (id INTEGER PRIMARY KEY, site TEXT, ts REAL, op TEXT, tg INTEGER, src INTEGER,
  lpcn INTEGER, slot INTEGER, data INTEGER, emerg INTEGER, freq TEXT);
CREATE INDEX IF NOT EXISTS grants_ts ON grants(site, ts);
CREATE TABLE IF NOT EXISTS data_msgs (id INTEGER PRIMARY KEY, site TEXT, ts REAL, ch INTEGER, freq TEXT, slot INTEGER,
  src INTEGER, dst INTEGER, proto TEXT, summary TEXT, hex TEXT);
CREATE INDEX IF NOT EXISTS data_ts ON data_msgs(site, ts);
CREATE TABLE IF NOT EXISTS positions (id INTEGER PRIMARY KEY, site TEXT, ts REAL, rid INTEGER, lat REAL, lon REAL,
  alt REAL, speed REAL, heading REAL, how TEXT, freq TEXT);
CREATE INDEX IF NOT EXISTS pos_ts ON positions(site, ts);
CREATE TABLE IF NOT EXISTS coverage (site TEXT, freq TEXT, minute INTEGER, mon REAL, keyed REAL, data REAL,
  PRIMARY KEY (site, freq, minute));
CREATE TABLE IF NOT EXISTS labels (kind TEXT, id INTEGER, label TEXT, PRIMARY KEY (kind, id));
CREATE TABLE IF NOT EXISTS radios (rid INTEGER PRIMARY KEY, first REAL, last REAL, ota_alias TEXT);
CREATE TABLE IF NOT EXISTS lpcn (site TEXT, lpcn INTEGER, freq TEXT, how TEXT, PRIMARY KEY (site, lpcn));
"""
_db = None
def db():
    global _db
    if _db is None:
        _db = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15)
        _db.execute("PRAGMA journal_mode=WAL"); _db.execute("PRAGMA synchronous=NORMAL")
        _db.executescript(SCHEMA); _db.commit()
    return _db

def q(sql, args=(), one=False):
    with DBL:
        cur = db().execute(sql, args); rows = cur.fetchall()
    return (rows[0] if rows else None) if one else rows

def x(sql, args=()):
    with DBL:
        c = db(); c.execute(sql, args); c.commit()

def meta_get(k, d=None):
    r = q("SELECT v FROM meta WHERE k=?", (k,), one=True)
    try: return json.loads(r[0]) if r else d
    except Exception: return d

def meta_set(k, v): x("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (k, json.dumps(v)))

# --------------------------------------------------------------------------------- sites
# A site: {"id","name","kind": conventional|capmax|capplus, "rate", "gain", "dwell", "rtls": [serials],
#          "channels": [{"freq": "453.25000", "label", "cc", "mode": dmr|fm, "ts1": {"tg","label"}, "ts2": {...}, "lpcn"}]}
PRESETS = [
    {"id": "bso-detention", "name": "BSO Dept. of Detention (conventional DMR)", "kind": "conventional",
     "notes": "RadioReference subcat 31016. Conventional MOTOTRBO repeaters; color code / TG / slot from RR.",
     "channels": [
        {"freq": "453.22500", "label": "Main Jail", "cc": 11, "ts1": {"tg": 109, "label": "Main Jail 1"}, "ts2": {"tg": 110, "label": "Main Jail 2"}},
        {"freq": "453.25000", "label": "North Broward Bureau", "cc": 15, "ts1": {"tg": 101, "label": "North Brwrd1"}, "ts2": {"tg": 102, "label": "North Brwrd2"}},
        {"freq": "453.32500", "label": "Courthouse", "cc": 6, "ts1": {"tg": 111, "label": "Courthouse"}},
        {"freq": "453.38750", "label": "Stockade", "cc": 7, "ts1": {"tg": 117, "label": "Stockade 1"}},
        {"freq": "453.43750", "label": "North Broward Maint", "cc": 14, "ts2": {"tg": 104, "label": "North Broward Maint"}},
        {"freq": "460.08750", "label": "Paul Rein DC (1/2)", "cc": 9, "ts1": {"tg": 113, "label": "Paul Rein 1"}, "ts2": {"tg": 114, "label": "Paul Rein 2"}},
        {"freq": "460.21250", "label": "Central Intake", "cc": 10, "ts1": {"tg": 111, "label": "CentralIntake1"}, "ts2": {"tg": 112, "label": "CentralIntake2"}},
        {"freq": "460.23750", "label": "Conte Facility", "cc": 13, "ts1": {"tg": 105, "label": "Conte 1"}},
        {"freq": "460.47500", "label": "Paul Rein DC (3/4)", "cc": 8, "ts1": {"tg": 115, "label": "Paul Rein 3"}, "ts2": {"tg": 116, "label": "Paul Rein 4"}},
     ]},
    {"id": "broward-capmax-s1", "name": "Broward County Capacity Max - Site 1 Fort Lauderdale (dev)", "kind": "capmax",
     "notes": "RadioReference sid 10234. Only the 460 MHz window: control channel 460.025 (alt 460.525) plus traffic 460.3375, "
              "so one RTL stays on the control channel all the time. Site 1 also has 453.3625, 453.800 and 470.1875.",
     "channels": [
        {"freq": "460.02500", "label": "Site 1 control", "cc": 1},
        {"freq": "460.33750", "label": "Site 1 traffic", "cc": 1},
        {"freq": "460.52500", "label": "Site 1 alt control", "cc": 1},
     ]},
]

def _norm_site(s):
    s = dict(s); s.setdefault("kind", "conventional"); s.setdefault("rate", 1920000); s.setdefault("gain", "LNA:40")
    s.setdefault("dwell", 300); s.setdefault("rtls", []); s.setdefault("notes", "")
    chans = []
    for c in s.get("channels") or []:
        try: f = float(c["freq"])
        except Exception: continue
        c = dict(c); c["freq"] = "%.5f" % f; c.setdefault("label", ""); c.setdefault("mode", "dmr")
        try: c["cc"] = int(c["cc"]) if c.get("cc") not in (None, "") else None
        except Exception: c["cc"] = None
        for k in ("ts1", "ts2"):
            t = c.get(k) or {}
            try: tg = int(t.get("tg")) if t.get("tg") not in (None, "") else None
            except Exception: tg = None
            c[k] = {"tg": tg, "label": str(t.get("label") or "")} if (tg or t.get("label")) else None
        chans.append(c)
    s["channels"] = sorted(chans, key=lambda c: float(c["freq"]))
    return s

def sites():
    out = {}
    for sid, name, kind, cfg, _ in q("SELECT id,name,kind,cfg,created FROM sites ORDER BY created"):
        try: out[sid] = _norm_site(dict(json.loads(cfg), id=sid, name=name, kind=kind))
        except Exception: pass
    return out

def save_site(s):
    s = _norm_site(s)
    sid = s.get("id") or re.sub(r"[^a-z0-9]+", "-", (s.get("name") or "site").lower()).strip("-") or "site"
    s["id"] = sid
    cfg = {k: v for k, v in s.items() if k not in ("id", "name", "kind")}
    old = q("SELECT created FROM sites WHERE id=?", (sid,), one=True)
    x("INSERT OR REPLACE INTO sites(id,name,kind,cfg,created) VALUES(?,?,?,?,?)",
      (sid, s.get("name") or sid, s["kind"], json.dumps(cfg), old[0] if old else time.time()))
    for c in s["channels"]:                           # slot labels double as talkgroup labels
        for k in ("ts1", "ts2"):
            t = c.get(k)
            if t and t.get("tg") and t.get("label") and not label("tg", t["tg"]): set_label("tg", t["tg"], t["label"])
    return s

def seed_presets():
    if meta_get("seeded"): return
    for p in PRESETS: save_site(p)
    # Broward Capacity Max talkgroups (RadioReference sid 10234) so the dev system reads in plain English
    for tg, lb in BROWARD_TGS: set_label("tg", tg, lb, keep=True)
    meta_set("seeded", True)

BROWARD_TGS = [(200004, "Animal Care"), (200005, "FLL Maintenance"), (200006, "FLL Security"), (200007, "FLL Ops Control"),
    (200008, "FLL CC"), (200011, "EM"), (200012, "Facilities Maint"), (200013, "Facilities Sec"), (200014, "Fleet Operations"),
    (200015, "EM Shelter"), (200016, "Highway-Bridge 1"), (200017, "Highway-Bridge 2"), (200018, "Fleet Services"),
    (200019, "Medical Examiner"), (200022, "Parks Region 1"), (200023, "Parks Region 2"), (200024, "Parks Region 3"),
    (200025, "Parks Region 4"), (200026, "Parks Region 5"), (200027, "Parks Maintenance"), (200028, "BCTED 1 Dispatch"),
    (200029, "BCTED 2"), (200030, "BCTED 3"), (200031, "WWS Ops 31"), (200032, "WWS Cust Service"), (200033, "WWS Console"),
    (200034, "WWS District 2"), (200035, "WWS Ops 35"), (200038, "WWS Ops 38"), (200103, "Landfill"), (200104, "WWS Engineering"),
    (200106, "BCT Mechanics"), (200100, "PE Security"), (200101, "PE Security 2"), (200102, "PE Security 3"),
    (200001, "Radio 1"), (200002, "Radio 2"), (200003, "Radio 3"), (200065, "Event 1"), (200066, "Event 2"),
    (201000, "BCPS T All Call"), (201001, "BCPS TCA Dispatch"), (201002, "BCPS TCA Mgmt"), (201003, "BCPS TCWA Dispatch"),
    (201004, "BCPS TCWA Mgmt"), (201005, "BCPS TNA Dispatch 1"), (201006, "BCPS TNA Mgmt"), (201009, "BCPS TNA Dispatch 2"),
    (201011, "BCPS TSA Dispatch"), (201012, "BCPS TSA Mgmt"), (201015, "BCPS Veh Maint"), (201018, "BCPS Safety/Security"),
    (300001, "Sunrise Water"), (300003, "Sunrise Gas"), (300006, "Sunrise Mini-Bus"), (300007, "Sunrise Leisure Svcs"),
    (300011, "Sunrise Public Works"), (300012, "Sunrise Utilities CS"), (300013, "Sunrise LS Event")] + \
    [(200067 + i, "12-JOP%d Patch" % (i + 1)) for i in range(16)] + [(200083, "14-CALL Patch")] + \
    [(200084 + i, "14-OPS%d Patch" % (i + 1)) for i in range(15)]

# --------------------------------------------------------------------------------- labels
_LBL = {}
def label(kind, i):
    if i is None: return ""
    k = (kind, int(i))
    if k not in _LBL:
        r = q("SELECT label FROM labels WHERE kind=? AND id=?", k, one=True); _LBL[k] = r[0] if r else ""
    return _LBL[k] or (H["rid_alias"](i) if kind == "rid" and H.get("rid_alias") else "") or \
                      (H["tg_tag"](i) if kind == "tg" and H.get("tg_tag") else "")

def set_label(kind, i, text, keep=False):
    i = int(i); text = (text or "").strip()
    if keep and q("SELECT 1 FROM labels WHERE kind=? AND id=?", (kind, i), one=True): return
    if text: x("INSERT OR REPLACE INTO labels(kind,id,label) VALUES(?,?,?)", (kind, i, text))
    else: x("DELETE FROM labels WHERE kind=? AND id=?", (kind, i))
    _LBL.pop((kind, i), None)

# --------------------------------------------------------------------------------- capture plan
def plan(site, n_devices=1):
    """Group the site's frequencies into RTL capture windows. Each window = (center_hz, rate, [freq strings])."""
    rate = int(site.get("rate") or 1920000)
    half = rate / 2 * 0.85 / 1e6                       # MHz usable either side of center (RTL roll-off)
    dc = 0.035                                         # keep channels this far from the RTL DC spike
    fs = sorted({c["freq"] for c in site["channels"] if c.get("mode", "dmr") == "dmr"}, key=float)
    groups, cur = [], []
    for f in fs:
        if cur and float(f) - float(cur[0]) > 2 * half - 0.02: groups.append(cur); cur = []
        cur.append(f)
    if cur: groups.append(cur)
    wins = []
    for g in groups:
        lo, hi = float(g[0]), float(g[-1])
        if hi - lo + dc + 0.0125 <= half:              # fits on one side: centre just above -> no channel near DC
            center = hi + dc + 0.00625
            if center - lo > half: center = lo - dc - 0.00625
        else:
            center = (lo + hi) / 2
            for _ in range(40):                        # nudge off any channel sitting on the DC spike
                if all(abs(float(f) - center) >= dc for f in g): break
                center += 0.0125
        wins.append({"center": round(center, 5), "rate": rate, "freqs": g,
                     "span": round(hi - lo, 4), "ok": all(abs(float(f) - center) <= half for f in g)})
    return wins

# --------------------------------------------------------------------------------- run state
RUN = {"site": None, "on": False, "wins": [], "assign": [], "rot": 0, "rot_at": 0, "started": 0,
       "devices": [], "leases": [], "err": None, "chmap": [], "lines": 0}
SLOTS = {}          # (ch, slot) -> live slot state
CHS = {}            # ch idx -> {"freq", "last_sync", "sync_n", "keyed_from", ...}
RECENT = collections.deque(maxlen=200)       # finished calls (live list)
GRANTS_R = collections.deque(maxlen=150)
DATA_R = collections.deque(maxlen=200)
UNK = collections.Counter()
COUNTS = collections.Counter()
PENDING_GRANTS = collections.deque(maxlen=60)   # for LPCN -> frequency learning
_GSEEN = {}
_COV = collections.defaultdict(lambda: [0.0, 0.0, 0.0])   # (freq, minute) -> [mon, keyed, data] not yet flushed
_PDP = {}            # (ch, slot) -> last PDP header
_TA = {}             # (ch, slot) -> talker alias assembly

def active():
    return bool(meta_get("active_site")) and bool(meta_get("enabled", False))

def active_site():
    sid = meta_get("active_site")
    return sites().get(sid) if sid else None

def _devices():
    """RTL serials we may use: the Looking Glass dongle (SDR_ARGS) first, then the site's extra RTLs."""
    dev = H["dev"]
    devs = [{"args": dev["args"], "ppm": float(dev.get("ppm", 0.0)), "lease": None}]
    site = active_site() or {}
    extra = [s for s in site.get("rtls") or [] if s and "rtl=%s" % s != dev["args"]]
    if extra:
        try:
            sys.path.insert(0, "/opt/clockwork-orange/lib"); import co_radio
            for sn in extra:
                ls, why = co_radio.lease("TRBO Monitor", allow_borrow=False, prefer=sn)
                if ls and ls.serial == sn: devs.append({"args": "rtl=%s" % sn, "ppm": 0.0, "lease": ls})
                elif ls: ls.release()
        except Exception as e:
            RUN["err"] = "extra RTL: %s" % e
    return devs

def release_devices():
    for d in RUN.get("devices") or []:
        if d.get("lease"):
            try: d["lease"].release()
            except Exception: pass
    RUN["devices"] = []

def build_cfg():
    """OP25 multi_rx config for the active site's current window(s). Returns the file path."""
    site = active_site()
    if not site: raise RuntimeError("no TRBO site selected")
    release_devices()
    wins = plan(site); devs = _devices(); RUN["devices"] = devs
    nd = max(1, len(devs)); ng = len(wins)
    if ng == 0: raise RuntimeError("site has no DMR frequencies")
    if ng <= nd: assign = list(range(ng))
    else: assign = [(RUN["rot"] + i) % ng for i in range(nd)]
    devices, chans, chmap = [], [], []
    for di, wi in enumerate(assign):
        w = wins[wi]; d = devs[di]
        devices.append({"args": d["args"], "gains": site.get("gain") or "LNA:40", "name": "sdr%d" % di,
                        "frequency": int(round(w["center"] * 1e6)), "offset": 0, "ppm": d["ppm"],
                        "rate": int(w["rate"]), "usable_bw_pct": 0.85, "tunable": False})
        for f in w["freqs"]:
            i = len(chans)
            chans.append({"name": "trbo-%s" % f, "device": "sdr%d" % di, "demod_type": "fsk4",
                          "destination": "ws://0.0.0.0:%d" % (AUDIO_PORT0 + i), "excess_bw": 0.2,
                          "filter_type": "rrc", "if_rate": 24000, "plot": "", "symbol_rate": 4800,
                          "frequency": int(round(float(f) * 1e6)), "enable_analog": "off",
                          "blacklist": "", "whitelist": "", "crypt_keys": ""})
            chmap.append(f)
    with LOCK:
        RUN.update(site=site["id"], wins=wins, assign=assign, chmap=chmap, started=time.time(), err=None)
        if not RUN["rot_at"]: RUN["rot_at"] = time.time()
        SLOTS.clear(); CHS.clear(); _PDP.clear(); _TA.clear()
        for i, f in enumerate(chmap): CHS[i] = {"freq": f, "last": 0, "frames": 0, "keyed_from": None, "last_key": 0, "cc_seen": collections.Counter()}
    path = os.path.join(H["tmp"], "op25_trbo.json")
    SLOTPICK.clear(); _write_masks({}, force=True)
    with open(path, "w") as fh:
        json.dump({"channels": chans, "devices": devices,
                   "trunking": {"module": "tk_lgslot.py", "chans": [{"slot_file": slot_file()}]}}, fh, indent=1)
    return path

# ---- audio slot priority: OP25 gives one mono audio stream per repeater, so when both timeslots are
# busy only one may play. tk_lgslot.py (inside OP25) applies the masks written here.
SLOTPICK = {}          # ch -> 1|2 while both slots of that repeater are busy
_MASKS = [None]
def slot_file(): return os.path.join(H.get("tmp") or "/tmp", "trbo_slots.json")

def _write_masks(masks, force=False):
    if not force and masks == _MASKS[0]: return
    _MASKS[0] = dict(masks)
    tmp = slot_file() + ".tmp"
    try:
        with open(tmp, "w") as f: json.dump({"masks": masks}, f)
        os.replace(tmp, slot_file())
    except Exception as e:
        RUN["err"] = "slot file: %s" % e

def pick_slots():
    """Both slots busy: play the one carrying a selected talkgroup (speaker in 'only selected' mode),
    otherwise keep whichever call started first until it ends."""
    masks = {}
    with LOCK:
        sel = set(SPK["tgs"]) if SPK.get("on") and SPK.get("mode") == "selected" else set()
        for ch in list(CHS):
            act = []
            for sl in (0, 1):
                st = SLOTS.get((ch, sl)); call = st and st["call"]
                if call: act.append((sl + 1, _ident(call)[1], call["start"]))
            pick = None
            if len(act) == 2:
                cur = SLOTPICK.get(ch)
                hits = [a for a in act if a[1] in sel]
                if hits: pick = cur if cur in [h[0] for h in hits] else min(hits, key=lambda a: a[2])[0]
                elif cur in (1, 2): pick = cur
                else: pick = min(act, key=lambda a: a[2])[0]
            if pick:
                SLOTPICK[ch] = pick; masks[str(ch)] = pick
            else:
                SLOTPICK.pop(ch, None)
    _write_masks(masks)

def rtl_present():
    try:
        out = subprocess.run(["lsusb"], capture_output=True, text=True, timeout=5).stdout
        return "0bda:2838" in out or "0bda:2832" in out
    except Exception:
        return True

def ensure_rtl():
    """The AIO RTL sometimes drops off USB when one decoder hands it to another. Power-cycle its rail."""
    if rtl_present(): return True
    try:
        subprocess.run(["co-rail", "sdr", "off"], timeout=15); time.sleep(3)
        subprocess.run(["co-rail", "sdr", "on"], timeout=15); time.sleep(6)
    except Exception: pass
    COUNTS["rtl_recover"] += 1
    return rtl_present()

# --------------------------------------------------------------------------------- parsing
TSRE = re.compile(r"^(\d\d)/(\d\d)/(\d\d) (\d\d):(\d\d):(\d\d)\.(\d+) \[(\d+)\] (.*)")
SLOTRE = re.compile(r"^Slot\((\d)\), CC\(([0-9a-f]+)\),? ?(.*)")
LCRE = re.compile(r"(VOICE|TERM|EMB) LC PF\(\d\), FLCO\(([0-9a-f]+)\), FID\(([0-9a-f]+)\), SVCOPT\(([0-9A-Fa-f]+)\), DSTADDR\(([0-9a-f]+)\), SRCADDR\(([0-9a-f]+)\)(?:, rs_errs=(-?\d+))?")
FULLLC = re.compile(r"^FULL LC: ((?:[0-9a-f]{2} ){11}[0-9a-f]{2}), rs_errs=(-?\d+)")
EMBRAW = re.compile(r"^EMB LC: ((?:[0-9a-f]{2} ){8}[0-9a-f]{2})")
PIRE = re.compile(r"PI HEADER: ALGID\(([0-9a-f]+)\), KEYID\(([0-9a-f]+)\)")
CSBKRE = re.compile(r"CSBK LB\(\d\), PF\(\d\), CSBKO\(([0-9a-f]+)\), FID\(([0-9a-f]+)\), DATA\(([0-9a-f]+)\)")
PDPH = re.compile(r"PDP HDR1 GF\(\d\), DPF\(([0-9a-f])\), SAP\(([0-9a-f])\), POC\(([0-9a-f])\), BF\(([0-9a-f]+)\), DEST\((\d+)\), SOURCE\((\d+)\)")
PDPD = re.compile(r"PDP RATE (\S+) DATA DEST\((\d+)\), SOURCE\((\d+)\) : ([0-9a-f ]+)")
VOICE_FLCO = {0x00: "group", 0x03: "private", 0x10: "group"}      # 0x10/FID 0x10 = Motorola (Cap Max) group voice

def _ts(m):
    mo, d, y, hh, mi, ss, frac = (int(v) for v in m.groups()[:7])
    return datetime.datetime(2000 + y, mo, d, hh, mi, ss).timestamp() + float("0." + m.group(7))

def feed(line):
    m = TSRE.match(line)
    if not m:
        return
    t = _ts(m); ch = int(m.group(8)); rest = m.group(9)
    with LOCK:
        RUN["lines"] += 1
        c = CHS.get(ch)
        if c is None: return
        sm = SLOTRE.match(rest)
        if sm:
            slot = int(sm.group(1)); cc = int(sm.group(2), 16); body = sm.group(3)
            _keyed(ch, c, t); c["cc_seen"][cc] += 1
            _slot_line(ch, slot, cc, body, t)
            return
        fm = FULLLC.match(rest)
        if fm: _full_lc(ch, fm, t); return
        em = EMBRAW.match(rest)
        if em: _emb_raw(ch, em, t); return
        if rest.startswith("SLCO="): _keyed(ch, c, t)

def _keyed(ch, c, t):
    c["frames"] += 1
    if c["last_key"] and t - c["last_key"] <= KEYED_GAP:
        _cov(c["freq"], t, keyed=t - c["last_key"])
    c["last_key"] = t; c["last"] = t

def _cov(freq, t, mon=0.0, keyed=0.0, data=0.0):
    a = _COV[(freq, int(t // 60))]; a[0] += mon; a[1] += keyed; a[2] += data

def _slot(ch, slot):
    k = (ch, slot)
    if k not in SLOTS: SLOTS[k] = {"call": None, "last_voice": 0, "last_seen": 0, "idle": 0}
    return SLOTS[k]

def _slot_line(ch, slot, cc, body, t):
    s = _slot(ch, slot); s["last_seen"] = t
    if body.endswith("VOICE") and ("BS VOICE" in body or "MS VOICE" in body or body == "VOICE"):
        _voice_tick(ch, slot, cc, t)
        return
    lm = LCRE.search(body)
    if lm:
        flco = int(lm.group(2), 16); fid = int(lm.group(3), 16); svc = int(lm.group(4), 16)
        dst = int(lm.group(5), 16); src = int(lm.group(6), 16); rs = int(lm.group(7) or 0)
        if flco in VOICE_FLCO and (fid in (0, 0x10)) and dst and rs >= 0:
            call = _voice_tick(ch, slot, cc, t)
            call["ids"][(VOICE_FLCO[flco], dst, src)] += 3 if lm.group(1) != "EMB" else 2
            if svc & 0x40: call["enc"] = True
            if svc & 0x80: call["emerg"] = True
            if lm.group(1) == "TERM": _end_call(ch, slot, t, term=True)
        else:
            UNK["LC flco %02x fid %02x" % (flco, fid)] += 1
        return
    pm = PIRE.search(body)
    if pm:
        call = _voice_tick(ch, slot, cc, t)
        call["enc"] = True; call["algid"] = pm.group(1); call["keyid"] = pm.group(2)
        return
    if body.startswith("IDLE"):
        s["idle"] = t; return
    if body.startswith(("PI(", "EMB ", "Data Type")):
        return
    cm = CSBKRE.search(body)
    if cm:
        _csbk(ch, slot, int(cm.group(1), 16), int(cm.group(2), 16), int(cm.group(3), 16), t); return
    hm = PDPH.search(body)
    if hm:
        _PDP[(ch, slot)] = {"dpf": int(hm.group(1), 16), "sap": int(hm.group(2), 16), "bf": int(hm.group(4), 16),
                            "dst": int(hm.group(5)), "src": int(hm.group(6)), "t": t}
        _cov(CHS[ch]["freq"], t, data=BURST)
        if int(hm.group(4), 16) == 0:                  # header only: confirmed-data response / ack
            COUNTS["data_ack"] += 1
        return
    dm = PDPD.search(body)
    if dm:
        try: payload = bytes.fromhex(dm.group(4).replace(" ", ""))
        except Exception: payload = b""
        hdr = _PDP.get((ch, slot)) or {}
        _cov(CHS[ch]["freq"], t, data=BURST * max(1, hdr.get("bf", 1)))
        _data(ch, slot, int(dm.group(3)), int(dm.group(2)), hdr.get("sap"), payload, t)
        return
    if body.startswith("Data Type"):
        return
    UNK[body.split("(")[0][:30]] += 1

def _voice_tick(ch, slot, cc, t):
    s = _slot(ch, slot); call = s["call"]
    if call and t - call["last"] > HANG:
        _end_call(ch, slot, call["last"], term=False); call = None
    if call is None:
        call = {"ch": ch, "freq": CHS[ch]["freq"], "slot": slot + 1, "cc": cc, "start": t, "last": t, "frames": 0,
                "ids": collections.Counter(), "enc": False, "emerg": False, "algid": None, "keyid": None}
        s["call"] = call; COUNTS["calls_started"] += 1
        _learn_lpcn(call)
    call["last"] = t; call["frames"] += 1; s["last_voice"] = t
    return call

def _ident(call):
    if not call["ids"]: return None, None, None
    (ctype, dst, src), _ = call["ids"].most_common(1)[0]
    return ctype, dst, src

def _end_call(ch, slot, t_end, term=False):
    s = _slot(ch, slot); call = s["call"]
    if not call: return
    s["call"] = None
    dur = max(BURST, (t_end - call["start"]) + BURST)
    ctype, tg, src = _ident(call)
    if tg is None and dur < 0.5: COUNTS["noise_dropped"] += 1; return
    rec = {"site": RUN["site"], "ch": ch, "freq": call["freq"], "slot": call["slot"], "start": call["start"],
           "end": call["start"] + dur, "dur": round(dur, 2), "tg": tg, "src": src, "ctype": ctype or "unknown",
           "enc": int(call["enc"]), "emerg": int(call["emerg"]), "algid": call["algid"], "keyid": call["keyid"],
           "term": int(term), "frames": call["frames"], "how": "voice"}
    try:
        x("INSERT INTO calls(site,ch,freq,slot,start,end,dur,tg,src,ctype,enc,emerg,algid,keyid,term,frames,how) "
          "VALUES(:site,:ch,:freq,:slot,:start,:end,:dur,:tg,:src,:ctype,:enc,:emerg,:algid,:keyid,:term,:frames,:how)", rec)
        if src: x("INSERT INTO radios(rid,first,last) VALUES(?,?,?) ON CONFLICT(rid) DO UPDATE SET last=excluded.last", (src, rec["start"], rec["end"]))
    except Exception as e:
        RUN["err"] = "db: %s" % e
    rec.update(tglabel=label("tg", tg), srclabel=label("rid", src))
    RECENT.appendleft(rec); COUNTS["calls"] += 1
    if H.get("publish"): H["publish"]({"kind": "TRBO_CALL", **{k: rec[k] for k in ("freq", "slot", "tg", "src", "dur", "enc", "emerg")}})

def _learn_lpcn(call):
    """A Cap Max grant for LPCN n / slot s followed within 2 s by voice on a monitored frequency, same slot = LPCN n is that frequency."""
    for g in list(PENDING_GRANTS):
        if 0 <= call["start"] - g["ts"] <= 2.0 and g["slot"] == call["slot"]:
            known = q("SELECT freq FROM lpcn WHERE site=? AND lpcn=?", (RUN["site"], g["lpcn"]), one=True)
            if not known:
                x("INSERT OR REPLACE INTO lpcn(site,lpcn,freq,how) VALUES(?,?,?,?)", (RUN["site"], g["lpcn"], call["freq"], "learned"))
                COUNTS["lpcn_learned"] += 1
            PENDING_GRANTS.remove(g); break

GRANT_OPS = {0x30: "private voice", 0x31: "group voice", 0x32: "broadcast voice", 0x33: "private data",
             0x34: "group data", 0x35: "private voice (duplex)", 0x36: "private data (duplex)", 0x37: "private data (multi)",
             0x38: "group data (multi)"}
CSBK_NAMES = {0x19: "C_ALOHA (site parameters)", 0x1c: "C_AHOY (radio poll / auth)", 0x1f: "C_RAND (random access)",
              0x20: "C_ACKD (ack to radio)", 0x21: "C_ACKU", 0x28: "C_BCAST (announcement)", 0x2e: "P_CLEAR", 0x2f: "P_PROTECT",
              0x39: "C_MOVE"}

def _csbk(ch, slot, op, fid, data, t):
    COUNTS["csbk"] += 1
    name = CSBK_NAMES.get(op) if fid == 0 else None
    COUNTS["csbk %s%02x%s" % ("" if fid == 0 else "moto ", op, (" " + name) if name else "")] += 1
    if fid == 0 and op in GRANT_OPS:
        lpcn = (data >> 52) & 0xFFF; ts = ((data >> 51) & 1) + 1; emerg = (data >> 49) & 1
        tgt = (data >> 24) & 0xFFFFFF; src = data & 0xFFFFFF
        key = (op, tgt, src, lpcn, ts)
        if t - _GSEEN.get(key, 0) < GRANT_DEDUPE: _GSEEN[key] = t; return
        _GSEEN[key] = t
        if len(_GSEEN) > 500:
            for k in [k for k, v in _GSEEN.items() if t - v > 30]: _GSEEN.pop(k, None)
        isdata = op in (0x33, 0x34, 0x36, 0x37, 0x38)
        fr = q("SELECT freq FROM lpcn WHERE site=? AND lpcn=?", (RUN["site"], lpcn), one=True)
        g = {"ts": t, "op": GRANT_OPS[op], "tg": tgt, "src": src, "lpcn": lpcn, "slot": ts, "data": int(isdata),
             "emerg": emerg, "freq": fr[0] if fr else None}
        try:
            x("INSERT INTO grants(site,ts,op,tg,src,lpcn,slot,data,emerg,freq) VALUES(?,?,?,?,?,?,?,?,?,?)",
              (RUN["site"], t, g["op"], tgt, src, lpcn, ts, int(isdata), emerg, g["freq"]))
            if src: x("INSERT INTO radios(rid,first,last) VALUES(?,?,?) ON CONFLICT(rid) DO UPDATE SET last=excluded.last", (src, t, t))
        except Exception as e:
            RUN["err"] = "db: %s" % e
        g.update(tglabel=label("tg", tgt) if op in (0x31, 0x32, 0x34, 0x38) else label("rid", tgt), srclabel=label("rid", src))
        GRANTS_R.appendleft(g); COUNTS["grants_data" if isdata else "grants_voice"] += 1
        if not isdata: PENDING_GRANTS.append(g)
    elif fid == 0 and op in (0x1c, 0x20):
        tgt = (data >> 24) & 0xFFFFFF; src = data & 0xFFFFFF
        for r in (tgt, src):
            if 0 < r < 0xFFFC00:
                try: x("INSERT INTO radios(rid,first,last) VALUES(?,?,?) ON CONFLICT(rid) DO UPDATE SET last=excluded.last", (r, t, t))
                except Exception: pass

# ---- packet data (LRRP / ARS / TMS)
def _data(ch, slot, src, dst, sap, p, t):
    COUNTS["data"] += 1
    proto, summary, pos = "raw", "%d bytes" % len(p), None
    if len(p) >= 28 and p[0] >> 4 == 4 and p[9] == 17:
        ihl = (p[0] & 15) * 4
        sport, dport = struct.unpack(">HH", p[ihl:ihl + 4]); body = p[ihl + 8:]
        sip, dip = ".".join(str(b) for b in p[12:16]), ".".join(str(b) for b in p[16:20])
        port = dport if dport in (4001, 4005, 4007, 4008, 4009, 4012, 5016) else sport
        if port == 4001:
            proto = "lrrp"; summary, pos = lrrp(body)
        elif port == 4005:
            proto = "ars"; summary = ars(body)
        elif port == 4007:
            proto = "tms"; summary = tms(body)
        else:
            proto = "udp:%d" % port; summary = "%s:%d > %s:%d, %d bytes" % (sip, sport, dip, dport, len(body))
        # MOTOTRBO radio IPs carry the radio ID in the low 3 bytes (12.x.y.z radios, 13.x.y.z servers)
        if not src: src = int.from_bytes(p[13:16], "big")
    elif sap is not None and sap != 4:
        proto = "sap%d" % sap
    rec = {"ts": t, "ch": ch, "freq": CHS[ch]["freq"], "slot": slot + 1, "src": src, "dst": dst, "proto": proto,
           "summary": summary, "hex": p[:96].hex()}
    try:
        x("INSERT INTO data_msgs(site,ts,ch,freq,slot,src,dst,proto,summary,hex) VALUES(?,?,?,?,?,?,?,?,?,?)",
          (RUN["site"], t, ch, rec["freq"], rec["slot"], src, dst, proto, summary, rec["hex"]))
    except Exception as e:
        RUN["err"] = "db: %s" % e
    rec.update(srclabel=label("rid", src), dstlabel=label("rid", dst))
    DATA_R.appendleft(rec); COUNTS["data_" + proto.split(":")[0]] += 1
    if pos:
        rid = src if src and src < 0xFFFC00 else dst
        _position(rid, pos, t, "lrrp", rec["freq"])

def _position(rid, pos, t, how, freq):
    try:
        x("INSERT INTO positions(site,ts,rid,lat,lon,alt,speed,heading,how,freq) VALUES(?,?,?,?,?,?,?,?,?,?)",
          (RUN["site"], t, rid, pos["lat"], pos["lon"], pos.get("alt"), pos.get("speed"), pos.get("heading"), how, freq))
    except Exception as e:
        RUN["err"] = "db: %s" % e
    COUNTS["positions"] += 1
    if H.get("record_position"):
        try: H["record_position"](rid, pos["lat"], pos["lon"], alias=label("rid", rid), heading=pos.get("heading"),
                                  speed=pos.get("speed"), src="trbo-" + how, ts=time.strftime("%H:%M:%S", time.localtime(t)))
        except Exception: pass

LRRP_TYPES = {0x04: "immediate location request", 0x05: "immediate location request", 0x06: "immediate location response",
              0x07: "immediate location response", 0x09: "triggered location start request",
              0x0a: "triggered location start response", 0x0b: "triggered location start response",
              0x0d: "triggered location data", 0x0f: "triggered location stop request",
              0x10: "triggered location stop response", 0x11: "triggered location stop response",
              0x14: "protocol request", 0x15: "protocol response"}
LRRP_RESULT = {0x00: "OK", 0x10: "reporting will stop", 0x11: "time expired", 0x14: "no fix (GPS not locked)", 0x15: "no fix",
               0x0e: "too many requests", 0x0f: "duplicate request", 0x12: "QoS not met", 0x13: "location unavailable"}

def _i32(b): return struct.unpack(">i", b)[0]

def lrrp(b):
    """Motorola LRRP: [type][len] then tokens. Positions are lat int32 * 90/2^31, lon int32 * 180/2^31."""
    if len(b) < 2: return "LRRP (empty)", None
    typ = b[0]; out = [LRRP_TYPES.get(typ, "type 0x%02x" % typ)]; pos = None
    i = 2; end = min(len(b), 2 + b[1])
    while i < end:
        tk = b[i]; i += 1
        try:
            if tk in (0x22, 0x24):                     # request id: length-prefixed
                n = b[i]; i += 1 + n
            elif tk == 0x34:                           # timestamp: 5 bytes
                v = int.from_bytes(b[i:i + 5], "big"); i += 5
                yr, mo, dy, hh, mi, ss = v >> 26, (v >> 22) & 15, (v >> 17) & 31, (v >> 12) & 31, (v >> 6) & 63, v & 63
                note = ""
                try:                                   # old GPS chips roll the week counter: 1024 weeks (19.6 y) back
                    fx = datetime.datetime(yr, mo, dy, hh, mi, ss)
                    while (datetime.datetime.utcnow() - fx).days > 7000: fx += datetime.timedelta(weeks=1024); note = " (radio GPS week rollover, corrected)"
                    yr, mo, dy, hh, mi, ss = fx.year, fx.month, fx.day, fx.hour, fx.minute, fx.second
                except ValueError: pass
                out.append("fix %04d-%02d-%02d %02d:%02d:%02dZ%s" % (yr, mo, dy, hh, mi, ss, note))
            elif tk in (0x37, 0x38):
                n = 1 if tk == 0x37 else 2; rc = int.from_bytes(b[i:i + n], "big"); i += n
                out.append("result: " + LRRP_RESULT.get(rc, "0x%02x" % rc))
            elif tk in (0x51, 0x54, 0x55, 0x66, 0x69):
                lat = _i32(b[i:i + 4]) * 90.0 / 2 ** 31; lon = _i32(b[i + 4:i + 8]) * 180.0 / 2 ** 31; i += 8
                if tk == 0x51: i += 2                  # + radius
                elif tk == 0x54: i += 5                # + radius + altitude
                elif tk == 0x55: i += 2
                elif tk == 0x69: i += 3                # + altitude
                if -90 <= lat <= 90 and -180 <= lon <= 180 and (lat or lon):
                    pos = {"lat": round(lat, 6), "lon": round(lon, 6)}
                    out.append("%.5f, %.5f" % (lat, lon))
            elif tk == 0x56:                           # heading, 2-degree units
                h = b[i] * 2; i += 1
                if pos: pos["heading"] = h
                out.append("heading %d°" % h)
            elif tk == 0x6c:                           # horizontal speed (ufloat: int.frac, km/h)
                n = 1
                while i + n < end and b[i + n - 1] & 0x80: n += 1
                v = 0
                for k in range(n): v = (v << 7) | (b[i + k] & 0x7f)
                i += n
                if i < end: frac = b[i]; i += 1
                else: frac = 0
                spd = v + frac / 128.0
                if pos: pos["speed"] = round(spd, 1)
                out.append("%.0f km/h" % spd)
            else:
                break                                  # unknown token: stop rather than mis-parse
        except IndexError:
            break
    return "LRRP " + "; ".join(out), pos

def ars(b):
    """ARS (automatic registration service): radio power-on registration / deregistration / query."""
    if len(b) < 3: return "ARS"
    hdr = b[2]; ptype = hdr & 0x0F
    kind = {0x0: "device registration (radio on)", 0x1: "device deregistration (radio off)", 0x4: "query", 0xF: "registration ack",
            0x5: "user registration", 0x6: "user deregistration", 0x7: "user registration ack"}.get(ptype, "type %d" % ptype)
    ident = ""
    i = 3
    while i < len(b) and b[i - 1] & 0x80 and i < 6: i += 1          # skip header extension bytes
    for j in range(i, len(b) - 1):
        n = b[j]
        if 1 <= n <= 24 and j + 1 + n <= len(b):
            s = b[j + 1:j + 1 + n]
            if all(32 <= c < 127 for c in s): ident = s.decode(); break
    return "ARS " + kind + (" id '%s'" % ident if ident else "")

def tms(b):
    """TMS text message. The text is UTF-16LE after a short header; pull the longest printable run."""
    best = ""
    for start in range(0, min(len(b), 40)):
        try: s = b[start:len(b) - ((len(b) - start) % 2)].decode("utf-16-le", "ignore")
        except Exception: continue
        run = re.findall(r"[ -~ -ɏ]{3,}", s)
        if run:
            r = max(run, key=len)
            if len(r) > len(best): best = r
    if best: return "TMS text: \"%s\"" % best.strip()
    return "TMS (%d bytes%s)" % (len(b), ", ack" if len(b) <= 6 else "")

# ---- raw LC bytes (FULL LC = header/terminator LC; EMB LC = embedded LC from the voice superframe)
def _full_lc(ch, m, t):
    by = bytes.fromhex(m.group(1).replace(" ", ""))
    _lc_bytes(ch, by[:9], t, int(m.group(2)))

def _emb_raw(ch, m, t):
    _lc_bytes(ch, bytes.fromhex(m.group(1).replace(" ", "")), t, 0, emb=True)

def _cur_call(ch):
    best = None
    for (c, sl), s in SLOTS.items():
        if c == ch and s["call"] and (best is None or s["call"]["last"] > best["last"]): best = s["call"]
    return best

def _lc_bytes(ch, by, t, rs, emb=False):
    flco = by[0] & 0x3F; fid = by[1]
    call = _cur_call(ch)
    if flco in VOICE_FLCO and fid in (0, 0x10) and not emb:
        # Cap Max masks the LC checksum (RAS), so OP25 reports rs_errs=-1 and skips its own VOICE LC line.
        # The bytes are still right: count them, but below the checked EMB/VOICE LC lines.
        dst = int.from_bytes(by[3:6], "big"); src = int.from_bytes(by[6:9], "big")
        if call and dst: call["ids"][(VOICE_FLCO[flco], dst, src)] += 1 if rs < 0 else 3
        return
    if flco == 0x04 or flco in (0x05, 0x06, 0x07):                    # talker alias header / blocks
        if not call: return
        ta = _TA.setdefault(ch, {})
        ta[flco] = by[2:9]
        if 0x04 in ta: _talker_alias(ch, ta, call)
        return
    if flco == 0x08 and call:                                         # in-band GPS (ETSI GPS info LC)
        v = int.from_bytes(by[2:9], "big")
        lon = v >> 24 & 0x1FFFFFF; lat = v & 0xFFFFFF
        if lon & 0x1000000: lon -= 1 << 25
        if lat & 0x800000: lat -= 1 << 24
        la, lo = lat * 180.0 / 2 ** 24, lon * 360.0 / 2 ** 25
        _, _, src = _ident(call)
        if src and (la or lo) and -90 <= la <= 90:
            _position(src, {"lat": round(la, 6), "lon": round(lo, 6)}, t, "gps-lc", CHS[ch]["freq"])

def _talker_alias(ch, ta, call):
    h = ta[0x04]; fmt = h[0] >> 6; n = (h[0] >> 1) & 0x1F
    bits = format(h[0] & 1, "01b") + "".join(format(b, "08b") for b in h[1:])
    for k in (0x05, 0x06, 0x07):
        if k in ta: bits += "".join(format(b, "08b") for b in ta[k])
    try:
        if fmt == 0:
            chars = [chr(int(bits[i:i + 7], 2)) for i in range(0, len(bits) - 6, 7)]
        else:
            bits = bits[1:]
            raw = bytes(int(bits[i:i + 8], 2) for i in range(0, len(bits) - 7, 8))
            chars = list(raw.decode("utf-16-be" if fmt == 3 else ("utf-8" if fmt == 2 else "latin-1"), "ignore"))
        text = "".join(chars)[:n].strip("\x00 ").strip()
    except Exception:
        return
    _, _, src = _ident(call)
    if src and text and len(text) >= min(n, 2) and all(c.isprintable() for c in text):
        cur = q("SELECT ota_alias FROM radios WHERE rid=?", (src,), one=True)
        if not cur or cur[0] != text:
            x("INSERT INTO radios(rid,first,last,ota_alias) VALUES(?,?,?,?) ON CONFLICT(rid) DO UPDATE SET ota_alias=excluded.ota_alias",
              (src, time.time(), time.time(), text))
            COUNTS["talker_alias"] += 1
            if not label("rid", src): set_label("rid", src, text)

# --------------------------------------------------------------------------------- housekeeping thread
def tick():
    """Every 2 s: close calls whose voice stopped, account monitored seconds, flush coverage, rotate windows."""
    last = time.time()
    while True:
        time.sleep(2)
        now = time.time(); dt_ = now - last; last = now
        try:
            with LOCK:
                running = RUN["on"] and H["op25_running"]()
                if running:
                    for (ch, sl), s in list(SLOTS.items()):
                        if s["call"] and now - s["call"]["last"] > HANG + 0.5:
                            _end_call(ch, sl, s["call"]["last"], term=False)
                    if now - RUN["started"] > 8:          # OP25 takes a few seconds to come up
                        for c in CHS.values(): _cov(c["freq"], now, mon=dt_)
                rows = [(RUN["site"], f, mi, a[0], a[1], a[2]) for (f, mi), a in _COV.items()]
                _COV.clear()
            if rows and RUN["site"]:
                with DBL:
                    c = db()
                    c.executemany("INSERT INTO coverage(site,freq,minute,mon,keyed,data) VALUES(?,?,?,?,?,?) "
                                  "ON CONFLICT(site,freq,minute) DO UPDATE SET mon=mon+excluded.mon, keyed=keyed+excluded.keyed, data=data+excluded.data", rows)
                    c.commit()
            if running and len(RUN["wins"]) > len(RUN["assign"]):
                site = active_site() or {}
                if now - RUN["rot_at"] >= float(site.get("dwell") or 300):
                    RUN["rot"] = (RUN["rot"] + len(RUN["assign"])) % len(RUN["wins"]); RUN["rot_at"] = now
                    threading.Thread(target=H["op25_start"], daemon=True).start()
        except Exception as e:
            RUN["err"] = "tick: %s" % e

# --------------------------------------------------------------------------------- live snapshot
def live():
    with LOCK:
        site = active_site()
        now = time.time()
        chans = []
        idx = {f: i for i, f in enumerate(RUN["chmap"])}
        for c in (site or {}).get("channels", []):
            f = c["freq"]; i = idx.get(f); st = CHS.get(i) if i is not None else None
            slots = []
            for sl in (1, 2):
                s = SLOTS.get((i, sl - 1)) if i is not None else None
                call = s["call"] if s else None
                cfg = c.get("ts%d" % sl) or {}
                d = {"slot": sl, "cfg_tg": cfg.get("tg"), "cfg_label": cfg.get("label") or label("tg", cfg.get("tg"))}
                if call:
                    ctype, tg, src = _ident(call)
                    d.update(active=True, tg=tg, src=src, ctype=ctype, tglabel=label("tg", tg), srclabel=label("rid", src),
                             secs=round(now - call["start"], 1), enc=call["enc"], emerg=call["emerg"], algid=call["algid"])
                elif s and now - s.get("idle", 0) < 3:
                    d.update(hang=True)
                slots.append(d)
            chans.append({"freq": f, "label": c.get("label"), "cc": c.get("cc"), "mode": c.get("mode", "dmr"),
                          "monitored": st is not None, "audio_port": (AUDIO_PORT0 + i) if i is not None else None,
                          "keyed": bool(st and now - st["last_key"] < 1.5),
                          "last": st["last"] if st else None,
                          "cc_seen": (st["cc_seen"].most_common(1)[0][0] if st and st["cc_seen"] else None),
                          "slots": slots})
        wins = RUN["wins"]
        return {"enabled": active(), "running": RUN["on"] and H["op25_running"](), "site": site, "err": RUN["err"],
                "windows": [dict(w, monitored=(i in RUN["assign"])) for i, w in enumerate(wins)],
                "rotating": len(wins) > len(RUN["assign"]) if wins else False,
                "next_rotate": (RUN["rot_at"] + float((site or {}).get("dwell") or 300) - now) if len(wins) > len(RUN["assign"]) else None,
                "devices": [d["args"] for d in RUN["devices"]], "channels": chans,
                "recent": list(RECENT)[:60], "grants": list(GRANTS_R)[:60], "data": list(DATA_R)[:60],
                "counts": dict(COUNTS), "unhandled": dict(UNK.most_common(12)), "lines": RUN["lines"], "speaker": speaker_status(),
                "lpcn": [{"lpcn": r[0], "freq": r[1], "how": r[2]} for r in q("SELECT lpcn,freq,how FROM lpcn WHERE site=? ORDER BY lpcn", (RUN["site"] or "",))]}

# --------------------------------------------------------------------------------- statistics
def erlang_b(a, n):
    b = 1.0
    for k in range(1, n + 1): b = a * b / (k + a * b)
    return b

def channels_for(a, gos=0.01):
    n = 0
    while erlang_b(a, n) > gos and n < 200: n += 1
    return n

def stats(site_id, t0, t1):
    """Everything the Traffic tab shows, for calls in [t0, t1)."""
    site = sites().get(site_id) or {"channels": []}
    calls = q("SELECT freq,slot,start,end,dur,tg,src,enc,emerg,ctype FROM calls WHERE site=? AND start>=? AND start<? ORDER BY start",
              (site_id, t0, t1))
    cov = q("SELECT freq,minute,mon,keyed,data FROM coverage WHERE site=? AND minute>=? AND minute<?", (site_id, int(t0 // 60), int(t1 // 60) + 1))
    grants = q("SELECT ts,op,tg,src,data FROM grants WHERE site=? AND ts>=? AND ts<?", (site_id, t0, t1))
    hour_len = 3600
    # coverage per channel and per hour (seconds monitored)
    mon_ch = collections.Counter(); keyed_ch = collections.Counter(); data_ch = collections.Counter()
    mon_hour = collections.defaultdict(collections.Counter)            # hour -> freq -> secs
    for f, mi, mon, keyed, dsec in cov:
        mon_ch[f] += mon; keyed_ch[f] += keyed; data_ch[f] += dsec
        mon_hour[int(mi * 60 // hour_len)][f] += mon
    # airtime
    air_cs = collections.Counter(); n_cs = collections.Counter()
    air_hour = collections.Counter(); n_hour = collections.Counter()
    air_hour_cs = collections.defaultdict(collections.Counter)
    tg_air = collections.Counter(); tg_n = collections.Counter(); tg_rad = collections.defaultdict(set)
    tg_hour = collections.defaultdict(collections.Counter); tg_enc = collections.Counter(); tg_em = collections.Counter()
    tg_freq = collections.defaultdict(collections.Counter)
    r_air = collections.Counter(); r_n = collections.Counter(); r_tgs = collections.defaultdict(set); r_last = {}
    for f, sl, st, en, du, tg, src, enc, em, ct in calls:
        h = int(st // hour_len); key = "%s/%d" % (f, sl)
        air_cs[key] += du; n_cs[key] += 1; air_hour[h] += du; n_hour[h] += 1; air_hour_cs[h][key] += du
        tk = tg if tg is not None else -1
        tg_air[tk] += du; tg_n[tk] += 1; tg_hour[tk][h] += du; tg_freq[tk][key] += 1
        if src: tg_rad[tk].add(src)
        if enc: tg_enc[tk] += 1
        if em: tg_em[tk] += 1
        if src:
            r_air[src] += du; r_n[src] += 1; r_last[src] = st
            if tg is not None: r_tgs[src].add(tg)
    # Cap Max: PTT counts from grants (covers traffic channels we are not listening to)
    g_tg = collections.Counter(); g_src = collections.Counter(); g_data = 0; g_hour = collections.Counter()
    for ts, op, tg, src, isdata in grants:
        if isdata: g_data += 1; continue
        g_tg[tg] += 1; g_hour[int(ts // hour_len)] += 1
        if src: g_src[src] += 1
    # average PTT length measured on the traffic channels we do hear -> estimate airtime for granted PTTs
    voice_durs = [c[4] for c in calls if c[5] is not None]
    avg_ptt = (sum(voice_durs) / len(voice_durs)) if voice_durs else None

    def cover_frac(h):
        m = mon_hour.get(h)
        if not m: return 0.0
        fs = [c["freq"] for c in site["channels"] if c.get("mode", "dmr") == "dmr"] or list(m)
        return min(1.0, sum(m.get(f, 0) for f in fs) / (len(fs) * hour_len))

    hours = sorted(set(list(air_hour) + list(mon_hour) + list(g_hour)))
    series = []
    for h in hours:
        cf = cover_frac(h)
        erl = (air_hour[h] / hour_len / cf) if cf > 0.02 else None
        series.append({"t": h * hour_len, "air": round(air_hour[h], 1), "calls": n_hour[h], "cover": round(cf, 3),
                       "erl": round(erl, 3) if erl is not None else None, "grants": g_hour.get(h, 0)})
    # heatmap: day-of-week x hour-of-day average erlangs (coverage-corrected)
    hm = collections.defaultdict(list)
    for s in series:
        if s["erl"] is None: continue
        lt = time.localtime(s["t"]); hm[(lt.tm_wday, lt.tm_hour)].append(s["erl"])
    heat = [{"dow": d, "hour": hr, "erl": round(sum(v) / len(v), 3), "n": len(v)} for (d, hr), v in hm.items()]
    busiest = max((s for s in series if s["erl"] is not None), key=lambda s: s["erl"], default=None)
    # channels / slots
    chrows = []
    for c in site["channels"]:
        f = c["freq"]
        for sl in (1, 2):
            key = "%s/%d" % (f, sl); mon = mon_ch.get(f, 0)
            peak = 0.0; peak_t = None
            for h, d in air_hour_cs.items():
                m = mon_hour.get(h, {}).get(f, 0)
                if m > 300 and d.get(key, 0) / m > peak: peak, peak_t = d.get(key, 0) / m, h * hour_len
            cfg = c.get("ts%d" % sl) or {}
            chrows.append({"freq": f, "slot": sl, "label": c.get("label"), "slot_label": cfg.get("label") or label("tg", cfg.get("tg")),
                           "calls": n_cs.get(key, 0), "air": round(air_cs.get(key, 0), 1), "mon": round(mon, 0),
                           "util": round(air_cs.get(key, 0) / mon, 4) if mon > 60 else None,
                           "keyed": round(keyed_ch.get(f, 0) / mon, 4) if mon > 60 else None,
                           "data_secs": round(data_ch.get(f, 0), 1), "peak_util": round(peak, 4), "peak_t": peak_t})
    # talkgroups
    tgrows = []
    for tg in set(tg_n) | set(g_tg):
        bh = 0.0; bh_t = None
        for h, secs in tg_hour[tg].items():
            cf = cover_frac(h)
            if cf > 0.02 and secs / hour_len / cf > bh: bh, bh_t = secs / hour_len / cf, h * hour_len
        tgrows.append({"tg": None if tg == -1 else tg, "label": label("tg", tg) if tg != -1 else "(unidentified)",
                       "calls": tg_n.get(tg, 0), "air": round(tg_air.get(tg, 0), 1),
                       "avg": round(tg_air[tg] / tg_n[tg], 1) if tg_n.get(tg) else None,
                       "radios": len(tg_rad.get(tg, ())), "enc": tg_enc.get(tg, 0), "emerg": tg_em.get(tg, 0),
                       "ptts": g_tg.get(tg, 0), "busy_erl": round(bh, 4), "busy_t": bh_t,
                       "where": [k for k, _ in tg_freq[tg].most_common(3)],
                       "hours": {str(h): round(v, 1) for h, v in tg_hour[tg].items()}})
    tgrows.sort(key=lambda r: (-(r["air"] or 0), -(r["ptts"] or 0)))
    radrows = []
    names = {r[0]: r[1] for r in q("SELECT rid, ota_alias FROM radios WHERE ota_alias IS NOT NULL")}
    lastpos = {r[0]: (r[1], r[2], r[3]) for r in q("SELECT rid, lat, lon, MAX(ts) FROM positions WHERE site=? GROUP BY rid", (site_id,))}
    for rid in set(r_n) | set(g_src):
        radrows.append({"rid": rid, "label": label("rid", rid), "ota": names.get(rid), "calls": r_n.get(rid, 0),
                        "ptts": g_src.get(rid, 0), "air": round(r_air.get(rid, 0), 1), "tgs": sorted(r_tgs.get(rid, ()))[:8],
                        "last": r_last.get(rid), "pos": lastpos.get(rid)})
    radrows.sort(key=lambda r: (-r["air"], -r["ptts"]))
    # concurrency (how many transmissions at once, across the site)
    evs = sorted([(c[2], 1) for c in calls] + [(c[3], -1) for c in calls])
    cur = mx = 0; at = collections.Counter(); prev = None
    for t, d in evs:
        if prev is not None and cur > 0: at[cur] += t - prev
        cur += d; mx = max(mx, cur); prev = t
    mon_total = sum(mon_ch.values())
    total_air = sum(c[4] for c in calls)
    dcounts = {r[0]: r[1] for r in q("SELECT proto, COUNT(*) FROM data_msgs WHERE site=? AND ts>=? AND ts<? GROUP BY proto", (site_id, t0, t1))}
    npos = q("SELECT COUNT(*) FROM positions WHERE site=? AND ts>=? AND ts<?", (site_id, t0, t1), one=True)[0]
    return {"site": site_id, "from": t0, "to": t1,
            "totals": {"calls": len(calls), "air": round(total_air, 1), "radios": len(set(c[6] for c in calls if c[6]) | set(g_src)),
                       "tgs": len(set(c[5] for c in calls if c[5] is not None) | set(g_tg)),
                       "enc": sum(1 for c in calls if c[7]), "emerg": sum(1 for c in calls if c[8]),
                       "ptts": sum(g_tg.values()), "data_grants": g_data, "data": sum(dcounts.values()), "data_by": dcounts,
                       "positions": npos, "monitored_h": round(mon_total / 3600.0, 2),
                       "avg_ptt": round(avg_ptt, 2) if avg_ptt else None,
                       "site_util": round(total_air / (mon_total * 2), 4) if mon_total > 60 else None,
                       "busiest": busiest, "max_concurrent": mx,
                       "busy_hour_paths_1pct": channels_for(busiest["erl"], 0.01) if busiest else None},
            "series": series, "heat": heat, "channels": chrows, "tgs": tgrows[:200], "radios": radrows[:500],
            "concurrency": {str(k): round(v, 1) for k, v in sorted(at.items())}}

# --------------------------------------------------------------------------------- import helper
_TONE = re.compile(r"\bCC\s*\d|\bTG\s*\d|\b(DPL|PL|CSQ|NAC|RAN)\b|^\d+(\.\d)?\s*(PL|DPL)$", re.I)
_LIC = re.compile(r"[A-Z]{1,4}\d{3,4}|RM|BM|M|FB\d?|MO|FX|BMO", re.I)
_MODES = ("DMR", "FMN", "FM", "NFM", "P25", "NXDN", "TRBO", "AM")

def parse_rr(text):
    """RadioReference frequency rows (copied from the page, tab- or space-separated) -> channels.
    Columns: Frequency, License, Type, Tone ('CC 15 / TG 101 / SL 1' or '114 DPL'), Alpha Tag, Description, Mode, Tag.
    Rows with the same frequency merge into one repeater with both slots filled."""
    out = {}
    for ln in text.splitlines():
        if not re.search(r"\d{3}\.\d{2,5}", ln): continue
        cols = ln.split("\t") if "\t" in ln else re.split(r"\s{2,}|\s*\|\s*", ln.strip())
        cols = [c.strip() for c in cols if c.strip()]
        fi = next((i for i, c in enumerate(cols) if re.fullmatch(r"\d{3}\.\d{2,5}", c)), None)
        if fi is None: continue
        f = "%.5f" % float(cols[fi]); rest = cols[fi + 1:]
        ti = next((i for i, c in enumerate(rest) if _TONE.search(c)), None)
        tone = rest[ti] if ti is not None else ""
        after = rest[ti + 1:] if ti is not None else [c for c in rest if not _LIC.fullmatch(c)]
        mode = next((c.upper() for c in after if c.upper() in _MODES), "")
        words = [c for c in after if c.upper() not in _MODES]
        alpha = words[0] if words else ""
        desc = words[1] if len(words) > 1 else ""
        cc = re.search(r"CC\s*(\d+)", tone, re.I); tg = re.search(r"TG\s*(\d+)", tone, re.I); sl = re.search(r"SL\s*(\d)", tone, re.I)
        is_dmr = bool(cc or tg) or mode in ("DMR", "TRBO")
        c = out.get(f)
        if c is None:
            c = out[f] = {"freq": f, "label": "", "cc": None, "mode": "dmr" if is_dmr else "fm", "desc": desc, "_fm": not is_dmr}
        elif is_dmr:
            c["mode"] = "dmr"                         # a frequency shared by analog + DMR is still a DMR repeater
        if cc: c["cc"] = int(cc.group(1))
        if tg: c["ts%s" % (sl.group(1) if sl else "1")] = {"tg": int(tg.group(1)), "label": alpha}
        if not c["label"] or (is_dmr and c.get("_fm")):
            base = re.sub(r"\s*\(?\s*-?\s*\d\s*\)?$", "", desc or alpha).strip()
            c["label"] = base or alpha; c["_fm"] = not is_dmr
    res = []
    for c in sorted(out.values(), key=lambda c: float(c["freq"])):
        c.pop("_fm", None); res.append(c)
    return res

# --------------------------------------------------------------------------------- uConsole speaker
# OP25 streams each repeater's decoded voice (8 kHz, 16-bit mono) on ws://127.0.0.1:9200+i. Here the
# service itself listens to those, decides per repeater whether to play (on/off, all talkgroups or only
# the selected ones, per-repeater mute) using the talkgroup the decoder just read, mixes, and plays it
# on the uConsole's speaker with aplay (PipeWire). Settings live in the database, so any browser
# (the uConsole window or a PC) controls the same speaker.
import socket, base64
SPK_DEFAULT = {"on": False, "mode": "all", "tgs": [], "muted": [], "gain": 2.0}
SPK = dict(SPK_DEFAULT)
SPK_STATE = {"playing": {}, "err": None, "proc": None, "last_audio": 0.0}
_AQ = {}                       # ch -> {"buf": bytearray, "since": first held sample time}
_AQL = threading.Lock()
_LAST_TG = {}                  # ch -> (tg, t) of the most recent call on that repeater (audio tail after the call ends)
CHUNK = 320                    # 20 ms at 8 kHz, 16-bit
PREBUF = 2400                  # 150 ms queued before a repeater starts playing (OP25 delivers in bursts)
HOLD = 1.0                     # s to hold audio while the talkgroup is still unknown

def speaker_load():
    SPK.update(SPK_DEFAULT); SPK.update(meta_get("speaker", {}) or {})

def speaker_set(d):
    for k in ("on", "mode", "tgs", "muted", "gain"):
        if k in d: SPK[k] = d[k]
    SPK["tgs"] = sorted({int(t) for t in SPK.get("tgs") or [] if str(t).strip().lstrip("-").isdigit()})
    SPK["muted"] = sorted({"%.5f" % float(f) for f in SPK.get("muted") or []})
    SPK["mode"] = "selected" if SPK.get("mode") == "selected" else "all"
    try: SPK["gain"] = max(0.25, min(8.0, float(SPK.get("gain") or 1)))
    except Exception: SPK["gain"] = 2.0
    SPK["on"] = bool(SPK.get("on"))
    meta_set("speaker", {k: SPK[k] for k in SPK_DEFAULT})
    return dict(SPK)

def _ws_open(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=3)
    key = base64.b64encode(os.urandom(16)).decode()
    s.sendall(("GET / HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
               "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n" % (port, key)).encode())
    hdr = b""
    while b"\r\n\r\n" not in hdr:
        c = s.recv(1024)
        if not c: raise ConnectionError("closed during handshake")
        hdr += c
    if b" 101 " not in hdr.split(b"\r\n", 1)[0]: raise ConnectionError("no websocket upgrade")
    return s, bytearray(hdr.split(b"\r\n\r\n", 1)[1])

def _ws_send(s, op, payload=b""):
    m = os.urandom(4); n = len(payload)
    s.sendall(bytes([0x80 | op, 0x80 | n]) + m + bytes(b ^ m[i % 4] for i, b in enumerate(payload)))

def _ws_reader(ch, port):
    """Feed one repeater's audio into its queue until the speaker is turned off or the port goes away."""
    while SPK["on"] and RUN["on"] and RUN["chmap"] and ch < len(RUN["chmap"]):
        try:
            s, buf = _ws_open(port); s.settimeout(2.0)
            while SPK["on"] and RUN["on"]:
                while len(buf) < 2:
                    try: c = s.recv(65536)
                    except socket.timeout: c = None
                    if c == b"": raise ConnectionError("closed")
                    if c: buf += c
                    elif not (SPK["on"] and RUN["on"]): raise ConnectionError("stopping")
                op = buf[0] & 15; n = buf[1] & 127; hl = 2
                if n == 126: hl = 4
                elif n == 127: hl = 10
                while len(buf) < hl:
                    c = s.recv(65536)
                    if not c: raise ConnectionError("closed")
                    buf += c
                if n == 126: n = int.from_bytes(buf[2:4], "big")
                elif n == 127: n = int.from_bytes(buf[2:10], "big")
                while len(buf) < hl + n:
                    c = s.recv(65536)
                    if not c: raise ConnectionError("closed")
                    buf += c
                payload = bytes(buf[hl:hl + n]); del buf[:hl + n]
                if op == 2:                                   # binary = PCM
                    with _AQL:
                        q_ = _AQ.setdefault(ch, {"buf": bytearray(), "since": None, "t": 0, "primed": False})
                        if q_["since"] is None: q_["since"] = time.time()
                        q_["buf"] += payload; q_["t"] = time.time()
                        if len(q_["buf"]) > 16000 * 3: del q_["buf"][:len(q_["buf"]) - 16000 * 3]   # never more than 3 s behind
                elif op == 9: _ws_send(s, 10, payload)        # ping -> pong
                elif op == 8: break
            try: s.close()
            except Exception: pass
        except Exception:
            pass
        time.sleep(2)

def _gate(ch, now):
    """'play', 'drop' or 'hold' for this repeater's queued audio right now."""
    f = CHS.get(ch, {}).get("freq")
    if f in SPK["muted"]: return "drop", None
    tgs = []
    with LOCK:
        pk = SLOTPICK.get(ch)
        for sl in ((pk - 1,) if pk else (0, 1)):
            s = SLOTS.get((ch, sl)); call = s and s["call"]
            if call:
                _, tg, _ = _ident(call)
                tgs.append(tg)
                if tg is not None: _LAST_TG[ch] = (tg, now)
    if not tgs and ch in _LAST_TG and now - _LAST_TG[ch][1] < 3: tgs = [_LAST_TG[ch][0]]
    known = [t for t in tgs if t is not None]
    if SPK["mode"] == "all": return "play", (known[0] if known else None)
    sel = set(SPK["tgs"])
    hit = [t for t in known if t in sel]
    if hit: return "play", hit[0]
    if known and len(known) == len(tgs): return "drop", known[0]
    return "hold", None

def _aplay():
    p = SPK_STATE.get("proc")
    if p and p.poll() is None: return p
    env = dict(os.environ); env.setdefault("XDG_RUNTIME_DIR", "/run/user/%d" % os.getuid())
    # Through PipeWire (shares the speaker with the desktop); plain ALSA aplay only if PipeWire isn't there,
    # since aplay opens the hardware directly and blocks every other app while it plays.
    p = None
    try:
        p = subprocess.Popen(["pw-cat", "--playback", "--raw", "--format", "s16", "--rate", "8000", "--channels", "1",
                              "--latency", "100ms", "-"], stdin=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
        time.sleep(0.15)
        if p.poll() is not None: p = None
    except Exception:
        p = None
    if p is None:
        p = subprocess.Popen(["aplay", "-q", "-t", "raw", "-f", "S16_LE", "-r", "8000", "-c", "1", "--buffer-time=200000"],
                             stdin=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
    SPK_STATE["proc"] = p; SPK_STATE["err"] = None
    return p

def _aplay_close():
    p = SPK_STATE.get("proc"); SPK_STATE["proc"] = None
    if p:
        try: p.stdin.close(); p.wait(timeout=2)
        except Exception:
            try: p.kill()
            except Exception: pass

def speaker_loop():
    """Mixer: every 20 ms take one chunk from each repeater allowed to play, mix, write to aplay
    (its blocking write keeps real time). Readers start/stop with the speaker and with TRBO."""
    import numpy as np
    readers = {}; last_pick = 0
    while True:
        try:
            if RUN["on"] and time.time() - last_pick > 0.1:
                pick_slots(); last_pick = time.time()
            if not (SPK["on"] and RUN["on"] and H["op25_running"]()):
                _aplay_close(); SPK_STATE["playing"] = {}
                with _AQL: _AQ.clear()
                readers = {k: t for k, t in readers.items() if t.is_alive()}
                time.sleep(0.1); continue
            for i, f in enumerate(RUN["chmap"]):             # one reader per monitored repeater
                key = (i, f, RUN["started"])
                if key not in readers or not readers[key].is_alive():
                    readers[key] = threading.Thread(target=_ws_reader, args=(i, AUDIO_PORT0 + i), daemon=True)
                    readers[key].start()
            now = time.time(); mix = None; playing = {}
            with _AQL: chans = [c for c, q_ in _AQ.items() if q_["buf"]]
            for ch in chans:
                g, tg = _gate(ch, now)
                with _AQL:
                    q_ = _AQ.get(ch)
                    if not q_ or not q_["buf"]: continue
                    if g == "drop" or (g == "hold" and q_["since"] and now - q_["since"] > HOLD and SPK["mode"] == "selected"):
                        q_["buf"].clear(); q_["since"] = None; continue
                    if g == "hold": continue
                    stale = now - q_["t"] > 0.12                  # no more audio coming: play out the tail
                    if not q_["primed"]:
                        if len(q_["buf"]) < PREBUF and not stale: continue
                        q_["primed"] = True
                    if len(q_["buf"]) < CHUNK and not stale: continue
                    part = bytes(q_["buf"][:CHUNK]); del q_["buf"][:CHUNK]
                    if not q_["buf"]: q_["since"] = None; q_["primed"] = False
                part += b"\0" * (CHUNK - len(part))
                a = np.frombuffer(part, dtype="<i2").astype(np.int32)
                mix = a if mix is None else mix + a
                playing[CHS.get(ch, {}).get("freq")] = tg
            if mix is None:
                SPK_STATE["playing"] = {}
                if SPK_STATE.get("proc") and now - SPK_STATE["last_audio"] > 3: _aplay_close()   # release the speaker when quiet
                time.sleep(0.02); continue
            SPK_STATE["playing"] = playing; SPK_STATE["last_audio"] = now
            out = np.clip(mix * float(SPK["gain"]), -32768, 32767).astype("<i2").tobytes()
            p = _aplay()
            try: p.stdin.write(out); p.stdin.flush()
            except Exception as e:
                SPK_STATE["err"] = "speaker: %s (is the audio output available?)" % e; _aplay_close(); time.sleep(1)
        except Exception as e:
            SPK_STATE["err"] = "speaker: %s" % e; time.sleep(1)

def speaker_status():
    p = SPK_STATE.get("proc")
    return dict(SPK, playing=[{"freq": f, "tg": t, "tglabel": label("tg", t)} for f, t in SPK_STATE["playing"].items()],
                open=bool(p and p.poll() is None), err=SPK_STATE["err"])

# --------------------------------------------------------------------------------- web API
def _range(a):
    now = time.time()
    try: t1 = float(a.get("to") or now)
    except Exception: t1 = now
    try: t0 = float(a.get("from") or (t1 - 86400))
    except Exception: t0 = t1 - 86400
    return t0, t1

def _csv(rows, head):
    import io, csv
    b = io.StringIO(); w = csv.writer(b); w.writerow(head); w.writerows(rows); return b.getvalue()

def _iso(t): return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t)) if t else ""

def register(app, hooks):
    """Wire TRBO Monitor into the Looking Glass Flask app. hooks: publish, record_position, op25_start,
    op25_stop, op25_running, dev, tmp, rid_alias, tg_tag."""
    from flask import request, jsonify, send_from_directory, Response
    H.update(hooks)
    db(); seed_presets(); speaker_load()
    threading.Thread(target=tick, daemon=True).start()
    threading.Thread(target=speaker_loop, daemon=True).start()

    def restart():
        threading.Thread(target=H["op25_start"], daemon=True).start()

    @app.route("/trbo")
    def trbo_page():
        r = send_from_directory(HERE, "trbo.html"); r.headers["Cache-Control"] = "no-store"; return r

    @app.route("/api/trbo/live")
    def trbo_live(): return jsonify(live())

    @app.route("/api/trbo/sites", methods=["GET", "POST"])
    def trbo_sites():
        if request.method == "POST":
            s = save_site(request.get_json(force=True) or {})
            if meta_get("active_site") == s["id"] and active(): restart()
            return jsonify({"ok": True, "site": s})
        ss = sites()
        return jsonify({"sites": list(ss.values()), "active": meta_get("active_site"), "enabled": active(),
                        "plans": {k: plan(v) for k, v in ss.items()}})

    @app.route("/api/trbo/sites/delete", methods=["POST"])
    def trbo_site_del():
        sid = (request.get_json(force=True) or {}).get("id")
        x("DELETE FROM sites WHERE id=?", (sid,))
        if meta_get("active_site") == sid:
            meta_set("active_site", None); meta_set("enabled", False); restart()
        return jsonify({"ok": True})

    @app.route("/api/trbo/activate", methods=["POST"])
    def trbo_activate():
        d = request.get_json(force=True) or {}
        if d.get("site"): meta_set("active_site", d["site"])
        meta_set("enabled", bool(d.get("enabled", True)))
        RUN["rot"] = 0; RUN["rot_at"] = 0
        restart()
        return jsonify({"ok": True, "enabled": active()})

    @app.route("/api/trbo/speaker", methods=["GET", "POST"])
    def trbo_speaker():
        if request.method == "POST": speaker_set(request.get_json(force=True) or {})
        return jsonify(speaker_status())

    @app.route("/api/trbo/plan", methods=["POST"])
    def trbo_plan(): return jsonify({"windows": plan(_norm_site(request.get_json(force=True) or {}))})

    @app.route("/api/trbo/import_rr", methods=["POST"])
    def trbo_import(): return jsonify({"channels": parse_rr((request.get_json(force=True) or {}).get("text", ""))})

    @app.route("/api/trbo/label", methods=["POST"])
    def trbo_label():
        d = request.get_json(force=True) or {}
        set_label(d.get("kind", "tg"), d["id"], d.get("label", "")); return jsonify({"ok": True})

    @app.route("/api/trbo/lpcn", methods=["POST"])
    def trbo_lpcn():
        d = request.get_json(force=True) or {}; sid = d.get("site") or meta_get("active_site")
        if d.get("freq"):
            x("INSERT OR REPLACE INTO lpcn(site,lpcn,freq,how) VALUES(?,?,?,?)", (sid, int(d["lpcn"]), "%.5f" % float(d["freq"]), "manual"))
        else:
            x("DELETE FROM lpcn WHERE site=? AND lpcn=?", (sid, int(d["lpcn"])))
        return jsonify({"ok": True})

    @app.route("/api/trbo/stats")
    def trbo_stats():
        t0, t1 = _range(request.args)
        return jsonify(stats(request.args.get("site") or meta_get("active_site") or "", t0, t1))

    @app.route("/api/trbo/calls")
    def trbo_calls():
        t0, t1 = _range(request.args); a = request.args; sid = a.get("site") or meta_get("active_site")
        sql = "SELECT start,dur,freq,slot,tg,src,ctype,enc,emerg,algid,term FROM calls WHERE site=? AND start>=? AND start<?"
        args = [sid, t0, t1]
        if a.get("tg"): sql += " AND tg=?"; args.append(int(a["tg"]))
        if a.get("rid"): sql += " AND src=?"; args.append(int(a["rid"]))
        if a.get("freq"): sql += " AND freq=?"; args.append(a["freq"])
        sql += " ORDER BY start DESC LIMIT ?"; args.append(min(5000, int(a.get("limit") or 500)))
        rows = [{"start": r[0], "dur": r[1], "freq": r[2], "slot": r[3], "tg": r[4], "src": r[5], "ctype": r[6], "enc": r[7],
                 "emerg": r[8], "algid": r[9], "term": r[10], "tglabel": label("tg", r[4]), "srclabel": label("rid", r[5])}
                for r in q(sql, args)]
        return jsonify({"calls": rows})

    @app.route("/api/trbo/data")
    def trbo_data():
        t0, t1 = _range(request.args); a = request.args; sid = a.get("site") or meta_get("active_site")
        sql = "SELECT ts,freq,slot,src,dst,proto,summary,hex FROM data_msgs WHERE site=? AND ts>=? AND ts<?"
        args = [sid, t0, t1]
        if a.get("proto"): sql += " AND proto LIKE ?"; args.append(a["proto"] + "%")
        sql += " ORDER BY ts DESC LIMIT ?"; args.append(min(5000, int(a.get("limit") or 500)))
        rows = [{"ts": r[0], "freq": r[1], "slot": r[2], "src": r[3], "dst": r[4], "proto": r[5], "summary": r[6], "hex": r[7],
                 "srclabel": label("rid", r[3]), "dstlabel": label("rid", r[4])} for r in q(sql, args)]
        g = q("SELECT ts,op,tg,src,lpcn,slot,freq FROM grants WHERE site=? AND ts>=? AND ts<? ORDER BY ts DESC LIMIT 300", (sid, t0, t1))
        grants = [{"ts": r[0], "op": r[1], "tg": r[2], "src": r[3], "lpcn": r[4], "slot": r[5], "freq": r[6],
                   "tglabel": label("tg", r[2]), "srclabel": label("rid", r[3])} for r in g]
        return jsonify({"data": rows, "grants": grants})

    @app.route("/api/trbo/positions")
    def trbo_positions():
        t0, t1 = _range(request.args); sid = request.args.get("site") or meta_get("active_site")
        rows = q("SELECT ts,rid,lat,lon,speed,heading,how FROM positions WHERE site=? AND ts>=? AND ts<? ORDER BY ts", (sid, t0, t1))
        trails = collections.defaultdict(list); lastrow = {}
        for r in rows:
            trails[r[1]].append([r[2], r[3], r[0]]); lastrow[r[1]] = r
        last = [{"rid": rid, "label": label("rid", rid), "lat": r[2], "lon": r[3], "ts": r[0], "speed": r[4], "heading": r[5],
                 "how": r[6], "n": len(trails[rid])} for rid, r in lastrow.items()]
        return jsonify({"positions": last, "trails": {str(k): v[-200:] for k, v in trails.items()}})

    @app.route("/api/trbo/radios")
    def trbo_radios():
        rows = q("SELECT rid, first, last, ota_alias FROM radios ORDER BY last DESC LIMIT 2000")
        return jsonify({"radios": [{"rid": r[0], "first": r[1], "last": r[2], "ota": r[3], "label": label("rid", r[0])} for r in rows]})

    @app.route("/api/trbo/rtls")
    def trbo_rtls():
        used = [d["args"] for d in RUN["devices"]]
        try:
            sys.path.insert(0, "/opt/clockwork-orange/lib"); import co_radio
            return jsonify({"rtls": co_radio.status(), "used": used})
        except Exception as e:
            return jsonify({"rtls": [], "used": used, "err": str(e)})

    @app.route("/api/trbo/export/<what>.csv")
    def trbo_export(what):
        t0, t1 = _range(request.args); sid = request.args.get("site") or meta_get("active_site")
        if what == "calls":
            rows = [(_iso(r[0]), r[1], r[2], r[3], r[4], label("tg", r[4]), r[5], label("rid", r[5]), r[6], r[7], r[8], r[9])
                    for r in q("SELECT start,dur,freq,slot,tg,src,ctype,enc,emerg,algid FROM calls "
                               "WHERE site=? AND start>=? AND start<? ORDER BY start", (sid, t0, t1))]
            body = _csv(rows, ["start", "seconds", "freq_mhz", "slot", "talkgroup", "tg_label", "radio", "radio_label",
                               "type", "encrypted", "emergency", "algid"])
        elif what == "talkgroups":
            st = stats(sid, t0, t1)
            body = _csv([(r["tg"], r["label"], r["calls"], r["ptts"], r["air"], r["avg"], r["radios"], r["busy_erl"],
                          _iso(r["busy_t"]), r["enc"], r["emerg"]) for r in st["tgs"]],
                        ["talkgroup", "label", "calls", "cc_grants", "airtime_s", "avg_call_s", "radios",
                         "busy_hour_erlangs", "busy_hour", "encrypted_calls", "emergencies"])
        elif what == "channels":
            st = stats(sid, t0, t1)
            body = _csv([(r["freq"], r["slot"], r["label"], r["slot_label"], r["calls"], r["air"], r["mon"], r["util"],
                          r["peak_util"], _iso(r["peak_t"]), r["keyed"], r["data_secs"]) for r in st["channels"]],
                        ["freq_mhz", "slot", "channel", "slot_label", "calls", "airtime_s", "monitored_s", "utilization",
                         "peak_hour_util", "peak_hour", "repeater_keyed", "data_s"])
        elif what == "hourly":
            st = stats(sid, t0, t1)
            body = _csv([(_iso(r["t"]), r["calls"], r["air"], r["erl"], r["cover"], r["grants"]) for r in st["series"]],
                        ["hour", "calls", "airtime_s", "erlangs", "coverage", "cc_grants"])
        elif what == "positions":
            body = _csv([(_iso(r[0]), r[1], label("rid", r[1]), r[2], r[3], r[4], r[5], r[6])
                         for r in q("SELECT ts,rid,lat,lon,speed,heading,how FROM positions "
                                    "WHERE site=? AND ts>=? AND ts<? ORDER BY ts", (sid, t0, t1))],
                        ["time", "radio", "radio_label", "lat", "lon", "speed_kmh", "heading", "source"])
        elif what == "data":
            body = _csv([(_iso(r[0]), r[1], r[2], r[3], r[4], r[5], r[6])
                         for r in q("SELECT ts,freq,slot,src,dst,proto,summary FROM data_msgs "
                                    "WHERE site=? AND ts>=? AND ts<? ORDER BY ts", (sid, t0, t1))],
                        ["time", "freq_mhz", "slot", "source", "destination", "protocol", "summary"])
        else:
            return jsonify({"err": "unknown export"}), 404
        fn = "trbo-%s-%s-%s.csv" % (sid, what, time.strftime("%Y%m%d", time.localtime(t1)))
        return Response(body, mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=%s" % fn})
