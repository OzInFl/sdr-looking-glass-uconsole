#!/usr/bin/env python3
"""
db.py - SQLite flat-file store for SDR Looking Glass.

Replaces the wholesale-rewrite JSON files (systems.json / tags.json / rids.json)
with one durable, concurrent-safe SQLite database (lookingglass.db) next to app.py.

Why: the scanner ingests DISCOVERED systems from a background thread while the UI
may also be editing the same registry - two wholesale writers to one JSON file is a
corruption race. SQLite (WAL + a write lock) makes that safe.

Design: the FULL record is stored as a JSON blob in the `doc` column - that is the
source of truth, so load_all() reproduces the in-memory dict byte-for-byte (absent
keys stay absent, str/int types round-trip). The typed columns (nac/wacn/sysid/...)
are just an indexed mirror for future queries; they are never read back into the dict.

Public API used by app.py:
    init_db(path)                         -> open/create the DB (idempotent)
    migrate_from_json(sys_p, tags_p, rids_p)  -> one-time import (idempotent)
    load_all()   -> {"active": <id|None>, "systems": {id: rec}}   (== old SYSTEMS)
    load_tags()  -> {"talkgroups": {id:name}, "radios": {id:name}} (== old TAGS)
    load_rids()  -> {id: name}                                     (== old RIDS)
    sync_all(SYSTEMS)                      -> make DB match the whole SYSTEMS dict
    upsert_system(sid, rec) / delete_system(sid) / set_active(sid)
    set_talkgroup(id,name) / set_radio(id,name) / set_rid(id,name)  (falsy name deletes)
    bulk_set(table, mapping)              -> upsert many id->name pairs
    export_systems_json(path)             -> dump systems back to legacy shape (rollback)
"""
import os, json, time, sqlite3, threading

_DB_PATH = None
_LOCAL = threading.local()          # one sqlite3.Connection per thread
_WLOCK = threading.RLock()          # serialize our own writers (reentrant: migrate calls upsert)

# ----------------------------------------------------------------- connection
def _conn():
    c = getattr(_LOCAL, "conn", None)
    if c is None:
        if _DB_PATH is None:
            raise RuntimeError("db.init_db() not called")
        c = sqlite3.connect(_DB_PATH, timeout=30, isolation_level=None,
                            check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA busy_timeout=30000")
        c.execute("PRAGMA foreign_keys=ON")
        _LOCAL.conn = c
    return c

def init_db(path):
    global _DB_PATH
    _DB_PATH = path
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS systems (
            id              TEXT PRIMARY KEY,
            name            TEXT,
            county          TEXT,
            type            TEXT,
            nac             TEXT,
            wacn            TEXT,
            sysid           TEXT,
            modulation      TEXT,
            phase2          INTEGER,          -- NULL when key absent, else 0/1
            control_channels TEXT,            -- json array (mirror)
            sdr             TEXT,             -- json object (mirror)
            doc             TEXT NOT NULL,    -- json of the FULL record = source of truth
            updated_at      REAL
        );
        CREATE INDEX IF NOT EXISTS ix_systems_wacn_sysid ON systems(wacn, sysid);
        CREATE INDEX IF NOT EXISTS ix_systems_nac        ON systems(nac);
        CREATE TABLE IF NOT EXISTS meta       (k TEXT PRIMARY KEY, v TEXT);
        CREATE TABLE IF NOT EXISTS talkgroups (id TEXT PRIMARY KEY, name TEXT);
        CREATE TABLE IF NOT EXISTS radios     (id TEXT PRIMARY KEY, name TEXT);
        CREATE TABLE IF NOT EXISTS rids       (id TEXT PRIMARY KEY, name TEXT);
    """)
    return _DB_PATH

# ----------------------------------------------------------------- helpers
def _sys_columns(sid, rec):
    """Extract the mirror columns from a full system record (doc stays the truth)."""
    ph = rec.get("phase2", None)
    return (
        str(sid),
        rec.get("name"), rec.get("county"), rec.get("type"),
        _s(rec.get("nac")), _s(rec.get("wacn")), _s(rec.get("sysid")),
        rec.get("modulation"),
        (None if "phase2" not in rec else (1 if ph else 0)),
        json.dumps(rec.get("control_channels", [])),
        json.dumps(rec.get("sdr", {})),
        json.dumps(rec),
        time.time(),
    )

def _s(v):
    return None if v is None else str(v)

# ----------------------------------------------------------------- writes
def upsert_system(sid, rec):
    with _WLOCK:
        _conn().execute(
            """INSERT INTO systems
                 (id,name,county,type,nac,wacn,sysid,modulation,phase2,control_channels,sdr,doc,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                 name=excluded.name, county=excluded.county, type=excluded.type,
                 nac=excluded.nac, wacn=excluded.wacn, sysid=excluded.sysid,
                 modulation=excluded.modulation, phase2=excluded.phase2,
                 control_channels=excluded.control_channels, sdr=excluded.sdr,
                 doc=excluded.doc, updated_at=excluded.updated_at""",
            _sys_columns(sid, rec))

def delete_system(sid):
    with _WLOCK:
        _conn().execute("DELETE FROM systems WHERE id=?", (str(sid),))

def set_active(sid):
    with _WLOCK:
        _conn().execute("INSERT INTO meta(k,v) VALUES('active',?) "
                        "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                        (json.dumps(sid),))

def sync_all(systems):
    """Make the DB match the whole SYSTEMS dict (the wholesale-save semantics).
    Snapshot the dict first so a concurrent mutation by another thread can't raise
    'dictionary changed size during iteration' mid-sync (callers also hold SYSLOCK)."""
    sysmap = dict(systems.get("systems", {}))
    active = systems.get("active")
    with _WLOCK:
        c = _conn()
        try:
            c.execute("BEGIN")
            keep = set(str(k) for k in sysmap.keys())
            existing = set(r[0] for r in c.execute("SELECT id FROM systems"))
            for gone in existing - keep:
                c.execute("DELETE FROM systems WHERE id=?", (gone,))
            for sid, rec in sysmap.items():
                c.execute(
                    """INSERT INTO systems
                         (id,name,county,type,nac,wacn,sysid,modulation,phase2,control_channels,sdr,doc,updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(id) DO UPDATE SET
                         name=excluded.name, county=excluded.county, type=excluded.type,
                         nac=excluded.nac, wacn=excluded.wacn, sysid=excluded.sysid,
                         modulation=excluded.modulation, phase2=excluded.phase2,
                         control_channels=excluded.control_channels, sdr=excluded.sdr,
                         doc=excluded.doc, updated_at=excluded.updated_at""",
                    _sys_columns(sid, rec))
            c.execute("INSERT INTO meta(k,v) VALUES('active',?) "
                      "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                      (json.dumps(active),))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK"); raise

def get_meta(k, default=None):
    """Read a JSON value from the generic meta key/value table."""
    row = _conn().execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    if not row or row[0] is None:
        return default
    try:
        return json.loads(row[0])
    except Exception:
        return default

def set_meta(k, v):
    """Persist a JSON value under a meta key (e.g. the pager capcode config)."""
    with _WLOCK:
        _conn().execute("INSERT INTO meta(k,v) VALUES(?,?) "
                        "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, json.dumps(v)))

def _name_set(table, ident, name):
    if table not in ("talkgroups", "radios", "rids"):   # whitelist (table name is interpolated)
        raise ValueError("bad table %r" % table)
    with _WLOCK:
        c = _conn()
        if name:
            c.execute("INSERT INTO %s(id,name) VALUES(?,?) "
                      "ON CONFLICT(id) DO UPDATE SET name=excluded.name" % table,
                      (str(ident), name))
        else:
            c.execute("DELETE FROM %s WHERE id=?" % table, (str(ident),))

def set_talkgroup(ident, name): _name_set("talkgroups", ident, name)
def set_radio(ident, name):     _name_set("radios", ident, name)
def set_rid(ident, name):       _name_set("rids", ident, name)

def bulk_set(table, mapping):
    """Upsert many id->name pairs (used by tag/rid CSV import)."""
    if table not in ("talkgroups", "radios", "rids"):
        raise ValueError("bad table %r" % table)
    rows = [(str(k), v) for k, v in (mapping or {}).items() if v]
    with _WLOCK:
        c = _conn()
        try:
            c.execute("BEGIN")
            c.executemany("INSERT INTO %s(id,name) VALUES(?,?) "
                          "ON CONFLICT(id) DO UPDATE SET name=excluded.name" % table, rows)
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK"); raise

# ----------------------------------------------------------------- reads
def load_all():
    c = _conn()
    systems = {}
    for r in c.execute("SELECT id, doc FROM systems"):
        try:
            systems[r[0]] = json.loads(r[1])
        except Exception:
            continue
    active = None
    row = c.execute("SELECT v FROM meta WHERE k='active'").fetchone()
    if row and row[0] is not None:
        try: active = json.loads(row[0])
        except Exception: active = None
    return {"active": active, "systems": systems}

def load_tags():
    c = _conn()
    tg = {r[0]: r[1] for r in c.execute("SELECT id,name FROM talkgroups")}
    rd = {r[0]: r[1] for r in c.execute("SELECT id,name FROM radios")}
    return {"talkgroups": tg, "radios": rd}

def load_rids():
    c = _conn()
    return {r[0]: r[1] for r in c.execute("SELECT id,name FROM rids")}

# ----------------------------------------------------------------- migration
def _count(table):
    return _conn().execute("SELECT COUNT(*) FROM %s" % table).fetchone()[0]

def migrate_from_json(systems_path, tags_path, rids_path):
    """Seed the live DB from the committed JSON files on first run.

    No-op once the systems table is populated, so it is safe to call on every startup.
    The JSON files are the diff-friendly SEED that ships in git; the live lookingglass.db
    is gitignored and mutates freely. The JSONs are LEFT IN PLACE (not archived) so a
    fresh clone always has them and `git pull` of the backend never trips over the db.
    """
    imported = False
    # Guard each table independently so a partially-seeded db (e.g. systems ingested
    # before tags) still gets its tag/rid seed on a later startup.
    if _count("systems") == 0:
        sysobj = _load_json(systems_path)
        if isinstance(sysobj, dict) and sysobj.get("systems"):
            with _WLOCK:
                for sid, rec in sysobj["systems"].items():
                    if isinstance(rec, dict):
                        upsert_system(sid, rec)
                set_active(sysobj.get("active"))
            imported = True
    if _count("talkgroups") == 0 and _count("radios") == 0:
        tagobj = _load_json(tags_path)
        if isinstance(tagobj, dict):
            bulk_set("talkgroups", tagobj.get("talkgroups", {}))
            bulk_set("radios", tagobj.get("radios", {}))
            imported = imported or bool(tagobj.get("talkgroups") or tagobj.get("radios"))
    if _count("rids") == 0:
        ridobj = _load_json(rids_path)
        if isinstance(ridobj, dict) and ridobj:
            bulk_set("rids", ridobj)
            imported = True
    return imported

def _dump_json(obj, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)

def export_systems_json(path):
    """Dump the systems store back to the {"active","systems"} shape."""
    _dump_json(load_all(), path)
    return path

def export_seeds(systems_path, tags_path, rids_path):
    """Write all three JSON seeds from the live DB (for committing data back to git)."""
    _dump_json(load_all(), systems_path)
    _dump_json(load_tags(), tags_path)
    _dump_json(load_rids(), rids_path)
    return (systems_path, tags_path, rids_path)

def _load_json(path):
    try:
        with open(path, encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return None
