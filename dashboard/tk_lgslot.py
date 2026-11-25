# tk_lgslot.py - minimal OP25 multi_rx "trunking" module for Looking Glass TRBO Monitor.
#
# No trunking at all: every decoder stays on its fixed frequency. Its only job is audio slot
# selection. OP25 sends ONE mono audio stream per DMR decoder, so when both timeslots of a repeater
# carry calls at once their voice would interleave. TRBO Monitor (trbo.py) decides which slot each
# repeater should play and writes {"masks": {"<tuner>": 1|2|3}} to the file named in the trunking
# config (chans[0].slot_file); this module applies changes with set_slotid (1 = TS1, 2 = TS2, 3 = both).
#
# multi_rx loads it with:  "trunking": {"module": "tk_lgslot.py", "chans": [{"slot_file": "/tmp/.../trbo_slots.json"}]}
# (Looking Glass puts the dashboard directory on PYTHONPATH so OP25 can import it.)
import json, os, sys, threading, time

class _rx:
    def __init__(self, msgq_id, freq):
        self.msgq_id = msgq_id; self.frequency = freq
        self.current_chan = None; self.current_state = None; self.current_type = None; self.tune_time = 0

class rx_ctl(object):
    def __init__(self, debug=0, frequency_set=None, nbfm_ctrl=None, fa_ctrl=None, chans={}):
        self.debug = debug; self.fa_ctrl = fa_ctrl; self.receivers = {}
        cfg = (chans[0] if isinstance(chans, list) and chans else chans) or {}
        self.slot_file = cfg.get("slot_file") if isinstance(cfg, dict) else None
        self.applied = {}; self._mtime = None
        if self.slot_file:
            threading.Thread(target=self._watch, daemon=True).start()

    # ---- multi_rx hooks (all no-ops except receiver bookkeeping)
    def add_receiver(self, msgq_id, config=None, meta_q=None, freq=0): self.receivers[msgq_id] = _rx(msgq_id, freq)
    def post_init(self): pass
    def set_debug(self, dbglvl): self.debug = dbglvl
    def process_qmsg(self, msg): pass
    def to_json(self): return json.dumps({"json_type": "trunk_update"})
    def dump_tgids(self): pass
    def ui_command(self, cmd, data, msgq_id): pass
    def get_call_log(self): return json.dumps({"json_type": "call_log", "log": []})
    def get_chan_status(self): return json.dumps({"json_type": "channel_update", "channels": []})

    # ---- slot masks from TRBO Monitor
    def _watch(self):
        while True:
            try:
                m = os.path.getmtime(self.slot_file)
                if m != self._mtime:
                    self._mtime = m
                    with open(self.slot_file) as f: masks = json.load(f).get("masks", {})
                    for t in self.receivers:
                        want = int(masks.get(str(t), 3))
                        if self.applied.get(t, 3) != want:
                            self.fa_ctrl({"tuner": t, "cmd": "set_slotid", "slotid": want})
                            self.applied[t] = want
                            if self.debug >= 10:
                                sys.stderr.write("tk_lgslot: tuner %d audio slot mask %d\n" % (t, want))
            except FileNotFoundError:
                pass
            except Exception as e:
                sys.stderr.write("tk_lgslot: %s\n" % e)
            time.sleep(0.15)
