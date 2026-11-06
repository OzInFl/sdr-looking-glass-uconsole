# SDR Looking Glass (uConsole edition)

A P25 trunked-radio analyzer for the **ClockworkPi uConsole** with the **HackerGadgets AIO V2**
board (RTL2832U). It runs [OP25](https://github.com/boatbod/op25) (boatbod) behind a web dashboard
on port **8080**, sized for the uConsole's 1280×720 screen and usable from any browser on the LAN.

**Receive only.** This build contains no transmit code.

## Install

The easy way is the [Clockwork Orange](https://github.com/OzInFl/clockwork-orange-uconsole) installer,
which builds OP25, clones this repo to `~/sdr-looking-glass` and sets up the `sdr-looking-glass` service:

```bash
curl -fsSL https://raw.githubusercontent.com/OzInFl/clockwork-orange-uconsole/main/get.sh | bash
```

By hand: build OP25, then

```bash
git clone https://github.com/OzInFl/sdr-looking-glass-uconsole ~/sdr-looking-glass
cd ~/sdr-looking-glass/dashboard
SDR_KIND=rtl OP25_APPS=~/op25/op25/op25/gr-op25_repeater/apps python3 app.py
```

Then open `http://<uconsole-ip>:8080`.

## What it does

- **Trunk tracking**: control-channel decode, grants, affiliations, registrations, encrypted-call
  flags (ALGID / key ID), live audio per talkgroup with a gain boost, talkgroup pin/lock (**Listen TG**).
- **CC Viewer** (`/cc`): every control-channel broadcast decoded into plain English (RFSS / network
  status, adjacent sites, channel identifiers, vendor TSBKs, data-channel announcements).
  **System Hunter** seeks or sweeps a band and runs each system it finds without saving it.
- **Diagnostics**: rule-based health checks (signal, sync losses, OP25 load, temperature, GPS, clock).
- **Drive Survey**: GPS + control-channel health logged every 2 s to CSV, with a KML map coloured by decode rate.
- **Radio Diagnostics / Watch**: per-radio uplink quality and voice error rate; printable watch reports.
- **RSSI** meter (needs the OP25 patch Clockwork Orange applies), **Pager** tab (POCSAG via
  `rtl_fm | multimon-ng`), radio **Map** (GPS / LRRP), **Scanner** (band discovery).
- **Keys**: test your *own* AES-256 / DES-OFB / ADP keys. They're stored in
  `~/.config/sdr-looking-glass/crypt_keys.json` (mode 0600), never in the database, and the API never returns them.
- **uConsole mode**: zoom, one panel per view with a tab strip, fullscreen.

## Configuration (environment)

| Variable | Meaning |
|---|---|
| `SDR_KIND` | `rtl` for the uConsole (RTL defaults, ppm clamp ±200) |
| `SDR_ARGS` | osmosdr device string, e.g. `rtl=<serial>` to pin a specific dongle |
| `SDR_PPM` | the dongle's frequency correction (calibrate on a known control channel) |
| `OP25_APPS` | path to OP25's `gr-op25_repeater/apps` |

Systems, talkgroup tags and radio aliases live in `dashboard/lookingglass.db` (created on first run,
never committed). Add systems with **Import** (RadioReference / CSV) or find them with System Hunter.

## Test without a radio

```bash
python3 dashboard/test_pocsag.py
```

## License

MIT. OP25, GNU Radio, multimon-ng and rtl-sdr are separate projects under their own licenses.
Monitoring laws vary by country and state: decoding is your responsibility, and so is keeping your
keys to systems you're authorized for.
