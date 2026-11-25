# SDR Looking Glass (uConsole edition)

A P25 trunked-radio analyzer (plus a MOTOTRBO / DMR monitor and traffic logger) for the **ClockworkPi uConsole** with the **HackerGadgets AIO V2**
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

## TRBO Monitor (`/trbo`): MOTOTRBO / DMR

A second mode for DMR systems, on the same radio and service. Open `http://<uconsole-ip>:8080/trbo`
(or **TRBO Monitor** in the Clockwork Orange Hub). While it runs, P25 trunking is paused;
**Stop (back to P25)** hands the radio back.

- **What it decodes**: conventional MOTOTRBO / DMR Tier II repeaters (both timeslots on every
  repeater at once), **Capacity Max / DMR Tier III** control channels (one grant per PTT, with
  logical channels learned automatically), and Capacity Plus / Connect Plus traffic channels.
- **How**: one fixed-tuned RTL capture per *window* (about 1.6 MHz) with an OP25 DMR decoder per
  frequency inside it, so nothing is missed while you listen. Frequencies are grouped into windows
  automatically. If a site needs more windows than you have RTLs, the windows rotate. Utilization is
  always airtime ÷ time actually monitored, so the numbers stay honest. A second RTL is used when you
  tick it in Setup.
- **Live**: every repeater and timeslot with talkgroup, radio, timer, encryption / emergency flags,
  per-repeater audio, plus the control-channel grants and packet data as they happen.
- **uConsole speaker**: the service itself plays decoded voice on the uConsole (PipeWire), so it works
  with no browser open. On/off, all talkgroups or only selected ones, per-repeater mute and volume,
  from any browser. When both timeslots of a repeater are busy, one plays (a selected talkgroup
  first, otherwise the call that started first), chosen through `tk_lgslot.py`, a tiny OP25 module.
- **Traffic & usage**: any period. Transmissions, airtime, utilization per repeater/slot (average and
  busiest hour), Erlangs per hour, a typical-week heatmap, talkgroup and radio tables, and a **Move to
  P25** calculator: tick talkgroups to see their combined busy-hour load and how many P25 voice paths
  (Erlang B) that needs. CSV export of every transmission, talkgroups, channels, hourly load, GPS and data.
- **Map**: radio GPS from LRRP location reports (and the in-band GPS LC), with trails.
  GPS week-rollover dates from older radios are corrected.
- **Data**: LRRP (GPS), ARS (radio registration), TMS (text messages) and other IP data; decoder counters.
- **Setup**: sites with channels (frequency, color code, per-slot talkgroup), paste rows straight from
  RadioReference, capture-plan preview, extra RTLs, Capacity Max logical-channel map.

Everything is logged to `dashboard/trbo.db` (SQLite; never committed). Two presets ship: BSO Department
of Detention (conventional, RadioReference subcat 31016) and Broward County Capacity Max Site 1 (sid 10234).

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

## Versions

`VERSION` holds the current version (shown in the dashboard); releases are tagged `vX.Y.Z` with notes in
[CHANGELOG.md](CHANGELOG.md) and on the GitHub Releases page.

## License

MIT. OP25, GNU Radio, multimon-ng and rtl-sdr are separate projects under their own licenses.
Monitoring laws vary by country and state: decoding is your responsibility, and so is keeping your
keys to systems you're authorized for.
