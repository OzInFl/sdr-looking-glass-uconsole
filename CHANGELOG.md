# Changelog

SDR Looking Glass, uConsole edition. Versions follow [semver](https://semver.org); each release is a git tag
`vX.Y.Z` with a GitHub Release. Clockwork Orange pins the version it installs in its `LOOKINGGLASS_VERSION`.

## [Unreleased]
- **TRBO Monitor** (`/trbo`): MOTOTRBO / DMR conventional repeaters and Capacity Max on OP25, every repeater
  and timeslot at once from one RTL capture window; live calls + audio, Capacity Max grants with learned
  logical channels, LRRP / ARS / TMS packet data, GPS map, and traffic logging (SQLite `trbo.db`) with
  utilization, Erlangs, heatmap, talkgroup / radio tables, a P25 migration calculator and CSV export
- TRBO uConsole speaker: decoded voice on the uConsole speaker (on/off, all or selected talkgroups, per-repeater
  mute, volume) with timeslot priority so two calls on one repeater never interleave (`tk_lgslot.py`)
- RTL hand-off recovery: if the dongle drops off USB when the decoder restarts, its power rail is cycled

## [1.0.1] - 2026-11-06
- Diagnostics: an OP25 audio websocket closing is info, not a software error

## [1.0.0] - 2026-11-06
First public release.
- P25 trunk tracking on OP25 with an RTL-SDR (`SDR_KIND=rtl`): calls, live audio, Listen TG, Boost
- CC Viewer with plain-English control-channel decode and System Hunter (seek / band hunt)
- Diagnostics, Drive Survey (CSV + KML), Radio Diagnostics, Watch reports
- RSSI meter, Pager tab (rtl_fm + multimon-ng), radio map (OpenStreetMap), Scanner
- Keys for your own AES-256 / DES-OFB / ADP keys (stored outside the database)
- uConsole mode for the 1280×720 screen
