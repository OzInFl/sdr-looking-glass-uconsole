# Changelog

SDR Looking Glass, uConsole edition. Versions follow [semver](https://semver.org); each release is a git tag
`vX.Y.Z` with a GitHub Release. Clockwork Orange pins the version it installs in its `LOOKINGGLASS_VERSION`.

## [1.0.0] - 2026-11-06
First public release.
- P25 trunk tracking on OP25 with an RTL-SDR (`SDR_KIND=rtl`): calls, live audio, Listen TG, Boost
- CC Viewer with plain-English control-channel decode and System Hunter (seek / band hunt)
- Diagnostics, Drive Survey (CSV + KML), Radio Diagnostics, Watch reports
- RSSI meter, Pager tab (rtl_fm + multimon-ng), radio map (OpenStreetMap), Scanner
- Keys for your own AES-256 / DES-OFB / ADP keys (stored outside the database)
- uConsole mode for the 1280×720 screen
