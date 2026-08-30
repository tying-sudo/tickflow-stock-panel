# TDX LAN Gateway plugin

This is an optional L1 data-provider extension.  It must stay within this
directory so upstream Tick Stock Panel updates can be merged without replacing
the core quote, cache, API, or UI paths.

## Deployment boundary

- Windows TDX VM runs `windows_gateway.py` and is the only component that
  talks to TDX Quant at `127.0.0.1:17709`.
- Tick Stock Panel stores only `TDX_GATEWAY_URL` and `TDX_GATEWAY_TOKEN` in
  its instance configuration.  Never copy TDX login, brokerage, account, or
  trading credentials to Linux.
- The gateway exposes only `/v1/health`, `/v1/kline`, and `/v1/realtime`.
  Realtime accepts an explicit symbol list only.  When the user enables the
  full-market scope, Tick Stock Panel resolves that configured universe and
  sends it in batches of at most 50; the Windows gateway caps TDX at six
  concurrent snapshot calls.  Do not add order, account, or trading routes.
- Daily K requests declare `adjust=none|front|back`.  The provider stores raw
  daily K, and derives an event factor by comparing TDX `front` and `none`
  closes.  Never write TDX `ForwardFactor` directly as `ex_factor`: its
  documented value is carried between events and is not a per-event ratio.
- TDX daily `Volume` is reported in shares.  The plugin converts it to lots
  (100 shares) before writing daily storage, because the core turnover formula
  uses the project's existing lots contract.  Do not move this conversion into
  core indicator code.
- TDX snapshots expose `Buyp/Buyv/Sellp/Sellv` five-element arrays.  The
  plugin maps these to the existing read-only `depth5.batch` contract for
  sealed-board detection; zero levels are preserved and no trading route is
  exposed.
- K-line requests are limited to six concurrent one-symbol TDX calls.  This
  ceiling was verified against the Windows client and protects it from
  unbounded full-market fan-out; do not raise it merely to accelerate a job.

## Upgrade check

After merging an upstream release, run `scripts/upgrade_check.py <target>` and
then verify the disabled-plugin, daily-K, minute-K, gateway-auth, and cache
regressions.  TDX minute requests are calendar-date bounded by TDX and the
gateway applies the requested intra-day time range after receiving those real
records.  TDX snapshots are merged through Tick Stock Panel's existing
full-market quote path, cache, persistence and SSE flow; no parallel realtime
pipeline is introduced.
