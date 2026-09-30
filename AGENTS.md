# Agent Documentation

## Standard

This project follows the **Go Project Standard v3.0**
(local reference: `/home/dom/Dokumente/Git/go-project-standard.md`; will be replaced by a URL).
That document holds the rules for every Go project: dependency injection, composition root,
errors, logging, lifecycle, code style, testing, tooling, Git workflow and review criteria.

This file holds only what is specific to datalogger. It wins for project-specific questions;
a deviation from a MUST rule of the standard is only valid if it is listed under
"Deviations" below with its reason. When code and this file disagree, fix one of them in
the same change.

**Current state:** the code follows standard v3 except for the deviations listed below.

---

## 1. Project Overview

Datalogger polls **devices** and forwards every reading to one or more **outputs**.

- **Inputs:** Modbus TCP/RTU (direct or range register reads), HTTP APIs (JSON via gjson
  paths, or the whole body as text).
- **Outputs:** InfluxDB 3.x, MQTT, CSV files (with age-based rotation).
- **Routing:** each output lists the devices it accepts; every reading of such a device is
  copied to that output.
- **Timestamps** are taken when the data is received from the device (not when the poll
  starts), in UTC.
- **Runtime:** a single binary configured by one YAML file (`-config`), no HTTP server, no
  database of its own. Shipped as binaries and a multi-arch `scratch` image on ghcr.io.

---

## 2. Tools

```bash
make check                                                # ./scripts/pre-commit.sh, check-only
golangci-lint fmt --config .golangci.yml                  # gofumpt + goimports
golangci-lint run --config .golangci.yml                  # full linter set from the standard
go run golang.org/x/tools/cmd/deadcode@v0.50.0 -test ./... # unused exported code
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...     # known vulnerabilities
go test -race ./...                                       # all unit tests, as in CI
go run github.com/vektra/mockery/v2@v2.53.7               # regenerate mocks (.mockery.yaml)
make build                                                # ./datalogger with version info (ldflags)
./datalogger -config config.yaml [-debug]                 # run; -version prints build info
docker compose -f docker-compose.dev.yaml up --build      # local image with ./config.yaml, ./test-dump
```

- CI: `.github/workflows/checks.yml` (push to main, PRs, reused by the release).
- Release: push a `vX.Y.Z` tag on `main`; `release.yml` runs the checks, then goreleaser
  (`.goreleaser.yaml`) builds archives and `ghcr.io/dombyte/datalogger` images
  (linux/amd64, arm64, arm/v7).
- Mocks: every interface is listed in `.mockery.yaml`; the output in `<pkg>/mocks` is never
  edited by hand, and CI fails when it differs from a fresh generation. Tests that use a
  package's mocks are black-box tests (`package <pkg>_test`), because the mocks import
  the package.

---

## 3. Project Structure

```
cmd/datalogger/          main: flags, logger, build info, config, signal context, exit code;
                         startup test runs run() end to end (HTTP device, CSV output, SIGINT)
internal/app/            composition root: Create* factories (config → Settings), routing,
                         Run/Shutdown
internal/config/         Config structs (viper/mapstructure), Load() (defaults + Validate)
internal/clock/          Clock interface (Now, tickers, After) + Sleep; clocktest/ fake
internal/datasource/     DataPoint type, DeviceReader interface
  modbus/                Modbus TCP/RTU reader: direct + range mode, chunking, reconnect
  http/                  HTTP reader: GET/POST, headers/body, JSON (gjson) / text
internal/output/         Writer interface
  csv/                   CSV writer: flush per row, rotation by max_age, backup cleanup
  influxdb/              InfluxDB 3 writer: batching, retries, v3 write API
  mqtt/                  MQTT v5 writer (paho.golang): one topic per point, JSON payload
internal/**/mocks/       mockery output
example/                 config.yaml (full reference) and docker-compose.yaml for users
scripts/pre-commit.sh    local checks (make check)
```

Each reader/writer package has its own `Settings` (and `Point`) struct, a `Deps` struct
validated by `New` (`ErrMissingDependency`), small interfaces for its client (`Client`,
`Dialer`, `Publisher`) and a constructor for the real client (`NewDialer`, `NewClient`).
Constructors never connect.

---

## 4. Dependency Direction

Imports (paths below `internal/`):

```
cmd/datalogger    → app, clock, config
app               → clock, config, datasource{,/modbus,/http}, output{,/csv,/influxdb,/mqtt}
datasource/modbus → clock, datasource, simonvetter/modbus
datasource/http   → clock, datasource, gjson
output            → datasource
output/csv        → clock, datasource
output/influxdb   → clock, datasource, influxdb3-go
output/mqtt       → clock, datasource, paho.golang
config            → viper
clock, datasource → stdlib only
```

Rules:
- Only `app` and `cmd/datalogger` import `config`; `app` maps config onto each package's
  `Settings`.
- Readers never import outputs and outputs never import readers; they only share
  `datasource.DataPoint`. Routing lives in `app`.
- Reader/writer packages never import each other (`modbus` ↛ `http`, `csv` ↛ `mqtt`, …).
- Nothing but `cmd/datalogger` imports `app`.

---

## 5. Data Flow and Ownership

```
DeviceReader.Start ──data channel (unbuffered)──▶ router goroutine (one per device, in app)
                                                    │ non-blocking send, copy per output
                                                    ▼
                                  per-output input channel (buffer_size, default 1000)
                                                    ▼
                                           Writer.Start (one goroutine per output)
```

- **Readers own** their data channel: they are the only senders and close it when they
  stop. A poll sends all its points after the reads are done.
- **The router** sends each point to every output whose `devices` list contains the device.
  The send is **non-blocking**: if an output channel is full, the point is **dropped for
  that output only** and a warning is logged. A slow output never blocks a device or the
  other outputs.
- **`app` owns** the output input channels and closes them during shutdown, after all
  routers have finished.
- **Writers own** their external resource (file, broker connection, InfluxDB client) and
  are the only code that writes to it. The channel returned by `Writer.Start` is closed
  when the writer has flushed and stopped (after an error on it, if it failed).
- There is no shared mutable state between components; all hand-over is by channel.

---

## 6. Lifecycle

### Startup
1. `cmd/datalogger`: parse flags, build the logger (console on stderr, `-debug` = debug
   level), log the build info, `config.Load` (defaults + validation).
2. `app.New` creates every reader and writer (`Create*` factories in `app/factory.go`).
   A **construction error** (an address or setting the client library rejects, a point
   outside the Modbus ranges, an HTTP request that cannot be built) ends the process with
   exit 1. **Connecting is not construction:** readers connect on their first poll, so a
   device that is down at startup does not stop the others and recovers on its own.
3. `app.Run` starts the writers, then the readers with one router each.

### Failure handling
- Poll errors are handled inside the reader: exponential backoff (100 ms doubling, max
  30 s, waited on the injected clock and cut short by shutdown), and a reconnect on
  connection errors, detected by type: Modbus on `net.OpError`, EOF, closed connection,
  `ECONNRESET`, `EPIPE` (the next poll dials again; timeouts and Modbus exceptions keep
  the connection); HTTP closes idle connections on every transport error (`net.Error`)
  and truncated bodies (status and parse errors do not). A reader never stops because of
  read errors.
- Write errors are handled inside the writer (InfluxDB: retries, then the batch is
  dropped and logged; MQTT/CSV: log and continue with the next point).
- A reader or writer that **stops** while the app is not shutting down makes `Run`
  return `ErrComponentStopped`; the process shuts down and exits 1, and the container
  runtime (`restart: unless-stopped`) restarts it. There is no in-process restart.

### Shutdown (producers first)
1. SIGINT/SIGTERM cancels the signal context (`signal.NotifyContext`); `Run` returns.
2. `Shutdown` cancels the readers: no new poll starts and a backoff wait ends at once.
   A poll that is already running finishes and its points are still delivered (an HTTP
   request in flight is aborted, so it has no data yet). Each reader then closes its
   data channel and the routers finish.
3. `app` closes the output input channels; each writer writes what is queued, flushes
   (CSV flush/close, InfluxDB final batch, MQTT disconnect) and stops.
4. `main` bounds all of this with one hard deadline of **8 s** (below Docker's 10 s stop
   timeout); after it the process exits 1. There is no second-signal force mode: a
   second signal is ignored, the deadline is the force.

Exit codes: 0 after a clean shutdown; 1 for startup errors, a stopped component, a
writer that failed, or a missed deadline.

---

## 7. Behaviour Reference

### Modbus reader
- Address URL: `tcp://host:port` or `rtu:///dev/ttyUSB0` (RTU: `speed`, `data_bits`,
  `parity` N/E/O, `stop_bits`). `slave_id` is required.
- **Register type** is chosen once per device from the first point with
  `function_code` 3 (holding) or 4 (input); default holding. Mixing 3 and 4 in one device
  is rejected by validation; use two devices.
- **Direct mode:** one read per point (`register`, `count`, default 1). Points are read with
  up to `parallelism` concurrent requests. A failed point is logged and skipped; the rest
  of the poll is still delivered.
- **Range mode:** the `ranges` (`"start-end"`, inclusive) are read in chunks of at most
  **125 registers** (Modbus protocol limit), then points are decoded from the collected
  registers. Every point address must be covered by a range (checked at construction).
  **Any failed chunk fails the whole poll** (no partial data).
- Each point's timestamp is the receive time of its data (direct: its own read; range: the
  chunk that holds its first register).
- Types: `int16`, `uint16`, `int32`, `uint32`, `float32`, `bool` (others are rejected by
  validation); value = raw × `scale` + `offset`, always delivered as float64. `scale`
  defaults to 1 (an explicit 0 is treated as unset). 32-bit types need `count: 2` (high
  word first); a smaller count is rejected by validation.
- Most Modbus devices handle only one request at a time: use `parallelism: 1` unless the
  device is known to support more.

### HTTP reader
- `method` GET (default) or POST with `body`; `headers` are sent as given; `insecure`
  disables certificate verification for this device.
- `response_type`: `json` (default; values via gjson `json_path`; `type` float*/int*/uint*/
  bool/string converts, otherwise numbers become float64 and bools stay bool) or `text`
  (the whole body as one string); anything else is rejected by validation. JSON points
  need a `json_path`.
- `scale`/`offset` apply to numeric JSON values: value × scale + offset as float64. With
  the defaults (1, 0) the value keeps its parsed type, so existing InfluxDB field types do
  not change; bools and strings are never scaled.
- Non-200 responses are errors. All points of one response share its receive timestamp.
- A point that cannot be extracted (e.g. its `json_path` is not in the response) is
  skipped; it is warned about once, further failures are logged at debug, and an info
  line follows when it can be read again.
- `parallelism` has no effect: one request per poll, points are parsed in order.
- A request that cannot be built (bad URL, method) fails startup; an unreachable endpoint
  does not.

### InfluxDB 3 writer
- Schema: measurement = device name, tags `point` and `unit` (unit omitted when empty),
  one field `value`, timestamp = reading time. The README documents queries on this
  schema; changing it is a breaking change (`!`).
- Batching: a full `batch_size` (default 10000) is written at once, a partial batch every
  `batch_timeout` (default 1 s). Retries: `max_retries` (default 3) with `retry_delay`
  (default 1 s); after that the batch is dropped and logged. The last batch is written
  when the input is closed on shutdown. The client does not connect at startup.
- Uses the v3 write API; gzip above 1000 bytes; `insecure` skips certificate checks.

### MQTT writer
- Broker `address` `tcp://`/`mqtt://` or `tls://`/`ssl://`/`mqtts://` (no scheme = tcp;
  other schemes fail at startup). TLS verifies the broker certificate against the host
  name; `insecure` uses TLS without verification (also for `tcp://`). MQTT v5, keep-alive
  30 s, clean start, 10 s connect timeout.
- Connects on the first point and reconnects after a publish error or a lost connection,
  with backoff (1 s doubling to 30 s). Points that arrive while no connection is possible
  are **dropped**; the count is logged on the next successful connect.
- Topic: `<topic>/<device>/<point>` (`topic` default `datalogger`); payload
  `{"value": …, "unit": "…", "timestamp": "<RFC3339Nano>"}`; `qos` 0–2, `retain`.
- `client_id` defaults to `logger-<8 random chars>`; username/password are optional.
- Topic and payload format are a public interface; changing them is a breaking change (`!`).

### CSV writer
- Columns: `timestamp` (RFC3339Nano), `device`, `point`, `value`, `unit`; header written
  when the file is empty. Every point is flushed immediately.
- Rotation: when the file is older than `max_age` (checked after each row), it is renamed
  to `<file>.<YYYYMMDD-HHMMSS>` and a new file is started. `max_backups`: 0 = keep none,
  > 0 = keep that many, < 0 = keep all; only files with exactly that name pattern are
  ever deleted.
- A path that cannot be opened fails startup.
- The directory is created if missing. In the container, write below `/app/data`
  (mounted from `./data`).

### Configuration
- One YAML file (`-config`, required); template with every option: `example/config.yaml`.
- Device and output names must be unique; outputs may only reference existing devices;
  every device needs at least one point; `parallelism` 1–100; `poll_interval` > 0;
  `timeout` > 0.
- `config.Load` reads the file, fills defaults (`applyDefaults`: device `timeout` = the
  poll interval, at most 10 s; point `scale` 1; HTTP method GET and response type json;
  MQTT topic `datalogger` and client ID `logger-<random>`) and then
  runs `Validate()`, which only checks and never changes the config.
- No environment overrides (see Deviations). Secrets (InfluxDB token, MQTT password)
  belong in the local `config.yaml` (gitignored, mounted read-only in the container),
  never in `example/config.yaml` with real values.

---

## 8. Design Decisions

- **Drop instead of block:** a full output channel drops the point for that output only. A
  stalled output must not stop polling or starve other outputs. Tune with `buffer_size`.
- **Receive-time timestamps in UTC:** readings are stamped when the device answered, so
  slow polls do not shift data; UTC matches InfluxDB and keeps CSV unambiguous.
- **Producers-first shutdown:** sources stop first, outputs drain afterwards, so no reading
  that was already taken is lost on a normal stop. One hard deadline (8 s, below Docker's
  10 s) bounds it; a slow final InfluxDB retry can use most of it.
- **Soft vs. hard startup failures:** components connect lazily and retry, so one device
  or broker that is down does not stop the others; settings a client library rejects are
  configuration errors and fail startup, where they are noticed at once.
- **Drop while an output is disconnected (MQTT):** buffering in memory for an unbounded
  outage would only move the loss; the dropped count is logged instead.
- **Exit on component death:** a reader/writer that ends unexpectedly is a bug or an
  unrecoverable state; exiting 1 and letting the container restart the process is simpler
  and safer than restarting single components in-process.
- **Range mode fails whole polls:** the points of one range are meant to be a consistent
  snapshot; a partial snapshot would mix old gaps with new values.
- **One register type per Modbus device:** keeps range planning simple; devices that need
  both are configured twice.
- **InfluxDB schema with `point` as tag:** one table per device with a fixed column set,
  queryable by point without schema changes when points are added.

---

## 9. Deviations from the Standard

Everything else in the code that does not match the standard is a backlog item below,
not an accepted deviation. Add a row here (rule, deviation, reason) only for a choice that
is meant to stay:

| Rule | Deviation | Reason |
|---|---|---|
| 5, 13: env overrides, secrets from env | Config comes only from the YAML file; no env overrides | The config is mostly lists of devices/outputs that env vars cannot address sensibly; the gitignored `config.yaml`, mounted read-only, serves as the secret file |
| 9: every interface has a mockery mock | `clock.Clock`/`clock.Ticker` are faked by the hand-written `clocktest.Fake` | Tests need time that moves consistently across tickers and `After` (`Advance`); call expectations cannot model that, and standard 9 itself asks for a fake clock |

---

## 10. Migration Backlog

Known gaps to standard v3, one `refactor/…` branch each; update this list when an item
is done.

None. Add new gaps here as they are found.

---

## 11. Adding a Device or Output Type

1. New package under `internal/datasource/<type>` or `internal/output/<type>` implementing
   the reader or writer contract: `Settings`, `Deps` checked in `New`, small interfaces
   for its client (listed in `.mockery.yaml`), the injected `clock.Clock` for any waiting,
   and no connecting in the constructor.
2. Config: add the type-specific struct under `DeviceSpecific`/`OutputSpecific`, its
   validation, and a commented example in `example/config.yaml`.
3. Composition root: add the `case` and a `create…` function in `internal/app/factory.go`
   that maps config onto `Settings` and creates the real client. Nothing else may
   reference the new package.
4. Behaviour: document timestamps, error/backoff behaviour and any public format (topics,
   columns, schema) in section 7.
5. Tests with mocks/fakes only (no real device, broker or DB); an optional integration test
   behind the build tag.
6. README: user-facing documentation of the new type.

---

## 12. Commit Scopes

Conventional Commits per the standard; no AI signatures. Scopes used here:

`config`, `modbus`, `http`, `csv`, `influxdb`, `mqtt`, `routing`, `shutdown`, `release`,
`docker`, `ci`, `deps`

Examples:

```
feat(mqtt): support TLS client certificates
fix(modbus): stop backoff wait on shutdown
refactor(config): move defaults out of Validate
feat(influxdb)!: store unit as field instead of tag
```
