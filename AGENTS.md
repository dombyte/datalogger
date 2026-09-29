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

**Current state:** the code predates standard v3. The gaps are listed under
"Migration backlog"; new code follows the standard, and existing code is migrated in
dedicated `refactor/` branches (with user confirmation), not inside feature work.

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
make build                                                # ./datalogger with version info (ldflags)
./datalogger -config config.yaml [-debug]                 # run; -version prints build info
docker compose -f docker-compose.dev.yaml up --build      # local image with ./config.yaml, ./data
```

- CI: `.github/workflows/checks.yml` (push to main, PRs, reused by the release).
- Release: push a `vX.Y.Z` tag on `main`; `release.yml` runs the checks, then goreleaser
  (`.goreleaser.yaml`) builds archives and `ghcr.io/dombyte/datalogger` images
  (linux/amd64, arm64, arm/v7).
- There is no mockery setup yet (see Migration backlog); once added, mocks are generated
  with `go run github.com/vektra/mockery/v2@v2.53.7` and checked for drift in CI.

---

## 3. Project Structure

```
main.go                  flag parsing, logger, wiring, routing, shutdown (composition root)
config/                  Config structs (viper/mapstructure), Load() (defaults + Validate)
datasource/              DataPoint type, DeviceReader interface
  modbus/                Modbus TCP/RTU reader: direct + range mode, chunking, backoff, reconnect
  http/                  HTTP reader: GET/POST, headers/body, JSON (gjson) / text, backoff
output/                  Writer interface
  csv/                   CSV writer: flush per point, rotation by max_age, backup cleanup
  influxdb/              InfluxDB 3 writer: batching, retries, v3 write API
  mqtt/                  MQTT v5 writer (paho.golang): one topic per point, JSON payload
example/                 config.yaml (full reference) and docker-compose.yaml for users
scripts/pre-commit.sh    local checks (make check)
```

Target layout (standard 8.3), reached through the migration backlog:

```
cmd/datalogger/main.go   flags, logger, signal context, exit code only
internal/app/            composition root: Create* factories, routing, lifecycle
internal/config/         YAML, Validate() without side effects
internal/datasource/     DataPoint, reader contract; modbus/, http/
internal/output/         writer contract; csv/, influxdb/, mqtt/
internal/<pkg>/mocks/    mockery output
```

---

## 4. Dependency Direction

Current imports:

```
main              → config, datasource, datasource/{modbus,http}, output, output/{csv,influxdb,mqtt}
datasource/modbus → config, datasource, simonvetter/modbus
datasource/http   → config, datasource, gjson
output            → datasource
output/csv        → config, datasource
output/influxdb   → config, datasource, influxdb3-go
output/mqtt       → config, datasource, paho.golang
config            → viper
datasource        → stdlib only
```

Rules:
- Readers never import outputs and outputs never import readers; they only share
  `datasource.DataPoint`. Routing lives in the composition root.
- Reader/writer packages never import each other (`modbus` ↛ `http`, `csv` ↛ `mqtt`, …).
- Nothing imports `main`/`internal/app`.
- Target (standard 3.2): only the composition root imports `config`; every reader/writer
  package declares its own `Settings` struct, mapped from config by the root.

---

## 5. Data Flow and Ownership

```
DeviceReader.Start ──dataCh (unbuffered)──▶ router goroutine (one per device)
                                              │ non-blocking send, copy per output
                                              ▼
                              per-output channel (buffer_size, default 1000)
                                              ▼
                                     Writer.Start       (one goroutine per output)
```

- **Readers own** their data channel and are the only senders on it. A poll sends all its
  points after the reads are done.
- **The router** sends each point to every output whose `devices` list contains the device.
  The send is **non-blocking**: if an output channel is full, the point is **dropped for
  that output only** and a warning is logged (debug level logs the point). A slow output
  never blocks a device or the other outputs.
- **Writers own** their external resource (file, broker connection, InfluxDB client) and
  are the only code that writes to it.
- There is no shared mutable state between components; all hand-over is by channel.

---

## 6. Lifecycle

### Startup
1. Parse flags, build the logger (console writer on stderr, `-debug` = debug level).
2. `config.Load` (defaults + validation); any error ends the process (exit 1).
3. Create readers and writers from config. **Current behaviour:** a device or output that
   fails to construct (e.g. Modbus not reachable, MQTT broker not reachable) is logged and
   **skipped**; the app runs without it (see Migration backlog).
4. Start writers (output context), start readers (source context), start routing, start the
   component monitor.

### Failure handling
- Poll errors are handled inside the reader: exponential backoff (100 ms doubling, max
  30 s), and a reconnect on connection errors, detected by type: Modbus on `net.OpError`,
  EOF, closed connection, `ECONNRESET`, `EPIPE` (timeouts and Modbus exceptions keep the
  connection); HTTP on every transport error (`net.Error`) and truncated bodies (status
  and parse errors do not). A reader never exits because of read errors.
- Write errors are handled inside the writer (InfluxDB: retries, then the batch is
  dropped and logged; MQTT/CSV: log and continue with the next point).
- A reader or writer that **exits** while the app is not shutting down is fatal: the
  component monitor logs it and the process exits 1; the container runtime
  (`restart: unless-stopped`) restarts it. There is no in-process restart.

### Shutdown (two-phase)
1. SIGINT/SIGTERM: cancel the **source** context; readers stop polling (no new data).
2. Outputs keep running for up to **10 s** to drain their channels.
3. Then the **output** context is cancelled; each writer drains what is left (bounded by
   its own 10 s timeout), flushes (CSV flush/close, InfluxDB final batch, MQTT disconnect)
   and exits.
4. `main` waits for all writers, then exits.

Producers-first ordering is deliberate and stays. The second-signal force mode is a
deviation scheduled for removal (see Migration backlog).

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
- `parallelism > 1` parses points concurrently (the request itself is one call).

### InfluxDB 3 writer
- Schema: measurement = device name, tags `point` and `unit` (unit omitted when empty),
  one field `value`, timestamp = reading time. The README documents queries on this
  schema; changing it is a breaking change (`!`).
- Batching: `batch_size` (default 10000) or `batch_timeout` (default 1 s), whichever comes
  first. Retries: `max_retries` (default 3) with `retry_delay` (default 1 s); after that the
  batch is dropped and logged.
- Uses the v3 write API; gzip above 1000 bytes; `insecure` skips certificate checks.

### MQTT writer
- Broker `address` `tcp://`, `tls://`/`ssl://` (scheme defaults to tcp); `insecure` wraps in
  TLS without verification. MQTT v5, keep-alive 30 s, clean start.
- Topic: `<topic>/<device>/<point>` (`topic` default `datalogger`); payload
  `{"value": …, "unit": "…", "timestamp": "<RFC3339Nano>"}`; `qos` 0–2, `retain`.
- `client_id` defaults to `logger-<8 random chars>`; username/password are optional.
- Topic and payload format are a public interface; changing them is a breaking change (`!`).

### CSV writer
- Columns: `timestamp` (RFC3339Nano), `device`, `point`, `value`, `unit`; header written
  when the file is empty. Every point is flushed immediately.
- Rotation: when the file is older than `max_age`, it is renamed to
  `<file>.<YYYYMMDD-HHMMSS>` and a new file is started. `max_backups`: 0 = keep none,
  > 0 = keep that many, < 0 = keep all.
- The directory is created if missing. In the container, write below `/app/data`
  (mounted from `./data`).

### Configuration
- One YAML file (`-config`, required); template with every option: `example/config.yaml`.
- Device and output names must be unique; outputs may only reference existing devices;
  every device needs at least one point; `parallelism` 1–100; `poll_interval` > 0.
- `config.Load` reads the file, fills defaults (`applyDefaults`: HTTP method GET and
  response type json, MQTT topic `datalogger` and client ID `logger-<random>`) and then
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
- **Two-phase shutdown:** sources stop first, outputs drain afterwards, so no reading that
  was already taken is lost on a normal stop.
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

---

## 10. Migration Backlog

Known gaps between the current code and standard v3, in suggested order. Each item is its
own `refactor/…` branch; update this list when an item is done.

1. **Dependency injection for clients:** Modbus, HTTP, InfluxDB and MQTT clients are created
   inside the packages. Declare small client interfaces in each package, create the real
   clients in `Create*` factories, add mockery mocks and testify-based tests.
2. **Injected `Clock`:** replace `time.Now`, `time.Sleep` (backoff) and `time.After` in loops
   with an injected clock; backoff waits must also stop on context cancel (today
   `time.Sleep` delays shutdown by up to 30 s).
3. **Startup failures:** a device/output that cannot be constructed is skipped today. It
   should either fail startup (exit 1) or start in a recovering state and reconnect; the
   choice goes into "Design Decisions".
4. **Lifecycle per standard 4.2/4.3:** `signal.NotifyContext`, `run() int` with exit code,
   one hard shutdown deadline, **remove the second-signal force mode**, replace the
   sleep-polling `monitorComponents` + `logger.Fatal` with an error channel/`errgroup`
   that makes `Run` return an error. The unused per-component `errCh` of readers goes away.
5. **Layout:** move to `cmd/datalogger` + `internal/…` (section 3 target), with the
   composition root in `internal/app` and per-package `Settings` structs.

---

## 11. Adding a Device or Output Type

1. New package under `datasource/<type>` or `output/<type>` implementing the reader or
   writer contract; its own settings struct and small interfaces for its client.
2. Config: add the type-specific struct under `DeviceSpecific`/`OutputSpecific`, its
   validation, and a commented example in `example/config.yaml`.
3. Composition root: add the `case` to the factory. Nothing else may reference the new
   package.
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
