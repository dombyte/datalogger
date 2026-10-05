# Agent Documentation

This file is the complete guide for working on datalogger: what the program does, how it is
built, and the rules every change follows (architecture, errors, logging, code style,
testing, tooling, Git workflow, review). It needs no other document.

Keywords: **MUST** = blocker if violated, **SHOULD** = fix unless there is a written
reason, **MAY** = allowed option. When code and this file disagree, fix one of them in
the same change.

**Current state:** the code follows every rule below; open gaps are listed in section 13.

---

## 1. Project Overview

Datalogger polls **devices** and forwards every reading to one or more **outputs**.

- **Inputs:** Modbus TCP/RTU (direct or range register reads), HTTP APIs (JSON via gjson
  paths, or the whole body as text).
- **Outputs:** InfluxDB 3.x, MQTT, CSV files (with age-based rotation), a read-only HTTP
  API with the latest value of every point.
- **Transforms:** a point may have an `expr` (expr-lang) that changes its value after a
  poll, using the other points of that poll and named lookup tables.
- **Routing:** each output lists the devices it accepts; every reading of such a device is
  copied to that output, except the points it lists in `exclude_points`.
- **Timestamps** are taken when the data is received from the device (not when the poll
  starts), in UTC.
- **Runtime:** a single binary configured by one YAML file (`-config`), no database of its
  own; the only HTTP server is the optional `api` output. Shipped as binaries and a multi-arch `scratch` image on ghcr.io.

---

## 2. Tools

```bash
make check                                                # ./scripts/pre-commit.sh, check-only
golangci-lint fmt --config .golangci.yml                  # gofumpt + goimports
golangci-lint run --config .golangci.yml                  # full linter set (section 10)
go run golang.org/x/tools/cmd/deadcode@v0.51.0 -test ./... # unused exported code
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...     # known vulnerabilities
go test -race ./...                                       # all unit tests, as in CI
go tool mockery                                           # regenerate mocks (.mockery.yaml)
make build                                                # ./datalogger with version info (ldflags)
./datalogger -config config.yaml [-debug]                 # run; -version prints build info
docker compose -f docker-compose.dev.yaml up --build      # local image with ./config.yaml, ./test-dump
```

- CI: `.github/workflows/checks.yml` (PRs, pushes to main, reused by the release; see
  section 11.1).
- Release: push a `vX.Y.Z` (or `vX.Y.Z-rcN`) tag on `main`; `release.yml` runs the
  checks, then goreleaser (`.goreleaser.yaml`) builds archives and
  `ghcr.io/dombyte/datalogger` images (linux/amd64, arm64, arm/v7).
- Mocks: mockery v3 runs as a Go tool (`tool` directive in `go.mod`, so Renovate updates
  it with the modules). Every interface is listed in `.mockery.yaml` (`Mock<Interface>`,
  `NewMock<Interface>(t)`); the output in `<pkg>/mocks` is never edited by hand, and CI
  fails when it differs from a fresh generation. Tests that use a package's mocks are
  black-box tests (`package <pkg>_test`), because the mocks import the package.

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
internal/transform/      point expressions (expr-lang): compile at startup, Apply per poll
internal/output/         Writer interface
  csv/                   CSV writer: flush per row, rotation by max_age, backup cleanup
  influxdb/              InfluxDB 3 writer: batching, retries, v3 write API
  mqtt/                  MQTT v5 writer (paho.golang): one topic per point, JSON payload
  api/                   HTTP API: latest value per point in memory, read-only JSON
internal/**/mocks/       mockery output
example/                 config.yaml (full reference) and docker-compose.yaml for users
scripts/pre-commit.sh    local checks (make check)
```

Each reader/writer package has its own `Settings` (and `Point`) struct, a `Deps` struct
validated by `New` (`ErrMissingDependency`), small interfaces for its client (`Client`,
`Dialer`, `Publisher`) and a constructor for the real client (`NewDialer`, `NewClient`;
`api.Listen` binds the port, injected as a `net.Listener`). Constructors never connect.

---

## 4. Dependency Direction

Imports (paths below `internal/`):

```
cmd/datalogger    → app, clock, config
app               → clock, config, datasource{,/modbus,/http},
                    output{,/csv,/influxdb,/mqtt,/api}, transform
transform         → datasource, expr-lang/expr
datasource/modbus → clock, datasource, simonvetter/modbus
datasource/http   → clock, datasource, gjson
output            → datasource
output/csv        → clock, datasource
output/influxdb   → clock, datasource, influxdb3-go
output/mqtt       → clock, datasource, paho.golang
output/api        → clock, datasource
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
DeviceReader.Start ──data channel (unbuffered, one []DataPoint per poll)──▶ router goroutine
                                                    │ (one per device, in app)
                                                    │ transform.Apply on the poll
                                                    │ non-blocking send, copy per output
                                                    ▼
                                  per-output input channel (buffer_size, default 1000)
                                                    ▼
                                           Writer.Start (one goroutine per output)
```

- **Readers own** their data channel: they are the only senders and close it when they
  stop. A poll sends all its points as one batch after the reads are done (a poll
  without points sends nothing), so the router sees one consistent snapshot per poll.
- **The router** applies the device's `transform.Transformer` to each poll (the router is
  its only user), then sends each point to every output whose `devices` list contains the
  device and whose `exclude_points` does not list the point.
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
   outside the Modbus ranges, an HTTP request that cannot be built, an expression that
   does not compile or names an unknown point or lookup, an API port that cannot be
   bound) ends the process with exit 1. **Connecting is not construction:** readers connect on their first poll, so a
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
   A Modbus read that is in flight finishes and starts no further read of its poll;
   the points read so far are still delivered (range mode: only a complete poll is; an
   HTTP request in flight is aborted, so it has no data yet). Each reader then closes its
   data channel and the routers finish.
3. `app` closes the output input channels; each writer writes what is queued, flushes
   (CSV flush/close, InfluxDB final batch, MQTT disconnect, API server shutdown) and
   stops.
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
  up to `parallelism` goroutines. A failed point is logged and skipped; the rest
  of the poll is still delivered. Reads not started yet are skipped after a connection
  error and after two timeouts in a row (the device does not answer; one timeout may be a
  register it ignores), so a silent device does not hold a poll for points × timeout.
- **Range mode:** the `ranges` (`"start-end"`, inclusive) are read in chunks of at most
  **125 registers** (Modbus protocol limit), then points are decoded from the collected
  registers. Every point address must be covered by a range (checked at construction).
  **Any failed chunk fails the whole poll** (no partial data); no further chunk is read
  after it.
- Each point's timestamp is the receive time of its data (direct: its own read; range: the
  chunk that holds its first register).
- Types: `int16`, `uint16`, `int32`, `uint32`, `float32`, `bool` (others are rejected by
  validation); value = raw × `scale` + `offset`, always delivered as float64. `scale`
  defaults to 1 (an explicit 0 is treated as unset). 32-bit types need `count: 2` (high
  word first); a smaller count is rejected by validation, as is a `count` above 125 or
  registers past 65535.
- `parallelism` does not make Modbus requests concurrent: they share the device's one
  connection, on which the client library sends one request at a time, so `1` is the
  right value.

### HTTP reader
- `method` GET (default) or POST with `body`; `headers` are sent as given; `insecure`
  disables certificate verification for this device.
- `response_type`: `json` (default; values via gjson `json_path`; `type` float*/int*/uint*/
  bool/string converts, numbers sent as strings included; otherwise numbers become
  float64 and bools and strings stay) or `text` (the whole body as one string); anything
  else is rejected by validation. JSON points need a `json_path`.
- `scale`/`offset` apply to numeric JSON values: value × scale + offset as float64. With
  the defaults (1, 0) the value keeps its parsed type, so existing InfluxDB field types do
  not change; bools and strings are never scaled.
- Non-200 responses are errors (up to 64 KiB of their body is read so the connection
  is reused); a body above 10 MiB is an error. All points of one response share its
  receive timestamp.
- A point that cannot be extracted is skipped: its `json_path` is not in the response,
  the value is `null`, an object or array without `type: string`, or does not fit its
  `type` (`"N/A"` for `int64`, a negative number for `uint*`); gjson would deliver 0 or
  false, which looks like a reading. Such a point is warned about once, further failures
  are logged at debug, and an info line follows when it can be read again.
- `parallelism` has no effect: one request per poll, points are parsed in order.
- A request that cannot be built (bad URL, method) fails startup; an unreachable endpoint
  does not.

### InfluxDB 3 writer
- Schema: measurement = device name, tags `point` and `unit` (unit omitted when empty),
  one field `value`, timestamp = reading time. The README documents queries on this
  schema; changing it is a breaking change (`!`).
- Batching: a full `batch_size` (default 10000) is written at once, a partial batch every
  `batch_timeout` (default 1 s). Retries: `max_retries` (default 3) with `retry_delay`
  (default 1 s), only for network errors, 5xx and 429; after that, or at once for other
  HTTP errors (bad token, unknown database, rejected lines), the batch is dropped and
  logged. The last batch is written when the input is closed on shutdown. The client
  does not connect at startup.
- A value line protocol cannot store (anything but float64, int64, uint64, bool,
  string) is skipped with a warning, so it cannot fail the batch of every device.
- Uses the v3 write API; gzip above 1000 bytes; 10 s timeout per write request (also
  with `insecure`, which skips certificate checks).

### MQTT writer
- Broker `address` `tcp://`/`mqtt://` or `tls://`/`ssl://`/`mqtts://` (no scheme = tcp;
  other schemes fail at startup). TLS verifies the broker certificate against the host
  name; `insecure` uses TLS without verification (also for `tcp://`). MQTT v5, keep-alive
  30 s, clean start, 10 s connect timeout.
- Connects on the first point and reconnects after a publish error or a lost connection,
  with backoff (1 s doubling to 30 s; the first redial after a publish error is
  immediate, the backoff is reset by the next successful publish). A message the broker
  rejects with an error reason code (e.g. ACL) is logged and the session kept. Points
  that arrive while no connection is possible are **dropped**; the count is logged on
  the next successful connect.
- Topic: `<topic>/<device>/<point>` (`topic` default `datalogger`); payload
  `{"value": …, "unit": "…", "timestamp": "<RFC3339Nano>"}` (NaN/±Inf values are
  `null`, as in the API); `qos` 0–2, `retain`.
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

### API output
- `listen` (`host:port`, required) is bound at startup; a port in use fails startup.
  `token` (optional) requires `Authorization: Bearer <token>` on `/api/…` (compared in
  constant time, never logged); `/healthz` is always open.
- Keeps the latest point per device and point in memory (nothing persisted); points of
  devices not in `devices` are ignored, `exclude_points` never arrive.
- `GET /api/devices` → `{"devices": [{"device", "points": <count>, "age_ms": <newest> |
  null}]}` sorted by name; `GET /api/devices/{device}` → `{"device", "points": {<point>:
  {"value", "unit", "timestamp", "age_ms"}}}`; `GET /api/devices/{device}/{point}` →
  `{"device", "point", "value", "unit", "timestamp", "age_ms"}`. Errors are
  `{"error": "…"}` with 404 (unknown device, point without value) or 401.
- `timestamp` is the reading time (RFC3339Nano, UTC); `age_ms` = now − timestamp on the
  injected clock, per request. NaN/±Inf values are `null` (no JSON form).
- Server timeouts: read header/read/write 5 s, idle 60 s. On shutdown running requests
  get 2 s (on the injected clock), then their connections are closed.
- Paths and response format are a public interface; changing them is a breaking change
  (`!`).

### Transforms (`expr`, `lookups`, `exclude_points`)
- A point's `expr` ([expr-lang](https://expr-lang.org)) replaces its value after
  `scale`/`offset`. The environment: `value` (this point), `points.<name>` (every point
  of the same poll, before any expression ran; nil if not read in this poll) and
  `lookups.<name>[code]` (top-level `lookups`: integer code → text; nil for an unknown
  code, so `?? "unknown"` works).
- Only points of the same device are visible: other devices poll on their own schedule
  and share no snapshot.
- Results: numbers are delivered as float64 (like the readers), bools and strings stay,
  nil skips the point for this poll, anything else is a runtime error.
- Compiled once in `transform.New`; syntax errors, unknown variables and unknown
  `points.x`/`lookups.x` names (checked by walking the AST) fail startup. A runtime
  error skips the point: warned once per point, then debug, info when it works again.
- `exclude_points` on an output lists `device/point` names that output does not
  receive; e.g. text values for InfluxDB, whose `value` field has one type per table.

### Configuration
- One YAML file (`-config`, required); template with every option: `example/config.yaml`.
- At least one device and one output are required; device and output names must be
  unique; outputs may only reference existing devices; `exclude_points` entries must be
  `device/point` with a device of that output and an existing point; API `listen` must be
  `host:port`; every device needs at least one point; point names are unique per
  device; device and point names must not contain `/`; the `topic`, devices and
  non-excluded points of an MQTT output must not contain `+` or `#` (wildcards: a broker
  closes the connection); `parallelism` 1–100; `poll_interval` > 0; `timeout` > 0.
- Unknown keys are an error (a typo would otherwise fall back to a default silently).
- `config.Load` reads the file, fills defaults (`applyDefaults`: device `timeout` = the
  poll interval, at most 10 s; point `scale` 1; HTTP method GET and response type json;
  MQTT topic `datalogger` and client ID `logger-<random>`) and then
  runs `Validate()`, which only checks and never changes the config.
- No environment overrides (see section 8). Secrets (InfluxDB token, MQTT password)
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
- **Transforms with expr-lang, not config keys:** sign changes, status codes, bit fields
  and values that depend on another point need more than a fixed set of keys (`invert`,
  `bit`, …); expr compiles at startup, runs without side effects and is cheap per point.
  Expressions run in the router on one poll at a time, so `points` is a consistent
  snapshot and no state is shared between devices.
- **Exclude instead of per-output transforms:** a decoded value is a separate point (e.g.
  `status` and `status_text` on the same register); outputs that cannot store it skip it
  with `exclude_points`, so every point has one value for all outputs.
- **InfluxDB schema with `point` as tag:** one table per device with a fixed column set,
  queryable by point without schema changes when points are added.
- **Config only from the YAML file, no env overrides:** the config is mostly lists of
  devices/outputs that env vars cannot address sensibly; the gitignored `config.yaml`,
  mounted read-only, serves as the secret file.
- **API output serves latest values only:** no history, no staleness setting; `age_ms`
  lets the client decide what is too old, and history belongs in InfluxDB or CSV. The
  values live in a mutex-guarded map inside the writer: its goroutine is the only
  writer, the HTTP handlers of the same component read copies, so nothing is shared
  between components. The port is bound in the factory so a port in use fails startup.
- **Hand-written fake clock instead of a mock:** `clock.Clock`/`clock.Ticker` are faked by
  `clocktest.Fake`, not by mockery. Tests need time that moves consistently across tickers
  and `After` (`Advance`); call expectations cannot model that.

---

## 9. Code Rules

### 9.1 Architecture (MUST)
- **Dependency injection:** every dependency a type uses (client, dialer, clock, logger,
  channel) is passed in through its constructor; nothing is created inside methods.
  Dependencies are **interfaces declared by the consumer**, next to the code that uses
  them, and kept small (only the methods it calls). Shared contracts are exported by
  their package: `datasource.DeviceReader`, `output.Writer`, `clock.Clock`.
- **Composition root:** `internal/app` is the only code that names concrete reader/writer
  types; it creates them through `Create*` factories that switch on the config type and
  return a typed error for an unknown one.
- **Required dependencies:** all dependencies are required. `New(Deps)` validates them
  once and returns `ErrMissingDependency` naming every missing field. No optional
  components behind `if x != nil`; a feature that can be switched off is a separate
  component. Missing **data** (no reading yet) is normal and returns an explicit answer.
- **Functional options** (MAY) only for optional *settings* with a default, and only from
  three of them on; never for dependencies.
- **No global state:** no package-level loggers, singletons, caches or lookup tables
  (build them in a constructor). Allowed package-level variables: `Err…` sentinels and the
  linker-set build info in `main` (`Version`, `Commit`, `BuildDate`, `GoVersion`, marked
  `//nolint:gochecknoglobals // written by the linker at build time`).
- **Lifecycle:** every goroutine has an owner that starts it, can stop it and waits for
  it. Waiting loops use the injected `clock.Clock`, capture `now` once per iteration and
  derive everything from it. Every channel has a documented owner (the sender closes)
  and a documented behaviour when full (section 5). Component exits are watched with
  channels, never with sleep-polling. `os.Exit` only in `main`.

### 9.2 Errors (MUST)
- Wrap with `%w` and context, prefixed with the package:
  `fmt.Errorf("modbus: read chunk %d-%d: %w", start, end, err)`.
- **Sentinels** (`ErrMissingDependency`, `ErrComponentStopped`) for conditions callers
  branch on; **custom types** when callers need data. Both work with `errors.Is`/`As`.
- **Never swallow** an error: handle it, return it, or log it with the reason it is safe
  to continue. `_ = f()` needs a `//nolint` with an explanation.
- **Log once**, at the boundary that handles the error; lower layers return, they do not
  log and return.
- Error strings: lowercase, no trailing punctuation.
- **No panic** (enforced by `forbidigo`). Startup errors go back to `main`, which exits 1.

### 9.3 Logging
- One `zerolog.Logger` is built in `main` and injected; every component gets
  `log.With().Str("component", name).Logger()`.
- Context fields, not formatted strings: `.Str("device", name).Err(err).Msg(...)`.
- Levels: `debug` per poll/point detail, `info` lifecycle, `warn` recovered problems,
  `error` failures that need attention.
- Never log secrets: InfluxDB token, MQTT password, HTTP auth headers, URLs with
  credentials.

### 9.4 Style (MUST, enforced by golangci-lint)
- gofumpt + goimports; import groups stdlib / third-party / project
  (`github.com/dombyte/datalogger`).
- Lines ≤ 100 chars (tab width 4), functions ≤ 40 lines and ≤ 40 statements,
  cyclomatic complexity < 8, ≤ 5 parameters (use a struct beyond that).
- No magic numbers outside constants; a constant with a comment beats a config knob
  nobody sets. Add options only when real use needs them (YAGNI; no abstraction before a
  second real use).
- Naming: packages lowercase and singular; files lowercase with underscores; one-letter
  receivers, consistent per type; stdlib acronyms (`ID`, `URL`, `HTTP`); `Err…` sentinels,
  `…Error` types; functions verb + noun.
- Layout: all code under `internal/` (no `pkg/`), at most 3 levels below it.
- Docs: package doc and doc comments on exported types MUST, on exported functions
  SHOULD; they start with the name. Inline comments explain **why**. Non-obvious design
  decisions go into section 8 of this file, not into long code comments.

### 9.5 Testing
- stdlib `testing` + testify (`require` for preconditions, `assert` for checks) + mockery
  mocks (section 2) + `clocktest.Fake`.
- Table-driven tests with named cases; `t.Parallel()` where there is no shared state.
- Unit tests use no network, no real clock and no sleeps: mocks, the fake clock,
  `httptest`, `t.TempDir()`. Tests against a real device, broker or DB are integration
  tests behind `//go:build integration` and are not part of `go test ./...`.
- Always run with `-race`. New code comes with tests; a bug fix comes with a regression
  test that fails before the fix.
- Coverage (SHOULD): ≥ 90 % per package with logic, ≥ 100 % for packages with five or more
  complex functions; `cmd/datalogger` and `app` are covered by the startup test instead.

---

## 10. Tooling, CI and Releases

- **Checks** (`make check` locally, `checks.yml` in CI, same pinned tool versions):
  `golangci-lint fmt` diff, `golangci-lint run`, deadcode, govulncheck, build,
  `go test -race`; CI also checks `go mod tidy` and mock drift. `scripts/pre-commit.sh`
  is check-only: it never rewrites or stages files.
- **Linters** (`.golangci.yml`, golangci-lint v2): revive, staticcheck, govet, unused,
  errcheck, errorlint, gosec, gocyclo, dupl, mnd, lll, misspell, unconvert, perfsprint,
  whitespace, importas, goprintffuncname, ineffassign, wastedassign, durationcheck,
  makezero, tparallel, copyloopvar, intrange, bodyclose, rowserrcheck, sqlclosecheck,
  gochecknoglobals, forbidigo (`panic`), nolintlint (every `//nolint` names the linter and
  gives a reason). Tests are excluded from revive, dupl, errcheck, gosec, lll,
  gochecknoglobals, forbidigo and mnd.
- **Tool versions** are pinned (pinned `go run` commands, the mockery `tool` in `go.mod`,
  the golangci-lint action `version:`) and bumped deliberately, the same locally and in CI.
- **Renovate** (`renovate.json`) updates dependencies for exactly the ecosystems used here:
  `gomod`, `dockerfile`, `docker-compose`, `github-actions`. Every pinned version in the
  repo is tracked; the golangci-lint `version:` of the CI action is seen by the
  `github-actions` manager. A pin no built-in manager sees (pinned `go run` commands in
  workflows, scripts and this file) needs a `customManagers` regex, added in the same
  change that adds the pin. Updates that need manual work (Go module majors, golangci-lint
  majors) are disabled with a `description` saying why. Text that only describes a
  pinning pattern must not match such a regex. GitHub vulnerability alerts stay enabled;
  Renovate's `vulnerabilityAlerts` reads them.
- **Images:** `Dockerfile` (dev) and `Dockerfile.goreleaser` build `scratch` images with
  CA certs and tzdata from pinned base images; `.dockerignore`/`.gitignore` keep local
  config, data and build output out.
- **Releases:** SemVer tags `vX.Y.Z` (pre-releases `vX.Y.Z-alphaN`/`-betaN`/`-rcN`) only
  on commits on `main`. `release.yml` triggers only on SemVer-shaped tags; a guard job
  checks the exact tag format and that the tag is on `main`, then the checks rerun
  (`checks.yml`), then goreleaser
  (`go mod verify` only, `-trimpath`, reproducible via `CommitDate`, `goamd64: v1`) builds
  the archives and images. Image tags: `vX.Y.Z` always; `vX.Y`, `vX`, `latest` only for
  stable releases. The changelog is grouped from commit subjects (section 11.2);
  pre-releases compare against the last stable tag.
- **Build info** (`Version`, `Commit`, `BuildDate`, `GoVersion`) is set via ldflags
  (`make build`, goreleaser) and shown by `-version` and in the startup log.

---

## 11. Git Workflow

### 11.1 Branches
- Changes go through a PR from a `type/description` branch (`feat/…`, `fix/…`, `docs/…`,
  `refactor/…`, `chore/…`, `ci/…`). The maintainer MAY commit or merge directly into
  `main` after `make check` passes (ruleset bypass for the admin role; bots such as
  Renovate always need a PR with green checks).
- `main` is protected by a repository ruleset: no deletion or force push, signed commits,
  PR required, the `checks.yml` jobs required with "branches must be up to date".
- CI minutes: `checks.yml` runs on every PR, every direct push to `main` and every
  release tag, but not again on the merge commit of a PR. A `gate` job asks the API
  (`repos/{repo}/commits/{sha}/pulls`) whether the pushed commit is a PR's
  `merge_commit_sha`; if so the other jobs are skipped, and if the call fails they run.
  This is safe only because "up to date" is on: the PR run tested the same tree that lands
  on `main`. A newer push to a PR cancels its superseded run; runs on `main` and tags
  always finish, and one release per tag runs at a time.
- Merge with a regular merge commit; squash or rebase only when the user confirms. Only
  rewrite history that is not on a remote.
- **Batching:** several small changes MAY share one PR. Create one batch branch from
  `main`; cut each change as its own local `type/description` branch from it, run
  `make check`, merge it back with `git merge --no-ff` (never push these branches). Push
  the batch branch once all changes are merged and open one PR.

### 11.2 Commits (MUST)

[Conventional Commits](https://www.conventionalcommits.org):
`<type>[(<scope>)][!]: <imperative, lowercase subject>`

| Type | Use for | Changelog group |
|---|---|---|
| `feat` | new or changed user-visible behaviour | Features |
| `fix` | bug fixes, incl. security hardening | Bug fixes |
| `perf` | performance improvements | Performance |
| `refactor` | restructuring without behaviour change | Refactoring |
| `docs` | README, AGENTS.md, comment-only changes | Documentation |
| `chore`, `build`, `ci`, `test`, `style` | tooling, CI, tests, formatting | Maintenance |
| `<type>(deps)` | dependency and toolchain bumps | Dependencies |

- `!` before the colon marks a breaking change for users upgrading (config keys, MQTT
  topic/payload, CSV columns, InfluxDB schema); a `BREAKING CHANGE:` footer alone is not
  enough, the changelog reads only the subject.
- Subject: imperative, lowercase first word, no trailing period, ≤ 72 characters.
- Merge commits keep git's default message.
- **No AI signatures:** no `Co-Authored-By:` AI trailer, no "generated by" line, in
  commits, PR descriptions or release notes.

Scopes used here: `config`, `modbus`, `http`, `csv`, `influxdb`, `mqtt`, `api`, `routing`,
`shutdown`, `transform`, `release`, `docker`, `ci`, `deps`

```
feat(mqtt): support TLS client certificates
fix(modbus): stop backoff wait on shutdown
refactor(config): move defaults out of Validate
feat(influxdb)!: store unit as field instead of tag
```

### 11.3 Review Criteria
The required CI checks MUST be green before a PR is merged.

- **Blocker:** does not build, failing tests, lint/format/gosec findings; global state,
  panic, a concrete type where an interface belongs, a dependency created inside a
  method; swallowed errors; new code without tests, drifted mocks; AI signatures or
  non-conventional commit subjects; secrets in code, config or logs.
- **Must fix:** missing package/type docs; a breaking change without `!`; a rule in this
  file that no longer matches the code.
- **Should fix:** duplication, nesting deeper than 4 levels, unclear names, missing
  function docs, coverage below the targets in 9.5.

### 11.4 Security
- Validate input at the boundary: config in `config.Validate`, device responses in the
  reader that parses them.
- Every outgoing call has a timeout (device `timeout`, MQTT connect timeout, InfluxDB
  client); clients verify certificates unless the documented `insecure` option is set.
- Secrets only in the gitignored `config.yaml` (section 8), never committed or logged.
- Dependencies: Renovate plus `govulncheck` in CI and in `make check`.

---

## 12. Adding a Device or Output Type

1. New package under `internal/datasource/<type>` or `internal/output/<type>` implementing
   the reader or writer contract: `Settings`, `Deps` checked in `New`, small interfaces
   for its client (listed in `.mockery.yaml`), the injected `clock.Clock` for any waiting,
   and no connecting in the constructor.
2. Config: add the type-specific struct under `DeviceSpecific`/`OutputSpecific`, its
   validation, and a commented example in `example/config.yaml`.
3. Composition root: add the `case` and a `create…` function in `internal/app/factory.go`
   that maps config onto `Settings` and creates the real client. Nothing else may
   reference the new package; update section 4.
4. Behaviour: document timestamps, error/backoff behaviour and any public format (topics,
   columns, schema) in section 7.
5. Tests with mocks/fakes only (no real device, broker or DB); an optional integration test
   behind the build tag.
6. README: user-facing documentation of the new type.
7. `make check` passes; branch `feat/…`, commit `feat(<scope>): …`.

---

## 13. Backlog

Known gaps to the rules above, one branch each; update this list when an item is done.

- **Coverage below 100 %:** `datasource/http` 99.2 %, `datasource/modbus` 98.3 %,
  `output/mqtt` 99.0 %, `output/csv` 95.6 %, `output/api` 97.8 %. The open blocks are
  defensive branches that tests cannot reach: errors already ruled out at construction
  (HTTP request build, Modbus client creation and range coverage), a library call that
  never fails (`SetUnitId`), the TLS server name fallback for an address without port
  (the dial fails first), CSV errors that need a broken file system (stat, directory
  listing, reopen right after a rename) and API server errors after shutdown or on a
  response write to a client that went away. Remove them or accept them per package.
- **Mixed value types in InfluxDB:** all points of a device share the `value` field, but
  HTTP points can deliver bool, int64 or string next to float64, and InfluxDB 3 keeps one
  type per column, so such a device's writes can be rejected. Today: keep such points
  out with `exclude_points` or in a separate device. A fix (e.g. typed fields
  `value`/`value_str`/`value_bool`) changes the schema and is a breaking change (`!`).
