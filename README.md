[![GitHub license](https://badgen.net/github/license/dombyte/datalogger)](https://github.com/dombyte/datalogger/blob/main/LICENSE)
[![Checks](https://github.com/dombyte/datalogger/actions/workflows/checks.yml/badge.svg)](https://github.com/dombyte/datalogger/actions/workflows/checks.yml)
[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/dombyte/datalogger.svg)](https://github.com/dombyte/datalogger)
[![Github tag](https://badgen.net/github/release/dombyte/datalogger/latest)](https://github.com/dombyte/datalogger/tags/)

# Datalogger

Datalogger polls devices (Modbus TCP/RTU, HTTP APIs) and forwards every reading to one
or more outputs (InfluxDB 3.x, MQTT, CSV files, a read-only HTTP API, a webhook). It is
a single binary configured by one YAML file.

- **Devices:** Modbus TCP and RTU (single registers or register ranges, several devices
  on one serial port), HTTP APIs (JSON via gjson paths, or plain text).
- **Outputs:** InfluxDB 3.x (one row per poll, a column per point), MQTT v5, CSV files
  with rotation, a read-only HTTP API with the latest values, webhooks.
- **Transforms:** expressions per point (sign, bit fields, status texts from lookup
  tables, values that depend on other points).
- **Robust:** devices and outputs reconnect with backoff on their own; a slow output
  never blocks the devices or the other outputs; on stop, queued readings are written.
- **Runs anywhere:** Docker image (amd64, arm64, armv7), `.deb`/`.rpm` packages with a
  systemd service, or a single static binary for Linux and macOS.

> **Coming from v1?** The config format and the InfluxDB schema changed; see
> [Upgrading from v1](#upgrading-from-v1).

## Installation

### Docker Compose

Images for linux/amd64, arm64 and arm/v7 on `ghcr.io/dombyte/datalogger`:

```bash
mkdir datalogger && cd datalogger
curl -LO https://raw.githubusercontent.com/dombyte/datalogger/main/example/docker-compose.yaml
curl -L -o config.yaml https://raw.githubusercontent.com/dombyte/datalogger/main/example/config.yaml
# edit config.yaml, then:
docker compose up -d
```

The compose file mounts `./config.yaml` read-only and `./data` to `/app/data` (write CSV
files below `/app/data`). Publish a port only if you use the `api` output. Pin a version
tag (`vX.Y.Z`, `vX.Y`, `v2`) instead of `latest` if you want controlled upgrades.

### Package (.deb / .rpm)

For Linux (amd64, arm64, armv7) from the
[releases](https://github.com/dombyte/datalogger/releases), with a systemd service:

```bash
sudo apt install ./datalogger_<version>_amd64.deb      # Debian, Ubuntu
sudo dnf install ./datalogger-<version>-1.x86_64.rpm   # Fedora, RHEL
```

The package creates the system user `datalogger` (member of `dialout` for Modbus RTU
serial ports). Write the config, then start the service:

```bash
sudo install -m 0640 -g datalogger /usr/share/doc/datalogger/examples/config.yaml \
  /etc/datalogger/config.yaml
sudoedit /etc/datalogger/config.yaml
sudo systemctl enable --now datalogger
journalctl -u datalogger -f
```

The service may only write below `/var/lib/datalogger`, its working directory: relative
CSV paths such as `./data/data.csv` land there. The `api` output needs a port above 1023.
An upgrade restarts a running service; removing the package stops it and keeps the
config, the data and the user.

### Binary

Download the archive for Linux (amd64, arm64, armv7) or macOS (amd64, arm64) from the
[releases](https://github.com/dombyte/datalogger/releases) and run:

```bash
datalogger -config config.yaml [-debug]   # -version prints the build info
```

### From source

With Go (version in `go.mod`): `make build` builds `./datalogger` with version info.

## Configuration

One YAML file, passed with `-config`. [example/config.yaml](example/config.yaml) is the
complete reference with every option explained. Unknown keys (for example a typo) stop
the program at startup. Keep tokens and passwords only in your local `config.yaml`.

Every device and output has a `type`; the settings of that type go in a block with the
same name (`http:`, `modbus:`, `influxdb:`, `mqtt:`, `csv:`, `api:`, `webhook:`). A
minimal config: one device, one output.

```yaml
devices:
  - name: meter
    type: http
    poll_interval: 5s
    http:
      address: "http://192.0.2.20/rpc/EM.GetStatus"
    points:
      - name: power
        json_path: "total_act_power"
        unit: "W"

outputs:
  - name: csv
    type: csv
    csv:
      file_path: "./data/data.csv"
      max_age: 24h
      max_backups: 7
    devices: [meter]
```

### How data flows

- Every **device** is polled at once on startup, then every `poll_interval`. Each poll
  is stamped with the time the device answered, in UTC: Modbus `direct` mode stamps every
  point with the time of its own read; Modbus `range` mode (last chunk) and HTTP (the
  response) stamp all points of a poll with one time.
- Every **output** lists the `devices` it receives. All their points are copied to it,
  except those listed in `exclude_points` (`device/point`).
- Each output has its own queue of polls (`buffer_size`, default 1000): every poll of a
  device is handed over as one batch. If an output is too slow and its queue is full,
  new polls are dropped for that output only and a warning is logged; devices and other
  outputs are not affected.

### Devices

| Type | Notes |
|---|---|
| `modbus` | `tcp://host:port` or `rtu:///dev/ttyUSB0`. `register_mode` (required): **direct** reads each point on its own (a failing point is skipped); **range** reads the register `ranges` (`"start-end"`) and decodes all points from them (a failing range skips the whole poll). Types `int16`, `uint16`, `int32`, `uint32`, `float32`, `bool`; value = raw × `scale` + `offset`. One register type (3 holding / 4 input) per device. Several devices may share one serial port, see [below](#several-devices-on-one-serial-port). |
| `http` | GET or POST with optional headers and body. `response_type: json` extracts values with [gjson paths](https://github.com/tidwall/gjson/blob/master/SYNTAX.md); `text` delivers the whole body as one string. A path that is missing or `null` skips the point (no fake `0`). |

### Several devices on one serial port

Devices with the same `rtu://` address share the port: an RS-485 bus with several
slaves, or one device split into two (e.g. a fast and a slow poll).

```yaml
devices:
  - name: meter
    type: modbus
    poll_interval: 1s
    modbus: {address: "rtu:///dev/ttyUSB0", slave_id: 1, speed: 9600, parity: "N",
             register_mode: direct}
    # ...
  - name: heat_pump
    type: modbus
    poll_interval: 10s
    modbus: {address: "rtu:///dev/ttyUSB0", slave_id: 2, speed: 9600, parity: "N",
             register_mode: direct}
    # ...
```

- `speed`, `data_bits`, `parity` and `stop_bits` must be the same on every device of a
  port; otherwise the program does not start.
- Requests run one after the other, so devices on a port wait for each other. A slave
  that does not answer holds the port for up to `timeout` per request.
- The port uses the largest `timeout` of its devices.

### Outputs

| Type | Notes |
|---|---|
| `influxdb` | InfluxDB 3.x, batched, with retries. `timeout` per write (default 10 s); `no_sync: true` lets InfluxDB answer before the write is in its WAL on disk (faster on slow storage, a crash can lose the last WAL flush). Schema [below](#influxdb-schema). |
| `mqtt` | MQTT v5, one topic per point: `<topic>/<device>/<point>` (default topic `datalogger`), payload `{"value": 230.5, "unit": "V", "timestamp": "2026-10-05T10:15:02.123Z"}`. `tcp://` (or `mqtt://`, or no scheme) or TLS via `tls://`, `ssl://`, `mqtts://` (`insecure: true` skips the certificate check). Points are dropped (and counted) while the broker is unreachable. |
| `csv` | Columns `timestamp, device, point, value, unit`, written as soon as no further poll is queued. Rotated after `max_age` to `<file>.<YYYYMMDD-HHMMSS>` (UTC); `max_backups` 0 = keep none, > 0 = keep that many, < 0 = keep all. |
| `api` | Read-only JSON API with the latest value of every point, see [below](#http-api). |
| `webhook` | Pushes readings as a JSON array to your HTTP endpoint, see [below](#webhook). |

### HTTP API

```yaml
outputs:
  - name: api
    type: api
    api:
      listen: ":8080"
      token: "" # optional: require "Authorization: Bearer <token>"
    devices: [inverter]
```

| Request | Response |
|---|---|
| `GET /api/devices` | `{"devices": [{"device": "inverter", "points": 3, "age_ms": 820}]}` (`age_ms` of the newest point, `null` before the first reading) |
| `GET /api/devices/{device}` | `{"device": "inverter", "points": {"power": {"value": 1234.5, "unit": "W", "timestamp": "2026-10-05T10:15:02.123Z", "age_ms": 820}}}` |
| `GET /api/devices/{device}/{point}` | `{"device": "inverter", "point": "power", "value": 1234.5, "unit": "W", "timestamp": "…", "age_ms": 820}` |
| `GET /healthz` | `200` (no token needed) |
| `GET /` | This documentation as a web page (no token needed) |
| `GET /openapi.json` | OpenAPI 3.1 description, e.g. for Postman or Bruno (no token needed) |

- `age_ms` is the time since the device answered. A device that stops answering keeps
  its last values while their age grows.
- Unknown devices and points without a value are `404`. NaN and infinite values are
  `null`.
- Values live in memory only: after a restart the API is empty until the first poll.

### Webhook

```yaml
outputs:
  - name: backend
    type: webhook
    webhook:
      url: "https://ingest.example.com/datalogger"
      method: POST # or PUT
      headers:
        Authorization: "Bearer <token>"
    devices: [inverter]
```

Every request has a JSON array as body (`Content-Type: application/json`):

```json
[
  {"device": "inverter", "point": "power", "value": 1234.5, "unit": "W",
   "timestamp": "2026-10-15T08:23:20.123Z"}
]
```

- Each poll is sent at once. If your endpoint is slow and polls queue up, they are sent
  together, up to `max_batch` readings (default 1000) per request.
- Any `2xx` answer is a success. Network errors, `5xx` and `429` are retried
  (`max_retries`, default 3, `retry_delay`, default 1 s, or the endpoint's `Retry-After`
  if that is longer, at most 1 minute); other answers such as `401` or
  `400` drop the batch at once. Dropped batches are logged with the status and the start
  of your endpoint's answer.
- `timeout` per request (default 10 s), `insecure: true` skips certificate checks.
  Headers are never logged, and neither are tokens in the URL.

### Transforming values

Every point may have an `expr` ([expr-lang](https://expr-lang.org/docs/language-definition))
that replaces its value after `scale`/`offset`. It can use `value`, the other points of
the same poll (`points.<name>`) and named lookup tables (`lookups.<name>[code]`):

```yaml
lookups:
  solis_status:
    0x0003: Generating
    0x1004: Grid Off

devices:
  - name: inverter
    # ...
    points:
      - name: status_text
        register: 33095
        type: uint16
        expr: 'lookups.solis_status[int(value)] ?? "unknown"'
      - name: fault_no_grid
        register: 33116
        type: uint16
        expr: "bitand(int(value), 0x0001) != 0"
      - name: battery_power
        register: 33149
        count: 2
        type: uint32
        expr: "points.battery_current_direction == 1 ? value : -value"

outputs:
  - name: influxdb
    # ...
    devices: [inverter]
    exclude_points: [inverter/status_text, inverter/fault_no_grid]
```

- Expressions see the values of the poll before any expression ran, and only points of
  the same device.
- A point that could not be read in this poll is `nil` in `points`; guard where it
  matters: `points.dir == nil ? nil : (points.dir == 1 ? value : -value)`. A result of
  `nil` skips the point for this poll.
- Numbers become float64; true/false and text are kept. In InfluxDB every point has its
  own column, so text next to numbers is fine; a point must keep its type, though.
- An expression that does not compile, or names an unknown point or lookup, stops the
  program at startup. One that fails while running skips the point and is logged.

## Running

- Devices and outputs connect in the background: one that is unreachable at startup (or
  later) is retried with backoff and does not stop the others. Settings that cannot work
  at all (an invalid address, a port in use, a bad expression) stop the program at
  startup.
- SIGINT/SIGTERM stop the devices first, then every output writes what is queued. After
  7 seconds outputs that are still busy (an endpoint that hangs) are stopped: they drop
  the rest, close their files and connections, and the program exits with 1.
- Exit code 0 after a clean stop, 1 on configuration or startup errors, when a component
  stops unexpectedly, or when the shutdown takes too long. Run it with a restart policy:
  `restart: unless-stopped` in the compose file; the packaged systemd service restarts
  on failure.
- Logs go to stderr (`docker compose logs -f`, `journalctl -u datalogger -f`). A device
  that fails is logged once as a warning, from the third failure in a row as an error,
  and again when it recovers. `-debug` adds per-poll and per-point detail.

## Upgrading from v1

v2 changes the config format and the InfluxDB schema: a v1 config does not start (its
keys are unknown to v2). The Docker tag `latest` is v2; pin `v1` to stay on v1 for now.

1. **Config:** the `device_specific:` and `output_specific:` levels are gone; the type
   block moves up one level. The InfluxDB options `batch_size`, `batch_timeout`,
   `max_retries` and `retry_delay` move into the `influxdb:` block; `batch_size` now
   counts rows (one per poll, one per read in Modbus direct mode).

   ```yaml
   # v1
   - name: db
     type: influxdb
     batch_size: 5000
     output_specific:
       influxdb:
         address: "http://influxdb:8181"

   # v2
   - name: db
     type: influxdb
     influxdb:
       address: "http://influxdb:8181"
       batch_size: 5000
   ```

2. **`parallelism`** is gone (Modbus requests on one connection always ran one after the
   other); delete it from every device.
3. **`buffer_size`** now counts polls, not points: one entry holds all points of a poll.
   Lower large values (v1 examples used 10000); the default 1000 fits most setups.
4. **InfluxDB:** the [schema](#influxdb-schema) changes from one row per point to one
   row per poll. Write v2 into a **new database**: the tables of v1 have the columns
   `point`, `unit` and `value` and would mix both layouts. The v1 database stays
   readable for older data; rewrite the queries as shown in
   [Query examples](#query-examples-sql-grafana-flightsql).

MQTT topics and payloads, CSV columns and the HTTP API paths are unchanged. Other
differences you may notice:

- Timestamps: Modbus `direct` mode stamps every point with the time of its own read;
  Modbus `range` mode and HTTP stamp all points of a poll with one time.
- CSV backups are named in UTC (`<file>.<YYYYMMDD-HHMMSS>`), like the timestamps in the
  file.
- Several Modbus devices on one serial port now share it (v1 opened it once per device),
  see [above](#several-devices-on-one-serial-port).
- An HTTP response from which no point can be extracted counts as a failed poll.
- Stricter checks at startup: Modbus `parity` (N, E, O) and `ranges`, at most 499 points
  per device in InfluxDB.

New in v2: the [webhook](#webhook) output, shared serial ports, API documentation at
`GET /` and `GET /openapi.json`, `.deb`/`.rpm` packages.

## InfluxDB schema

One row per poll: the table is the device, every point is a column, there are no tags.
Modbus devices in `direct` mode read every point on its own and keep its read time, so
their polls are one row per point (the other columns are `NULL`); aggregate with
`date_bin` and skip `NULL` values, as in the queries below.

| Element | Content | Example |
|---|---|---|
| Table | device name | `spine` |
| Column per point | the reading of that point; type from its value (float, integer, boolean, string) | `a_voltage` |
| `time` | when the device answered (one time per poll) | |

```
spine a_voltage=230.5,b_voltage=231.1,freq=50.01 1712345678901234567
```

- A point that was not read in a poll (or whose `expr` returned `nil`) is `NULL` in
  that row. Aggregates such as `avg` skip `NULL`.
- Adding a point adds a column (older rows are `NULL`); a removed point keeps its
  column and history. Renaming a point starts a new column.
- A point keeps the type of its first value: a value of another type is skipped and
  logged. To change a point's type, give it a new name. For HTTP points, `type:` pins it.
- A table holds at most 500 columns, one of them `time`, so a device may send at most
  499 points to InfluxDB (excluded points do not count; a point may not be named
  `time`). Split a larger device into two. Columns of removed points still count.
- The unit is not stored; set it in the dashboard.

### Query examples (SQL, Grafana FlightSQL)

Several points of a device, averaged to an interval:
```sql
SELECT
  date_bin(INTERVAL '${__interval_ms} milliseconds', time) AS "Time",
  avg(a_voltage) AS "A Phase Voltage",
  avg(b_voltage) AS "B Phase Voltage"
FROM spine
WHERE time >= $__timeFrom() AND time <= $__timeTo()
GROUP BY 1
ORDER BY 1
```

Raw values:
```sql
SELECT time AS "Time", a_voltage AS "A Phase Voltage"
FROM spine
WHERE time >= $__timeFrom() AND time <= $__timeTo()
ORDER BY time
```

Latest value (gauge):
```sql
SELECT time AS "Time", a_voltage AS "A Phase Voltage"
FROM spine
WHERE time >= now() - INTERVAL '1 minute' AND a_voltage IS NOT NULL
ORDER BY time DESC
LIMIT 1
```

Point names are column names: write names with capitals or special characters in double
quotes (`"L1-Power"`).
