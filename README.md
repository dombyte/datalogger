[![GitHub license](https://badgen.net/github/license/dombyte/datalogger)](https://github.com/dombyte/datalogger/blob/main/LICENSE)
[![Checks](https://github.com/dombyte/datalogger/actions/workflows/checks.yml/badge.svg)](https://github.com/dombyte/datalogger/actions/workflows/checks.yml)
[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/dombyte/datalogger.svg)](https://github.com/dombyte/datalogger)
[![Github tag](https://badgen.net/github/release/dombyte/datalogger/latest)](https://github.com/dombyte/datalogger/tags/)

# Datalogger

Datalogger polls devices (Modbus TCP/RTU, HTTP APIs) and forwards every reading to one
or more outputs (InfluxDB 3.x, MQTT, CSV files, a read-only HTTP API). It is a single
binary configured by one YAML file.

## Installation

**Docker Compose** (images for linux/amd64, arm64 and arm/v7 on
`ghcr.io/dombyte/datalogger`):

```bash
mkdir datalogger && cd datalogger
curl -LO https://raw.githubusercontent.com/dombyte/datalogger/main/example/docker-compose.yaml
curl -L -o config.yaml https://raw.githubusercontent.com/dombyte/datalogger/main/example/config.yaml
# edit config.yaml, then:
docker compose up -d
```

The compose file mounts `./config.yaml` read-only and `./data` to `/app/data` (write CSV
files below `/app/data`). Publish a port only if you use the `api` output. Pin a version
tag (`vX.Y.Z`, `vX.Y`, `vX`) instead of `latest` if you want controlled upgrades.

**Binary:** download the archive for Linux or macOS (amd64, arm64, armv7) from the
[releases](https://github.com/dombyte/datalogger/releases) and run:

```bash
datalogger -config config.yaml [-debug]   # -version prints the build info
```

**From source** (Go, see `go.mod` for the version): `make build`.

## Configuration

One YAML file, passed with `-config`. [example/config.yaml](example/config.yaml) is the
complete reference with every option explained. Unknown keys (for example a typo) stop
the program at startup. Keep tokens and passwords only in your local `config.yaml`.

A minimal config: one device, one output.

```yaml
devices:
  - name: meter
    type: http
    poll_interval: 5s
    device_specific:
      http:
        address: "http://192.0.2.20/rpc/EM.GetStatus"
    points:
      - name: power
        json_path: "total_act_power"
        unit: "W"

outputs:
  - name: csv
    type: csv
    output_specific:
      csv:
        file_path: "./data/data.csv"
        max_age: 24h
        max_backups: 7
    devices: [meter]
```

### How data flows

- Every **device** is polled at once on startup, then every `poll_interval`. Each reading
  is stamped with the time the device answered, in UTC.
- Every **output** lists the `devices` it receives. All their points are copied to it,
  except those listed in `exclude_points` (`device/point`).
- Each output has its own queue (`buffer_size`, default 1000). If an output is too slow
  and its queue is full, new points are dropped for that output only and a warning is
  logged; devices and other outputs are not affected.

### Devices

| Type | Notes |
|---|---|
| `modbus` | `tcp://host:port` or `rtu:///dev/ttyUSB0`. **direct** mode reads each point on its own (a failing point is skipped). **range** mode reads register ranges and decodes all points from them (a failing range skips the whole poll). Types `int16`, `uint16`, `int32`, `uint32`, `float32`, `bool`; value = raw × `scale` + `offset`. One register type (3 holding / 4 input) per device. |
| `http` | GET or POST with optional headers and body. `response_type: json` extracts values with [gjson paths](https://github.com/tidwall/gjson/blob/master/SYNTAX.md); `text` delivers the whole body as one string. A path that is missing or `null` skips the point (no fake `0`). |

### Outputs

| Type | Notes |
|---|---|
| `influxdb` | InfluxDB 3.x, batched, with retries. `timeout` per write (default 10 s); `no_sync: true` lets InfluxDB answer before the write is in its WAL on disk (faster on slow storage, a crash can lose the last second). Schema [below](#influxdb-schema). |
| `mqtt` | MQTT v5, one topic per point: `<topic>/<device>/<point>` (default topic `datalogger`), payload `{"value": 230.5, "unit": "V", "timestamp": "2026-10-05T10:15:02.123Z"}`. `tcp://` or TLS via `tls://`, `ssl://`, `mqtts://`. Points are dropped (and counted) while the broker is unreachable. |
| `csv` | Columns `timestamp, device, point, value, unit`, flushed per row. Rotated after `max_age` to `<file>.<YYYYMMDD-HHMMSS>`; `max_backups` 0 = keep none, > 0 = keep that many, < 0 = keep all. |
| `api` | Read-only JSON API with the latest value of every point, see [below](#http-api). |

### HTTP API

```yaml
outputs:
  - name: api
    type: api
    output_specific:
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

- `age_ms` is the time since the device answered. A device that stops answering keeps
  its last values while their age grows.
- Unknown devices and points without a value are `404`. NaN and infinite values are
  `null`.
- Values live in memory only: after a restart the API is empty until the first poll.

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
- Numbers become float64; true/false and text are kept. InfluxDB stores one type per
  field and device, so keep text and true/false values out of it with `exclude_points`.
- An expression that does not compile, or names an unknown point or lookup, stops the
  program at startup. One that fails while running skips the point and is logged.

## Running

- Devices and outputs connect in the background: one that is unreachable at startup (or
  later) is retried with backoff and does not stop the others. Settings that cannot work
  at all (an invalid address, a port in use, a bad expression) stop the program at
  startup.
- SIGINT/SIGTERM stop the devices first, then every output writes what is queued; this
  is bounded by 8 seconds.
- Exit code 0 after a clean stop, 1 on configuration or startup errors, when a component
  stops unexpectedly, or when the shutdown takes too long. Run it with a restart policy
  (`restart: unless-stopped` in the compose file).

## InfluxDB schema

| Element | Content | Example |
|---|---|---|
| Measurement (table) | device name | `spine` |
| Tag `point` | point name | `a_voltage` |
| Tag `unit` | unit (omitted when empty) | `V` |
| Field `value` | the reading | `230.5` |
| Time | when the device answered | |

```
spine,point=a_voltage,unit=V value=230.5 1712345678901234567
```

### Query examples (SQL, Grafana FlightSQL)

One point in the dashboard time range:
```sql
SELECT time AS "Time", value AS "A Phase Voltage"
FROM spine
WHERE point = 'a_voltage' AND time >= $__timeFrom() AND time <= $__timeTo()
```

Several points, or all points of a device:
```sql
SELECT time AS "Time", point, value
FROM spine
WHERE point IN ('a_voltage', 'b_voltage', 'c_voltage')
  AND time >= $__timeFrom() AND time <= $__timeTo()
```

Averaged to the Grafana interval:
```sql
SELECT
  date_bin(INTERVAL '${__interval_ms} milliseconds', time) AS "Time",
  avg(value) AS "A Phase Voltage"
FROM spine
WHERE point = 'a_voltage' AND time >= $__timeFrom() AND time <= $__timeTo()
GROUP BY 1
ORDER BY 1
```

Latest value of every point:
```sql
SELECT point, value, unit, time
FROM (
  SELECT point, value, unit, time,
         row_number() OVER (PARTITION BY point ORDER BY time DESC) AS rn
  FROM spine
  WHERE time >= now() - INTERVAL '1 hour'
)
WHERE rn = 1
ORDER BY point
```
