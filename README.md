# Datalogger

A flexible data logging tool for collecting metrics from various sources (Modbus, HTTP) and writing to multiple outputs (InfluxDB, MQTT, CSV).

## Configuration

Configuration is done via YAML file (passed with `-config` flag). See [example/config.yaml](example/config.yaml) for a complete reference.

**Input types:**
- **Modbus:** TCP/RTU with range or direct register access
- **HTTP:** JSON responses with [gjson path syntax](https://github.com/tidwall/gjson/blob/master/SYNTAX.md) for value extraction

**Output types:** InfluxDB 3.x, MQTT, CSV

## Running

```bash
datalogger -config config.yaml [-debug]   # -version prints the build info
```

- Devices and outputs connect in the background: one that is unreachable at startup (or
  later) is retried with backoff and does not stop the others. Settings that cannot work
  at all (for example an unsupported address scheme) stop the program at startup.
- SIGINT/SIGTERM stop the devices first, then every output writes what is queued; this
  is bounded by 8 seconds.
- Exit code 0 after a clean stop, 1 on configuration or startup errors, when a component
  stops unexpectedly, or when the shutdown takes too long. Run it with a restart policy
  (for example `restart: unless-stopped` in Docker Compose).

## InfluxDB 3.x Schema Design

### Schema Structure

| Element | Type | Description | Example |
|---------|------|-------------|---------|
| Measurement | Table name | Device name | `spine` |
| Tag: `point` | Metadata | Identifies the measurement point | `a_voltage`, `temperature` |
| Tag: `unit` | Metadata | Unit of measurement (optional) | `V`, `C`, `%` |
| Field: `value` | Measured data | The actual reading value | `230.5`, `45.2` |
| Timestamp | Time | When the reading was taken | Unix nanoseconds |

**Line Protocol format:**
```
spine,point=a_voltage,unit=V value=230.5 1712345678901234567
```

## InfluxDB 3.x Query Examples

**Basic time series query for a single point:**
```sql
SELECT time AS "Time", value AS "A Phase Voltage"
FROM spine
WHERE point = 'a_voltage'
```

**With Grafana time range filter (FlightSQL):**
```sql
SELECT time AS "Time", value AS "A Phase Voltage"
FROM spine
WHERE point = 'a_voltage' AND time >= $__timeFrom() AND time <= $__timeTo()
```

**Multiple points in one query:**
```sql
SELECT time AS "Time", value AS "Voltage"
FROM spine
WHERE point IN ('a_voltage', 'b_voltage', 'c_voltage')
  AND time >= $__timeFrom() AND time <= $__timeTo()
```

**Query all points from a device:**
```sql
SELECT time AS "Time", point, value
FROM spine
WHERE time >= $__timeFrom() AND time <= $__timeTo()
```

**Query with unit filter:**
```sql
SELECT time AS "Time", point, value, unit
FROM spine
WHERE unit = 'V' AND time >= $__timeFrom() AND time <= $__timeTo()
```

**Aggregate by point (average voltage):**
```sql
SELECT point, AVG(value) AS "Average Value"
FROM spine
WHERE point LIKE '%voltage%' AND time >= $__timeFrom() AND time <= $__timeTo()
GROUP BY point
```

**Latest value for each point:**
```sql
SELECT point, value AS "Latest Value", time AS "Time"
FROM spine
WHERE time >= $__timeFrom() AND time <= $__timeTo()
GROUP BY point
ORDER BY time DESC
LIMIT 10
```

**With Grafana time range filter and interval:**
```sql
SELECT
  date_bin(INTERVAL ${__interval_ms} milliseconds, time) AS "Time",
  avg(value) AS "A Phase Voltage"
FROM spine
WHERE
  point = 'a_voltage'
  AND time >= $__timeFrom()
  AND time <= $__timeTo()
GROUP BY date_bin(INTERVAL ${__interval_ms} milliseconds, time)
```

**With Grafana time range filter and custom variable:**
```sql
SELECT
  time_bucket AS "Time",
  avg(value) AS "A Phase Voltage"
FROM (
  SELECT
    CASE
      WHEN '${group_interval:raw}' = 'raw' THEN time
      ELSE date_bin(INTERVAL '${group_interval:raw}', time)
    END AS time_bucket,
    value
  FROM spine
  WHERE
    point = 'a_voltage'
    AND time >= $__timeFrom()
    AND time <= $__timeTo()
)
GROUP BY time_bucket
```

**With Grafana time range filter and custom raw-variable:**
```sql
SELECT
  time_bucket AS "Time",
  avg(value) AS "A Phase Voltage"
FROM (
  SELECT
    CASE
      WHEN '${group_interval:raw}' = 'raw' THEN time
      WHEN '${group_interval:raw}' = '100ms' THEN date_bin(INTERVAL 100 milliseconds, time)
      WHEN '${group_interval:raw}' = '250ms' THEN date_bin(INTERVAL 250 milliseconds, time)
      WHEN '${group_interval:raw}' = '500ms' THEN date_bin(INTERVAL 500 milliseconds, time)
      WHEN '${group_interval:raw}' = '1s' THEN date_bin(INTERVAL 1000 milliseconds, time)
      WHEN '${group_interval:raw}' = '5s' THEN date_bin(INTERVAL 5000 milliseconds, time)
      WHEN '${group_interval:raw}' = '10s' THEN date_bin(INTERVAL 10000 milliseconds, time)
      WHEN '${group_interval:raw}' = '30s' THEN date_bin(INTERVAL 30000 milliseconds, time)
      WHEN '${group_interval:raw}' = '1m' THEN date_bin(INTERVAL 60000 milliseconds, time)
      WHEN '${group_interval:raw}' = '5m' THEN date_bin(INTERVAL 300000 milliseconds, time)
      WHEN '${group_interval:raw}' = '10m' THEN date_bin(INTERVAL 600000 milliseconds, time)
      WHEN '${group_interval:raw}' = '30m' THEN date_bin(INTERVAL 1800000 milliseconds, time)
      WHEN '${group_interval:raw}' = '1h' THEN date_bin(INTERVAL 3600000 milliseconds, time)
    END AS time_bucket,
    value
  FROM spine
  WHERE
    point = 'a_voltage'
    AND time >= $__timeFrom()
    AND time <= $__timeTo()
)
GROUP BY time_bucket
```
