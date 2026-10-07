package app

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/config"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/datasource/http"
	"github.com/dombyte/datalogger/internal/datasource/modbus"
	"github.com/dombyte/datalogger/internal/output"
	"github.com/dombyte/datalogger/internal/output/api"
	"github.com/dombyte/datalogger/internal/output/csv"
	"github.com/dombyte/datalogger/internal/output/influxdb"
	"github.com/dombyte/datalogger/internal/output/mqtt"
	"github.com/dombyte/datalogger/internal/transform"
)

// createSource creates the reader of a device and the transformer for its points.
func createSource(
	d config.Device,
	lookups map[string]map[int]string,
	log zerolog.Logger,
	clk clock.Clock,
) (source, error) {
	r, err := createReader(d, log, clk)
	if err != nil {
		return source{}, err
	}
	s := transform.Settings{
		Device:  d.Name,
		Points:  make([]string, len(d.Points)),
		Exprs:   make(map[string]string),
		Lookups: lookups,
	}
	for i, p := range d.Points {
		s.Points[i] = p.Name
		if p.Expr != "" {
			s.Exprs[p.Name] = p.Expr
		}
	}
	tr, err := transform.New(s, log)
	if err != nil {
		return source{}, err
	}
	return source{reader: r, transform: tr}, nil
}

// createReader creates the reader for a device type.
func createReader(
	d config.Device,
	log zerolog.Logger,
	clk clock.Clock,
) (datasource.DeviceReader, error) {
	switch d.Type {
	case "modbus":
		return createModbusReader(d, log, clk)
	case "http":
		return createHTTPReader(d, log, clk)
	default:
		return nil, fmt.Errorf("unknown device type %q", d.Type)
	}
}

// createWriter creates the writer for an output type.
func createWriter(
	o config.Output,
	log zerolog.Logger,
	clk clock.Clock,
) (output.Writer, error) {
	switch o.Type {
	case "csv":
		c := o.OutputSpecific.Csv
		return csv.New(csv.Deps{
			Settings: csv.Settings{
				Name:       o.Name,
				FilePath:   c.FilePath,
				MaxAge:     c.MaxAge,
				MaxBackups: c.MaxBackups,
			},
			Clock: clk,
			Log:   log,
		})
	case "influxdb":
		return createInfluxDBWriter(o, log, clk)
	case "mqtt":
		return createMQTTWriter(o, log, clk)
	case "api":
		return createAPIWriter(o, log, clk)
	default:
		return nil, fmt.Errorf("unknown output type %q", o.Type)
	}
}

// createModbusReader maps a device config onto a Modbus reader and its dialer. An
// error is a configuration error; an unreachable device is not one (it reconnects).
func createModbusReader(
	d config.Device,
	log zerolog.Logger,
	clk clock.Clock,
) (datasource.DeviceReader, error) {
	m := d.DeviceSpecific.Modbus
	dialer, err := modbus.NewDialer(modbus.ConnSettings{
		Address:  m.Address,
		SlaveID:  m.SlaveID,
		Speed:    m.Speed,
		DataBits: m.DataBits,
		Parity:   m.Parity,
		StopBits: m.StopBits,
		Timeout:  d.Timeout,
	}, log)
	if err != nil {
		return nil, err
	}

	return modbus.New(modbus.Deps{
		Settings: modbus.Settings{
			Name:         d.Name,
			PollInterval: d.PollInterval,
			Parallelism:  d.Parallelism,
			RangeMode:    m.RegisterMode == "range",
			Ranges:       m.Ranges,
			Points:       modbusPoints(d.Points),
		},
		Dialer: dialer,
		Clock:  clk,
		Log:    log,
	})
}

// modbusPoints maps config points onto Modbus points.
func modbusPoints(points []config.Point) []modbus.Point {
	out := make([]modbus.Point, len(points))
	for i, p := range points {
		out[i] = modbus.Point{
			Name:         p.Name,
			Register:     p.Register,
			Count:        p.Count,
			Type:         p.Type,
			FunctionCode: p.FunctionCode,
			Scale:        p.Scale,
			Offset:       p.Offset,
			Unit:         p.Unit,
		}
	}
	return out
}

// createHTTPReader maps a device config onto an HTTP reader with its own client.
func createHTTPReader(
	d config.Device,
	log zerolog.Logger,
	clk clock.Clock,
) (datasource.DeviceReader, error) {
	h := d.DeviceSpecific.HTTP
	points := make([]http.Point, len(d.Points))
	for i, p := range d.Points {
		points[i] = http.Point{
			Name:     p.Name,
			JSONPath: p.JSONPath,
			Type:     p.Type,
			Scale:    p.Scale,
			Offset:   p.Offset,
			Unit:     p.Unit,
		}
	}

	return http.New(http.Deps{
		Settings: http.Settings{
			Name:         d.Name,
			PollInterval: d.PollInterval,
			Address:      h.Address,
			Method:       h.Method,
			Headers:      h.Headers,
			Body:         h.Body,
			ResponseType: h.ResponseType,
			Points:       points,
		},
		Client: http.NewClient(d.Timeout, h.Insecure),
		Clock:  clk,
		Log:    log,
	})
}

// createInfluxDBWriter maps an output config onto an InfluxDB writer and its client.
func createInfluxDBWriter(
	o config.Output,
	log zerolog.Logger,
	clk clock.Clock,
) (output.Writer, error) {
	i := o.OutputSpecific.Influxdb
	client, err := influxdb.NewClient(influxdb.ConnSettings{
		Address:  i.Address,
		Token:    i.Token,
		Database: i.Database,
		Insecure: i.Insecure,
		Timeout:  i.Timeout,
		NoSync:   i.NoSync,
	})
	if err != nil {
		return nil, err
	}
	return influxdb.New(influxdb.Deps{
		Settings: influxdb.Settings{
			Name:         o.Name,
			BatchSize:    o.BatchSize,
			BatchTimeout: o.BatchTimeout,
			MaxRetries:   o.MaxRetries,
			RetryDelay:   o.RetryDelay,
		},
		Client: client,
		Clock:  clk,
		Log:    log,
	})
}

// createMQTTWriter maps an output config onto an MQTT writer and its dialer.
func createMQTTWriter(
	o config.Output,
	log zerolog.Logger,
	clk clock.Clock,
) (output.Writer, error) {
	m := o.OutputSpecific.Mqtt
	dialer, err := mqtt.NewDialer(mqtt.ConnSettings{
		Address:  m.Address,
		ClientID: m.ClientID,
		Username: m.Username,
		Password: m.Password,
		Insecure: m.Insecure,
	})
	if err != nil {
		return nil, err
	}
	return mqtt.New(mqtt.Deps{
		Settings: mqtt.Settings{
			Name:   o.Name,
			Topic:  m.Topic,
			QoS:    byte(m.QoS), // validated: 0-2
			Retain: m.Retain,
		},
		Dialer: dialer,
		Clock:  clk,
		Log:    log,
	})
}

// createAPIWriter binds the API address and creates the API writer on it.
func createAPIWriter(
	o config.Output,
	log zerolog.Logger,
	clk clock.Clock,
) (output.Writer, error) {
	a := o.OutputSpecific.API
	ln, err := api.Listen(a.Listen)
	if err != nil {
		return nil, err
	}
	w, err := api.New(api.Deps{
		Settings: api.Settings{Name: o.Name, Devices: o.Devices, Token: a.Token},
		Listener: ln,
		Clock:    clk,
		Log:      log,
	})
	if err != nil {
		return nil, errors.Join(err, ln.Close())
	}
	return w, nil
}
