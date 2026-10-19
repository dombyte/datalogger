package app

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

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
	"github.com/dombyte/datalogger/internal/output/webhook"
	"github.com/dombyte/datalogger/internal/transform"
)

// rtuScheme starts the address of a Modbus device on a serial port.
const rtuScheme = "rtu://"

// createBuses creates one Bus per serial port of the Modbus devices, by address. The
// port has one timeout for all requests: the largest of its devices, so no device is
// cut off before its own timeout.
func createBuses(devices []config.Device, log zerolog.Logger) (map[string]*modbus.Bus, error) {
	ports := make(map[string]modbus.ConnSettings)
	users := make(map[string][]string)
	for _, d := range devices {
		if d.Type != "modbus" || !strings.HasPrefix(d.Modbus.Address, rtuScheme) {
			continue
		}
		s, ok := ports[d.Modbus.Address]
		if !ok {
			s = modbusConnSettings(d) // validation checked that the serial settings match
		}
		s.Timeout = max(s.Timeout, d.Timeout)
		ports[d.Modbus.Address] = s
		users[d.Modbus.Address] = append(users[d.Modbus.Address], d.Name)
	}

	buses := make(map[string]*modbus.Bus, len(ports))
	for _, port := range slices.Sorted(maps.Keys(ports)) {
		bus, err := createBus(port, ports[port], log)
		if err != nil {
			return nil, fmt.Errorf("serial port %s: %w", port, err)
		}
		log.Info().Str("port", port).Strs("devices", users[port]).
			Stringer("timeout", ports[port].Timeout).Msg("Modbus devices share serial port")
		buses[port] = bus
	}
	return buses, nil
}

// createBus creates the Bus of one serial port.
func createBus(port string, s modbus.ConnSettings, log zerolog.Logger) (*modbus.Bus, error) {
	opener, err := modbus.NewDialer(s, log)
	if err != nil {
		return nil, err
	}
	return modbus.NewBus(modbus.BusDeps{Port: port, Opener: opener, Log: log})
}

// modbusConnSettings maps a device config onto the connection settings of its device.
func modbusConnSettings(d config.Device) modbus.ConnSettings {
	m := d.Modbus
	return modbus.ConnSettings{
		Address:  m.Address,
		SlaveID:  m.SlaveID,
		Speed:    m.Speed,
		DataBits: m.DataBits,
		Parity:   m.Parity,
		StopBits: m.StopBits,
		Timeout:  d.Timeout,
	}
}

// createSource creates the reader of a device and the transformer for its points.
func createSource(
	d config.Device,
	lookups map[string]map[int]string,
	buses map[string]*modbus.Bus,
	log zerolog.Logger,
	clk clock.Clock,
) (source, error) {
	r, err := createReader(d, buses, log, clk)
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
	buses map[string]*modbus.Bus,
	log zerolog.Logger,
	clk clock.Clock,
) (datasource.DeviceReader, error) {
	switch d.Type {
	case "modbus":
		return createModbusReader(d, buses[d.Modbus.Address], log, clk)
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
		c := o.Csv
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
	case "webhook":
		return createWebhookWriter(o, log, clk)
	default:
		return nil, fmt.Errorf("unknown output type %q", o.Type)
	}
}

// createModbusReader maps a device config onto a Modbus reader and its dialer: its own
// connection, or its slave on bus (nil for a TCP device). An error is a configuration
// error; an unreachable device is not one (it reconnects).
func createModbusReader(
	d config.Device,
	bus *modbus.Bus,
	log zerolog.Logger,
	clk clock.Clock,
) (datasource.DeviceReader, error) {
	m := d.Modbus
	var dialer modbus.Dialer
	if bus != nil {
		dialer = bus.Dialer(m.SlaveID)
	} else {
		libDialer, err := modbus.NewDialer(modbusConnSettings(d), log)
		if err != nil {
			return nil, err
		}
		dialer = libDialer
	}

	return modbus.New(modbus.Deps{
		Settings: modbus.Settings{
			Name:         d.Name,
			PollInterval: d.PollInterval,
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
	h := d.HTTP
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
	i := o.Influxdb
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
			BatchSize:    i.BatchSize,
			BatchTimeout: i.BatchTimeout,
			MaxRetries:   i.MaxRetries,
			RetryDelay:   i.RetryDelay,
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
	m := o.Mqtt
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
	a := o.API
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

// createWebhookWriter maps an output config onto a webhook writer with its own client.
func createWebhookWriter(
	o config.Output,
	log zerolog.Logger,
	clk clock.Clock,
) (output.Writer, error) {
	h := o.Webhook
	return webhook.New(webhook.Deps{
		Settings: webhook.Settings{
			Name:       o.Name,
			URL:        h.URL,
			Method:     h.Method,
			Headers:    h.Headers,
			MaxBatch:   h.MaxBatch,
			MaxRetries: h.MaxRetries,
			RetryDelay: h.RetryDelay,
		},
		Client: webhook.NewClient(h.Timeout, h.Insecure),
		Clock:  clk,
		Log:    log,
	})
}
