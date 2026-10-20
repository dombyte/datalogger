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

// createBuses creates the Bus of every Modbus device: one per serial port, shared by
// the devices on it, and one per TCP device. A serial port has one timeout for all
// requests: the largest of its devices, so no device is cut off before its own timeout.
// It returns the bus of each device by name.
func createBuses(devices []config.Device, log zerolog.Logger) (map[string]*modbus.Bus, error) {
	conns, users := modbusConnections(devices)
	byKey := make(map[string]*modbus.Bus, len(conns))
	for _, key := range slices.Sorted(maps.Keys(conns)) {
		bus, err := createBus(conns[key], users[key], log)
		if err != nil {
			return nil, err
		}
		byKey[key] = bus
	}

	buses := make(map[string]*modbus.Bus)
	for _, d := range devices {
		if d.Type == "modbus" {
			buses[d.Name] = byKey[connKey(d)]
		}
	}
	return buses, nil
}

// modbusConnections returns the settings and the device names of every Modbus
// connection, by connection key.
func modbusConnections(
	devices []config.Device,
) (map[string]modbus.ConnSettings, map[string][]string) {
	conns := make(map[string]modbus.ConnSettings)
	users := make(map[string][]string)
	for _, d := range devices {
		if d.Type != "modbus" {
			continue
		}
		key := connKey(d)
		s, ok := conns[key]
		if !ok {
			s = modbusConnSettings(d) // validation checked that the serial settings match
		}
		s.Timeout = max(s.Timeout, d.Timeout)
		conns[key] = s
		users[key] = append(users[key], d.Name)
	}
	return conns, users
}

// connKey identifies the connection of a Modbus device: devices on one serial port
// share it, every TCP device has its own.
func connKey(d config.Device) string {
	if strings.HasPrefix(d.Modbus.Address, rtuScheme) {
		return d.Modbus.Address
	}
	return "device " + d.Name // never an rtu:// address
}

// createBus creates the Bus of one connection, used by the named devices.
func createBus(s modbus.ConnSettings, devices []string, log zerolog.Logger) (*modbus.Bus, error) {
	serial := strings.HasPrefix(s.Address, rtuScheme)
	opener, err := modbus.NewDialer(s, log)
	switch {
	case err != nil && serial:
		return nil, fmt.Errorf("serial port %s: %w", s.Address, err)
	case err != nil:
		return nil, fmt.Errorf("device %s: %w", devices[0], err)
	case serial:
		log.Info().Str("port", s.Address).Strs("devices", devices).
			Stringer("timeout", s.Timeout).Msg("Modbus devices share serial port")
	}
	return modbus.NewBus(modbus.BusDeps{Address: s.Address, Opener: opener, Log: log})
}

// modbusConnSettings maps a device config onto the settings of its connection.
func modbusConnSettings(d config.Device) modbus.ConnSettings {
	m := d.Modbus
	return modbus.ConnSettings{
		Address:  m.Address,
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
		return createModbusReader(d, buses[d.Name], log, clk)
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

// createModbusReader maps a device config onto a Modbus reader of its slave on bus.
func createModbusReader(
	d config.Device,
	bus *modbus.Bus,
	log zerolog.Logger,
	clk clock.Clock,
) (datasource.DeviceReader, error) {
	m := d.Modbus
	return modbus.New(modbus.Deps{
		Settings: modbus.Settings{
			Name:         d.Name,
			PollInterval: d.PollInterval,
			RangeMode:    m.RegisterMode == "range",
			Ranges:       m.Ranges,
			Points:       modbusPoints(d.Points),
		},
		Dialer: bus.Dialer(m.SlaveID),
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
