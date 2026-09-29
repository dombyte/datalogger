package app

import (
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/config"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/datasource/http"
	"github.com/dombyte/datalogger/internal/datasource/modbus"
)

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
