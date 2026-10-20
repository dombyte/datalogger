package app

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/config"
	"github.com/dombyte/datalogger/internal/datasource"
	dsmocks "github.com/dombyte/datalogger/internal/datasource/mocks"
	"github.com/dombyte/datalogger/internal/output"
	outmocks "github.com/dombyte/datalogger/internal/output/mocks"
	"github.com/dombyte/datalogger/internal/transform"
)

// fakeSource returns a source whose reader sends points as one poll, then waits for
// ctx and closes; exprs maps point names to expressions.
func fakeSource(
	t *testing.T, name string, exprs map[string]string, points ...datasource.DataPoint,
) source {
	t.Helper()
	names := make([]string, 0, len(points))
	for _, dp := range points {
		names = append(names, dp.PointName)
	}
	tr, err := transform.New(transform.Settings{Device: name, Points: names, Exprs: exprs},
		zerolog.Nop())
	require.NoError(t, err)
	return source{reader: fakeReader(t, name, points...), transform: tr}
}

// fakeReader returns a reader mock that sends points, then waits for ctx and closes.
func fakeReader(t *testing.T, name string, points ...datasource.DataPoint) *dsmocks.MockDeviceReader {
	t.Helper()
	r := dsmocks.NewMockDeviceReader(t)
	r.EXPECT().Name().Return(name).Maybe()
	r.EXPECT().Start(mock.Anything).RunAndReturn(func(ctx context.Context) <-chan []datasource.DataPoint {
		ch := make(chan []datasource.DataPoint)
		go func() {
			defer close(ch)
			ch <- points
			<-ctx.Done()
		}()
		return ch
	}).Once()
	return r
}

// recorder is a writer mock that records points and batches until its input is
// closed.
type recorder struct {
	mu      sync.Mutex
	points  []datasource.DataPoint
	batches int
	err     error // sent when the input is closed
}

func (rec *recorder) got() []datasource.DataPoint {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.points
}

func fakeWriter(t *testing.T, name string, rec *recorder) *outmocks.MockWriter {
	t.Helper()
	w := outmocks.NewMockWriter(t)
	w.EXPECT().Name().Return(name).Maybe()
	w.EXPECT().Start(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, in <-chan []datasource.DataPoint) <-chan error {
			done := make(chan error, 1)
			go func() {
				defer close(done)
				for batch := range in {
					rec.mu.Lock()
					rec.points = append(rec.points, batch...)
					rec.batches++
					rec.mu.Unlock()
				}
				if rec.err != nil {
					done <- rec.err
				}
			}()
			return done
		}).Once()
	return w
}

// noTransform returns a transformer without expressions.
func noTransform(t *testing.T) *transform.Transformer {
	t.Helper()
	tr, err := transform.New(transform.Settings{}, zerolog.Nop())
	require.NoError(t, err)
	return tr
}

func point(device string) datasource.DataPoint {
	return datasource.DataPoint{DeviceName: device, PointName: "p", Value: 1.0}
}

// runApp runs the app until the test cancels ctx; it returns Run's error channel.
func runApp(a *App) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()
	return cancel, runErr
}

func TestRoutesToListedOutputsAndDrainsOnShutdown(t *testing.T) {
	t.Parallel()
	all, onlyDev1 := &recorder{}, &recorder{}
	a := newApp(
		[]source{
			fakeSource(t, "dev1", nil, point("dev1"), point("dev1")),
			fakeSource(t, "dev2", nil, point("dev2")),
		},
		[]outputRoute{
			{writer: fakeWriter(t, "all", all), devices: []string{"dev1", "dev2"}, bufferSize: 10},
			{writer: fakeWriter(t, "dev1", onlyDev1), devices: []string{"dev1"}, bufferSize: 10},
		},
		zerolog.Nop(),
	)

	cancel, runErr := runApp(a)
	require.Eventually(t, func() bool { return len(all.got()) == 3 }, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-runErr)
	require.NoError(t, a.Shutdown())

	assert.Len(t, all.got(), 3)
	assert.Equal(t, []datasource.DataPoint{point("dev1"), point("dev1")}, onlyDev1.got())
	assert.Equal(t, 1, onlyDev1.batches, "one batch per poll")
}

func TestReaderThatStopsFailsRun(t *testing.T) {
	t.Parallel()
	r := dsmocks.NewMockDeviceReader(t)
	r.EXPECT().Name().Return("dev1").Maybe()
	r.EXPECT().Start(mock.Anything).RunAndReturn(func(context.Context) <-chan []datasource.DataPoint {
		ch := make(chan []datasource.DataPoint)
		close(ch)
		return ch
	}).Once()
	a := newApp([]source{{reader: r, transform: noTransform(t)}}, nil, zerolog.Nop())

	_, runErr := runApp(a)
	err := <-runErr
	require.ErrorIs(t, err, ErrComponentStopped)
	assert.ErrorContains(t, err, "device dev1")
	assert.NoError(t, a.Shutdown())
}

func TestWriterThatStopsFailsRunAndShutdown(t *testing.T) {
	t.Parallel()
	w := outmocks.NewMockWriter(t)
	w.EXPECT().Name().Return("broken").Maybe()
	w.EXPECT().Start(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, <-chan []datasource.DataPoint) <-chan error {
			done := make(chan error, 1)
			done <- errors.New("disk full")
			close(done)
			return done
		}).Once()
	a := newApp(nil, []outputRoute{{writer: w, bufferSize: 1}}, zerolog.Nop())

	_, runErr := runApp(a)
	require.ErrorIs(t, <-runErr, ErrComponentStopped)
	assert.ErrorContains(t, a.Shutdown(), "output broken: disk full")
}

func TestRouteDropsOnlyForFullOutput(t *testing.T) {
	t.Parallel()
	data := make(chan []datasource.DataPoint, 1)
	data <- []datasource.DataPoint{point("dev1"), point("dev1"), point("dev1")}
	close(data)
	full := make(chan []datasource.DataPoint) // nobody reads
	roomy := make(chan []datasource.DataPoint, 1)

	route(data, noTransform(t), "dev1",
		[]target{{name: "full", in: full}, {name: "roomy", in: roomy}}, zerolog.Nop())

	require.Len(t, roomy, 1, "one batch per poll")
	assert.Len(t, <-roomy, 3)
}

// Each output gets a batch of its own without its excluded points; a poll that has
// nothing left for an output sends nothing to it.
func TestRouteSendsFilteredCopyPerOutput(t *testing.T) {
	t.Parallel()
	a := datasource.DataPoint{DeviceName: "dev1", PointName: "a", Value: 1.0}
	b := datasource.DataPoint{DeviceName: "dev1", PointName: "b", Value: 2.0}
	data := make(chan []datasource.DataPoint, 2)
	data <- []datasource.DataPoint{a, b}
	data <- []datasource.DataPoint{b}
	close(data)
	all := make(chan []datasource.DataPoint, 2)
	noB := make(chan []datasource.DataPoint, 2)

	route(data, noTransform(t), "dev1", []target{
		{name: "all", in: all},
		{name: "noB", in: noB, exclude: map[string]bool{"b": true}},
	}, zerolog.Nop())

	require.Len(t, noB, 1, "the second poll has nothing for noB")
	assert.Equal(t, []datasource.DataPoint{a}, <-noB)
	require.Len(t, all, 2)
	assert.Equal(t, []datasource.DataPoint{a, b}, <-all)
	assert.Equal(t, []datasource.DataPoint{b}, <-all)
}

// Regression: a stalled output logged one warning per dropped point (hundreds per
// second); now one warning per stall and one line with the count when it recovers.
func TestRouteLogsDropsOncePerStall(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	in := make(chan []datasource.DataPoint, 1)
	targets := []target{{name: "slow", in: in}}
	log := zerolog.New(&logs)

	poll := []datasource.DataPoint{point("dev1"), point("dev1")}
	for range 3 {
		targets[0].send(poll, log) // the first fills the buffer, two polls are dropped
	}
	<-in
	targets[0].send(poll, log) // room again
	for range 2 {
		targets[0].send(poll, log) // a new stall
	}

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	require.Len(t, lines, 3)
	assert.Contains(t, lines[0], `"level":"warn"`)
	assert.Contains(t, lines[1], `"dropped":4`)
	assert.Contains(t, lines[1], `"level":"info"`)
	assert.Contains(t, lines[2], `"level":"warn"`)
}

func TestNewFailsOnlyOnConfigurationErrors(t *testing.T) {
	t.Parallel()
	device := func(address string) config.Device {
		return config.Device{
			Name: "inverter", Type: "modbus", PollInterval: time.Second,
			Modbus: &config.ModbusConfig{
				Address: address, SlaveID: 1, RegisterMode: "direct",
			},
			Points: []config.Point{{Name: "p", Register: 1, Type: "uint16", Scale: 1}},
		}
	}
	clk := clocktest.NewFake(time.Now())

	_, err := New(&config.Config{Devices: []config.Device{device("invalid://x")}}, zerolog.Nop(), clk)
	assert.ErrorContains(t, err, "device inverter")

	// Nothing listens on port 1: the reader is created and connects in the background.
	a, err := New(&config.Config{Devices: []config.Device{device("tcp://127.0.0.1:1")}},
		zerolog.Nop(), clk)
	require.NoError(t, err)
	assert.Len(t, a.sources, 1)
}

func TestRouteTransformsAndExcludesPerOutput(t *testing.T) {
	t.Parallel()
	dp := func(name string, v any) datasource.DataPoint {
		return datasource.DataPoint{DeviceName: "solis", PointName: name, Value: v}
	}
	all, noText := &recorder{}, &recorder{}
	a := newApp(
		[]source{fakeSource(t, "solis",
			map[string]string{
				"power":  "points.dir == 1 ? value : -value",
				"status": `value == 3 ? "Generating" : "other"`,
			},
			dp("dir", 0.0), dp("power", 1200.0), dp("status", 3.0),
		)},
		[]outputRoute{
			{writer: fakeWriter(t, "all", all), devices: []string{"solis"}, bufferSize: 10},
			{
				writer: fakeWriter(t, "no_text", noText), devices: []string{"solis"},
				excludePoints: []string{"solis/status", "other/power"}, bufferSize: 10,
			},
		},
		zerolog.Nop(),
	)

	cancel, runErr := runApp(a)
	require.Eventually(t, func() bool { return len(all.got()) == 3 && len(noText.got()) == 2 },
		time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-runErr)
	require.NoError(t, a.Shutdown())

	assert.Equal(t, []datasource.DataPoint{
		dp("dir", 0.0), dp("power", -1200.0), dp("status", "Generating"),
	}, all.got())
	assert.Equal(t, []datasource.DataPoint{dp("dir", 0.0), dp("power", -1200.0)}, noText.got(),
		"solis/status is excluded; other/power belongs to another device")
}

func TestNewFailsOnInvalidExpression(t *testing.T) {
	t.Parallel()
	d := config.Device{
		Name: "meter", Type: "http", PollInterval: time.Second, Timeout: time.Second,
		HTTP: &config.HTTPConfig{
			Address: "http://localhost", Method: "GET", ResponseType: "json",
		},
		Points: []config.Point{{Name: "p", JSONPath: "p", Scale: 1, Expr: "points.q"}},
	}

	_, err := New(&config.Config{Devices: []config.Device{d}}, zerolog.Nop(),
		clocktest.NewFake(time.Now()))

	require.ErrorIs(t, err, transform.ErrInvalidExpression)
	assert.ErrorContains(t, err, "device meter")
}

func TestExampleConfigExpressionsCompile(t *testing.T) {
	t.Parallel()
	cfg, err := config.Load("../../example/config.yaml")
	require.NoError(t, err)

	for _, d := range cfg.Devices {
		_, err := createSource(d, cfg.Lookups, nil, zerolog.Nop(), clocktest.NewFake(time.Now()))
		assert.NoError(t, err, "device %s", d.Name)
	}
}

func TestNewCreatesEveryOutputType(t *testing.T) {
	t.Parallel()
	device := config.Device{
		Name: "meter", Type: "http", PollInterval: time.Second, Timeout: time.Second,
		HTTP: &config.HTTPConfig{
			Address: "http://localhost", Method: "GET", ResponseType: "json",
		},
		Points: []config.Point{{Name: "p", JSONPath: "p", Scale: 1}},
	}
	csvOut := config.Output{
		Name: "csv", Type: "csv", Devices: []string{"meter"},
		Csv: &config.CsvConfig{FilePath: filepath.Join(t.TempDir(), "data.csv")},
	}
	influxOut := config.Output{
		Name: "influx", Type: "influxdb", BufferSize: 5,
		Influxdb: &config.InfluxdbConfig{Address: "http://db:8181", Token: "t", Database: "d"},
	}
	mqttOut := config.Output{
		Name: "mqtt", Type: "mqtt",
		Mqtt: &config.MqttConfig{Address: "tcp://broker:1883", ClientID: "c"},
	}
	apiOut := config.Output{
		Name: "api", Type: "api",
		API: &config.APIConfig{Listen: "127.0.0.1:0"},
	}
	hookOut := config.Output{
		Name: "hook", Type: "webhook",
		Webhook: &config.WebhookConfig{URL: "https://example.com/hook"},
	}
	clk := clocktest.NewFake(time.Now())

	a, err := New(&config.Config{
		Devices: []config.Device{device},
		Outputs: []config.Output{csvOut, influxOut, mqttOut, apiOut, hookOut},
	}, zerolog.Nop(), clk)
	require.NoError(t, err, "constructors do not connect")
	require.Len(t, a.outputs, 5)
	assert.Equal(t, defaultBufferSize, a.outputs[0].bufferSize)
	assert.Equal(t, 5, a.outputs[1].bufferSize)
	stopWriter(t, a.outputs[3].writer) // releases the bound port

	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = busy.Close() }()

	for name, change := range map[string]func(*config.Output){
		"unknown output type": func(o *config.Output) { o.Type = "kafka" },
		// The blocks are pointers: each case sets its own instead of changing the shared one.
		"influxdb address": func(o *config.Output) {
			*o = influxOut
			o.Influxdb = &config.InfluxdbConfig{Token: "t", Database: "d"}
		},
		"mqtt scheme": func(o *config.Output) {
			*o = mqttOut
			o.Mqtt = &config.MqttConfig{Address: "http://broker", ClientID: "c"}
		},
		"api port in use": func(o *config.Output) {
			*o = apiOut
			o.API = &config.APIConfig{Listen: busy.Addr().String()}
		},
		"webhook scheme": func(o *config.Output) {
			*o = hookOut
			o.Webhook = &config.WebhookConfig{URL: "ftp://example.com/hook"}
		},
	} {
		out := csvOut
		change(&out)
		_, err := New(&config.Config{Devices: []config.Device{device}, Outputs: []config.Output{out}},
			zerolog.Nop(), clk)
		assert.ErrorContains(t, err, "output "+out.Name, name)
	}

	device.Type = "snmp"
	_, err = New(&config.Config{Devices: []config.Device{device}}, zerolog.Nop(), clk)
	assert.ErrorContains(t, err, `unknown device type "snmp"`)
}

// stopWriter starts w with a closed input and waits until it stopped.
func stopWriter(t *testing.T, w output.Writer) {
	t.Helper()
	in := make(chan []datasource.DataPoint)
	close(in)
	for err := range w.Start(context.Background(), in) {
		require.NoError(t, err)
	}
}

// Modbus devices on one serial port share one bus with the largest timeout of its
// devices, a TCP device has its own; they run and stop with the readers.
func TestNewSharesSerialPort(t *testing.T) {
	t.Parallel()
	device := func(name, address string, timeout time.Duration) config.Device {
		return config.Device{
			Name: name, Type: "modbus", PollInterval: time.Second, Timeout: timeout,
			Modbus: &config.ModbusConfig{
				Address: address, SlaveID: 1, Speed: 9600, RegisterMode: "direct",
			},
			Points: []config.Point{{Name: "p", Register: 1, Type: "uint16", Scale: 1}},
		}
	}
	const port = "rtu:///dev/datalogger-test-missing"
	var logs bytes.Buffer
	a, err := New(&config.Config{Devices: []config.Device{
		device("meter", port, time.Second),
		device("inverter", port, 3*time.Second),
		device("gateway", "tcp://127.0.0.1:1", time.Second),
	}}, zerolog.New(zerolog.SyncWriter(&logs)), clocktest.NewFake(time.Now()))
	require.NoError(t, err)
	require.Len(t, a.buses, 2, "the serial port and the TCP device")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, a.Run(ctx))
	require.NoError(t, a.Shutdown(), "the readers stop before their bus")
	assert.Contains(t, logs.String(), // read after Shutdown: the readers log too
		`"port":"`+port+`","devices":["meter","inverter"],"timeout":"3s"`)
}
