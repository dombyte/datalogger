package app

import (
	"context"
	"errors"
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

// recorder is a writer mock that records points until its input is closed.
type recorder struct {
	mu     sync.Mutex
	points []datasource.DataPoint
	err    error // sent when the input is closed
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
		func(_ context.Context, in <-chan datasource.DataPoint) <-chan error {
			done := make(chan error, 1)
			go func() {
				defer close(done)
				for dp := range in {
					rec.mu.Lock()
					rec.points = append(rec.points, dp)
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
		func(context.Context, <-chan datasource.DataPoint) <-chan error {
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
	full := make(chan datasource.DataPoint) // nobody reads
	roomy := make(chan datasource.DataPoint, 3)

	route(data, noTransform(t), []target{{name: "full", in: full}, {name: "roomy", in: roomy}},
		zerolog.Nop())

	assert.Len(t, roomy, 3)
}

func TestNewFailsOnlyOnConfigurationErrors(t *testing.T) {
	t.Parallel()
	device := func(address string) config.Device {
		return config.Device{
			Name: "inverter", Type: "modbus", PollInterval: time.Second, Parallelism: 1,
			DeviceSpecific: config.DeviceSpecific{Modbus: config.ModbusConfig{
				Address: address, SlaveID: 1, RegisterMode: "direct",
			}},
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
		DeviceSpecific: config.DeviceSpecific{HTTP: config.HTTPConfig{
			Address: "http://localhost", Method: "GET", ResponseType: "json",
		}},
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
		_, err := createSource(d, cfg.Lookups, zerolog.Nop(), clocktest.NewFake(time.Now()))
		assert.NoError(t, err, "device %s", d.Name)
	}
}
