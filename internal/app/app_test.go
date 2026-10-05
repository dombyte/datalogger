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
)

// fakeReader returns a reader mock that sends points, then waits for ctx and closes.
func fakeReader(t *testing.T, name string, points ...datasource.DataPoint) *dsmocks.MockDeviceReader {
	t.Helper()
	r := dsmocks.NewMockDeviceReader(t)
	r.EXPECT().Name().Return(name).Maybe()
	r.EXPECT().Start(mock.Anything).RunAndReturn(func(ctx context.Context) <-chan datasource.DataPoint {
		ch := make(chan datasource.DataPoint)
		go func() {
			defer close(ch)
			for _, dp := range points {
				ch <- dp
			}
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
		[]datasource.DeviceReader{
			fakeReader(t, "dev1", point("dev1"), point("dev1")),
			fakeReader(t, "dev2", point("dev2")),
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
	r.EXPECT().Start(mock.Anything).RunAndReturn(func(context.Context) <-chan datasource.DataPoint {
		ch := make(chan datasource.DataPoint)
		close(ch)
		return ch
	}).Once()
	a := newApp([]datasource.DeviceReader{r}, nil, zerolog.Nop())

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
	data := make(chan datasource.DataPoint, 3)
	for range 3 {
		data <- point("dev1")
	}
	close(data)
	full := make(chan datasource.DataPoint) // nobody reads
	roomy := make(chan datasource.DataPoint, 3)

	route(data, map[string]chan<- datasource.DataPoint{"full": full, "roomy": roomy}, zerolog.Nop())

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
	assert.Len(t, a.readers, 1)
}
