// Package app is the composition root: it creates readers and writers from the
// config, routes data points between them and runs the producers-first shutdown.
package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/config"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/output"
	"github.com/dombyte/datalogger/internal/transform"
)

const (
	// defaultBufferSize is the output channel size when buffer_size is not set.
	defaultBufferSize = 1000

	// pathSeparator separates device and point in exclude_points ("device/point").
	pathSeparator = "/"
)

// ErrComponentStopped is returned by Run when a reader or writer stops on its own.
var ErrComponentStopped = errors.New("app: component stopped unexpectedly")

// source is a reader with the transformer that is applied to each of its polls.
type source struct {
	reader    datasource.DeviceReader
	transform *transform.Transformer
}

// outputRoute is a writer with the devices it accepts, the points it skips
// ("device/point") and its input buffer size.
type outputRoute struct {
	writer        output.Writer
	devices       []string
	excludePoints []string
	bufferSize    int
}

// App runs all readers and writers of one config.
type App struct {
	sources []source
	outputs []outputRoute
	log     zerolog.Logger

	cancelSources context.CancelFunc
	cancelOutputs context.CancelFunc
	inputs        []chan datasource.DataPoint
	routers       sync.WaitGroup
	writers       sync.WaitGroup
	stopping      atomic.Bool

	mu        sync.Mutex
	writerErr error
}

// New creates every reader and writer of cfg. An error means the config cannot be run
// (for example an address the client library rejects); devices and brokers that are
// unreachable are not an error, the components connect in the background.
func New(cfg *config.Config, log zerolog.Logger, clk clock.Clock) (*App, error) {
	sources := make([]source, 0, len(cfg.Devices))
	for _, d := range cfg.Devices {
		s, err := createSource(d, cfg.Lookups, log, clk)
		if err != nil {
			return nil, fmt.Errorf("device %s: %w", d.Name, err)
		}
		sources = append(sources, s)
	}

	outputs := make([]outputRoute, 0, len(cfg.Outputs))
	for _, o := range cfg.Outputs {
		w, err := createWriter(o, log, clk)
		if err != nil {
			return nil, fmt.Errorf("output %s: %w", o.Name, err)
		}
		bufferSize := o.BufferSize
		if bufferSize <= 0 {
			bufferSize = defaultBufferSize
		}
		outputs = append(outputs, outputRoute{
			writer: w, devices: o.Devices, excludePoints: o.ExcludePoints, bufferSize: bufferSize,
		})
	}

	return newApp(sources, outputs, log), nil
}

// newApp wires already created components; tests use it with mocks.
func newApp(sources []source, outputs []outputRoute, log zerolog.Logger) *App {
	return &App{
		sources: sources,
		outputs: outputs,
		log:     log.With().Str("component", "app").Logger(),
	}
}

// Run starts all components and blocks until ctx is cancelled (nil) or a component
// stops on its own (ErrComponentStopped). Call Shutdown afterwards in both cases.
func (a *App) Run(ctx context.Context) error {
	sourceCtx, cancelSources := context.WithCancel(context.Background())
	outputCtx, cancelOutputs := context.WithCancel(context.Background())
	a.cancelSources, a.cancelOutputs = cancelSources, cancelOutputs

	failed := make(chan error, len(a.sources)+len(a.outputs))
	inputs := a.startWriters(outputCtx, failed)
	a.startReaders(sourceCtx, inputs, failed)
	a.log.Info().Int("devices", len(a.sources)).Int("outputs", len(a.outputs)).
		Msg("Datalogger started")

	select {
	case <-ctx.Done():
		a.log.Info().Msg("Shutdown signal received")
		return nil
	case err := <-failed:
		return err
	}
}

// Shutdown stops the readers first, lets the writers drain what is queued and waits
// for them to finish. It returns the errors of writers that failed. The caller bounds
// it with a deadline.
func (a *App) Shutdown() error {
	a.stopping.Store(true)
	a.log.Info().Msg("Stopping data sources")
	a.cancelSources()
	a.routers.Wait()

	a.log.Info().Msg("Draining outputs")
	for _, in := range a.inputs {
		close(in)
	}
	a.writers.Wait()
	a.cancelOutputs()

	a.mu.Lock()
	defer a.mu.Unlock()
	return a.writerErr
}

// startWriters starts every writer and returns its input channels by name. A writer
// that stops before Shutdown closed its input is reported on failed.
func (a *App) startWriters(
	ctx context.Context,
	failed chan<- error,
) map[string]chan<- datasource.DataPoint {
	inputs := make(map[string]chan<- datasource.DataPoint, len(a.outputs))
	for _, o := range a.outputs {
		in := make(chan datasource.DataPoint, o.bufferSize)
		a.inputs = append(a.inputs, in)
		inputs[o.writer.Name()] = in

		done := o.writer.Start(ctx, in)
		a.writers.Add(1)
		go a.watchWriter(o.writer.Name(), done, failed)
	}
	return inputs
}

// watchWriter waits for a writer to finish and records why it stopped.
func (a *App) watchWriter(name string, done <-chan error, failed chan<- error) {
	defer a.writers.Done()

	var err error
	for e := range done {
		err = errors.Join(err, e)
	}
	if err != nil {
		a.log.Error().Err(err).Str("output", name).Msg("Output writer failed")
		a.mu.Lock()
		a.writerErr = errors.Join(a.writerErr, fmt.Errorf("output %s: %w", name, err))
		a.mu.Unlock()
	}
	if !a.stopping.Load() {
		failed <- fmt.Errorf("%w: output %s", ErrComponentStopped, name)
	}
}

// startReaders starts every reader with a router that transforms its polls and copies
// the points to the outputs that list the device.
func (a *App) startReaders(
	ctx context.Context,
	inputs map[string]chan<- datasource.DataPoint,
	failed chan<- error,
) {
	for _, s := range a.sources {
		name := s.reader.Name()
		targets := a.targets(name, inputs)
		data := s.reader.Start(ctx)
		a.routers.Add(1)
		go func() {
			defer a.routers.Done()
			route(data, s.transform, targets, a.log)
			if ctx.Err() == nil {
				failed <- fmt.Errorf("%w: device %s", ErrComponentStopped, name)
			}
		}()
	}
}

// targets returns the outputs that accept the device, each with the points of the
// device it skips.
func (a *App) targets(device string, inputs map[string]chan<- datasource.DataPoint) []target {
	var targets []target
	for _, o := range a.outputs {
		if !slices.Contains(o.devices, device) {
			continue
		}
		t := target{name: o.writer.Name(), in: inputs[o.writer.Name()], exclude: map[string]bool{}}
		for _, entry := range o.excludePoints {
			if d, point, _ := strings.Cut(entry, pathSeparator); d == device {
				t.exclude[point] = true
			}
		}
		targets = append(targets, t)
	}
	return targets
}
