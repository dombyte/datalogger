// Package mqtt publishes data points to an MQTT v5 broker, one topic per point.
package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

// Backoff between failed connection attempts: doubles up to maxBackoff.
const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
	backoffFactor  = 2
)

// ErrMissingDependency is returned by New when a required dependency is missing.
var ErrMissingDependency = errors.New("mqtt: missing dependency")

// Settings configures a Writer.
type Settings struct {
	Name   string
	Topic  string // prefix: <topic>/<device>/<point>
	QoS    byte
	Retain bool
}

// Deps are the dependencies of a Writer; all are required.
type Deps struct {
	Settings Settings
	Dialer   Dialer
	Clock    clock.Clock
	Log      zerolog.Logger
}

// Writer publishes every point as JSON {"value","unit","timestamp"}. It connects on the
// first point and reconnects after errors; points that arrive while no connection is
// possible are dropped and counted.
type Writer struct {
	settings Settings
	dialer   Dialer
	clock    clock.Clock
	logger   zerolog.Logger

	session  Publisher // nil while disconnected
	backoff  time.Duration
	nextDial time.Time
	dropped  int
}

// New creates a Writer; it does not connect.
func New(d Deps) (*Writer, error) {
	var missing []string
	if d.Dialer == nil {
		missing = append(missing, "Dialer")
	}
	if d.Clock == nil {
		missing = append(missing, "Clock")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrMissingDependency, strings.Join(missing, ", "))
	}
	return &Writer{
		settings: d.Settings,
		dialer:   d.Dialer,
		clock:    d.Clock,
		logger:   d.Log.With().Str("component", "mqtt").Str("output", d.Settings.Name).Logger(),
	}, nil
}

// Name returns the output name.
func (w *Writer) Name() string {
	return w.settings.Name
}

// Start publishes points until input is closed (or ctx is cancelled), then
// disconnects and closes the returned channel.
func (w *Writer) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer w.disconnect()
		for {
			select {
			case <-ctx.Done():
				return
			case dp, ok := <-input:
				if !ok {
					return
				}
				w.publish(ctx, dp)
			}
		}
	}()
	return done
}

// publish sends one point, connecting first if needed.
func (w *Writer) publish(ctx context.Context, dp datasource.DataPoint) {
	if !w.connected(ctx) {
		w.dropped++
		return
	}

	topic := fmt.Sprintf("%s/%s/%s", w.settings.Topic, dp.DeviceName, dp.PointName)
	payload, err := json.Marshal(map[string]any{
		"value":     dp.Value,
		"unit":      dp.Unit,
		"timestamp": dp.Timestamp.Format(time.RFC3339Nano),
	})
	if err != nil {
		w.logger.Error().Err(err).Str("topic", topic).Msg("Failed to encode MQTT payload")
		return
	}

	_, err = w.session.Publish(ctx, &paho.Publish{
		Topic:   topic,
		Payload: payload,
		QoS:     w.settings.QoS,
		Retain:  w.settings.Retain,
	})
	if err != nil {
		w.logger.Error().Err(err).Str("topic", topic).Msg("Failed to publish, reconnecting")
		w.disconnect()
		return
	}
	w.logger.Debug().Str("topic", topic).Msg("Published MQTT message")
}

// connected returns true with an open session; it dials when the backoff allows.
func (w *Writer) connected(ctx context.Context) bool {
	if w.session != nil {
		select {
		case <-w.session.Done():
			w.logger.Warn().Msg("MQTT connection lost")
			w.session = nil
		default:
			return true
		}
	}
	if w.clock.Now().Before(w.nextDial) {
		return false
	}

	session, err := w.dialer.Dial(ctx)
	if err != nil {
		w.backoff = min(max(w.backoff*backoffFactor, initialBackoff), maxBackoff)
		w.nextDial = w.clock.Now().Add(w.backoff)
		w.logger.Error().Err(err).Dur("retry_in", w.backoff).Msg("MQTT connect failed")
		return false
	}

	w.session, w.backoff = session, 0
	log := w.logger.Info()
	if w.dropped > 0 {
		log = log.Int("dropped", w.dropped)
		w.dropped = 0
	}
	log.Msg("Connected to MQTT broker")
	return true
}

// disconnect ends the current session, if any.
func (w *Writer) disconnect() {
	if w.session == nil {
		return
	}
	if err := w.session.Disconnect(&paho.Disconnect{}); err != nil {
		w.logger.Debug().Err(err).Msg("MQTT disconnect failed")
	}
	w.session = nil
}
