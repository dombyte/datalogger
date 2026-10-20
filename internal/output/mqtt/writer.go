// Package mqtt publishes data points to an MQTT v5 broker, one topic per point.
package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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

	// reasonCodeFailure is the first MQTT v5 reason code that reports an error.
	reasonCodeFailure = 0x80

	// maxInflight bounds the publishes of one poll that wait for their acknowledgement
	// at the same time (QoS 1 and 2); the broker's receive maximum may limit them more.
	maxInflight = 64
)

// errNotSent is the result of a QoS 0 publish skipped after a failure in its poll.
var errNotSent = errors.New("not sent after a failed publish")

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
	topics   map[topicKey]string // topic of every point published so far
}

// topicKey identifies the topic of a point.
type topicKey struct {
	device, point string
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
		topics:   make(map[topicKey]string),
	}, nil
}

// Name returns the output name.
func (w *Writer) Name() string {
	return w.settings.Name
}

// Start publishes points until input is closed (or ctx is cancelled), then
// disconnects and closes the returned channel.
func (w *Writer) Start(ctx context.Context, input <-chan []datasource.DataPoint) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer w.disconnect()
		for {
			select {
			case <-ctx.Done():
				return
			case points, ok := <-input:
				if !ok {
					return
				}
				w.publishPoll(ctx, points)
			}
		}
	}()
	return done
}

// publishPoll publishes the points of one poll, connecting first if needed. With QoS 1
// and 2 the publishes run in parallel, up to maxInflight: a poll costs about one round
// trip to the broker instead of one per point. The next poll starts when all are
// answered, so the messages of a topic stay in order.
func (w *Writer) publishPoll(ctx context.Context, points []datasource.DataPoint) {
	if !w.connected(ctx) {
		w.dropped += len(points)
		return
	}
	msgs := w.messages(points)
	var results []result
	if w.settings.QoS == 0 {
		results = w.publishInOrder(ctx, msgs)
	} else {
		results = w.publishParallel(ctx, msgs)
	}
	w.handleResults(msgs, results)
}

// result is the outcome of one publish.
type result struct {
	resp *paho.PublishResponse
	err  error
}

// publishInOrder publishes one message after the other (QoS 0 waits for nothing, so
// parallel publishes would only add goroutines). After a failure the rest is not sent:
// it would fail on the same connection.
func (w *Writer) publishInOrder(ctx context.Context, msgs []*paho.Publish) []result {
	results := make([]result, len(msgs))
	for i, msg := range msgs {
		resp, err := w.session.Publish(ctx, msg)
		results[i] = result{resp: resp, err: err}
		if err != nil {
			for j := i + 1; j < len(msgs); j++ {
				results[j] = result{err: errNotSent}
			}
			break
		}
	}
	return results
}

// publishParallel publishes the messages concurrently, at most maxInflight at a time,
// and waits for all of them.
func (w *Writer) publishParallel(ctx context.Context, msgs []*paho.Publish) []result {
	results := make([]result, len(msgs))
	session := w.session
	slots := make(chan struct{}, maxInflight)
	var wg sync.WaitGroup
	for i, msg := range msgs {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			resp, err := session.Publish(ctx, msg)
			results[i] = result{resp: resp, err: err}
		})
	}
	wg.Wait()
	return results
}

// handleResults logs the publishes of a poll. A message the broker rejected (PUBACK or
// PUBREC with an error reason code, e.g. the ACL denies the topic) keeps the session;
// any other failure drops it, once per poll. The first redial is immediate, repeated
// failures without a successful publish back off like failed dials, so a broker that
// closes the connection on every publish is not redialed for every poll.
func (w *Writer) handleResults(msgs []*paho.Publish, results []result) {
	var failed int
	var firstErr error
	var firstTopic string
	for i, r := range results {
		topic := msgs[i].Topic
		switch {
		case r.err == nil:
			w.backoff = 0
			w.logger.Debug().Str("topic", topic).Msg("Published MQTT message")
		case r.resp != nil && r.resp.ReasonCode >= reasonCodeFailure:
			w.logger.Error().Err(r.err).Str("topic", topic).Uint8("reason_code", r.resp.ReasonCode).
				Msg("Broker rejected MQTT message")
		default:
			if failed == 0 {
				firstErr, firstTopic = r.err, topic
			}
			failed++
		}
	}
	if failed == 0 {
		return
	}
	w.logger.Error().Err(firstErr).Str("topic", firstTopic).Int("failed", failed).
		Msg("Failed to publish, reconnecting")
	w.disconnect()
	w.nextDial = w.clock.Now().Add(w.backoff)
	w.backoff = nextBackoff(w.backoff)
}

// payload is the JSON message of one point. The keys are in alphabetical order, as
// json.Marshal writes a map, so the payload is the same as with the earlier map.
type payload struct {
	Timestamp string `json:"timestamp"`
	Unit      string `json:"unit"`
	Value     any    `json:"value"`
}

// messages returns the publish of every point; a point whose value cannot be encoded
// is logged and skipped.
func (w *Writer) messages(points []datasource.DataPoint) []*paho.Publish {
	msgs := make([]*paho.Publish, 0, len(points))
	for _, dp := range points {
		topic := w.topic(dp)
		b, err := json.Marshal(payload{
			Timestamp: dp.Timestamp.Format(time.RFC3339Nano),
			Unit:      dp.Unit,
			Value:     dp.JSONValue(), // NaN and ±Inf have no JSON form: null
		})
		if err != nil {
			w.logger.Error().Err(err).Str("topic", topic).Msg("Failed to encode MQTT payload")
			continue
		}
		msgs = append(msgs, &paho.Publish{
			Topic: topic, Payload: b, QoS: w.settings.QoS, Retain: w.settings.Retain,
		})
	}
	return msgs
}

// topic returns <topic>/<device>/<point>, built once per point.
func (w *Writer) topic(dp datasource.DataPoint) string {
	key := topicKey{device: dp.DeviceName, point: dp.PointName}
	topic, ok := w.topics[key]
	if !ok {
		topic = w.settings.Topic + "/" + dp.DeviceName + "/" + dp.PointName
		w.topics[key] = topic
	}
	return topic
}

// nextBackoff doubles the backoff, from initialBackoff up to maxBackoff.
func nextBackoff(backoff time.Duration) time.Duration {
	return min(max(backoff*backoffFactor, initialBackoff), maxBackoff)
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
		w.backoff = nextBackoff(w.backoff)
		w.nextDial = w.clock.Now().Add(w.backoff)
		w.logger.Error().Err(err).Stringer("retry_in", w.backoff).Msg("MQTT connect failed")
		return false
	}

	// The backoff is reset by the first successful publish, not here: a broker that
	// accepts the connection but drops it on publish must not be redialed per point.
	w.session = session
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
