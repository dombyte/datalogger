package mqtt_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/clock/clocktest"
	"github.com/dombyte/datalogger/internal/datasource"
	"github.com/dombyte/datalogger/internal/output/mqtt"
	"github.com/dombyte/datalogger/internal/output/mqtt/mocks"
)

var start = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

type harness struct {
	t      *testing.T
	clock  *clocktest.Fake
	dialer *mocks.MockDialer
	input  chan datasource.DataPoint
	done   <-chan error
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		clock:  clocktest.NewFake(start),
		dialer: mocks.NewMockDialer(t),
		input:  make(chan datasource.DataPoint),
	}
	w, err := mqtt.New(mqtt.Deps{
		Settings: mqtt.Settings{Name: "broker", Topic: "logger", QoS: 1, Retain: true},
		Dialer:   h.dialer,
		Clock:    h.clock,
		Log:      zerolog.Nop(),
	})
	require.NoError(t, err)
	h.done = w.Start(context.Background(), h.input)
	return h
}

// send hands over a point; it returns once the writer took it, which is after the
// writer finished the previous point.
func (h *harness) send(name string) {
	h.input <- datasource.DataPoint{
		DeviceName: "meter", PointName: name, Value: 1.5, Timestamp: start, Unit: "W",
	}
}

func (h *harness) stop() {
	close(h.input)
	for err := range h.done {
		require.NoError(h.t, err)
	}
}

// session returns a publisher mock whose Done channel stays open until lost is closed.
func session(t *testing.T) (*mocks.MockPublisher, chan struct{}) {
	t.Helper()
	p := mocks.NewMockPublisher(t)
	lost := make(chan struct{})
	p.EXPECT().Done().Return(lost).Maybe()
	return p, lost
}

// published records the publishes on p.
func published(p *mocks.MockPublisher, err error) chan *paho.Publish {
	ch := make(chan *paho.Publish, 10)
	p.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, pub *paho.Publish) (*paho.PublishResponse, error) {
			ch <- pub
			return &paho.PublishResponse{}, err
		})
	return ch
}

func TestNewChecksDependencies(t *testing.T) {
	t.Parallel()
	_, err := mqtt.New(mqtt.Deps{Clock: clocktest.NewFake(start)})
	assert.ErrorIs(t, err, mqtt.ErrMissingDependency)
	_, err = mqtt.New(mqtt.Deps{Dialer: mocks.NewMockDialer(t)})
	assert.ErrorIs(t, err, mqtt.ErrMissingDependency)
}

func TestPublishesTopicAndPayloadOnOneSession(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	p, _ := session(t)
	h.dialer.EXPECT().Dial(mock.Anything).Return(p, nil).Once()
	pubs := published(p, nil)
	p.EXPECT().Disconnect(mock.Anything).Return(nil).Once()

	h.send("power")
	h.send("energy")
	h.stop()

	first := <-pubs
	assert.Equal(t, "logger/meter/power", first.Topic)
	assert.JSONEq(t, `{"value":1.5,"unit":"W","timestamp":"2026-09-29T12:00:00Z"}`,
		string(first.Payload))
	assert.Equal(t, byte(1), first.QoS)
	assert.True(t, first.Retain)
	assert.Equal(t, "logger/meter/energy", (<-pubs).Topic)
}

func TestDropsPointsUntilBackoffAllowsNextDial(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.dialer.EXPECT().Dial(mock.Anything).Return(nil, errors.New("refused")).Once()

	h.send("a") // dial fails, point dropped, next dial allowed in 1 s
	h.send("b") // within the backoff: dropped without dialing
	// The writer takes a point only after it finished the previous one, so once "sync"
	// is handed over, "b" is done; "sync" itself may see the clock before or after the
	// advance, so it may or may not be published.
	h.send("sync")

	p, _ := session(t)
	h.dialer.EXPECT().Dial(mock.Anything).Return(p, nil).Once()
	pubs := published(p, nil)
	p.EXPECT().Disconnect(mock.Anything).Return(nil).Once()

	h.clock.Advance(time.Second)
	h.send("c")
	h.stop()
	close(pubs)

	var topics []string
	for pub := range pubs {
		topics = append(topics, pub.Topic)
	}
	assert.Contains(t, topics, "logger/meter/c")
	assert.NotContains(t, topics, "logger/meter/a")
	assert.NotContains(t, topics, "logger/meter/b")
}

func TestReconnectsAfterPublishErrorAndLostConnection(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	failing, _ := session(t)
	published(failing, errors.New("broken pipe"))
	failing.EXPECT().Disconnect(mock.Anything).Return(errors.New("closed")).Once()
	lost, lostCh := session(t)
	lostPubs := published(lost, nil)
	good, _ := session(t)
	goodPubs := published(good, nil)
	good.EXPECT().Disconnect(mock.Anything).Return(nil).Once()

	h.dialer.EXPECT().Dial(mock.Anything).Return(failing, nil).Once()
	h.dialer.EXPECT().Dial(mock.Anything).Return(lost, nil).Once()
	h.dialer.EXPECT().Dial(mock.Anything).Return(good, nil).Once()

	h.send("a") // publish fails: disconnect
	h.send("b") // redial, published on the second session
	<-lostPubs
	close(lostCh) // the broker drops the second session
	h.send("c")   // noticed before publishing: redial
	h.stop()
	assert.Equal(t, "logger/meter/c", (<-goodPubs).Topic)
}

func TestNameAndStopOnCancelWithoutSession(t *testing.T) {
	t.Parallel()
	w, err := mqtt.New(mqtt.Deps{
		Settings: mqtt.Settings{Name: "broker"},
		Dialer:   mocks.NewMockDialer(t), // never dialed: no point arrives
		Clock:    clocktest.NewFake(start),
		Log:      zerolog.Nop(),
	})
	require.NoError(t, err)
	assert.Equal(t, "broker", w.Name())

	ctx, cancel := context.WithCancel(context.Background())
	done := w.Start(ctx, make(chan datasource.DataPoint))
	cancel()
	_, open := <-done
	assert.False(t, open, "the writer stops when ctx is cancelled")
}

func TestSkipsPointThatCannotBeEncoded(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	p, _ := session(t)
	h.dialer.EXPECT().Dial(mock.Anything).Return(p, nil).Once()
	pubs := published(p, nil)
	p.EXPECT().Disconnect(mock.Anything).Return(nil).Once()

	h.input <- datasource.DataPoint{DeviceName: "meter", PointName: "nan", Value: math.NaN()}
	h.send("power")
	h.stop()

	assert.Equal(t, "logger/meter/power", (<-pubs).Topic, "NaN is skipped, the session stays")
	assert.Empty(t, pubs)
}
