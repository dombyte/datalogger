package mqtt

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

// brokerRTT is the simulated time a broker takes to acknowledge a QoS 1 publish.
const brokerRTT = 500 * time.Microsecond

// ackPublisher acknowledges every publish after brokerRTT, like a broker on the LAN.
type ackPublisher struct{}

func (ackPublisher) Publish(context.Context, *paho.Publish) (*paho.PublishResponse, error) {
	time.Sleep(brokerRTT)
	return &paho.PublishResponse{}, nil
}

func (ackPublisher) Disconnect(*paho.Disconnect) error { return nil }
func (ackPublisher) Done() <-chan struct{}             { return nil }

// ackDialer opens ackPublisher sessions.
type ackDialer struct{}

func (ackDialer) Dial(context.Context) (Publisher, error) { return ackPublisher{}, nil }

// realClock is the real clock; the benchmark measures real time.
type realClock = clock.Real

// poll returns a poll of n points of one device.
func poll(n int) []datasource.DataPoint {
	points := make([]datasource.DataPoint, n)
	for i := range points {
		points[i] = datasource.DataPoint{
			DeviceName: "meter", PointName: "p" + strconv.Itoa(i),
			Value: 230.5, Unit: "V", Timestamp: time.Date(2026, 10, 5, 10, 15, 2, 0, time.UTC),
		}
	}
	return points
}

// BenchmarkPublishPollQoS1 publishes polls of 50 points with QoS 1 to a broker that
// answers after brokerRTT; one op is one poll.
func BenchmarkPublishPollQoS1(b *testing.B) {
	w, err := New(Deps{
		Settings: Settings{Name: "broker", Topic: "datalogger", QoS: 1},
		Dialer:   ackDialer{}, Clock: realClock{}, Log: zerolog.Nop(),
	})
	if err != nil {
		b.Fatal(err)
	}
	input := make(chan []datasource.DataPoint)
	done := w.Start(context.Background(), input)
	points := poll(50)

	for b.Loop() {
		input <- points // taken once the previous poll is published
	}
	close(input)
	for err := range done {
		b.Fatal(err)
	}
}
