package mqtt

import (
	"testing"

	"github.com/rs/zerolog"
)

// BenchmarkMessages encodes the topics and payloads of a poll of 20 points.
func BenchmarkMessages(b *testing.B) {
	w, err := New(Deps{
		Settings: Settings{Name: "broker", Topic: "datalogger"},
		Dialer:   ackDialer{}, Clock: realClock{}, Log: zerolog.Nop(),
	})
	if err != nil {
		b.Fatal(err)
	}
	points := poll(20)

	for b.Loop() {
		if msgs := w.messages(points); len(msgs) != len(points) {
			b.Fatal("points skipped")
		}
	}
}
