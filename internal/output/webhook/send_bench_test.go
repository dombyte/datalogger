package webhook

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

// okClient reads every request body and answers 200, like a fast endpoint.
type okClient struct{}

func (okClient) Do(req *http.Request) (*http.Response, error) {
	if _, err := io.Copy(io.Discard, req.Body); err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))},
		nil
}

// BenchmarkSendPoll encodes and sends polls of 100 readings; one op is one poll.
func BenchmarkSendPoll(b *testing.B) {
	w, err := New(Deps{
		Settings: Settings{Name: "hook", URL: "http://collector/readings"},
		Client:   okClient{}, Clock: clock.Real{}, Log: zerolog.Nop(),
	})
	if err != nil {
		b.Fatal(err)
	}
	points := make([]datasource.DataPoint, 100)
	for i := range points {
		points[i] = datasource.DataPoint{
			DeviceName: "meter", PointName: "p" + strconv.Itoa(i), Value: 230.5, Unit: "V",
			Timestamp: time.Date(2026, 10, 5, 10, 15, 2, 123000000, time.UTC),
		}
	}
	input := make(chan []datasource.DataPoint)
	done := w.Start(context.Background(), input)

	for b.Loop() {
		input <- points // taken once the previous poll is sent
	}
	close(input)
	for err := range done {
		b.Fatal(err)
	}
}
