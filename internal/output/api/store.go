package api

import (
	"maps"
	"sync"
	"time"

	"github.com/dombyte/datalogger/internal/datasource"
)

// store holds the latest point per device and point name. The writer goroutine is its
// only writer; the HTTP handlers read copies.
type store struct {
	mu     sync.RWMutex
	latest map[string]map[string]datasource.DataPoint // device → point → latest
}

// newStore creates a store that knows the served devices, so a device without a
// reading yet is empty instead of unknown.
func newStore(devices []string) *store {
	latest := make(map[string]map[string]datasource.DataPoint, len(devices))
	for _, d := range devices {
		latest[d] = make(map[string]datasource.DataPoint)
	}
	return &store{latest: latest}
}

// set replaces the stored points; points of devices that are not served are ignored.
func (s *store) set(points []datasource.DataPoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, dp := range points {
		if latest, ok := s.latest[dp.DeviceName]; ok {
			latest[dp.PointName] = dp
		}
	}
}

// device returns a copy of the latest points of a device and whether it is served.
func (s *store) device(name string) (map[string]datasource.DataPoint, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	points, ok := s.latest[name]
	return maps.Clone(points), ok
}

// summary is the number of points of a device and the time of its newest one (zero
// without points).
type summary struct {
	device string
	points int
	newest time.Time
}

// summaries returns the summary of every served device, without copying its points.
func (s *store) summaries() []summary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]summary, 0, len(s.latest))
	for name, points := range s.latest {
		sum := summary{device: name, points: len(points)}
		for _, dp := range points {
			if dp.Timestamp.After(sum.newest) {
				sum.newest = dp.Timestamp
			}
		}
		out = append(out, sum)
	}
	return out
}
