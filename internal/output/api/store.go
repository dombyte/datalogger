package api

import (
	"maps"
	"sync"

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

// set replaces the stored point; points of devices that are not served are ignored.
func (s *store) set(dp datasource.DataPoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if points, ok := s.latest[dp.DeviceName]; ok {
		points[dp.PointName] = dp
	}
}

// device returns a copy of the latest points of a device and whether it is served.
func (s *store) device(name string) (map[string]datasource.DataPoint, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	points, ok := s.latest[name]
	return maps.Clone(points), ok
}

// devices returns a copy of the latest points of every served device.
func (s *store) devices() map[string]map[string]datasource.DataPoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]map[string]datasource.DataPoint, len(s.latest))
	for name, points := range s.latest {
		out[name] = maps.Clone(points)
	}
	return out
}
