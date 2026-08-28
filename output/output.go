// Package output provides the core types and interfaces for output writers.
package output

import (
	"context"

	"github.com/dombyte/datalogger/datasource"
)

// OutputWriter is the interface that all output writers must implement.
type OutputWriter interface {
	// Name returns the output name
	Name() string

	// Start begins the output writer goroutine
	// Receives DataPoints from its channel
	Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error

	// Validate checks configuration before startup
	Validate() error

	// Devices returns list of device names this output accepts
	Devices() []string
}
