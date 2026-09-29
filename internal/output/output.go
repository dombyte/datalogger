// Package output provides the core types and interfaces for output writers.
package output

import (
	"context"

	"github.com/dombyte/datalogger/internal/datasource"
)

// Writer forwards data points to one external system.
type Writer interface {
	// Name returns the output name.
	Name() string

	// Start starts the writer in the background. It writes every point from input
	// until input is closed, then flushes, releases its resource and closes the
	// returned channel. An error is sent on the channel first if the writer stopped
	// because it cannot continue. Cancelling ctx aborts early (queued points may be
	// lost).
	Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error
}
