// Package csv writes data points to a CSV file with age-based rotation.
package csv

import (
	"context"
	stdcsv "encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

const (
	dirPerm  = 0o755
	filePerm = 0o644

	// backupTimeFormat is the suffix of rotated files: <file>.<YYYYMMDD-HHMMSS>.
	backupTimeFormat = "20060102-150405"
)

// ErrMissingDependency is returned by New when a required dependency is missing.
var ErrMissingDependency = errors.New("csv: missing dependency")

// Settings configures a Writer.
type Settings struct {
	Name       string
	FilePath   string
	MaxAge     time.Duration // rotate when the file is older; 0 = never
	MaxBackups int           // rotated files to keep: 0 none, < 0 all
}

// Deps are the dependencies of a Writer; all are required.
type Deps struct {
	Settings Settings
	Clock    clock.Clock
	Log      zerolog.Logger
}

// Writer appends every point as one CSV row. Rows are buffered and flushed when no
// further poll is queued, so a backlog is written in large blocks.
type Writer struct {
	settings Settings
	clock    clock.Clock
	logger   zerolog.Logger
	backupRe *regexp.Regexp
	file     *os.File
	csv      *stdcsv.Writer
	opened   time.Time // for rotation
	failed   bool      // the previous write failed

	// The points of one poll often share their timestamp: the last one is formatted once.
	lastTime  time.Time
	lastStamp string
}

// New creates the directory and opens (or creates) the file. An error here means the
// path cannot be used.
func New(d Deps) (*Writer, error) {
	if d.Clock == nil {
		return nil, fmt.Errorf("%w: Clock", ErrMissingDependency)
	}
	w := &Writer{
		settings: d.Settings,
		clock:    d.Clock,
		logger:   d.Log.With().Str("component", "csv").Str("output", d.Settings.Name).Logger(),
		backupRe: regexp.MustCompile("^" + regexp.QuoteMeta(filepath.Base(d.Settings.FilePath)) +
			`\.\d{8}-\d{6}$`),
	}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

// Name returns the output name.
func (w *Writer) Name() string {
	return w.settings.Name
}

// Start writes points until input is closed (or ctx is cancelled), then closes the
// file and the returned channel.
func (w *Writer) Start(ctx context.Context, input <-chan []datasource.DataPoint) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer w.close()
		for {
			select {
			case <-ctx.Done():
				return
			case points, ok := <-input:
				if !ok {
					return
				}
				w.writePoll(points)
				if len(input) == 0 {
					w.flush()
				}
			}
		}
	}()
	return done
}

// writePoll buffers one row per point. A failed row is logged and the file reopened;
// the next row goes to the new file.
func (w *Writer) writePoll(points []datasource.DataPoint) {
	for _, dp := range points {
		if err := w.write(dp); err != nil {
			w.logger.Error().Str("device", dp.DeviceName).Str("point", dp.PointName).Err(err).
				Msg("Failed to write CSV row")
			w.failed = true
			w.reopen()
		}
	}
}

// flush writes the buffered rows to the file and logs a failure or the recovery
// after one. After a failure the file is reopened: the rows that were buffered are
// lost.
func (w *Writer) flush() {
	w.csv.Flush()
	if err := w.csv.Error(); err != nil {
		w.logger.Error().Err(err).Msg("Failed to write CSV rows")
		w.failed = true
		w.reopen()
		return
	}
	if w.failed {
		w.logger.Info().Msg("Wrote CSV rows (recovered from previous error)")
		w.failed = false
	}
}

// open creates the directory, opens the file for appending and writes the header into
// an empty file.
func (w *Writer) open() error {
	if err := os.MkdirAll(filepath.Dir(w.settings.FilePath), dirPerm); err != nil {
		return fmt.Errorf("csv: create directory: %w", err)
	}
	// Read access for the first row of an existing file (firstRowTime).
	file, err := os.OpenFile(w.settings.FilePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, filePerm)
	if err != nil {
		return fmt.Errorf("csv: open %s: %w", w.settings.FilePath, err)
	}
	w.file, w.csv, w.opened = file, stdcsv.NewWriter(file), w.clock.Now()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("csv: stat %s: %w", w.settings.FilePath, err)
	}
	if info.Size() == 0 {
		return w.writeHeader()
	}
	// The age of an existing file counts from its first row, not from this start: a
	// process that restarts more often than max_age would otherwise never rotate.
	if first, ok := firstRowTime(io.NewSectionReader(file, 0, info.Size())); ok {
		w.opened = first
	}
	return nil
}

// firstRowTime returns the timestamp of the first data row of a CSV file.
func firstRowTime(file io.Reader) (time.Time, bool) {
	r := stdcsv.NewReader(file)
	r.FieldsPerRecord = -1
	if _, err := r.Read(); err != nil { // header
		return time.Time{}, false
	}
	row, err := r.Read()
	if err != nil {
		return time.Time{}, false
	}
	first, err := time.Parse(time.RFC3339Nano, row[0])
	return first, err == nil
}

// reopen replaces the file and its CSV writer after a failed write: the buffered writer
// keeps its first error, so without a new one every later row would fail too. If the
// file cannot be opened, the next point tries again.
func (w *Writer) reopen() {
	// The write error is already logged and the file may be closed (failed rotation).
	if err := w.file.Close(); err != nil {
		w.logger.Debug().Err(err).Msg("Closing CSV file after a failed write")
	}
	if err := w.open(); err != nil {
		w.logger.Error().Err(err).Msg("Failed to reopen CSV file")
	}
}

// close flushes and closes the current file.
func (w *Writer) close() {
	w.csv.Flush()
	if err := errors.Join(w.csv.Error(), w.file.Close()); err != nil {
		w.logger.Error().Err(err).Msg("Failed to close CSV file")
	}
}

// write buffers one row and rotates the file when it is older than MaxAge.
func (w *Writer) write(dp datasource.DataPoint) error {
	err := w.csv.Write([]string{
		w.timestamp(dp.Timestamp),
		dp.DeviceName,
		dp.PointName,
		formatValue(dp.Value),
		dp.Unit,
	})
	if err != nil {
		return fmt.Errorf("csv: write: %w", err)
	}

	if w.settings.MaxAge > 0 && w.clock.Now().Sub(w.opened) >= w.settings.MaxAge {
		return w.rotate()
	}
	return nil
}

// timestamp returns t formatted for a row, reusing the last result for the same time.
func (w *Writer) timestamp(t time.Time) string {
	// == instead of Equal: the same instant in another location formats differently.
	if t != w.lastTime || w.lastStamp == "" {
		w.lastTime, w.lastStamp = t, t.Format(time.RFC3339Nano)
	}
	return w.lastStamp
}

// writeHeader writes and flushes the header of a new file.
func (w *Writer) writeHeader() error {
	header := []string{"timestamp", "device", "point", "value", "unit"}
	_ = w.csv.Write(header) //nolint:errcheck // kept by the writer, reported by Error below
	w.csv.Flush()
	if err := w.csv.Error(); err != nil {
		return fmt.Errorf("csv: write header: %w", err)
	}
	return nil
}

// rotate renames the file to <file>.<timestamp>, removes backups beyond MaxBackups and
// starts a new file.
func (w *Writer) rotate() error {
	w.close()

	backup := w.backupName()
	if err := os.Rename(w.settings.FilePath, backup); err != nil {
		return fmt.Errorf("csv: rotate: %w", err)
	}
	w.logger.Debug().Str("backup", backup).Msg("CSV file rotated")

	w.removeOldBackups()

	if err := w.open(); err != nil {
		return fmt.Errorf("csv: open after rotation: %w", err)
	}
	return nil
}

// backupName returns <file>.<YYYYMMDD-HHMMSS> for now, one second later for every
// backup of that name that exists already: Rename would replace it.
func (w *Writer) backupName() string {
	for t := w.clock.Now(); ; t = t.Add(time.Second) {
		backup := w.settings.FilePath + "." + t.Format(backupTimeFormat)
		if _, err := os.Lstat(backup); errors.Is(err, os.ErrNotExist) {
			return backup
		}
	}
}

// removeOldBackups keeps the newest MaxBackups rotated files (all if MaxBackups < 0).
// Only files named <file>.<YYYYMMDD-HHMMSS> are touched.
func (w *Writer) removeOldBackups() {
	if w.settings.MaxBackups < 0 {
		return
	}
	entries, err := os.ReadDir(filepath.Dir(w.settings.FilePath))
	if err != nil {
		w.logger.Warn().Err(err).Msg("Failed to list CSV backups")
		return
	}

	var backups []string
	for _, e := range entries {
		if w.backupRe.MatchString(e.Name()) {
			backups = append(backups, e.Name())
		}
	}
	slices.Sort(backups) // the timestamp suffix sorts oldest first

	for _, name := range backups[:max(len(backups)-w.settings.MaxBackups, 0)] {
		path := filepath.Join(filepath.Dir(w.settings.FilePath), name)
		if err := os.Remove(path); err != nil {
			w.logger.Warn().Str("file", path).Err(err).Msg("Failed to delete old CSV backup")
			continue
		}
		w.logger.Debug().Str("file", path).Msg("Deleted old CSV backup")
	}
}

// formatValue formats a value for CSV output.
func formatValue(v any) string {
	switch val := v.(type) {
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case bool:
		return strconv.FormatBool(val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
