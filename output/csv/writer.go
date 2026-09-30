// Package csv provides CSV output functionality.
package csv

import (
	"context"
	stdcsv "encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
)

const (
	dirPerm  = 0o755
	filePerm = 0o644

	// drainTimeout bounds how long the writer keeps writing queued points on shutdown.
	drainTimeout = 10 * time.Second
)

// Writer writes DataPoints to a CSV file.
type Writer struct {
	config       config.Output
	logger       zerolog.Logger
	file         *os.File
	writer       *stdcsv.Writer
	devices      []string
	filePath     string
	maxAge       time.Duration
	maxBackups   int
	lastWrite    time.Time
	fileCreated  time.Time // When the current file was created (for rotation)
	lastWriteErr error     // Track if previous write failed
}

// New creates a new Writer.
func New(outputConfig config.Output, logger *zerolog.Logger) (*Writer, error) {
	w := &Writer{
		config:     outputConfig,
		logger:     logger.With().Str("output", "csv").Str("name", outputConfig.Name).Logger(),
		devices:    outputConfig.Devices,
		filePath:   outputConfig.OutputSpecific.Csv.FilePath,
		maxAge:     outputConfig.OutputSpecific.Csv.MaxAge,
		maxBackups: outputConfig.OutputSpecific.Csv.MaxBackups,
	}

	return w, w.openFile()
}

// openFile opens or creates the CSV file and initializes the writer.
func (w *Writer) openFile() error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(w.filePath)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	file, err := os.OpenFile(w.filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, filePerm)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", w.filePath, err)
	}

	w.file = file
	w.writer = stdcsv.NewWriter(file)

	// Write header if file is empty
	if info, err := file.Stat(); err == nil && info.Size() == 0 {
		header := []string{"timestamp", "device", "point", "value", "unit"}
		if err := w.writer.Write(header); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
		w.writer.Flush()
		if err := w.writer.Error(); err != nil {
			return fmt.Errorf("failed to flush CSV header: %w", err)
		}
	}

	w.lastWrite = time.Now()
	w.fileCreated = time.Now() // Track when this file was created for rotation
	return nil
}

// Name returns the output name.
func (w *Writer) Name() string {
	return w.config.Name
}

// Devices returns the list of device names this output accepts.
func (w *Writer) Devices() []string {
	return w.devices
}

// Validate validates the CSV writer configuration.
func (w *Writer) Validate() error {
	// Configuration was already validated when creating the writer
	return nil
}

// Start starts the CSV writer goroutine.
func (w *Writer) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				w.drainAndClose(input)
				return
			case dp, ok := <-input:
				if !ok {
					w.logger.Debug().Msg("CSV writer: input channel closed")
					w.writer.Flush()
					w.file.Close()
					return
				}
				w.handlePoint(dp)
			}
		}
	}()

	return errCh
}

// handlePoint writes one point and logs failures and the recovery after one.
func (w *Writer) handlePoint(dp datasource.DataPoint) {
	log := w.logger.With().Str("device", dp.DeviceName).Str("point", dp.PointName).Logger()
	log.Debug().Msg("CSV writer: writing point")

	if err := w.writePoint(dp); err != nil {
		log.Error().Err(err).Msg("Failed to write CSV point")
		w.lastWriteErr = err
		return
	}

	if w.lastWriteErr != nil {
		log.Info().Msg("Wrote CSV point (recovered from previous error)")
	} else {
		log.Debug().Msg("Successfully wrote CSV point")
	}
	w.lastWriteErr = nil
}

// drainAndClose drains remaining points from the input channel and closes the file.
func (w *Writer) drainAndClose(input <-chan datasource.DataPoint) {
	w.logger.Info().Msg("CSV writer: shutdown started, draining remaining points")
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
	defer drainCancel()

	w.logger.Info().Msg("CSV writer: draining remaining points")
	for {
		select {
		case <-drainCtx.Done():
			w.writer.Flush()
			w.file.Close()
			return
		case dp, ok := <-input:
			if !ok {
				w.writer.Flush()
				w.file.Close()
				return
			}
			w.logger.Debug().
				Str("device", dp.DeviceName).
				Str("point", dp.PointName).
				Msg("CSV writer: writing point")
			if err := w.writePoint(dp); err != nil {
				w.logger.Error().Err(err).Msg("Failed to write CSV point")
			}
		default:
			w.writer.Flush()
			w.file.Close()
			return
		}
	}
}

// writePoint writes a single DataPoint to the CSV file.
func (w *Writer) writePoint(dp datasource.DataPoint) error {
	// Format value based on type
	valueStr := formatValue(dp.Value)

	record := []string{
		dp.Timestamp.Format(time.RFC3339Nano),
		dp.DeviceName,
		dp.PointName,
		valueStr,
		dp.Unit,
	}

	if err := w.writer.Write(record); err != nil {
		return err
	}

	// Flush to disk on every write for continuous writing
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return err
	}
	w.lastWrite = time.Now()

	// Check if we need to rotate the file based on max_age
	// max_age specifies how old the file can be before it gets rotated
	if w.maxAge > 0 && time.Since(w.fileCreated) >= w.maxAge {
		if err := w.rotateFile(); err != nil {
			return err
		}
	}

	return nil
}

// rotateFile closes the current file and opens a new one with a timestamp suffix.
// The old file is kept with a timestamp in the name.
// Old backup files are deleted according to maxBackups: 0 = delete all, > 0 = keep that many.
func (w *Writer) rotateFile() error {
	// Close current file
	if w.file != nil {
		w.writer.Flush()
		w.file.Close()
	}

	// Rename current file with timestamp
	timestamp := time.Now().Format("20060102-150405")
	oldPath := w.filePath
	newPath := fmt.Sprintf("%s.%s", w.filePath, timestamp)
	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("failed to rename file for rotation: %w", err)
	}

	w.logger.Debug().Str("old_file", newPath).Msg("CSV file rotated")

	// Clean up old backups according to maxBackups setting
	if err := w.cleanupOldBackups(); err != nil {
		w.logger.Warn().Err(err).Msg("Failed to cleanup old CSV backups")
		// Don't return error, just log warning
	}

	// Open new file
	if err := w.openFile(); err != nil {
		return fmt.Errorf("failed to open new file after rotation: %w", err)
	}

	return nil
}

// cleanupOldBackups removes old backup files to maintain maxBackups limit.
// Keeps the most recent backups and deletes the oldest ones.
func (w *Writer) cleanupOldBackups() error {
	// Get all backup files matching the pattern: filePath.TIMESTAMP
	pattern := w.filePath + ".*"
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	// Filter out non-backup files and get their modification times
	backups := w.collectBackupFiles(files)

	// Sort by modification time (oldest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].mtime.Before(backups[j].mtime)
	})

	// Delete oldest backups according to maxBackups setting
	w.deleteOldBackups(backups)

	return nil
}

// collectBackupFiles collects all backup files and their modification times.
func (w *Writer) collectBackupFiles(files []string) []struct {
	path  string
	mtime time.Time
} {
	var backups []struct {
		path  string
		mtime time.Time
	}
	for _, f := range files {
		// Skip the current file (without timestamp suffix)
		if f == w.filePath {
			continue
		}
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		backups = append(backups, struct {
			path  string
			mtime time.Time
		}{f, info.ModTime()})
	}
	return backups
}

// deleteOldBackups deletes old backup files according to maxBackups setting.
// maxBackups == 0 means delete all backups
// maxBackups > 0 means keep at most maxBackups backups
// maxBackups < 0 means keep all backups (no cleanup)
func (w *Writer) deleteOldBackups(backups []struct {
	path  string
	mtime time.Time
},
) {
	if w.maxBackups == 0 {
		// Delete all backups
		w.deleteAllBackups(backups)
	} else if w.maxBackups > 0 {
		// Keep at most maxBackups backups, delete the oldest ones
		w.deleteOldestBackups(backups)
	}
	// If maxBackups < 0, keep all backups (no cleanup)
}

// deleteAllBackups deletes all backup files.
func (w *Writer) deleteAllBackups(backups []struct {
	path  string
	mtime time.Time
},
) {
	for _, backup := range backups {
		w.deleteBackupFile(backup.path)
	}
}

// deleteOldestBackups deletes the oldest backups to maintain maxBackups limit.
func (w *Writer) deleteOldestBackups(backups []struct {
	path  string
	mtime time.Time
},
) {
	for len(backups) >= w.maxBackups {
		oldest := backups[0]
		w.deleteBackupFile(oldest.path)
		backups = backups[1:]
	}
}

// deleteBackupFile deletes a single backup file and logs the result.
func (w *Writer) deleteBackupFile(path string) {
	if err := os.Remove(path); err != nil {
		w.logger.Warn().Str("file", path).Err(err).Msg("Failed to delete old backup")
	} else {
		w.logger.Debug().Str("file", path).Msg("Deleted old CSV backup")
	}
}

// formatValue formats a value for CSV output.
func formatValue(v interface{}) string {
	switch val := v.(type) {
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
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
