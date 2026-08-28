// Package csv provides CSV output functionality.
package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
	"github.com/rs/zerolog"
)

// CSVWriter writes DataPoints to a CSV file.
type CSVWriter struct {
	config      config.Output
	logger      zerolog.Logger
	file        *os.File
	writer      *csv.Writer
	devices     []string
	filePath    string
	maxAge      time.Duration
	maxBackups  int
	lastWrite   time.Time
	fileCreated time.Time // When the current file was created (for rotation)
}

// NewCSVWriter creates a new CSVWriter.
func NewCSVWriter(outputConfig config.Output, logger *zerolog.Logger) (*CSVWriter, error) {
	w := &CSVWriter{
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
func (w *CSVWriter) openFile() error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(w.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	file, err := os.OpenFile(w.filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", w.filePath, err)
	}

	w.file = file
	w.writer = csv.NewWriter(file)

	// Write header if file is empty
	if info, err := file.Stat(); err == nil && info.Size() == 0 {
		if err := w.writer.Write([]string{"timestamp", "device", "point", "value", "unit"}); err != nil {
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
func (w *CSVWriter) Name() string {
	return w.config.Name
}

// Devices returns the list of device names this output accepts.
func (w *CSVWriter) Devices() []string {
	return w.devices
}

// Validate validates the CSV writer configuration.
func (w *CSVWriter) Validate() error {
	// Configuration was already validated when creating the writer
	return nil
}

// Start starts the CSV writer goroutine.
func (w *CSVWriter) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
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

				w.logger.Debug().Str("device", dp.DeviceName).Str("point", dp.PointName).Msg("CSV writer: writing point")
				if err := w.writePoint(dp); err != nil {
					w.logger.Error().Err(err).Msg("Failed to write CSV point")
				}
			}
		}
	}()

	return errCh
}

// drainAndClose drains remaining points from the input channel and closes the file.
func (w *CSVWriter) drainAndClose(input <-chan datasource.DataPoint) {
	w.logger.Info().Msg("CSV writer: shutdown started, draining remaining points")
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
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
			w.logger.Debug().Str("device", dp.DeviceName).Str("point", dp.PointName).Msg("CSV writer: writing point")
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
func (w *CSVWriter) writePoint(dp datasource.DataPoint) error {
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
func (w *CSVWriter) rotateFile() error {
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
func (w *CSVWriter) cleanupOldBackups() error {
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
func (w *CSVWriter) collectBackupFiles(files []string) []struct {
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
func (w *CSVWriter) deleteOldBackups(backups []struct {
	path  string
	mtime time.Time
}) {
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
func (w *CSVWriter) deleteAllBackups(backups []struct {
	path  string
	mtime time.Time
}) {
	for _, backup := range backups {
		w.deleteBackupFile(backup.path)
	}
}

// deleteOldestBackups deletes the oldest backups to maintain maxBackups limit.
func (w *CSVWriter) deleteOldestBackups(backups []struct {
	path  string
	mtime time.Time
}) {
	for len(backups) >= w.maxBackups {
		oldest := backups[0]
		w.deleteBackupFile(oldest.path)
		backups = backups[1:]
	}
}

// deleteBackupFile deletes a single backup file and logs the result.
func (w *CSVWriter) deleteBackupFile(path string) {
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
	case int:
		return strconv.Itoa(val)
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
