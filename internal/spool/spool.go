// Package spool keeps the polls of an output on disk while the output does not take
// them, and hands them over in order once it does again (store and forward). The file
// survives a restart; delivery is at least once: after a crash up to a second of polls
// may be sent twice.
package spool

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/clock"
	"github.com/dombyte/datalogger/internal/datasource"
)

const (
	dirPerm  = 0o750
	filePerm = 0o640

	// offsetInterval is how often the read position is saved while the spool is
	// replayed: more often costs a write per poll, less often more duplicates after a
	// crash.
	offsetInterval = time.Second

	// offsetSuffix names the file with the read position, next to the spool file.
	offsetSuffix = ".offset"
)

// ErrMissingDependency is returned by New when a required dependency is missing.
var ErrMissingDependency = errors.New("spool: missing dependency")

// Settings configures a Spool.
type Settings struct {
	Name     string // the output, for logs
	Path     string // the spool file; the read position is kept in Path + ".offset"
	MaxBytes int64  // the file never grows above this; new polls are dropped then
}

// Deps are the dependencies of a Spool; all are required.
type Deps struct {
	Settings Settings
	Clock    clock.Clock
	Log      zerolog.Logger
}

// Spool is the store and forward of one output. Its goroutine is the only code that
// uses the file; Backlog and Delivered may be read from any goroutine.
type Spool struct {
	settings Settings
	clock    clock.Clock
	logger   zerolog.Logger

	file   *os.File      // appends
	read   *os.File      // reads from offset on
	reader *bufio.Reader // on read
	size   int64         // bytes in the file
	offset int64         // bytes handed over

	head      []datasource.DataPoint // the next poll on disk, decoded
	headBytes int64
	saved     time.Time // when offset was last saved
	dropped   int       // points dropped since the spool last had room
	failed    bool      // the last write failed

	backlog   atomic.Int64 // polls on disk not handed over yet
	delivered atomic.Int64 // polls handed to the output
}

// New opens (or creates) the spool file and its read position. A file cut short by a
// crash loses its incomplete last line.
func New(d Deps) (*Spool, error) {
	if d.Clock == nil {
		return nil, fmt.Errorf("%w: Clock", ErrMissingDependency)
	}
	s := &Spool{
		settings: d.Settings,
		clock:    d.Clock,
		logger: d.Log.With().Str("component", "spool").
			Str("output", d.Settings.Name).Logger(),
	}
	if err := s.open(); err != nil {
		return nil, errors.Join(err, s.closeFiles())
	}
	if n := s.backlog.Load(); n > 0 {
		s.logger.Info().Int64("polls", n).Msg("Spool holds polls from the last run")
	}
	return s, nil
}

// open opens the files, repairs the tail, restores the read position and counts the
// polls behind it.
func (s *Spool) open() error {
	if err := os.MkdirAll(filepath.Dir(s.settings.Path), dirPerm); err != nil {
		return fmt.Errorf("spool: create directory: %w", err)
	}
	var err error
	s.file, err = os.OpenFile(s.settings.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, filePerm)
	if err != nil {
		return fmt.Errorf("spool: open %s: %w", s.settings.Path, err)
	}
	if s.size, err = completeLines(s.file); err != nil {
		return err
	}
	s.read, err = os.Open(s.settings.Path)
	if err != nil {
		return fmt.Errorf("spool: open %s: %w", s.settings.Path, err)
	}
	s.offset = min(readOffset(s.settings.Path+offsetSuffix), s.size)
	if _, err := s.read.Seek(s.offset, io.SeekStart); err != nil {
		return fmt.Errorf("spool: seek: %w", err)
	}
	n, err := countLines(io.NewSectionReader(s.read, s.offset, s.size-s.offset))
	if err != nil {
		return err
	}
	s.backlog.Store(n)
	s.reader = bufio.NewReader(s.read)
	return nil
}

// completeLines truncates f after its last newline (a write cut short by a crash) and
// returns its size.
func completeLines(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("spool: stat: %w", err)
	}
	size := info.Size()
	end := size
	buf := make([]byte, 1)
	for end > 0 {
		if _, err := f.ReadAt(buf, end-1); err != nil {
			return 0, fmt.Errorf("spool: read: %w", err)
		}
		if buf[0] == '\n' {
			break
		}
		end--
	}
	if end < size {
		if err := f.Truncate(end); err != nil {
			return 0, fmt.Errorf("spool: truncate: %w", err)
		}
	}
	return end, nil
}

// readOffset returns the saved read position, 0 if there is none.
func readOffset(path string) int64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0 // no position yet: replay from the start
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// countLines counts the polls (lines) in r.
func countLines(r io.Reader) (int64, error) {
	var n int64
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, maxLine)
	for sc.Scan() {
		n++
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("spool: count: %w", err)
	}
	return n, nil
}

// Backlog returns the number of polls on disk that were not handed over yet.
func (s *Spool) Backlog() int64 {
	return s.backlog.Load()
}

// Delivered returns the number of polls handed to the output.
func (s *Spool) Delivered() int64 {
	return s.delivered.Load()
}

// Start forwards polls from in to out until in is closed or ctx is cancelled, then saves
// the read position, closes the file and out, and closes the returned channel. A poll
// goes straight to out while nothing is spooled and out has room; otherwise it is
// appended to the file, which is handed to out in order whenever out has room.
func (s *Spool) Start(
	ctx context.Context,
	in <-chan []datasource.DataPoint,
	out chan<- []datasource.DataPoint,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(out)
		defer s.close()
		s.run(ctx, in, out)
	}()
	return done
}

// run is the forwarding loop.
func (s *Spool) run(
	ctx context.Context,
	in <-chan []datasource.DataPoint,
	out chan<- []datasource.DataPoint,
) {
	for {
		s.loadHead()
		var send chan<- []datasource.DataPoint
		if s.head != nil {
			send = out // a nil channel never sends: nothing on disk
		}
		select {
		case <-ctx.Done():
			return
		case points, ok := <-in:
			if !ok {
				return // the backlog stays on disk for the next start
			}
			s.accept(points, out)
		case send <- s.head:
			s.handedOver()
		}
	}
}

// accept hands a poll to out if nothing is spooled and out has room, else spools it.
func (s *Spool) accept(points []datasource.DataPoint, out chan<- []datasource.DataPoint) {
	if s.offset == s.size {
		select {
		case out <- points:
			s.delivered.Add(1)
			s.recovered()
			return
		default:
		}
	}
	s.append(points)
}

// append writes a poll to the file, unless that would exceed MaxBytes or fails: then
// the poll is dropped, warned about once until the spool takes polls again.
func (s *Spool) append(points []datasource.DataPoint) {
	line, err := encodePoll(points)
	if err == nil && s.size+int64(len(line)) > s.settings.MaxBytes {
		err = fmt.Errorf("spool full (%d bytes)", s.settings.MaxBytes)
	}
	if err == nil {
		_, err = s.file.Write(line)
		if err != nil {
			err = fmt.Errorf("write: %w", err)
		}
	}
	if err != nil {
		s.drop(len(points), err)
		return
	}
	s.recovered()
	s.size += int64(len(line))
	s.backlog.Add(1)
}

// recovered logs at info when a poll is taken after polls were dropped.
func (s *Spool) recovered() {
	if s.failed {
		s.logger.Info().Int("dropped", s.dropped).Msg("Spool takes polls again")
		s.dropped, s.failed = 0, false
	}
}

// drop counts the points of a poll that cannot be spooled.
func (s *Spool) drop(n int, err error) {
	if s.dropped == 0 && !s.failed {
		s.logger.Warn().Err(err).Msg("Cannot spool, dropping polls until it has room")
	}
	s.dropped += n
	s.failed = true
}

// loadHead decodes the next poll on disk, if any and none is loaded. A line that cannot
// be decoded (written by another version, or damaged) is logged and skipped.
func (s *Spool) loadHead() {
	for s.head == nil && s.offset < s.size {
		line, err := s.reader.ReadBytes('\n')
		if err != nil {
			s.logger.Error().Err(err).Msg("Cannot read spool, dropping its content")
			s.reset()
			return
		}
		points, err := decodePoll(line)
		if err != nil {
			s.logger.Warn().Err(err).Msg("Skipping a poll the spool cannot decode")
			s.skip(int64(len(line)))
			continue
		}
		s.head, s.headBytes = points, int64(len(line))
	}
}

// handedOver records that the head was handed to the output.
func (s *Spool) handedOver() {
	s.delivered.Add(1)
	s.head = nil
	s.skip(s.headBytes)
}

// skip moves the read position past n bytes; an empty spool starts the file again.
func (s *Spool) skip(n int64) {
	s.offset += n
	s.backlog.Add(-1)
	if s.offset >= s.size {
		s.reset()
		return
	}
	if now := s.clock.Now(); now.Sub(s.saved) >= offsetInterval {
		s.saveOffset(now)
	}
}

// reset empties the file once everything was handed over, so it does not grow forever.
func (s *Spool) reset() {
	s.head = nil
	s.backlog.Store(0)
	if err := s.file.Truncate(0); err != nil {
		s.logger.Error().Err(err).Msg("Cannot empty spool")
	}
	if _, err := s.read.Seek(0, io.SeekStart); err != nil {
		s.logger.Error().Err(err).Msg("Cannot rewind spool")
	}
	s.reader.Reset(s.read)
	s.size, s.offset = 0, 0
	s.saveOffset(s.clock.Now())
}

// saveOffset writes the read position; a failure only costs duplicates after a crash.
func (s *Spool) saveOffset(now time.Time) {
	s.saved = now
	path := s.settings.Path + offsetSuffix
	if err := os.WriteFile(path, []byte(strconv.FormatInt(s.offset, 10)), filePerm); err != nil {
		s.logger.Warn().Err(err).Msg("Cannot save spool position")
	}
}

// close saves the read position and closes the files.
func (s *Spool) close() {
	if n := s.backlog.Load(); n > 0 {
		s.logger.Info().Int64("polls", n).Msg("Spool keeps polls for the next start")
	}
	s.saveOffset(s.clock.Now())
	if err := s.closeFiles(); err != nil {
		s.logger.Error().Err(err).Msg("Closing spool failed")
	}
}

// closeFiles closes the open files.
func (s *Spool) closeFiles() error {
	var errs []error
	for _, f := range []*os.File{s.file, s.read} {
		if f != nil {
			errs = append(errs, f.Close())
		}
	}
	return errors.Join(errs...)
}
