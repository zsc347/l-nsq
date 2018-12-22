package writers

import (
	"io"
	"time"
)

// SpreadWriter defines a writer which try to write buffer to destination in an interval
type SpreadWriter struct {
	w        io.Writer
	interval time.Duration
	buf      [][]byte
	exitCh   chan int
}

// NewSpreadWriter create a new writer
func NewSpreadWriter(w io.Writer, interval time.Duration, exitCh chan int) *SpreadWriter {
	return &SpreadWriter{
		w:        w,
		interval: interval,
		buf:      make([][]byte, 0),
		exitCh:   exitCh,
	}
}

// Write append bytes to buf arr
func (s *SpreadWriter) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	s.buf = append(s.buf, b)
	return len(p), nil
}

// Flush try to  write buf arr to destination one by one within interval
func (s *SpreadWriter) Flush() {
	sleep := s.interval / time.Duration(len(s.buf))
	ticker := time.NewTicker(sleep)
	for _, b := range s.buf {
		s.w.Write(b)
		select {
		case <-ticker.C:
		case <-s.exitCh:
		}
	}
	ticker.Stop()
	s.buf = s.buf[:0]
}
