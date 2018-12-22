package writers

import (
	"bufio"
	"io"
)

// BoundaryBufferedWriter defines a writer which will automatically
// flush when buffer avaliable is not enough
type BoundaryBufferedWriter struct {
	bw *bufio.Writer
}

// NewBoundaryBufferedWriter create a boundary writer with a size
func NewBoundaryBufferedWriter(w io.Writer, size int) *BoundaryBufferedWriter {
	return &BoundaryBufferedWriter{
		bw: bufio.NewWriterSize(w, size),
	}
}

// Write write to writer, if buffer is not enough for bytes, flush the buffer
func (b *BoundaryBufferedWriter) Write(p []byte) (int, error) {
	if len(p) > b.bw.Available() {
		err := b.bw.Flush()
		if err != nil {
			return 0, err
		}
	}
	return b.bw.Write(p)
}

// Flush flush writer buffer
func (b *BoundaryBufferedWriter) Flush() error {
	return b.bw.Flush()
}
