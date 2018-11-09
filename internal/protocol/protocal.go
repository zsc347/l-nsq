package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(conn net.Conn) error
}

// SendResponse is a server side utility function to prefix data with length header
// and write to the supplied Writer
func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)

	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// SendFrameResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	// we can simply binary.Write(w, binary.BigEndian, size)
	// but create a buf before can reuse the buf for writer,
	// otherwise in each call to binary.Write, we need to create a buf

	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	// write 4 byte (uint32) for size
	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err
}
