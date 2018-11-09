package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
)

func TestWriteInt32(t *testing.T) {
	var size int32 = 1234
	beBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(beBuf, uint32(size))
	t.Logf("%v+\n", beBuf)

	var buffer bytes.Buffer
	writer := bufio.NewWriter(&buffer)
	binary.Write(writer, binary.BigEndian, int32(size))
	t.Logf("%v+\n", beBuf)

	buffer.Reset()
	binary.Write(writer, binary.BigEndian, uint32(size))
	t.Logf("%v+\n", beBuf)
}
