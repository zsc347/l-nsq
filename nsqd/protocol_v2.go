package nsqd

import (
	"io"
	"net"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type protocolV2 struct {
	ctx *context
}

func readMPUB(r io.Reader, tmp []byte, topic *Topic, maxMessageSize int64, maxBodySize int64) ([]*Message, error) {
	// TODO
	panic("NOT IMPLEMENT YET")
}

func (p *protocolV2) IOLoop(conn net.Conn) error {
	return nil
}
