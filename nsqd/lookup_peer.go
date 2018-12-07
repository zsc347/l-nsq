package nsqd

import (
	"net"

	"github.com/l-nsq/internal/lg"
)

type lookupPeer struct {
	logf            lg.AppLogFunc
	addr            string
	conn            net.Conn
	state           int32
	connectCallback func(*lookupPeer)
	maxBodySize     int64
	Info            peerInfo
}

type peerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
}
