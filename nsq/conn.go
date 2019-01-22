package nsq

import (
	"crypto/tls"
	"io"
	"net"
	"sync"
)

// IdentifyResponse represents the metadata
// returned from an IDENTIFY command to nsqd
type IdentifyResponse struct {
	MaxRdyCount  int64 `json:"max_rdy_count"`
	TLSv1        bool  `json:"tls_v1"`
	Deflate      bool  `json:"deflate"`
	Snappy       bool  `json:"snappy"`
	AuthRequired bool  `json:"auth_required"`
}

type msgResponse struct {
	msg     *Message
	cmd     *Command
	success bool
	backoff bool
}

// Conn represents a connection to nsqd
//
// Conn exposes a set of callbacks for the
// various events that occur on a connection
type Conn struct {
	// 64bit atomic vars need to be first for proper alighment on 32bit platforms
	messagesInFlight int64
	maxRdyCount      int64
	rdyCount         int64
	lastRdyCount     int64
	lastMsgTimestamp int64

	mtx sync.Mutex

	config *Config

	conn    *net.TCPConn
	tlsConn *tls.Conn
	addr    string

	delegate ConnDelegate

	logger   logger
	logLvl   LogLevel
	logFmt   string
	logGuard sync.RWMutex

	r io.Reader
	w io.Writer

	cmdChan         chan *Command
	msgResponseChan chan *msgResponse
	exitChan        chan int
	drainReady      chan int

	closeFlag int32
	stopper   sync.Once
	wd        sync.WaitGroup

	readLoopRunning int32
}
