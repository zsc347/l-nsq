package nsq

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
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

// AuthResponse represents the metadata
// returned from an AUTH command to nsqd
type AuthResponse struct {
	Identity        string `json:"identify"`
	IdentityURL     string `json:"identity_url"`
	PermissionCount int64  `json:"permission_count"`
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

// NewConn returns a new Conn instance
func NewConn(addr string, config *Config, delegate ConnDelegate) *Conn {
	if !config.initialized {
		panic("Config must be created with NewConfig()")
	}
	return &Conn{
		addr: addr,

		config:   config,
		delegate: delegate,

		maxRdyCount:      2500,
		lastMsgTimestamp: time.Now().UnixNano(),

		cmdChan:         make(chan *Command),
		msgResponseChan: make(chan *msgResponse),
		exitChan:        make(chan int),
		drainReady:      make(chan int),
	}
}

// SetLogger assigns the logger to use as well as level.
//
// The format parameter is expected to be a printf compatible string with
// a single %s argument. This is useful if you want to provide additional
// context to the log messages that the connection will print, the default
// is ('%s')
//
// The logger parameter is an interface that requires the following
// methos to be implemented (such as the stdlib log.Logger)
//
//   Output(calldepth int, s string)
func (c *Conn) SetLogger(l logger, lvl LogLevel, format string) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	c.logger = l
	c.logLvl = lvl
	c.logFmt = format
	if c.logFmt == "" {
		c.logFmt = "(%s)"
	}
}

func (c *Conn) getLogger() (logger, LogLevel, string) {
	c.logGuard.RLock()
	defer c.logGuard.RUnlock()

	return c.logger, c.logLvl, c.logFmt
}

// Connect dials and bootstraps the nsqd connection
// (including IDENTIFY) and returns the IdentifyResponse
func (c *Conn) Connect() (*IdentifyResponse, error) {
	dialer := &net.Dialer{
		LocalAddr: c.config.LocalAddr,
		Timeout:   c.config.DialTimeout,
	}

	conn, err := dialer.Dial("tcp", c.addr)
	if err != nil {
		return nil, err
	}
	c.conn = conn.(*net.TCPConn)
	c.r = conn
	c.w = conn

	_, err = c.Write(MagicV2)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", c.addr, err)
	}

	resp, err := c.identify()
	if err != nil {
		return nil, err
	}

	if resp != nil && resp.AuthRequired {
		if c.config.AuthSecret == "" {
			c.log(LogLevelError, "Auth Required")
			return nil, errors.New("Auth Required")
		}
		err := c.auth(c.config.AuthSecret)
		if err != nil {
			c.log(LogLevelError, "Auth failed %s", err)
			return nil, err
		}
	}

	c.wg.Add(2)
	atomic.StoreInt32(&c.readLoopRunning, 1)
	go c.readLoop()
	go c.writeLoop()
	return resp, nil
}
