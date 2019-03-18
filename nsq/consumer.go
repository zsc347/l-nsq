package nsq

import (
	"errors"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is the message processing interface for Consumer
//
// Implement this interface for handlers that return whether or not
// message processing completed successfully
//
// When the return value is nil Consumer will automatically handle FINishing.
//
// When the returned value is not-nil, Consumer will automiatially handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// HandlerFunc is a convenience type to avoid having to declare a struct
// to implement the Handler interface, it can be used like this:
//
// consumer.AddHandler(nsg.HandlerFunc(func(m *Message) error {
//     // handle the message
// }))
type HandlerFunc func(message *Message) error

// HandleMessage implements the Handler interface
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// DiscoveryFilter is an interface accepted by `SetBehaviorDelegate()`
// for filtering the nsqds returned from disvovery via nsqlookupd
type DiscoveryFilter interface {
	Filter([]string) []string
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed"
// (i.e. the number of attempts exceed the Consumer specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

// ConsumerStats represents a snapshot of the state of a Consumer's connections
// and the messages it has seen
type ConsumerStats struct {
	MessageReceived uint64
	MessageFinished uint64
	MessageRequeued uint64
	Connections     int
}

var instCount int64

type backoffSignal int

const (
	backoffFlag backoffSignal = iota
	continueFlag
	resumeFlag
)

// Consumer is a high-level type to consume from NSQ.
//
// A Consumer instance is supplied a Handler that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel. See: Handler/HandlerFunc
// for details on implementing the interface to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection
// (and reconnection) to any discovered nsqds.
type Consumer struct {
	messagesReceived uint64
	messagesFinished uint64
	messagesRequeued uint64
	totalRdyCount    int64
	backoffDuration  int64
	backoffCounter   int32
	maxInFlight      int32

	mtx sync.RWMutex

	logger   logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	behaviorDelegate interface{}

	id      int64
	topic   string
	channel string
	config  Config

	rngMtx sync.Mutex
	rng    *rand.Rand

	needRDYRedistributed int32

	backoffMtx sync.RWMutex

	incomingMessages chan *Message

	rdyRetryMtx    sync.RWMutex
	rdyRetryTimers map[string]*time.Timer

	pendingConnections map[string]*Conn
	connections        map[string]*Conn

	nsqdTCPAddrs []string

	// used at connection close to force a possible reconnect
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupQueryIndex   int

	wg              sync.WaitGroup
	runningHandlers int32
	stopFlag        int32
	connectedFlag   int32
	stopHandler     sync.Once
	exitHandler     sync.Once

	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
	exitChan chan int
}

// NewConsumer creates a new instance of Consumer for the specified topic/channel
//
// The only valid way to create a Config is via NewConfig, using a struct
// literal will panic.
// After Config is passed into NewConsumer the values are no longer mutable
// (they are copied)
func NewConsumer(topic string, channel string, config *Config) (*Consumer, error) {
	config.assertInitialized()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	r := &Consumer{
		id: atomic.AddInt64(&instCount, 1),

		topic:   topic,
		channel: channel,
		config:  *config,

		logger:      log.New(os.Stderr, "", log.Flags()),
		logLvl:      LogLevelError,
		maxInFlight: int32(config.MaxInFlight),

		incomingMessages: make(chan *Message),

		rdyRetryTimers:     make(map[string]*time.Timer),
		pendingConnections: make(map[string]*Conn),
		connections:        make(map[string]*Conn),

		lookupdRecheckChan: make(chan int, 1),

		rng: rand.New(rand.NewSource(time.Now().UnixNano())),

		StopChan: make(chan int),
		exitChan: make(chan int),
	}
	r.wg.Add(1)
	go r.rdyLoop()
	return r, nil
}

// Stats retrieves the current connection and message statistics for a Consumer
func (r *Consumer) Stats() *ConsumerStats {
	return &ConsumerStats{
		MessageReceived: atomic.LoadUint64(&r.messagesReceived),
		MessageFinished: atomic.LoadUint64(&r.messagesFinished),
		MessageRequeued: atomic.LoadUint64(&r.messagesRequeued),
		Connections:     len(r.conns()),
	}
}

func (r *Consumer) conns() []*Conn {
	r.mtx.RLock()
	conns := make([]*Conn, 0, len(r.connections))
	for _, c := range r.connections {
		conns = append(conns, c)
	}
	r.mtx.RUnlock()
	return conns
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the stdlib log.Logger):
//
// Output(calldepth int, s string)
//
func (r *Consumer) SetLogger(l logger, lvl LogLevel) {
	r.logGuard.Lock()
	defer r.logGuard.Unlock()

	r.logger = l
	r.logLvl = lvl
}

func (r *Consumer) getLogger() (logger, LogLevel) {
	r.logGuard.RLock()
	defer r.logGuard.RUnlock()

	return r.logger, r.logLvl
}

// SetBehaviorDelegate takes a type implementing one or more
// of the following interfaces that modify the behavior
// of the `Consumer`:
//
// DiscoveryFilter
//
func (r *Consumer) SetBehaviorDelegate(cb interface{}) {
	matched := false
	if _, ok := cb.(DiscoveryFilter); ok {
		matched = true
	}

	if !matched {
		panic("behavior delegate does not have any recognized methods")
	}

	r.behaviorDelegate = cb
}

// perConnMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd
// the Consumer is responsible for.
func (r *Consumer) perConnMaxInFlight() int64 {
	b := float64(r.getMaxInFlight())
	s := b / float64(len(r.conns()))
	return int64(math.Min(math.Max(1, s), b))
}

func (r *Consumer) getMaxInFlight() int32 {
	return atomic.LoadInt32(&r.maxInFlight)
}

// IsStarved indicates whether any connections for this consumer are blocked
// on processing before being able to receive more messages (ie. RDY count of
// 0 and not exiting)
func (r *Consumer) IsStarved() bool {
	for _, conn := range r.conns() {
		threshold := int64(float64(atomic.LoadInt64(&conn.lastRdyCount)) * 0.85)
		inFlight := atomic.LoadInt64(&conn.messagesInFlight)
		if inFlight >= threshold && inFlight > 0 && !conn.IsClosing() {
			return true
		}
	}
	return false
}

// ChangeMaxInFlight sets a new maximum number of messages this consumer
// instance will allow in-flight, and updates all existing connections as
// appropriate.
//
// For example, ChangeMaxInFlight(0) would pause message flow
//
// If already connected, it updates the reader RDY state for each connection.
func (r *Consumer) ChangeMaxInFlight(maxInFlight int) {
	if r.getMaxInFlight() == int32(maxInFlight) {
		return
	}

	atomic.StoreInt32(&r.maxInFlight, int32(maxInFlight))

	for _, c := range r.conns() {
		r.maybeUpdateRDY(c)
	}
}

func (r *Consumer) inBackoff() bool {
	return atomic.LoadInt32(&r.backoffCounter) > 0
}

func (r *Consumer) inBackoffTimeout() bool {
	return atomic.LoadInt64(&r.backoffDuration) > 0
}

func (r *Consumer) ConnectToNSQDLookupd(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}
	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	if err := validateLookupAddr(addr); err != nil {
		return err
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	r.mtx.Lock()
	for _, x := range r.lookupdHTTPAddrs {
		if x == addr {
			r.mtx.Unlock()
			return nil
		}
	}
	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs, addr)
	numLookupd := len(r.lookupdHTTPAddrs)
	r.mtx.Unlock()

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		r.queryLookupd()
		r.wg.Add(1)
		go r.lookupLoop()
	}

	return nil
}

// ConnectToNSQLookupds adds multiple nsqlookupd address to the list for this
// Consumer
// If adding the first adress it initiatews an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Consumer) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQDLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateLookupAddr(addr string) error {
	if strings.Contains(addr, "/") {
		_, err := url.Parse(addr)
		if err != nil {
			return err
		}
		return nil
	}
	if !strings.Contains(addr, ":") {
		return errors.New("missing port")
	}
	return nil
}

func (r *Consumer) maybeUpdateRDY(conn *Conn) {
	inBackoff := r.inBackoff()
	inBackoffTimeout := r.inBackoffTimeout()

	if inBackoff || inBackoffTimeout {
		r.log(LogLevelDebug, "(%s) skip sending RDY inBackoff:%v || inBackoffTimeout:%v",
			conn, inBackoff, inBackoffTimeout)
		return
	}

	remain := conn.RDY()
	lastRdyCount := conn.LastRDY()
	count := r.perConnMaxInFlight()

	if remain <= 1 || remain < (lastRdyCount/4) || (count > 0 && count < remain) {
		r.updateRDY(conn, count)
	} else {
		r.log(LogLevelDebug, "(%s) skip sending RDY %d (%d remain out of last RDY %d)",
			conn, count, remain, lastRdyCount)
	}
}
