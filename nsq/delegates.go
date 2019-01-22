package nsq

import "time"

type logger interface {
	Output(calldepth int, s string) error
}

// LogLevel specifies the severity of a given log message
type LogLevel int

// Log levels
const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
)

// MessageDelegate is an interface of methods that are used
// as callcacks in Message
type MessageDelegate interface {
	// OnFinish is called when the finish() method
	// is triggered on the Message
	OnFinish(*Message)

	// OnRequeue is called when the Requeue() method
	// is triggered on the Message
	OnRequeue(m *Message, delay time.Duration, backoff bool)

	// OnTouch is called when the Touch() method
	// is triggerred on the Message
	OnTouch(*Message)
}

// ConnDelegate is an interface of methods that are used as
// callbacks in Conn
type ConnDelegate interface {
	// OnResponse is called when the connection
	// receives a FrameTypeResponse from nsqd
	OnResponse(*Conn, []byte)

	// OnError is called when the connection
	// receives a FrameTypeError from nsqd
	OnError(*Conn, []byte)

	// OnMessage is called when the connection
	// receives a FrameTypeMessage from nsqd
	OnMessage(*Conn, *Message)

	// OnMessageFinished is called when the connection
	// handles a FIN command from a message handler
	OnMessageFinished(*Conn, *Message)

	// OnMessageRequeued is called when the connection
	// handles a FIN command from a message handler
	OnMessageRequeued(*Conn, *Message)

	// OnBackoff is called when the connection triggers a backoff state
	OnBackoff(*Conn)

	// OnContinue is called when the connection finishes a message without
	// adjusting backoff state
	OnContinue(*Conn)

	// OnResume is called when the connection triggers a resume state
	OnResume(*Conn)

	// OnIOError is called when the connection experiences
	// a low-level TCP transport error
	OnIOError(*Conn, error)

	// OnHeartbeat is called when the connection
	// receives a heartbeat from nsqd
	OnHeartbeat(*Conn)

	// OnClose is called when the connection
	// closes, after all cleanup
	OnClose(*Conn)
}

// keeps the exported Consumer struct clean of the exported methods
// required to implement the ConnDelegate interface
type consumerConnDelegate struct {
	r *Consumer
}

// keeps the exported Consumer struct clean of the exported methods
// required to implement the ConnDelegate interface
// func (d *consumerConnDelegate) OnResponse(c *Conn, data []byte) {
// 	d.r.onConnResponse(c, data)
// }

type producerConnDelegate struct {
	w *Producer
}

func (d *producerConnDelegate) OnResponse(c *Conn, data []byte) {
	d.w.onConnResponse(c, data)
}

func (d *producerConnDelegate) OnError(c *Conn, data []byte) {
	d.w.onConnError(c, data)
}

func (d *producerConnDelegate) OnMessage(*Conn, *Message) {}

func (d *producerConnDelegate) OnMessageFinished(*Conn, *Message) {}

func (d *producerConnDelegate) OnMessageRequeued(*Conn, *Message) {}

func (d *producerConnDelegate) OnBackoff(c *Conn) {}

func (d *producerConnDelegate) OnContinue(c *Conn) {}

func (d *producerConnDelegate) OnResume(c *Conn) {}

func (d *producerConnDelegate) OnIOError(c *Conn, err error) {
	d.w.onConnIOError(c, err)
}

func (d *producerConnDelegate) OnHeartbeat(c *Conn) {
	d.w.onConnHeartbeat(c)
}

func (d *producerConnDelegate) OnClose(c *Conn) {
	d.w.onConnClose(c)
}
