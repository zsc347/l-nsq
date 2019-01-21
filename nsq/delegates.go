package nsq

import "time"

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
