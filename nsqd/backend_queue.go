package nsqd

// BackendQueue defined interface of backend queue
type BackendQueue interface {
	Put([]byte) error
	ReadChan() chan []byte
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}
