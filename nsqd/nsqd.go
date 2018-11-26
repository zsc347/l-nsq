package nsqd

import (
	"sync"
	"sync/atomic"
)

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit paltforms
	clientIDSequence int64
	opts             atomic.Value

	clientLock sync.RWMutex
	clients    map[int64]Client
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

func (n *NSQD) Notify(v interface{}) {
	// TODO
}

type Client interface {
	Stats() ClientStats
	IsProducer() bool
}
