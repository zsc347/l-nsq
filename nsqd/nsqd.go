package nsqd

import (
	"sync/atomic"
)

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit paltforms
	clientIDSequence int64
	opts             atomic.Value
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}
