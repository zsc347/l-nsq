package nsqd

import (
	"sync/atomic"
)

type NSQD struct {
	opts atomic.Value
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}
