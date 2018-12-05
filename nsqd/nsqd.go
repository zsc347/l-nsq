package nsqd

import (
	"crypto/tls"
	"sync"
	"sync/atomic"

	"github.com/l-nsq/internal/util"
)

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit paltforms
	clientIDSequence int64

	sync.RWMutex

	opts atomic.Value

	isLoading int32
	errValue  atomic.Value // set by channel, set each time when put message

	clientLock sync.RWMutex
	clients    map[int64]Client

	tlsConfig *tls.Config

	waitGroup util.WaitGroupWrapper

	notifyChan chan interface{}
	exitChan   chan int
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete
	// shoud not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := atomic.LoadInt32(&n.isLoading) == 0

	n.waitGroup.Wrap(func() {
		select {
		case <-n.exitChan:
		case n.notifyChan <- v:
			if !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

func (n *NSQD) PersistMetadata() error {
	// TODO
	return nil
}

func (n *NSQD) SetHealth(err error) {
	n.er
}

type Client interface {
	Stats() ClientStats
	IsProducer() bool
}
