package nsqd

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/l-nsq/internal/clusterinfo"
	"github.com/l-nsq/internal/lg"
	"github.com/l-nsq/internal/util"
)

type errStore struct {
	err error
}

// NSQD defined nsqd context structure
type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit paltforms
	clientIDSequence int64

	sync.RWMutex

	opts atomic.Value

	isLoading int32
	errValue  atomic.Value // set by channel, set each time when put message
	startTime time.Time

	topicMap map[string]*Topic

	clientLock sync.RWMutex
	clients    map[int64]Client

	lookupPeers atomic.Value

	tcpListener  net.Listener
	httpListener net.Listener

	tlsConfig *tls.Config

	waitGroup util.WaitGroupWrapper

	notifyChan chan interface{}
	exitChan   chan int

	ci *clusterinfo.ClusterInfo
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// Notify allow chanel or topic to notify nsqd instance
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
				n.logf(lg.ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// PersistMetadata persist nsqd metadata
func (n *NSQD) PersistMetadata() error {
	// TODO
	panic("NOT IMPLEMENTED")
}

// SetHealth set error
func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

// GetError return nsqd error
func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

// GetHealth tells wheter nsqd is in a goog state, OK or NOK
func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

// IsHealthy return whether error occured
func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

// RealTCPAddr return real tcp address
func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	return n.tcpListener.Addr().(*net.TCPAddr)
}

// RealHTTPAddr retrun real http address
func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	return n.httpListener.Addr().(*net.TCPAddr)
}

// GetStartTime return nsqd start time
func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

// Client is an interface for nsqd client
type Client interface {
	Stats() ClientStats
	IsProducer() bool
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely, we already have this topic, so try read lock first
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	// if not exist, use write lock
	n.Lock()
	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	t = NewTopic(topicName, &context{n}, deleteCallback)
	n.topicMap[topicName] = t
	n.Unlock()

	n.logf(lg.INFO, "TOPIC(%s): created", t.name)

	// topic is created but messagePump not yet started

	// if loading metadata at startup, no lookupd connections yet, topic started after load
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	// if using lookupd, make a blocking call to get the topics, and immetiately create them
	// this makes sure that any message received is buffered to the right channels
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(lg.WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}

		for _, channelName := range channelNames {
			if strings.HasSuffix(topicName, "#ephemeral") {
				continue
			}
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddress) > 0 {
		n.logf(lg.ERROR, "no available nsqlookupd to query for channels to pre-creqte for topic %s", t.name)
	}

	// now that all channels are added, start topic messagePump
	t.Start()
	return t
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we don't leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()

	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// AddClient add a client to nsqd instance
func (n *NSQD) AddClient(clientID int64, client Client) {
	n.clientLock.Lock()
	// clientId has been assured unique
	n.clients[clientID] = client
	n.clientLock.Unlock()
}

// RemoveClient remove client
func (n *NSQD) RemoveClient(clientID int64) {
	n.clientLock.Lock()
	_, ok := n.clients[clientID]
	if !ok {
		n.clientLock.Unlock()
		return
	}
	delete(n.clients, clientID)
	n.clientLock.Unlock()
}
