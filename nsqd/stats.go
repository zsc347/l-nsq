package nsqd

import (
	"runtime"
	"sort"
	"sync/atomic"

	"github.com/l-nsq/internal/quantile"
)

// TopicStats define topic stats
type TopicStats struct {
	TopicName            string           `json:"topic_name"`
	Channels             []ChannelStats   `json:"channels"`
	Depth                int64            `json:"depth"`
	BackendDepth         int64            `json:"backend_depth"`
	MessageCount         uint64           `json:"message_count"`
	Paused               bool             `json:"paused"`
	E2eProcessingLatency *quantile.Result `json:"e2d_processing_latency"`
}

// NewTopicStats construct TopicStats
func NewTopicStats(t *Topic, channels []ChannelStats) TopicStats {
	return TopicStats{
		TopicName:    t.name,
		Channels:     channels,
		Depth:        t.Depth(),
		BackendDepth: t.backend.Depth(),
		MessageCount: atomic.LoadUint64(&t.messageCount),
		Paused:       t.IsPaused(),

		E2eProcessingLatency: t.AggregateChannelE2eProcessingLatency().Result(),
	}
}

// ChannelStats define channelStats
type ChannelStats struct {
	ChannelName          string           `json:"channel_name"`
	Depth                int64            `json:"depth"`
	BackendDepth         int64            `json:"backend_depth"`
	InFlightCount        int              `json:"in_flight_count"`
	DeferredCount        int              `json:"deferred_count"`
	MessageCount         uint64           `json:"message_count"`
	RequeueCount         uint64           `json:"requeue_count"`
	TimeoutCount         uint64           `json:"timeout_count"`
	Clients              []ClientStats    `json:"clients"`
	Paused               bool             `json:"paused"`
	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

// NewChannelStats return channel stats
func NewChannelStats(c *Channel, clients []ClientStats) ChannelStats {
	c.inFlightMutex.Lock()
	inflight := len(c.inFlightMessages)
	c.inFlightMutex.Unlock()
	c.deferredMutex.Lock()
	deferred := len(c.deferedMessages)
	c.deferredMutex.Unlock()

	return ChannelStats{
		ChannelName:          c.name,
		Depth:                c.Depth(),
		BackendDepth:         c.backend.Depth(),
		InFlightCount:        inflight,
		DeferredCount:        deferred,
		MessageCount:         atomic.LoadUint64(&c.messageCount),
		RequeueCount:         atomic.LoadUint64(&c.requeueCount),
		TimeoutCount:         atomic.LoadUint64(&c.timeoutCount),
		Clients:              clients,
		Paused:               c.IsPaused(),
		E2eProcessingLatency: c.e2eProcessingLatencyStream.Result(),
	}
}

// PubCount tell count of messages published by topic
type PubCount struct {
	Topic string `json:"topic"`
	Count uint64 `json:"count"`
}

// ClientStats define statistics structure
type ClientStats struct {
	ClientID        string `json:"client_id"`
	Hostname        string `json:"hostname"`
	Version         string `json:"version"`
	RemoteAddress   string `json:"remote_address"`
	State           int32  `json:"state"`
	ReadyCount      int64  `json:"ready_count"`
	InFlightCount   int64  `json:"in_flight_count"`
	MessageCount    uint64 `json:"message_count"`
	FinishCount     uint64 `json:"finish_count"`
	RequeueCount    uint64 `json:"requeue_count"`
	ConnectTime     int64  `json:"connect_ts"`
	SampleRate      int32  `json:"sample_rate"`
	Deflate         bool   `json:"deflate"`
	Snappy          bool   `json:"snappy"`
	UserAgent       string `json:"user_agent"`
	Authed          bool   `json:"authed,omitempty"`
	AuthIdentity    string `json:"auth_identity,omitempty"`
	AuthIdentityURL string `json:"auth_identity_url,omitempty"`

	PubCounts []PubCount `json:"pub_count,omitemtpy"`

	TLS                          bool   `json:"tls"`
	CipherSuite                  string `json:"tls_cipher_suite"`
	TLSVersion                   string `json:"tls_version"`
	TLSNegotiateProtocol         string `json:"tls_negotiate_protocol"`
	TLSNegotiateProtocolIsMutual bool   `json:"tls_negotiate_protocol_is_mutual"`
}

type memStats struct {
	HeapObjects       uint64 `json:"heap_objects"`
	HeapIdleBytes     uint64 `json:"heap_idle_bytes"`
	HeapInUseBytes    uint64 `json:"heap_in_use_bytes"`
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	GCPauseUsec100    uint64 `json:"gc_pause_usec_100"`
	GCPauseUsec99     uint64 `json:"gc_pause_usec_99"`
	GCPauseUsec95     uint64 `json:"gc_pause_usec_95"`
	NextGCBytes       uint64 `json:"next_gc_bytes"`
	GCTotalRuns       uint32 `json:"gc_total_runs"`
}

// Topics define slice of Topic
type Topics []*Topic

// Len return length of topics
func (t Topics) Len() int { return len(t) }

// Swap swap element in Topics
func (t Topics) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

// TopicsByName support sort topics by name
type TopicsByName struct {
	Topics
}

// Less compare topic by name
func (t TopicsByName) Less(i, j int) bool { return t.Topics[i].name < t.Topics[j].name }

// Channels define slice of Channel
type Channels []*Channel

// Len return length of Channels
func (c Channels) Len() int { return len(c) }

// Swap swap element
func (c Channels) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// ChannelsByName support sort channels by name
type ChannelsByName struct {
	Channels
}

// Less compare by channel name
func (c ChannelsByName) Less(i, j int) bool {
	return c.Channels[i].name < c.Channels[j].name
}

// GetStats return topic stats
func (n *NSQD) GetStats(topic string, channel string) []TopicStats {
	n.RLock()
	var realTopics []*Topic
	if topic == "" { // empty string means get all topic
		realTopics = make([]*Topic, 0, len(n.topicMap))
		for _, t := range n.topicMap {
			realTopics = append(realTopics, t)
		}
	} else if val, exists := n.topicMap[topic]; exists {
		realTopics = []*Topic{val}
	} else { // nil do nothing, return empty list
		n.RUnlock()
		return []TopicStats{}
	}
	n.Unlock()
	sort.Sort(TopicsByName{realTopics})

	topics := make([]TopicStats, 0, len(realTopics))
	for _, t := range realTopics {
		t.RLock()
		var realChannels []*Channel
		if channel == "" {
			realChannels = make([]*Channel, 0, len(t.channelMap))
			for _, c := range t.channelMap {
				realChannels = append(realChannels, c)
			}
		} else if val, exists := t.channelMap[channel]; exists {
			realChannels = []*Channel{val}
		} else {
			t.RUnlock()
			continue
		}
		t.RUnlock()
		sort.Sort(ChannelsByName{realChannels})

		channels := make([]ChannelStats, 0, len(t.channelMap))
		for _, c := range t.channelMap {
			c.RLock()
			clients := make([]ClientStats, 0, len(c.clients))
			for _, client := range c.clients {
				clients = append(clients, client.Stats())
			}
			c.RUnlock()
			channels = append(channels, NewChannelStats(c, clients))
		}
		topics = append(topics, NewTopicStats(t, channels))
	}
	return topics
}

// GetProducerStats find producer and return stats
func (n *NSQD) GetProducerStats() []ClientStats {
	n.clientLock.RLock()
	var producers []Client

	for _, c := range n.clients {
		if c.IsProducer() {
			producers = append(producers, c)
		}
	}
	n.clientLock.RUnlock()

	producersStats := make([]ClientStats, 0, len(producers))
	for _, p := range producers {
		producersStats = append(producersStats, p.Stats())
	}
	return producersStats
}

func getMemStats() memStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// sort the GC pause array
	length := len(ms.PauseNs)
	if int(ms.NumGC) < length {
		length = int(ms.NumGC)
	}

	gcPauses := make(Uint64Slice, length)
	copy(gcPauses, ms.PauseNs[:length])
	sort.Sort(gcPauses)

	return memStats{
		ms.HeapObjects,
		ms.HeapIdle,
		ms.HeapInuse,
		ms.HeapReleased,
		percentile(100.0, gcPauses, len(gcPauses)) / 1000,
		percentile(99.0, gcPauses, len(gcPauses)) / 1000,
		percentile(95.0, gcPauses, len(gcPauses)) / 1000,
		ms.NextGC,
		ms.NumGC,
	}
}
