package nsqd

import (
	"runtime"
	"sort"

	"github.com/l-nsq/internal/quantile"
)

type TopicStats struct {
	TopicName            string           `json:"topic_name"`
	Channels             []ChannelStats   `json:"channels"`
	Depth                int64            `json:"depth"`
	BackendDepth         int64            `json:"backend_depth"`
	MessageCount         uint64           `json:"message_count"`
	Paused               bool             `json:"paused"`
	E2eProcessingLatency *quantile.Result `json:"e2d_processing_latency"`
}

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

type PubCount struct {
	Topic string `json:"topic"`
	Count uint64 `json:"count"`
}

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
	GCToTalRuns       uint32 `json:"gc_total_runs"`
}

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
