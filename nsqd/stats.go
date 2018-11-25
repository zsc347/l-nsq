package nsqd

import (
	"github.com/l-nsq/internal/quantile"
)

type TopicStats struct {
	TopicName            string
	Channels             []ChannelStats
	Depth                int64
	BackendDepth         int64
	MessageCount         uint64
	Paused               bool
	E2eProcessingLatency *quantile.Result
}

type ChannelStats struct {
	ChannelName          string
	Depth                int64
	BackendDepth         int64
	InFlightCount        int
	DeferredCount        int
	MessageCount         uint64
	RequeueCount         uint64
	TimeoutCount         uint64
	Clients              []ClientStats
	Paused               bool
	E2eProcessingLatency *quantile.Result
}

type PubCount struct {
	Topic string
	Count uint64
}

type ClientStats struct {
	ClientID        string
	Hostname        string
	Version         string
	RemoteAddress   string
	State           int32
	ReadyCount      int64
	InFlightCount   int64
	MessageCount    uint64
	FinishCount     uint64
	RequeueCount    uint64
	ConnectTime     int64
	SampleRate      int32
	Deflate         bool
	Snappy          bool
	UserAgent       string
	Authed          bool
	AuthIdentity    string
	AuthIdentityURL string

	PubCounts []PubCount

	TLS                          bool
	CipherSuite                  string
	TLSVersion                   string
	TLSNegotiateProtocol         string
	TLSNegotiateProtocolIsMutual bool
}

type memStats struct {
	HeapObjects       uint64
	HeapIdleBytes     uint64
	HeapInUseBytes    uint64
	HeapReleasedBytes uint64
	GCPauseUsec100    uint64
	GCPauseUsec99     uint64
	GCPauseUsec95     uint64
	NextGCBytes       uint64
	GCToTalRuns       uint32
}
