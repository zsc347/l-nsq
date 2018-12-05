package nsqd

import (
	"time"

	"github.com/l-nsq/internal/lg"
)

// Options combines all nsqd configurable options
type Options struct {
	ID       int64 `flag:"node-id" cfg:"id"`
	Logger   Logger
	logLevel lg.LogLevel // private, not really an option

	AuthHTTPAddresses           []string      `falg:"auth-http-address" cfg:"nsqlookupd_tcp_address"`
	HTTPClientConnectionTimeout time.Duration `flag:"http-client-connec-timeout" cfg:"http_client_connect_timeout"`
	HTTPClientRequestTimeout    time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"`

	DataPath        string        `flag:"data-path"`
	MemQueueSize    int64         `flag:"mem-queue-size"`
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`
	SyncEvery       int64         `flag:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout"`
	MaxMsgSize    int64         `flag:"max-msg-size"`
	ClientTimeout time.Duration

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"`

	// msg and command options
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
}
