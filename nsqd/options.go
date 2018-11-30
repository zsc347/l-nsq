package nsqd

import (
	"time"

	"github.com/l-nsq/internal/lg"
)

type Options struct {
	ID       int64 `flag:"node-id" cfg:"id"`
	Logger   Logger
	logLevel lg.LogLevel // private, not really an option

	DataPath        string        `flag:"data-path"`
	MemQueueSize    int64         `flag:"mem-queue-size"`
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`
	SyncEvery       int64         `flag:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// msg and command options
	MaxMsgSize int64 `flag:"max-msg-size"`

	// msg and command options
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
}
