package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/l-nsq/internal/lg"
)

// Options combines all nsqd configurable options
type Options struct {
	// basic options
	ID int64 `flag:"node-id" cfg:"id"`

	LogLevel  string `flag:"log-level"`
	LogPrefix string `flag:"log-prefix"`
	Logger    Logger
	logLevel  lg.LogLevel // private, not really an option

	TCPAddress   string `flag:"tcp-address"`   // ?
	HTTPAddress  string `flag:"http-address"`  // ?
	HTTPSAddress string `flag:"https-address"` // ?

	BroadcastAddress     string   `flag:"broadcast-address"`
	NSQLookupdTCPAddress []string `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"`

	AuthHTTPAddresses           []string      `falg:"auth-http-address" cfg:"nsqlookupd_tcp_address"`
	HTTPClientConnectionTimeout time.Duration `flag:"http-client-connec-timeout" cfg:"http_client_connect_timeout"`
	HTTPClientRequestTimeout    time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"`

	DataPath        string        `flag:"data-path"`
	MemQueueSize    int64         `flag:"mem-queue-size"`
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`
	SyncEvery       int64         `flag:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	// ?
	QueueScanInterval        time.Duration
	QueueScanRefreshInterval time.Duration
	QueueScanSelectionCount  int
	QueueScanWorkerPoolMax   int
	QueueScanDirtyPercent    float64

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// msg and command options
	MsgTimeout time.Duration `flag:"msg-timeout"`
	// msg and command options
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
	MaxMsgSize    int64         `flag:"max-msg-size"`
	MaxBodySize   int64         `flag:"max-body-size"`
	MaxReqTimeout time.Duration `flag:"max-req-timeout"`
	ClientTimeout time.Duration

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`
	MaxRdyCount            int64         `flag:"max-rdy-count"`
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"`

	// statsd integration
	StatsdAddress       string        `flag:"statsd-address"`
	StatsdPrefix        string        `flag:"statsd-prefix"`
	StatsdInterval      time.Duration `flag:"statsd-interval"`
	StatsdMemStats      bool          `flag:"statsd-mem-stats"`
	StatsdUDPPacketSize int           `flag:"statsd-udp-packet-size"`

	// compression
	DeflateEnabled  bool `flag:"deflate"`
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	SnappyEnabled   bool `flag:"snappy"`

	// TLS config
	TLSRequired   int    `flag:"tls-required"`
	TLSMinVersion uint64 `flag:"tls-min-version"`
}

// NewOptions return a new option
func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)

	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  "info",

		TCPAddress:       "0.0.0.0:4150",
		HTTPAddress:      "0.0.0.0:4151",
		HTTPSAddress:     "0.0.0.0:4152",
		BroadcastAddress: hostname,

		NSQLookupdTCPAddress: make([]string, 0),
		AuthHTTPAddresses:    make([]string, 0),

		HTTPClientConnectionTimeout: 2 * time.Second,
		HTTPClientRequestTimeout:    5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 1 * time.Second,

		StatsdPrefix:        "nsq.%s",
		StatsdInterval:      60 * time.Second,
		StatsdMemStats:      true,
		StatsdUDPPacketSize: 508,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}

}
