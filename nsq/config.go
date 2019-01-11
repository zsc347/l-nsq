package nsq

import (
	"net"
	"time"
)

// BackoffStrategy defines a strategy for calculating the duration of time
// a consumer should backoff for a given attempt
type BackoffStrategy interface {
	Calculate(attempt int) time.Duration
}

// Config is a struct of NSQ options
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic
// After Config is passed into a high-level type (like Consumer, Producer, etc.) the values
// are no longer mutable(they are copied).
//
// Use Set(option string, value interface{}) as an alternate way to set parameters.
type Config struct {
	initialized bool

	// used to Initialize, Validate
	configHandlers []configHandler

	DialTimeout time.Duration `opt:"dial_timeout" default:"ls"`

	// Deadlines for network reads and writes
	ReadTimeout  time.Duration `opt:"read_timeout" min:"100ms" max:"5m" default:"60s"`
	WriteTimeout time.Duration `opt:"write_timeout" min:"100ms" max:"5m" default:"1s"`

	// LocalAddr is the local address to use when dialing an nsqd.
	// If empty, a local address is automatically chosen.
	LocalAddr net.Addr `opt:"local_addr"`

	// Duration between polling lookupd for new producers, and fractional jitter to add to
	// the lookupd pool loop. This helps evently distribute requests even if multiple consumers
	// restart at the same time
	//
	// NOTE: when not using nsqlookupd, LookupdPollInterval represents the duration of time
	// between reconnction attempts
	LookupdPollInterval time.Duration `opt:"lookupd_poll_interval" min:"10ms" max:"5m" default:"60s"`
	LookupdPollJitter   float64       `opt:"lookupd_poll_jitter" min:"0" max:"1" default:"0.3"`

	// Maximun duration when REQueueing (for doubling of deferred requeue)
	MaxRequeueDelay     time.Duration `opt:"max_requeue_delay" min:"0" max:"1" default:"15m"`
	DefaultRequeueDelay time.Duration `opt:"default_requeue_delay" min:"0" max:"60m" default:"90m"`

	// Backoff strategy, defaults to exponential backoff.
	// Overwrite this to define alternative backoff algorithms
	BackoffStrategy BackoffStrategy `opt:"backoff_strategy" default:"exponential"`
	// Maximum amount of time to backoff when processing fails 0 == no backoff
	MaxBackoffDuration time.Duration `opt:"max_backoff_duration" min:"0" max:"60m" default:"2m"`
	// Unit of time for calculating consumer backoff
	BackoffMultiplier time.Duration `opt:"backoff_multiplier" min:"0" max:"60m" default:"1s"`

	// Maximum number of times this consumer will attempt to process a message before giving up
	MaxAttempts uint16 `opt:"max_attempts" min:"0" max:"65535" default:"5"`

	// Duration to wait for a message from a producer when in a state when RDY
	// counts are re-distributes (ie. max_in_flight < num producers)
	LowRdyIdleTimeout time.Duration `opt:"low_rdy_idle_timeout" min:"1s" max:"5m" default:"10s"`

	// Identifiers sent to nsqd representing this client
	// UserAgent is in the spirit of HTTP (default: "<client_library_name>/<version>")
	ClientID  string `opt:"client_id"`
	Hostname  string `opt:"hostname"`
	UserAgent string `opt:"user_agent"`
}

type configHandler interface {
	HandlesOption(c *Config, option string) bool
	Set(c *Config, option string, value interface{}) error
	Validate(c *Config) error
}
