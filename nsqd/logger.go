package nsqd

import (
	"github.com/l-nsq/internal/lg"
)

// Logger is an alias for lg.Logger
type Logger lg.Logger

func (n *NSQD) logf(level lg.LogLevel, f string, args ...interface{}) {
	opts := n.getOpts()
	lg.Logf(opts.Logger, opts.logLevel, level, f, args)
}
