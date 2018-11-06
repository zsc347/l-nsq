package nsqd

import (
	"github.com/l-nsq/internal/lg"
)

type Options struct {
	Logger   Logger
	logLevel lg.LogLevel // private, not really an option
}
