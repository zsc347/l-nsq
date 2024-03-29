package lg

import (
	"log"
	"os"
	"testing"

	"github.com/l-nsq/internal/test"
)

type options struct {
	LogLevel string `flag:"log-level"`
	Verbose  bool   `flag:"verbose"`
	Logger   Logger
	logLevel LogLevel
}

func newOptions() *options {
	return &options{
		LogLevel: "info",
	}
}

type app struct {
	opts *options
}

func (n *app) logf(level LogLevel, f string, args ...interface{}) {
	Logf(n.opts.Logger, n.opts.logLevel, level, f, args)
}

func newApp(opts *options) *app {
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, "[app] ", log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	n := &app{
		opts: opts,
	}

	var err error
	opts.logLevel, err = ParseLogLevel(opts.LogLevel, opts.Verbose)
	if err != nil {
		n.logf(FATAL, "%s", err)
		os.Exit(1)
	}
	n.logf(INFO, "app 0.1")
	return n
}

type mockLoggeer struct {
	Count int
}

func (l *mockLoggeer) Output(maxdepth int, s string) error {
	l.Count++
	return nil
}

func TestLogging(t *testing.T) {
	logger := &mockLoggeer{}
	opts := newOptions()
	opts.Logger = logger

	// Test only fatal get through
	opts.LogLevel = "FaTaL"
	nsqd := newApp(opts)
	logger.Count = 0

	for i := 1; i <= 5; i++ {
		nsqd.logf(LogLevel(i), "Test")
	}
	test.Equal(t, 1, logger.Count)

	// Test only warnings or higher get throuth
	opts.LogLevel = "Warn"
	logger.Count = 0
	nsqd = newApp(opts)
	for i := 1; i <= 5; i++ {
		nsqd.logf(LogLevel(i), "Test")
	}
	test.Equal(t, 3, logger.Count)

	// Test everything get throuth
	opts.LogLevel = "debuG"
	nsqd = newApp(opts)
	logger.Count = 0
	for i := 1; i <= 5; i++ {
		nsqd.logf(LogLevel(i), "Test")
	}
	test.Equal(t, 5, logger.Count)

	// Test everything gets through with vervose = true
	opts.LogLevel = "fatal"
	opts.Verbose = true
	nsqd = newApp(opts)
	logger.Count = 0
	for i := 1; i <= 5; i++ {
		nsqd.logf(LogLevel(i), "Test")
	}
	test.Equal(t, 5, logger.Count)
}

func TestNoLogger(t *testing.T) {
	opts := newOptions()
	opts.Logger = NilLogger{}
	app := newApp(opts)
	app.logf(ERROR, "should never be logged")
}
