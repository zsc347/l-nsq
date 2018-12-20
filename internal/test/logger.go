package test

// Logger define test logger interface
type Logger interface {
	Output(maxdepth int, s string) error
}

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

// NewTestLogger return a logger for
func NewTestLogger(tbl tbLog) Logger {
	return &testLogger{tbl}
}
