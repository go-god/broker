package broker

import (
	"runtime/debug"
)

// Logger is logger interface.
type Logger interface {
	Printf(string, ...interface{})
}

// LoggerFunc is a bridge between Logger and any third party logger.
type LoggerFunc func(string, ...interface{})

// Printf implements Logger interface.
func (f LoggerFunc) Printf(msg string, args ...interface{}) { f(msg, args...) }

// DummyLogger dummy logger writes nothing.
var DummyLogger = LoggerFunc(func(string, ...interface{}) {})

// Recovery catch go runtime panic
func Recovery(logger Logger) {
	if err := recover(); err != nil {
		logger.Printf("handler msg panic:%v", err)
		logger.Printf("full_stack:%s", string(debug.Stack()))
	}
}
