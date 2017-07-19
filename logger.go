package consumergroup

import "fmt"

// Constants defining log levels.
const (
	debugLevel = 0 + iota
	infoLevel
	warnLevel
	errorLevel
)

// Logger is a simple log interface. The dafault implementation prints to stdout.
type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

type defaultLogger struct {
	level int
}

func newDefaultLogger(level int) *defaultLogger {
	logger := new(defaultLogger)
	logger.level = level
	return logger
}

func (logger *defaultLogger) Debug(args ...interface{}) {
	if logger.level <= debugLevel {
		fmt.Println(append([]interface{}{"[DEBUG] "}, args...)...)
	}
}

func (logger *defaultLogger) Debugf(format string, args ...interface{}) {
	logger.Debug(fmt.Sprintf(format, args...))
}

func (logger *defaultLogger) Info(args ...interface{}) {
	if logger.level <= infoLevel {
		fmt.Println(append([]interface{}{"[INFO] "}, args...)...)
	}
}

func (logger *defaultLogger) Infof(format string, args ...interface{}) {
	logger.Info(fmt.Sprintf(format, args...))
}
func (logger *defaultLogger) Warn(args ...interface{}) {
	if logger.level <= warnLevel {
		fmt.Println(append([]interface{}{"[WARN] "}, args...)...)
	}
}

func (logger *defaultLogger) Warnf(format string, args ...interface{}) {
	logger.Warn(fmt.Sprintf(format, args...))
}

func (logger *defaultLogger) Error(args ...interface{}) {
	if logger.level <= errorLevel {
		fmt.Println(append([]interface{}{"[ERROR] "}, args...)...)
	}
}

func (logger *defaultLogger) Errorf(format string, args ...interface{}) {
	logger.Error(fmt.Sprintf(format, args...))
}
