package consumergroup

import "fmt"

// Logger is a simple log interface. The dafault implementation prints logs to stdout.
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

// Constants defining log levels.
const (
	DEBUG = 0 + iota
	INFO
	WARN
	ERROR
)

// InnerLogger is the default implementation of the Logger interface that consumer groups used
// to print logs to stdout.
type InnerLogger struct {
	level int
}

// NewInnerLog creates an InnerLogger instance.
func NewInnerLog(level int) *InnerLogger {
	logger := new(InnerLogger)
	logger.level = level
	return logger
}

func (logger *InnerLogger) Debug(args ...interface{}) {
	if logger.level <= DEBUG {
		fmt.Println(append([]interface{}{"[DEBUG] "}, args...)...)
	}
}

func (logger *InnerLogger) Debugf(format string, args ...interface{}) {
	logger.Debug(fmt.Sprintf(format, args...))
}

func (logger *InnerLogger) Info(args ...interface{}) {
	if logger.level <= INFO {
		fmt.Println(append([]interface{}{"[INFO] "}, args...)...)
	}
}

func (logger *InnerLogger) Infof(format string, args ...interface{}) {
	logger.Info(fmt.Sprintf(format, args...))
}
func (logger *InnerLogger) Warn(args ...interface{}) {
	if logger.level <= WARN {
		fmt.Println(append([]interface{}{"[WARN] "}, args...)...)
	}
}

func (logger *InnerLogger) Warnf(format string, args ...interface{}) {
	logger.Warn(fmt.Sprintf(format, args...))
}

func (logger *InnerLogger) Error(args ...interface{}) {
	if logger.level <= ERROR {
		fmt.Println(append([]interface{}{"[ERROR] "}, args...)...)
	}
}

func (logger *InnerLogger) Errorf(format string, args ...interface{}) {
	logger.Error(fmt.Sprintf(format, args...))
}
