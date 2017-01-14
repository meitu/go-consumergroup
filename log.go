package consumergroup

import "fmt"

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

const (
	DEBUG = 0 + iota
	INFO
	WARN
	ERROR
)

type InnerLogger struct {
	level int
}

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
	if logger.level <= DEBUG {
		fmt.Printf("[DEBUG] "+format, args...)
	}
}

func (logger *InnerLogger) Info(args ...interface{}) {
	if logger.level <= INFO {
		fmt.Println(append([]interface{}{"[INFO] "}, args...)...)
	}
}

func (logger *InnerLogger) Infof(format string, args ...interface{}) {
	if logger.level <= INFO {
		fmt.Printf("[INFO] "+format, args...)
	}
}
func (logger *InnerLogger) Warn(args ...interface{}) {
	if logger.level <= WARN {
		fmt.Println(append([]interface{}{"[WARN] "}, args...)...)
	}
}

func (logger *InnerLogger) Warnf(format string, args ...interface{}) {
	if logger.level <= WARN {
		fmt.Printf("[WARN] "+format, args...)
	}
}

func (logger *InnerLogger) Error(args ...interface{}) {
	if logger.level <= ERROR {
		fmt.Println(append([]interface{}{"[ERROR] "}, args...)...)
	}
}

func (logger *InnerLogger) Errorf(format string, args ...interface{}) {
	if logger.level <= ERROR {
		fmt.Printf("[ERROR] "+format, args...)
	}
}
