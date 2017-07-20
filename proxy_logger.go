package consumergroup

import "fmt"

type proxyLogger struct {
	prefix       string
	targetLogger Logger
}

func newProxyLogger(prefix string, logger Logger) *proxyLogger {
	return &proxyLogger{
		targetLogger: logger,
		prefix:       prefix,
	}
}

func (pl *proxyLogger) write(level int, args ...interface{}) {
	args = append([]interface{}{pl.prefix}, args...)
	switch level {
	case debugLevel:
		pl.targetLogger.Debug(args...)
	case infoLevel:
		pl.targetLogger.Info(args...)
	case warnLevel:
		pl.targetLogger.Warn(args...)
	case errorLevel:
		pl.targetLogger.Error(args...)
	}
}

func (pl *proxyLogger) Debug(args ...interface{}) {
	pl.write(debugLevel, args...)
}

func (pl *proxyLogger) Debugf(format string, args ...interface{}) {
	pl.Debug(fmt.Sprintf(format, args...))
}

func (pl *proxyLogger) Info(args ...interface{}) {
	pl.write(infoLevel, args...)
}

func (pl *proxyLogger) Infof(format string, args ...interface{}) {
	pl.Info(fmt.Sprintf(format, args...))
}

func (pl *proxyLogger) Warn(args ...interface{}) {
	pl.write(warnLevel, args...)
}

func (pl *proxyLogger) Warnf(format string, args ...interface{}) {
	pl.Warn(fmt.Sprintf(format, args...))
}

func (pl *proxyLogger) Error(args ...interface{}) {
	pl.write(errorLevel, args...)
}

func (pl *proxyLogger) Errorf(format string, args ...interface{}) {
	pl.Error(fmt.Sprintf(format, args...))
}
