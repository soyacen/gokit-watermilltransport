package watermilltransport

import (
	"github.com/ThreeDotsLabs/watermill"
	"go.uber.org/zap"
)

type ZapLogger struct {
	l *zap.Logger
}

func NewZapLogger(logger *zap.Logger) *ZapLogger {
	return &ZapLogger{}
}

func (l *ZapLogger) Error(msg string, err error, fields watermill.LogFields) {
	var zapFields []zap.Field
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	l.l.Error(msg, zapFields...)
}
func (l *ZapLogger) Info(msg string, fields watermill.LogFields) {
	var zapFields []zap.Field
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	l.l.Info(msg, zapFields...)
}
func (l *ZapLogger) Debug(msg string, fields watermill.LogFields) {
	var zapFields []zap.Field
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	l.l.Debug(msg, zapFields...)
}
func (l *ZapLogger) Trace(msg string, fields watermill.LogFields) {
	var zapFields []zap.Field
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	l.l.Debug(msg, zapFields...)
}
func (l *ZapLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	var zapFields []zap.Field
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	return &ZapLogger{
		l: l.l.With(zapFields...),
	}
}
