package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/global"
)

type logger struct {
	l *slog.Logger
}

func newLogger(attributes []attribute.KeyValue) *logger {
	lp := global.GetLoggerProvider()

	handler := otelslog.NewHandler(
		libName,
		otelslog.WithLoggerProvider(lp),
		otelslog.WithVersion(libVersion),
		otelslog.WithAttributes(attributes...),
	)

	l := slog.New(handler)

	return &logger{
		l: l,
	}
}

// LogDebug logs a debug message.
func (l *logger) LogDebug(ctx context.Context, msg string, args ...any) {
	l.l.DebugContext(ctx, msg, args...)
}

// LogInfo logs an info message.
func (l *logger) LogInfo(ctx context.Context, msg string, args ...any) {
	l.l.InfoContext(ctx, msg, args...)
}

// LogWarn logs a warning message.
func (l *logger) LogWarn(ctx context.Context, msg string, args ...any) {
	l.l.WarnContext(ctx, msg, args...)
}

// LogError logs an error message.
func (l *logger) LogError(ctx context.Context, msg string, err error, args ...any) {
	tmpArgs := make([]any, 0, len(args)+1)
	tmpArgs = append(tmpArgs, slog.Any("err", err))
	tmpArgs = append(tmpArgs, args...)

	l.l.ErrorContext(ctx, msg, tmpArgs...)
}
