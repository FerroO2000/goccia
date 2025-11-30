package internal

import (
	"log/slog"
	"os"
	"runtime"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
)

var defaultLogger = slog.Default()

type Logger struct {
	*slog.Logger

	kind string
	name string
}

func NewLogger(stageKind, stageName string) *Logger {
	var handler slog.Handler

	if runtime.GOOS == "windows" {
		w := colorable.NewColorableStdout()
		handler = tint.NewHandler(w, nil)
	} else {
		w := os.Stderr
		handler = tint.NewHandler(w, &tint.Options{
			NoColor: !isatty.IsTerminal(w.Fd()),
		})
	}

	return &Logger{
		Logger: slog.New(handler),

		kind: stageKind,
		name: stageName,
	}
}

func (l *Logger) getInfo() slog.Attr {
	return slog.Group("info", slog.String("kind", l.kind), slog.String("name", l.name))
}

func (l *Logger) getArgs(args ...any) []any {
	return append([]any{l.getInfo()}, args...)
}

func (l *Logger) Info(msg string, args ...any) {
	l.Logger.Info(msg, l.getArgs(args...)...)
}

func (l *Logger) Error(msg string, err error, args ...any) {
	tmpArgs := append([]any{tint.Err(err)}, args...)
	l.Logger.Error(msg, l.getArgs(tmpArgs...)...)
}

func (l *Logger) Warn(msg string, args ...any) {
	l.Logger.Warn(msg, l.getArgs(args...)...)
}
