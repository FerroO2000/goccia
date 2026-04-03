package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"golang.org/x/term"
)

const (
	ansiReset = "\033[0m"
	ansiBold  = "\033[1m"

	ansiGray    = "\033[90m"
	ansiCyan    = "\033[36m"
	ansiGreen   = "\033[32m"
	ansiYellow  = "\033[33m"
	ansiRed     = "\033[31m"
	ansiBoldRed = "\033[1;31m"
)

type consoleExporter struct {
	sb              *strings.Builder
	timestampFormat string
	colorEnabled    bool
}

func newConsoleExporter() *consoleExporter {
	sb := &strings.Builder{}
	sb.Grow(256)

	return &consoleExporter{
		sb:              sb,
		timestampFormat: time.RFC3339Nano,
		colorEnabled:    term.IsTerminal(int(os.Stdout.Fd())),
	}
}

func (ce *consoleExporter) print(r *sdklog.Record) {
	ce.printTimestamp(r.Timestamp())
	ce.printSpace()
	ce.printSeverity(r.Severity())
	ce.printSpace()
	ce.printBody(r.Body().String())

	r.WalkAttributes(func(kv log.KeyValue) bool {
		ce.printSpace()
		ce.printAttribute(&kv)
		return true
	})

	ce.sb.WriteRune('\n')
	fmt.Print(ce.sb.String())
	ce.sb.Reset()
}

func (ce *consoleExporter) printSpace() {
	ce.sb.WriteRune(' ')
}

func (ce *consoleExporter) printTimestamp(t time.Time) {
	tStr := t.Format(ce.timestampFormat)

	if !ce.colorEnabled {
		ce.sb.WriteString(tStr)
		return
	}

	ce.sb.WriteString(ansiGray)
	ce.sb.WriteString(tStr)
	ce.sb.WriteString(ansiReset)
}

func (ce *consoleExporter) printSeverity(s log.Severity) {
	if !ce.colorEnabled {
		ce.sb.WriteString(s.String())
		return
	}

	color := ""
	switch {
	case s <= log.SeverityDebug4:
		color = ansiCyan
	case s <= log.SeverityInfo4:
		color = ansiGreen
	case s <= log.SeverityWarn4:
		color = ansiYellow
	case s <= log.SeverityError4:
		color = ansiRed
	default:
		color = ansiBoldRed
	}

	ce.sb.WriteString(color)
	ce.sb.WriteString(s.String())
	ce.sb.WriteString(ansiReset)
}

func (ce *consoleExporter) printBody(body string) {
	if !ce.colorEnabled {
		ce.sb.WriteString(body)
		return
	}

	ce.sb.WriteString(ansiBold)
	ce.sb.WriteString(body)
	ce.sb.WriteString(ansiReset)
}

func (ce *consoleExporter) printAttribute(kv *log.KeyValue) {
	keyStr := kv.Key
	valueStr := kv.Value.String()

	if !ce.colorEnabled {
		ce.sb.WriteString(keyStr)
		ce.sb.WriteRune('=')
		ce.sb.WriteString(valueStr)
		return
	}

	ce.sb.WriteString(ansiGray)
	ce.sb.WriteString(keyStr)
	ce.sb.WriteRune('=')
	ce.sb.WriteString(ansiReset)
	ce.sb.WriteString(valueStr)
}

func (ce *consoleExporter) Export(_ context.Context, records []sdklog.Record) error {
	for _, r := range records {
		ce.print(&r)
	}
	return nil
}

func (ce *consoleExporter) Shutdown(_ context.Context) error { return nil }

func (ce *consoleExporter) ForceFlush(_ context.Context) error { return nil }

type severityProcessor struct {
	sdklog.Processor

	minLevel log.Severity
}

func newSeverityProcessor(minLevel log.Severity, exporter sdklog.Processor) *severityProcessor {
	return &severityProcessor{
		Processor: exporter,

		minLevel: minLevel,
	}
}

func (sp *severityProcessor) OnEmit(ctx context.Context, record *sdklog.Record) error {
	if record.Severity() < sp.minLevel {
		return nil
	}
	return sp.Processor.OnEmit(ctx, record)
}

func (sp *severityProcessor) Enabled(ctx context.Context, param sdklog.EnabledParameters) bool {
	if param.Severity < sp.minLevel {
		return false
	}
	return sp.Processor.Enabled(ctx, param)
}
