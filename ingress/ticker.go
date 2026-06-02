package ingress

import (
	"context"
	"time"

	"github.com/FerroO2000/goccia/ingress/metrics"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default values for the Ticker stage configuration.
const (
	DefaultTickerConfigInterval = 100 * time.Millisecond
)

// TickerConfig structs contains the configuration for the Ticker stage.
type TickerConfig struct {
	// Interval is the duration between ticks.
	Interval time.Duration
}

// NewTickerConfig returns the default configuration for the Ticker stage.
func NewTickerConfig() *TickerConfig {
	return &TickerConfig{
		Interval: 100 * time.Millisecond,
	}
}

// Validate checks the configuration.
func (c *TickerConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotNegative(ac, "Interval", &c.Interval, DefaultTickerConfigInterval)
	config.CheckNotZero(ac, "Interval", &c.Interval, DefaultTickerConfigInterval)
}

// ─── Message ────────────────────────────────────────────────────────────────|

var _ msgBody = (*TickerMessage)(nil)

// TickerMessage is the message returned by the Ticker stage.
type TickerMessage struct {
	TickNumber int
}

// NewTickerMessage returns a new Ticker message.
func NewTickerMessage() *TickerMessage {
	return &TickerMessage{}
}

// Destroy cleans up the message.
func (tm *TickerMessage) Destroy() {}

// ─── Environment ────────────────────────────────────────────────────────────|

type tickerEnv struct {
	*env.BaseEnv[*TickerConfig, *metrics.TickerStage]
}

func newTickerEnv(config *TickerConfig) *tickerEnv {
	return &tickerEnv{
		BaseEnv: env.NewIngressEnv(config, metrics.NewTickerStage()),
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*tickerEnv] = (*tickerRunner)(nil)

type tickerRunner struct {
	*runnerBase[*tickerEnv, *TickerMessage]
}

func newTickerRunner(outConnector msgConn[*TickerMessage]) *tickerRunner {
	return &tickerRunner{
		runnerBase: newRunnerBase[*tickerEnv](outConnector),
	}
}

func (tr *tickerRunner) Run(ctx context.Context) {
	defer tr.notifyRunDone()

	ticker := time.NewTicker(tr.env.Config.Interval)
	defer ticker.Stop()

	ticks := 0
	for {
		ticks++

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msgOut := tr.handleTrigger(ctx, ticks)
			if err := tr.outConnector.Write(msgOut); err != nil {
				msgOut.Destroy()
				tr.env.Tel.LogError("failed to write message to output connector", err)
			}
		}
	}
}

func (tr *tickerRunner) handleTrigger(ctx context.Context, tick int) *msg[*TickerMessage] {
	_, span := tr.env.Tel.StartTrace(ctx, "triggered ticker message")
	defer span.End()

	tickerMsg := NewTickerMessage()
	tickerMsg.TickNumber = tick

	msg := message.NewMessage(tickerMsg)
	triggerTime := time.Now()
	msg.SetReceiveTime(triggerTime)
	msg.SetTimestamp(triggerTime)

	tr.env.Metrics.IncrementTriggeredMessages()

	span.SetAttributes(attribute.Int("tick_number", tick))
	msg.SaveSpan(span)

	return msg
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// TickerStage is an ingress stage that ticks periodically.
type TickerStage struct {
	*stage.IngressStage[*TickerMessage, *tickerEnv]
}

// NewTickerStage returns a new Ticker stage.
func NewTickerStage(outConnector msgConn[*TickerMessage], cfg *TickerConfig) *TickerStage {
	return &TickerStage{
		IngressStage: stage.NewIngressStageFromRunner[*TickerMessage](
			"ticker", newTickerEnv(cfg), newTickerRunner(outConnector),
		),
	}
}
