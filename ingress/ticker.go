package ingress

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

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

///////////////
//  MESSAGE  //
///////////////

var _ msgEnv = (*TickerMessage)(nil)

// TickerMessage is the message returned by the Ticker stage.
type TickerMessage struct {
	TickNumber int
}

func newTickerMessage() *TickerMessage {
	return &TickerMessage{}
}

// Destroy cleans up the message.
func (tm *TickerMessage) Destroy() {}

//////////////
//  SOURCE  //
//////////////

var _ source[*TickerMessage] = (*tickerSource)(nil)

type tickerSource struct {
	tel *internal.Telemetry

	ticker *time.Ticker

	// Metrics
	triggeredMessages atomic.Int64
}

func newTickerSource() *tickerSource {
	return &tickerSource{}
}

func (ts *tickerSource) setTelemetry(tel *internal.Telemetry) {
	ts.tel = tel
}

func (ts *tickerSource) init(interval time.Duration) {
	ts.ticker = time.NewTicker(interval)
}

func (ts *tickerSource) run(ctx context.Context, outConnector msgConn[*TickerMessage]) {
	defer ts.ticker.Stop()

	ticks := 0

	for {
		ticks++

		select {
		case <-ctx.Done():
			return
		case <-ts.ticker.C:
			msgOut := ts.handleTrigger(ctx, ticks)
			if err := outConnector.Write(msgOut); err != nil {
				msgOut.Destroy()
				ts.tel.LogError("failed to write message to output connector", err)
			}
		}
	}
}

func (ts *tickerSource) handleTrigger(ctx context.Context, tick int) *msg[*TickerMessage] {
	_, span := ts.tel.NewTrace(ctx, "triggered ticker message")
	defer span.End()

	tickerMsg := newTickerMessage()
	tickerMsg.TickNumber = tick

	msg := message.NewMessage(tickerMsg)
	triggerTime := time.Now()
	msg.SetReceiveTime(triggerTime)
	msg.SetTimestamp(triggerTime)

	span.SetAttributes(attribute.Int("tick_number", tick))
	msg.SaveSpan(span)

	return msg
}

/////////////
//  STAGE  //
/////////////

// TickerStage is an ingress stage that ticks periodically.
type TickerStage struct {
	*stage[*TickerMessage, *TickerConfig]

	source *tickerSource
}

// NewTickerStage returns a new Ticker stage.
func NewTickerStage(outConnector msgConn[*TickerMessage], cfg *TickerConfig) *TickerStage {
	source := newTickerSource()

	return &TickerStage{
		stage: newStage("ticker", source, outConnector, cfg),

		source: source,
	}
}

// Init initializes the stage.
func (s *TickerStage) Init(ctx context.Context) error {
	s.source.init(s.cfg.Interval)

	return s.stage.Init(ctx)
}
