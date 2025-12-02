package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FerroO2000/goccia"
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/examples/telemetry"
	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
)

const connectorSize = 2048

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancelCtx()

	telemetry.Init(ctx, "can-client-example")

	tickerToCustom := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)
	customToCannelloni := connector.NewRingBuffer[*processor.CannelloniMessage](connectorSize)
	cannelloniToUDP := connector.NewRingBuffer[*processor.CannelloniEncodedMessage](connectorSize)

	tickerCfg := ingress.DefaultTickerConfig()
	tickerCfg.Interval = time.Millisecond * 10
	tickerStage := ingress.NewTickerStage(tickerToCustom, tickerCfg)

	customCfg := processor.DefaultCustomConfig(goccia.StageRunningModeSingle)
	customCfg.Name = "ticker_to_cannelloni"
	customStage := processor.NewCustomStage(newTickerToCannelloniHandler(), tickerToCustom, customToCannelloni, customCfg)

	cannelloniCfg := processor.DefaultCannelloniConfig(goccia.StageRunningModeSingle)
	cannelloniStage := processor.NewCannelloniEncoderStage(customToCannelloni, cannelloniToUDP, cannelloniCfg)

	udpCfg := egress.DefaultUDPConfig(goccia.StageRunningModeSingle)
	udpStage := egress.NewUDPStage(cannelloniToUDP, udpCfg)

	pipeline := goccia.NewPipeline()

	pipeline.AddStage(tickerStage)
	pipeline.AddStage(customStage)
	pipeline.AddStage(cannelloniStage)
	pipeline.AddStage(udpStage)

	if err := pipeline.Init(ctx); err != nil {
		panic(err)
	}

	go pipeline.Run(ctx)
	defer pipeline.Close()

	<-ctx.Done()
}
