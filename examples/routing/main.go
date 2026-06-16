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

	telemetry.Init(ctx, "routing-example")
	defer telemetry.Close()

	tickerToRouter := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)
	routerToEven := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)
	routerToOdd := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)
	evenToSink := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)
	oddToSink := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)

	tickerCfg := ingress.NewTickerConfig()
	tickerCfg.Interval = 500 * time.Millisecond
	tickerStage := ingress.NewTickerStage(tickerToRouter, tickerCfg)

	var evenRouteID int
	var oddRouteID int
	routerStage := processor.NewRouterStage(func(tickerMsg *ingress.TickerMessage) int {
		if tickerMsg.TickNumber%2 == 0 {
			return evenRouteID
		}

		return oddRouteID
	}, tickerToRouter)

	evenRouteID = routerStage.AddRoute("even", routerToEven)
	oddRouteID = routerStage.AddRoute("odd", routerToOdd)

	evenCfg := processor.NewGenericConfig(goccia.StageRunningModeSingle)
	evenCfg.Name = "print_even"
	evenStage := processor.NewGenericStage(newPrintTickHandler("even"), routerToEven, evenToSink, evenCfg)

	oddCfg := processor.NewGenericConfig(goccia.StageRunningModeSingle)
	oddCfg.Name = "print_odd"
	oddStage := processor.NewGenericStage(newPrintTickHandler("odd"), routerToOdd, oddToSink, oddCfg)

	evenSinkStage := egress.NewSinkStage(evenToSink)
	oddSinkStage := egress.NewSinkStage(oddToSink)

	pipeline := goccia.NewPipeline()

	pipeline.AddStage(tickerStage)
	pipeline.AddStage(routerStage)
	pipeline.AddStage(evenStage)
	pipeline.AddStage(oddStage)
	pipeline.AddStage(evenSinkStage)
	pipeline.AddStage(oddSinkStage)

	if err := pipeline.Init(ctx); err != nil {
		panic(err)
	}

	go pipeline.Run(ctx)

	<-ctx.Done()

	closeCtx, cancelCloseCtx := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCloseCtx()

	pipeline.Close(closeCtx)
}
