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
	"github.com/FerroO2000/goccia/link"
	"github.com/FerroO2000/goccia/processor"
)

const connectorSize = 512

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelCtx()

	telemetry.Init(ctx, "http-echo-example")
	defer telemetry.Close()

	httpIngressToEcho := connector.NewRingBuffer[*ingress.HTTPMessage](connectorSize)
	echoToHTTPEgress := connector.NewRingBuffer[*egress.HTTPMessage](connectorSize)

	httpLink := link.NewHTTP(link.NewHTTPConfig())

	httpIngressCfg := ingress.NewHTTPConfig()
	httpIngressStage := ingress.NewHTTPStage(httpLink, httpIngressToEcho, httpIngressCfg)

	echoCfg := processor.NewGenericConfig(goccia.StageRunningModeSingle)
	echoCfg.Name = "http_echo"
	echoStage := processor.NewGenericStage(newEchoHandler(), httpIngressToEcho, echoToHTTPEgress, echoCfg)

	httpEgressStage := egress.NewHTTPStage(httpLink, echoToHTTPEgress)

	pipeline := goccia.NewPipeline()
	pipeline.AddStage(httpIngressStage)
	pipeline.AddStage(echoStage)
	pipeline.AddStage(httpEgressStage)

	if err := pipeline.Init(ctx); err != nil {
		panic(err)
	}

	go pipeline.Run(ctx)

	<-ctx.Done()

	closeCtx, cancelCloseCtx := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCloseCtx()

	pipeline.Close(closeCtx)
}
