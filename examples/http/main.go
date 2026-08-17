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

type responseBody struct {
	EchoCount int `json:"echo_count"`
}

type jsonMessage = processor.JSONMessage[responseBody]

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelCtx()

	telemetry.Init(ctx, "http-echo-example")
	defer telemetry.Close()

	httpIngressToEcho := connector.NewRingBuffer[*ingress.HTTPMessage](connectorSize)
	echoToJSON := connector.NewRingBuffer[*jsonMessage](connectorSize)
	jsonToHTTPResponse := connector.NewRingBuffer[*processor.JSONEncodedMessage](connectorSize)
	httpResponseToEgress := connector.NewRingBuffer[*egress.HTTPMessage](connectorSize)

	httpLink := link.NewHTTP()

	httpIngressCfg := ingress.NewHTTPConfig()
	httpIngressStage := ingress.NewHTTPStage(httpLink, httpIngressToEcho, httpIngressCfg)

	echoCfg := processor.NewGenericConfig(goccia.StageRunningModeSingle)
	echoCfg.Name = "echo"
	echoStage := processor.NewGenericStage(newEchoHandler(), httpIngressToEcho, echoToJSON, echoCfg)

	jsonEncCfg := processor.NewJSONEncoderConfig(goccia.StageRunningModePool)
	jsonEncStage := processor.NewJSONEncoderStage(echoToJSON, jsonToHTTPResponse, jsonEncCfg)

	httpResponseCfg := processor.NewGenericConfig(goccia.StageRunningModeSingle)
	httpResponseCfg.Name = "http_response"
	httpResponseStage := processor.NewGenericStage(newHTTPResponseHandler(), jsonToHTTPResponse, httpResponseToEgress, httpResponseCfg)

	httpEgressCfg := egress.NewHTTPConfig()
	httpEgressStage := egress.NewHTTPStage(httpLink, httpResponseToEgress, httpEgressCfg)

	pipeline := goccia.NewPipeline()

	pipeline.AddStage(httpIngressStage)
	pipeline.AddStage(echoStage)
	pipeline.AddStage(jsonEncStage)
	pipeline.AddStage(httpResponseStage)
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
