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

	telemetry.Init(ctx, "kafka-producer-example")

	tickerToCustom := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)
	customToKafka := connector.NewRingBuffer[*egress.KafkaMessage](connectorSize)

	tickerCfg := ingress.NewTickerConfig()
	tickerCfg.Interval = time.Millisecond
	tickerStage := ingress.NewTickerStage(tickerToCustom, tickerCfg)

	customCfg := processor.NewCustomConfig(goccia.StageRunningModePool)
	customCfg.Name = "ticker_to_kafka"
	customStage := processor.NewCustomStage(newTickerToKafkaHandler(), tickerToCustom, customToKafka, customCfg)

	kafkaCfg := egress.DefaultKafkaConfig(goccia.StageRunningModePool)
	kafkaStage := egress.NewKafkaStage(customToKafka, kafkaCfg)

	pipeline := goccia.NewPipeline()

	pipeline.AddStage(tickerStage)
	pipeline.AddStage(customStage)
	pipeline.AddStage(kafkaStage)

	if err := pipeline.Init(ctx); err != nil {
		panic(err)
	}

	go pipeline.Run(ctx)
	defer pipeline.Close()

	<-ctx.Done()
}
