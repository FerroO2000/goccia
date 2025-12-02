package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

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

	telemetry.Init(ctx, "kafka-example")

	kafkaToRaw := connector.NewRingBuffer[*ingress.KafkaMessage](connectorSize)
	customToKafka := connector.NewRingBuffer[*egress.KafkaMessage](connectorSize)

	kafkaIngressCfg := ingress.DefaultKafkaConfig("example-topic")
	kafkaIngressStage := ingress.NewKafkaStage(kafkaToRaw, kafkaIngressCfg)

	customCfg := processor.DefaultCustomConfig(goccia.StageRunningModePool)
	customCfg.Name = "ingress_to_egress"
	customStage := processor.NewCustomStage(newIngressToEgressHandler(), kafkaToRaw, customToKafka, customCfg)

	kafkaEgressCfg := egress.DefaultKafkaConfig(goccia.StageRunningModePool)
	kafkaEgressStage := egress.NewKafkaStage(customToKafka, kafkaEgressCfg)

	pipeline := goccia.NewPipeline()

	pipeline.AddStage(kafkaIngressStage)
	pipeline.AddStage(customStage)
	pipeline.AddStage(kafkaEgressStage)

	if err := pipeline.Init(ctx); err != nil {
		panic(err)
	}

	go pipeline.Run(ctx)
	defer pipeline.Close()

	<-ctx.Done()
}
