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

	telemetry.Init(ctx, "file-example")

	fileIngressToCustom := connector.NewRingBuffer[*ingress.FileMessage](connectorSize)
	customToFileEgress := connector.NewRingBuffer[*ingress.FileMessage](connectorSize)

	fileIngressCfg := ingress.NewFileConfig()
	fileIngressCfg.WatchedDirs = []string{"./data/in"}
	fileIngressStage := ingress.NewFileStage(fileIngressToCustom, fileIngressCfg)

	customCfg := processor.NewCustomConfig(goccia.StageRunningModeSingle)
	customCfg.Name = "file_to_file"
	customStage := processor.NewCustomStage(newFileHandler(), fileIngressToCustom, customToFileEgress, customCfg)

	fileEgressCfg := egress.NewFileConfig("./data/out/out.txt")
	fileEgressStage := egress.NewFileStage(customToFileEgress, fileEgressCfg)

	pipeline := goccia.NewPipeline()

	pipeline.AddStage(fileIngressStage)
	pipeline.AddStage(customStage)
	pipeline.AddStage(fileEgressStage)

	if err := pipeline.Init(ctx); err != nil {
		panic(err)
	}

	go pipeline.Run(ctx)
	defer pipeline.Close()

	<-ctx.Done()
}
