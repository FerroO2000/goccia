package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/FerroO2000/goccia"
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
)

const connectorSize = 2048

func main() {
	ctx, cancelCtx := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancelCtx()

	fileIngressToCsv := connector.NewRingBuffer[*ingress.FileMessage](connectorSize)
	decoderToEncoder := connector.NewRingBuffer[*processor.CSVMessage](connectorSize)
	encoderToFileEgress := connector.NewRingBuffer[*processor.CSVEncodedMessage](connectorSize)

	fileIngressCfg := ingress.DefaultFileConfig()
	fileIngressCfg.WatchedDirs = []string{"./data/in"}
	fileIngressStage := ingress.NewFileStage(fileIngressToCsv, fileIngressCfg)

	csvDecoderCfg := processor.DefaultCSVConfig(goccia.StageRunningModeSingle)
	csvDecoderCfg.AddColumnDef(processor.NewCSVColumnDef("frame", processor.CSVColumnTypeInt))
	csvDecoderCfg.AddColumnDef(processor.NewCSVColumnDef("time", processor.CSVColumnTypeFloat))
	csvDecoderCfg.AddColumnDef(processor.NewCSVColumnDef("x", processor.CSVColumnTypeFloat))
	csvDecoderCfg.AddColumnDef(processor.NewCSVColumnDef("y", processor.CSVColumnTypeFloat))
	csvDecoderCfg.AddColumnDef(processor.NewCSVColumnDef("z", processor.CSVColumnTypeFloat))
	csvDecoderCfg.AddColumnDef(processor.NewCSVColumnDef("rot_x", processor.CSVColumnTypeFloat))
	csvDecoderCfg.AddColumnDef(processor.NewCSVColumnDef("rot_y", processor.CSVColumnTypeFloat))
	csvDecoderCfg.AddColumnDef(processor.NewCSVColumnDef("rot_z", processor.CSVColumnTypeFloat))

	csvDecoderStage := processor.NewCSVDecoderStage(fileIngressToCsv, decoderToEncoder, csvDecoderCfg)

	csvEncoderCfg := processor.DefaultCSVConfig(goccia.StageRunningModeSingle)
	csvEncoderCfg.Columns = csvDecoderCfg.Columns
	csvEncoderStage := processor.NewCSVEncoderStage(decoderToEncoder, encoderToFileEgress, csvEncoderCfg)

	fileEgressCfg := egress.DefaultFileConfig("./data/out/out.csv")
	fileEgressStage := egress.NewFileStage(encoderToFileEgress, fileEgressCfg)

	pipeline := goccia.NewPipeline()

	pipeline.AddStage(fileIngressStage)
	pipeline.AddStage(csvDecoderStage)
	pipeline.AddStage(csvEncoderStage)
	pipeline.AddStage(fileEgressStage)

	if err := pipeline.Init(ctx); err != nil {
		panic(err)
	}

	go pipeline.Run(ctx)
	defer pipeline.Close()

	<-ctx.Done()
}
