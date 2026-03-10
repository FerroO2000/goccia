package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia"
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/egress"
	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/processor"
)

const (
	benchDuration        = 10 * time.Second
	bufferSize           = 1024
	processingDelay      = 1 * time.Millisecond
	processorCount       = 5
	processorParallelism = 8

	totalMessages = 1_000_000
)

type conn = connector.MessageConnector[*ingress.TickerMessage]
type msg = ingress.TickerMessage

var processorIdx = 0

func newProcessorStage(inConn conn) (*processor.CustomStage[*msg, msg, *msg], conn) {
	outConn := connector.NewRingBuffer[*msg](bufferSize)

	processorHandler := newProcessorStageHandler()

	var processorCfg *processor.CustomConfig
	if processorParallelism == 1 {
		processorCfg = processor.NewCustomConfig(goccia.StageRunningModeSingle)
	} else {
		processorCfg = processor.NewCustomConfig(goccia.StageRunningModePool)
		processorCfg.Stage.Pool.MaxWorkers = processorParallelism
		processorCfg.Stage.Pool.InitialWorkers = processorParallelism
		processorCfg.Stage.Pool.AutoScaleEnabled = false
	}

	processorCfg.Name = fmt.Sprintf("processor_%d", processorIdx)
	processorIdx++

	return processor.NewCustomStage(processorHandler, inConn, outConn, processorCfg), outConn
}

func main() {
	tikerConn := connector.NewRingBuffer[*ingress.TickerMessage](bufferSize)
	sinkConn := connector.NewRingBuffer[*ingress.TickerMessage](bufferSize)

	sinkStage := egress.NewSinkStage(sinkConn)

	pipeline := goccia.NewPipeline()

	var prevConn conn
	prevConn = tikerConn
	for range processorCount {
		processorStage, processorConn := newProcessorStage(prevConn)
		pipeline.AddStage(processorStage)
		prevConn = processorConn
	}

	counterStageHandler := newCounterStageHandler()
	counterStageCfg := processor.NewCustomConfig(goccia.StageRunningModeSingle)
	counterStage := processor.NewCustomStage(counterStageHandler, prevConn, sinkConn, counterStageCfg)

	pipeline.AddStage(counterStage)
	pipeline.AddStage(sinkStage)

	if err := pipeline.Init(context.Background()); err != nil {
		panic(err)
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	go pipeline.Run(ctx)
	time.Sleep(time.Second)

	start := time.Now()
	// deadline := time.Now().Add(benchDuration)
	// for time.Now().Before(deadline) {
	// 	tikerConn.Write(message.NewMessage(&ingress.TickerMessage{}))
	// }

	for range totalMessages {
		tikerConn.Write(message.NewMessage(&ingress.TickerMessage{}))
	}

	elapsed := time.Since(start)
	cancelCtx()
	// defer pipeline.Close()

	secs := elapsed.Seconds()
	msgs := totalMessages
	throughput := float64(msgs) / secs

	log.Printf("throughput: %.2f msgs/s (total: %d, elapsed: %s)", throughput, msgs, elapsed)
	time.Sleep(time.Second * 3)
}

type processorStageHandler struct {
	processor.CustomHandlerBase
}

func newProcessorStageHandler() *processorStageHandler {
	return &processorStageHandler{}
}

func (h *processorStageHandler) Handle(_ context.Context, in, out *msg) error {
	// out.TickNumber = in.TickNumber
	// for range 100 {
	// 	out.TickNumber++
	// }
	// time.Sleep(processingDelay)

	// time.Sleep(processingDelay)

	return nil
}

type counterStageHandler struct {
	processor.CustomHandlerBase

	counter atomic.Int64
}

func newCounterStageHandler() *counterStageHandler {
	return &counterStageHandler{}
}

func (h *counterStageHandler) Handle(_ context.Context, _, _ *msg) error {
	h.counter.Add(1)
	return nil
}
