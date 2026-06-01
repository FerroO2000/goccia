---
icon: lucide/rocket
---

# Get Started

## Install

Add Goccia to your Go module:

``` bash
go get github.com/FerroO2000/goccia
```

## Build a Pipeline

A pipeline is a directed graph of stages joined by typed connectors:

``` go
const connectorSize = 512

tickerToCustom := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)
customToSink := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)

tickerCfg := ingress.NewTickerConfig()
tickerStage := ingress.NewTickerStage(tickerToCustom, tickerCfg)

customCfg := processor.NewCustomConfig(goccia.StageRunningModeSingle)
customStage := processor.NewCustomStage(
    handler,
    tickerToCustom,
    customToSink,
    customCfg,
)

sinkStage := egress.NewSinkStage(customToSink)

pipeline := goccia.NewPipeline()
pipeline.AddStage(tickerStage)
pipeline.AddStage(customStage)
pipeline.AddStage(sinkStage)
```

Each output connector is the input connector of the following stage. During
initialization, Goccia uses those connectors to build the stage graph.

## Run and Close

Initialize the pipeline, run it in a goroutine, and close it after the run
context is canceled:

``` go
if err := pipeline.Init(ctx); err != nil {
    panic(err)
}

go pipeline.Run(ctx)

<-ctx.Done()
pipeline.Close(closeCtx)
```

`Pipeline.Close` waits for stages to drain in graph order before releasing
their resources.

## Choose a Running Mode

Processor and egress configurations accept a running mode:

``` go
cfg := processor.NewCustomConfig(goccia.StageRunningModeSingle)
```

Use `StageRunningModeSingle` when ordering or simplicity matters most. Use
`StageRunningModePool` for parallel processing with bounded internal fan-out
and fan-in queues.
