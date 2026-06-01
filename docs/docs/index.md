---
icon: lucide/droplets
---

# Goccia

Build composable streaming pipelines in Go.

Goccia connects ingress, processor, and egress stages with typed ring buffers.
Each stage can run with one worker or a dynamically scaled worker pool, while
the pipeline coordinates startup, backpressure, graceful shutdown, and
telemetry.

``` mermaid
flowchart LR
  ingress[Ingress] --> processor[Processor]
  processor --> egress[Egress]
```

## Why Goccia?

- Compose typed pipelines from small reusable stages.
- Choose single-worker or pooled execution per stage.
- Preserve backpressure with bounded connectors.
- Drain queued work during graceful shutdown.
- Export metrics and traces through OpenTelemetry.

## Start here

Read [Get started](getting-started.md) to install the module and understand the
shape of a pipeline. Continue with [Pipeline concepts](concepts/pipeline.md) for
the lifecycle model, or browse the [examples](examples.md) for complete
applications.

The Go API is also available on
[`pkg.go.dev`](https://pkg.go.dev/github.com/FerroO2000/goccia).
