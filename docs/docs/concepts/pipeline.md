---
icon: lucide/git-branch
---

# Pipeline concepts

## Stage graph

Goccia models a pipeline as a directed acyclic graph:

``` mermaid
flowchart LR
  A[Ingress] --> B[Processor]
  B --> C[Processor]
  C --> D[Egress]
```

A connector identifies each edge. The graph determines initialization order
and shutdown propagation.

## Backpressure

Connectors are bounded queues. When a queue is full, its producer waits until a
consumer reads a message. This bounds memory usage and propagates pressure
upstream instead of silently dropping work.

## Graceful shutdown

Shutdown follows message flow:

``` text
stop producers
-> drain queued and in-flight messages
-> stop consumers
-> release resources
```

For a pooled processor, Goccia introduces internal fan-out and fan-in queues:

``` mermaid
flowchart LR
  input[External input] --> fanout[Fan-out queue]
  fanout --> workers[Worker pool]
  workers --> fanin[Fan-in queue]
  fanin --> output[External output]
```

The fan-out bridge closes the internal input queue after its external input
stops. Workers consume the remaining messages and then exit. Only after all
workers finish does the runner close the fan-in queue, allowing the output
bridge to drain and stop.

This ordering keeps downstream consumers alive while producers can still be
blocked by backpressure.
