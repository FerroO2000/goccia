---
icon: lucide/git-merge
---

# Merge Processor

`MergeStage` combines multiple input connectors into one output connector.

``` go
cfg := processor.NewMergeConfig()
stage := processor.NewMergeStage(
	[]connector.MessageConnector[*MyMessage]{inA, inB, inC},
	out,
	cfg,
)
```

## Messages

### Input Message

Accepted body type: `T`.

Additional input interfaces: none required beyond `message.Body`. Every input
connector must carry the same body type.

### Output Message

Produced body type: the same `T` body received from any input connector.

Additional interfaces: preserved from `T`. The stage does not clone, copy, or
transform the message. It forwards each incoming message envelope to the output
connector.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `OutputQueueSize` | `256` | Size of the internal fan-in ring buffer between input readers and the output bridge. |

At least one input connector is required.

## Internals

`MergeStage` uses a custom single runner instead of the generic worker pool.
The runner starts one reader goroutine per input connector. Each reader moves
messages into an internal MPSC fan-in buffer, and one output bridge drains that
buffer into the output connector.

The stage keeps reading until every input connector is closed and drained. It
then closes the internal buffer, waits for the output bridge to finish, and
closes the output connector during stage close.
