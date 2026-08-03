---
icon: lucide/layers
---

# Aggregate Processor

`AggregateStage` collects messages into batches and emits one aggregate message
for each batch. A batch is emitted when it reaches the configured size or when
no new input arrives before the timeout.

``` go
cfg := processor.NewAggregateConfig()
cfg.BatchSize = 64
cfg.Timeout = time.Second

stage := processor.NewAggregateStage(in, out, cfg)
```

## Messages

### Input Message

Accepted body type: `T`.

Additional input interfaces: none required beyond `message.Body`.

### Output Message

Produced body type: `*processor.AggregateMessage[T]`.

`AggregateMessage.Batch` contains the original message envelopes in input
order:

``` go
for _, msg := range aggregate.Batch {
	body := msg.GetBody()
	// Process body.
}
```

The aggregate owns the messages in `Batch`. Destroying the aggregate output
also destroys every contained message, so consumers must not destroy individual
batch entries separately.

## Batching Behavior

The stage emits a batch when either condition is met:

- `BatchSize` messages have been collected.
- A read waits for `Timeout` without receiving another message.

The timeout is restarted after each input message. It therefore measures
inactivity since the most recently received message, rather than the total age
of the batch. A steady stream can keep a partial batch open until it reaches
`BatchSize`.

A non-empty partial batch is also emitted when the stage context is canceled or
the input connector closes. Empty batches are never emitted.

## Metadata And Tracing

The output envelope receives:

- The receive time of the first message in the batch.
- The timestamp of the last message in the batch.
- A new root trace span named `aggregate messages`.

The aggregate span links every valid input span instead of selecting one input
as its parent. Its `batch_size` attribute records the number of contained
messages, and its span context is propagated through the output envelope for
downstream stages.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `BatchSize` | `64` | Number of input messages that completes a batch. Values less than one fall back to the default. |
| `Timeout` | `1 * time.Second` | Maximum inactivity between messages before emitting a non-empty partial batch. Values less than or equal to zero fall back to the default. |

The stage is intentionally single-threaded and does not accept a running mode.

## Metrics

--8<-- "processor/metrics/aggregate_stage.doc.md"

The common processor processed-message counter is incremented by the number of
input messages placed into emitted batches.

## Internals

`AggregateStage` uses a custom single runner. It accumulates input message
envelopes in a preallocated slice, clones the slice when creating an output,
and reuses the accumulator for the next batch. This transfers ownership of the
messages to `AggregateMessage` without copying their bodies.

If writing an aggregate to the output connector fails, the runner destroys it
and all messages it owns. The output connector is closed when the stage closes.
