---
icon: lucide/filter
---

# Filter Processor

`FilterStage` keeps messages whose predicate returns `true` and drops messages
whose predicate returns `false`.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := processor.NewFilterConfig(goccia.StageRunningModePool)
stage := processor.NewFilterStage(keepFn, in, out, cfg)
```

## Messages

### Input Message

Accepted body type: `T`.

Additional input interfaces: none required beyond `message.Body`. If `T`
implements `message.Serializable`, `message.ReOrderable`, or another interface,
that interface is preserved for kept messages.

### Output Message

Produced body type: the same `T` body received from the input connector, when
the predicate returns `true`.

Additional interfaces: preserved from `T`. The stage does not create a new body
or add interfaces; a serializable input remains serializable, a reorderable
input remains reorderable, and a filtered-out message is marked dropped.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |

The predicate is configured through the `filterFn func(T) bool` constructor
argument, not through the config object.

## Internals

The stage uses the generic worker-backed processor runner. The worker preserves
kept messages by forwarding the original envelope. Filtered messages are marked
dropped and counted in filter metrics.
