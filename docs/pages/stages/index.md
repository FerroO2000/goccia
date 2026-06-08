---
icon: lucide/library
---

# Built-in Stages

Goccia ships stages in three categories:

- [Ingress](ingress/index.md): create messages from external sources.
- [Processor](processor/index.md): transform, filter, fan out, or reorder messages.
- [Egress](egress/index.md): deliver messages to external destinations.

## Message Interfaces

Every message body implements Goccia's standard `message.Body` contract by
providing `Destroy()`. Some built-in message types implement additional
interfaces:

| Interface | Extra method | Used by |
| --- | --- | --- |
| `message.Serializable` | `GetBytes() []byte` | Byte-oriented processors and TCP, UDP, or file egress stages |
| `message.ReOrderable` | `GetSequenceNumber() uint64` | `ROBStage` |
| `processor.CANMessageCarrier` | `GetRawMessages() []CANRawMessage` | `CANStage` |

## Execution Badges

[Pool-capable](../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

This badge marks stages that can use `StageRunningModePool`. It links to the
[pooled execution mode](../concepts/stages.md#pooled-execution-mode) concept
section for the queueing, scaling, and shutdown model.

Most processor and egress stages use a common running-mode configuration:

``` go
cfg := processor.NewGenericConfig(goccia.StageRunningModePool)
```

`StageRunningModeSingle` runs one executor. `StageRunningModePool` enables the
generic worker pool and exposes `cfg.Stage.Pool` for worker counts, queue sizes,
and auto-scaling. Stages with custom runners, such as ingress stages, `tee`,
`rob`, `sink`, file egress, and TCP egress, document their own execution model.

For lifecycle details, see [Stages](../concepts/stages.md).
