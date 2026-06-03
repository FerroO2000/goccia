---
icon: lucide/list-ordered
---

# ROB Processor

`ROBStage` reorders messages by sequence number and flushes buffered messages
when gaps cannot be filled.

``` go
cfg := processor.NewROBConfig()
stage := processor.NewROBStage(in, out, cfg)
```

## Messages

### Input Message

Accepted body requirement: `message.ReOrderable`.

The stage reads the sequence number from `GetSequenceNumber()` and copies it
into the message envelope before enqueuing into the reorder buffer.

### Output Message

Produced body type: the same `T` body received from the input connector, emitted
in reordered sequence.

Additional interfaces: at least `message.ReOrderable`, because that is required
for input. Any other interfaces implemented by `T`, such as
`processor.CANMessageCarrier`, are preserved.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `MaxSeqNum` | `255` | Maximum possible sequence number before wraparound. |
| `PrimaryBufferSize` | `128` | In-order window buffer size. |
| `AuxiliaryBufferSize` | `128` | Auxiliary buffer for messages outside the primary window. |
| `FlushTreshold` | `0.3` | Auxiliary-buffer fullness threshold that triggers a primary flush. |
| `TimeSmootherEnabled` | `true` | Enables the internal time smoother. |
| `EstimatorAlpha` | `0.8` | Data smoothing factor for the double exponential estimator. |
| `EstimatorBeta` | `0.5` | Trend smoothing factor for the double exponential estimator. |
| `ResetTimeout` | `100 * time.Millisecond` | Read timeout after which pending reorder state may be flushed and reset. |

The stage is intentionally single-threaded and does not accept a running mode.

## Internals

`ROBStage` uses a custom runner and the internal `rob.ROB` implementation. It
reads with a timeout, updates the envelope sequence number from the message
body, enqueues into primary or auxiliary buffers, emits ordered messages through
the output connector, and flushes on shutdown.
