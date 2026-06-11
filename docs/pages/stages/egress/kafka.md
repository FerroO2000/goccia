---
icon: lucide/message-square
---

# Kafka Egress

`KafkaStage` writes `KafkaMessage` values to Kafka.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := egress.DefaultKafkaConfig(goccia.StageRunningModePool)
stage := egress.NewKafkaStage(in, cfg)
```

## Messages

### Input Message

Accepted body type: `*egress.KafkaMessage`.

| Field or method | Description |
| --- | --- |
| `Topic` | Kafka topic to write. |
| `Key` | Kafka record key. |
| `Value` | Kafka record value. |
| `AddHeader(key, value)` | Adds a Kafka header before delivery. |

Additional input interfaces: none. `KafkaMessage` only implements the standard
`message.Body` contract.

This egress stage produces no downstream output message.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `Brokers` | `[]string{"localhost:9092"}` | Kafka broker addresses. |
| `Balancer` | `kafka.RoundRobin` | Partition balancer. |
| `MaxAttempts` | `10` | Delivery attempt limit. |
| `WriteBackoffMin` | `100 * time.Millisecond` | Minimum write retry backoff. |
| `WriteBackoffMax` | `1 * time.Second` | Maximum write retry backoff. |
| `BatchSize` | `100` | Message count that triggers a batch flush. |
| `BatchBytes` | `1048576` | Byte size that triggers a batch flush. |
| `BatchTimeout` | `1 * time.Second` | Time limit for incomplete batches. |
| `ReadTimeout` | `10 * time.Second` | Writer read timeout. |
| `WriteTimeout` | `10 * time.Second` | Writer write timeout. |
| `RequiredAcks` | `kafka.RequireNone` | Produce acknowledgement requirement. |
| `Async` | `true` | Non-blocking writes. Errors are ignored by kafka-go in async mode. |
| `Compression` | `kafka.Snappy` | Kafka compression codec. |
| `Transport` | `nil` | Optional kafka-go transport. |
| `AllowAutoTopicCreation` | `true` | Allow missing topics to be created. |

## Internals

The stage uses `github.com/segmentio/kafka-go`. It creates a `kafka.Writer`,
injects OpenTelemetry trace context into Kafka headers, then calls
`WriteMessages(ctx, kafka.Message{...})`.
