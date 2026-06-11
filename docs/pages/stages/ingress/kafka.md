---
icon: lucide/message-square
---

# Kafka Ingress

`KafkaStage` consumes Kafka messages and emits `KafkaMessage` values.

``` go
cfg := ingress.DefaultKafkaConfig("events")
cfg.GroupID = "goccia-consumer"
stage := ingress.NewKafkaStage(out, cfg)
```

## Messages

### Output Message

Produced body type: `*ingress.KafkaMessage`.

| Field | Description |
| --- | --- |
| `Topic` | Kafka topic that produced the record. |
| `Key` | Kafka record key. |
| `Value` | Kafka record value. |
| `Headers` | Kafka headers, including any trace context extracted by the stage. |

Additional interfaces: `message.Serializable`. `GetBytes()` returns `Value`.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Brokers` | `[]string{"localhost:9092"}` | Kafka broker addresses. |
| `GroupID` | `"group"` | Consumer group id. |
| `Topics` | constructor args | Topics consumed by the group. No topic is added unless you pass one. |
| `Dialer` | `nil` | Optional `kafka.Dialer`. |
| `QueueCapacity` | `100` | Internal kafka-go reader queue capacity. |
| `MinBytes` | `1` | Minimum fetch size accepted by the consumer. |
| `MaxBytes` | `1 << 20` | Maximum fetch size accepted by the consumer. |
| `MaxWait` | `10 * time.Second` | Maximum wait for broker fetch data. |
| `ReadBatchTimeout` | `10 * time.Second` | Timeout while reading from a fetched batch. |
| `GroupBalancers` | range, round-robin | Consumer group balancing strategies. |
| `HeartbeatInterval` | `3 * time.Second` | Consumer group heartbeat interval. |
| `CommitInterval` | `0` | Offset commit interval. `0` commits synchronously. |
| `PartitionWatchInterval` | `5 * time.Second` | Partition-change polling interval. |
| `WatchPartitionChanges` | `false` | Rebalance on partition changes when enabled. |
| `SessionTimeout` | `30 * time.Second` | Consumer group session timeout. |
| `RebalanceTimeout` | `30 * time.Second` | Rebalance join timeout. |
| `JoinGroupBackoff` | `5 * time.Second` | Backoff before rejoining after an error. |
| `RetentionTime` | `7 * 24 * time.Hour` | Consumer group retention time. |
| `StartOffset` | `kafka.FirstOffset` | Offset used for partitions without committed offsets. |
| `ReadBackoffMin` | `100 * time.Millisecond` | Minimum read backoff. |
| `ReadBackoffMax` | `1 * time.Second` | Maximum read backoff. |
| `IsolationLevel` | `kafka.ReadUncommitted` | Transactional record visibility. |
| `MaxAttempts` | `3` | Connection attempt limit. |

## Internals

The stage uses `github.com/segmentio/kafka-go`. It creates a `kafka.Reader`,
calls `ReadMessage(ctx)` in the runner, extracts trace context from Kafka
headers when present, and writes received messages to the output connector.
