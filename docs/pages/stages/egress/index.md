---
icon: lucide/log-out
---

# Egress Stages

Egress stages consume messages and do not write to downstream connectors.

| Stage | Constructor | Capability | Destination |
| --- | --- | --- | --- |
| [Sink](sink.md) | `egress.NewSinkStage` | Single runner | Drop and destroy messages |
| [TCP](tcp.md) | `egress.NewTCPStage` | Single worker | TCP connection |
| [UDP](udp.md) | `egress.NewUDPStage` | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | UDP socket |
| [File](file.md) | `egress.NewFileStage` | Single worker | Append-only file |
| [Kafka](kafka.md) | `egress.NewKafkaStage` | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | Kafka producer |
| [QuestDB](questdb.md) | `egress.NewQuestDBStage` | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | QuestDB line protocol |
