---
icon: lucide/log-in
---

# Ingress Stages

Ingress stages produce messages and write them to an output connector. They do
not accept a running mode because each source owns its own runner.

| Stage | Constructor | Source |
| --- | --- | --- |
| [Ticker](ticker.md) | `ingress.NewTickerStage` | Periodic timer |
| [TCP](tcp.md) | `ingress.NewTCPStage` | TCP listener |
| [UDP](udp.md) | `ingress.NewUDPStage` | UDP socket |
| [File](file.md) | `ingress.NewFileStage` | Watched files |
| [Kafka](kafka.md) | `ingress.NewKafkaStage` | Kafka consumer group |
| [eBPF](ebpf.md) | `ingress.NewEBPFStage` | eBPF ring buffer |
