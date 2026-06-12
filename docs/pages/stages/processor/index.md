---
icon: lucide/cpu
---

# Processor Stages

Processor stages read from an input connector and write to an output connector.

| Family     | Stage                            | Constructor                           | Capability                                                                                        | Purpose                              |
| ---------- | -------------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------ |
| Generic    | [Generic](generic.md)            | `processor.NewGenericStage`           | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | User-defined transform               |
| Generic    | [Filter](filter.md)              | `processor.NewFilterStage`            | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | Drop messages by predicate           |
| Generic    | [Tee](tee.md)                    | `processor.NewTeeStage`               | Single runner                                                                                     | Clone one stream to many outputs     |
| Generic    | [Merge](merge.md)                | `processor.NewMergeStage`             | Single runner                                                                                     | Combine many streams into one output |
| CSV        | [Decoder](csv-decoder.md)        | `processor.NewCSVDecoderStage`        | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | Bytes to typed CSV rows              |
| CSV        | [Encoder](csv-encoder.md)        | `processor.NewCSVEncoderStage`        | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | Typed CSV rows to bytes              |
| CAN        | [CAN](can.md)                    | `processor.NewCANStage`               | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | Raw CAN frames to decoded signals    |
| Cannelloni | [Decoder](cannelloni-decoder.md) | `processor.NewCannelloniDecoderStage` | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | Cannelloni bytes to CAN frames       |
| Cannelloni | [Encoder](cannelloni-encoder.md) | `processor.NewCannelloniEncoderStage` | [Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool } | CAN frames to Cannelloni bytes       |
| Ordering   | [ROB](rob.md)                    | `processor.NewROBStage`               | Single runner                                                                                     | Reorder by sequence number           |
