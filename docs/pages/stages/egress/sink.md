---
icon: lucide/trash-2
---

# Sink Egress

`SinkStage` consumes and destroys every incoming message.

``` go
stage := egress.NewSinkStage(in)
```

## Messages

### Input Message

Accepted body type: any `T` that implements `message.Body`.

Additional input interfaces: none required. If the input body implements
`message.Serializable`, `message.ReOrderable`, or another interface, `SinkStage`
does not inspect it; it only destroys the incoming envelope.

This egress stage produces no downstream output message.

## Configuration

`SinkStage` has no config object. It is intended mainly for tests, examples,
and benchmarks where the pipeline needs a terminal consumer.

## Internals

The stage uses a custom single runner. It repeatedly reads from the input
connector and calls `Destroy()` on each message envelope until the connector is
closed and drained.
