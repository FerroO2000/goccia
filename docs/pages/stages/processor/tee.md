---
icon: lucide/git-fork
---

# Tee Processor

`TeeStage` clones each input message to multiple output connectors.

``` go
stage := processor.NewTeeStage(in, outA, outB, outC)
```

## Messages

### Input Message

Accepted body type: `T`.

Additional input interfaces: none required beyond `message.Body`. Any
interfaces implemented by `T` are preserved in the cloned output envelopes.

### Output Message

Produced body type: the same `T` body received from the input connector, cloned
to each output connector.

Additional interfaces: preserved from `T`. The stage does not deep-copy the
message body or add interfaces. It clones the envelope metadata and increments
the message body's reference count, so all outputs share the same body until
the envelopes are destroyed.

## Configuration

`TeeStage` has no config object. At least one output connector is required.

## Internals

`TeeStage` uses a custom single runner instead of the generic worker pool. The
runner reads one message, creates one clone per output connector, writes each
clone, and closes every output connector when the input stream drains.
