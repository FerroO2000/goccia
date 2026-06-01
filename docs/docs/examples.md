---
icon: lucide/code-xml
---

# Examples

Runnable applications live in the
[`examples`](https://github.com/FerroO2000/goccia/tree/master/examples)
directory.

| Example | What it demonstrates |
| --- | --- |
| CAN | UDP ingress, Cannelloni decoding, reordering, CAN processing, and QuestDB egress |
| CSV | File ingress, CSV decoding and encoding, and file egress |
| eBPF | XDP ingress, custom processing, and sink egress |
| File | File ingress, custom processing, and file egress |
| Kafka | Ticker and Kafka ingress with Kafka egress |

## Run an example

The examples share a Makefile:

``` bash
cd examples
make <example> <command>
```

For example, run the CSV pipeline with:

``` bash
cd examples
make csv up
make csv run
```

See the
[`examples` guide](https://github.com/FerroO2000/goccia/blob/master/examples/README.md)
for the complete command list and infrastructure requirements.
