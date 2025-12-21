local ebpf = import 'ebpf.libsonnet';
local file = import 'file.libsonnet';
local kafka = import 'kafka.libsonnet';
local tcp = import 'tcp.libsonnet';
local udp = import 'udp.libsonnet';

[
  ebpf,
  file,
  kafka,
  tcp,
  udp,
]
