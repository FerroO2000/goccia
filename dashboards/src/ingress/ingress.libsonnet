local ebpf = import 'ebpf.libsonnet';
local file = import 'file.libsonnet';
local http = import 'http.libsonnet';
local kafka = import 'kafka.libsonnet';
local tcp = import 'tcp.libsonnet';
local udp = import 'udp.libsonnet';

[
  ebpf,
  file,
  http,
  kafka,
  tcp,
  udp,
]
