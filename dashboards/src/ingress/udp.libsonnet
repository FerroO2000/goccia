local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'udp';

l.row(
  common.getTitle('UDP'),
  [
    p.stat.byteRate('Received Bytes Rate', prometheus.rate('received_bytes_total', stageKind, stageName)),
    p.stat.base('Received Bytes Total', prometheus.counter('received_bytes_total', stageKind, stageName), unit='decbytes'),

    p.stat.base('Received Messages', prometheus.counter('received_messages_total', stageKind, stageName)),
  ]
)
