local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'tcp';

l.row(
  common.getTitle('TCP'),
  [
    p.stat.base('Open Connections', prometheus.counter('open_connections', stageKind, stageName)),

    p.stat.byteRate('Received Bytes Rate', prometheus.rate('received_bytes_total', stageKind, stageName)),
    p.stat.base('Received Bytes Total', prometheus.counter('received_bytes_total', stageKind, stageName), unit='decbytes'),

    p.stat.base('Received Messages', prometheus.counter('received_messages_total', stageKind, stageName)),
  ]
)
