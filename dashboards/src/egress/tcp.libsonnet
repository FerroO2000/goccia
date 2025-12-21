local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'tcp';

l.row(
  common.getTitle('TCP'),
  common.panels(stageName) +
  [
    p.stat.byteRate('Delivered Bytes Rate', prometheus.rate('delivered_bytes_total', stageKind, stageName)),
    p.stat.base('Delivered Bytes Total', prometheus.counter('delivered_bytes_total', stageKind, stageName), unit='decbytes'),
  ]
)
