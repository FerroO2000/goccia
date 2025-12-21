local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'file';

l.row(
  common.getTitle('File'),
  [
    p.stat.base('Readers Total', prometheus.counter('readers_total', stageKind, stageName)),
    p.stat.base('Active Readers', prometheus.counter('active_readers_total', stageKind, stageName)),

    p.stat.byteRate('Read Bytes Rate', prometheus.rate('read_bytes_total', stageKind, stageName)),
    p.stat.base('Read Bytes Total', prometheus.counter('read_bytes_total', stageKind, stageName), unit='decbytes'),
  ]
)
