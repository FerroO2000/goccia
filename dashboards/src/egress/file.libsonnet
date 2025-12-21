local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'file';

l.row(
  common.getTitle('File'),
  common.panels(stageName) +
  [
    p.stat.byteRate('Written Bytes Rate', prometheus.rate('written_bytes_total', stageKind, stageName)),
    p.stat.base('Written Bytes Total', prometheus.counter('written_bytes_total', stageKind, stageName), unit='decbytes'),

    p.stat.base('Write Errors', prometheus.counter('write_errors_total', stageKind, stageName), color='red'),
    p.stat.base('Flush Errors', prometheus.counter('flush_errors_total', stageKind, stageName), color='red'),
  ]
)
