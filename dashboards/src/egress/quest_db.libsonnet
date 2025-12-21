local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'questdb';

l.row(
  common.getTitle('Quest DB'),
  common.panels(stageName) +
  [
    p.stat.base('Inserted Rows', prometheus.counter('inserted_rows_total', stageKind, stageName)),
  ]
)
