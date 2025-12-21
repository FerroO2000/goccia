local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'filter';

l.row(
  common.getTitle('Filter'),
  [
    p.stat.base('Filtered Messages', prometheus.counter('filtered_messages_total', stageKind, stageName)),
  ]
)
