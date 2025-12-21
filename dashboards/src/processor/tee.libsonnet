local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'tee';

l.row(
  common.getTitle('Tee'),
  [
    p.stat.base('Cloned Messages', prometheus.counter('cloned_messages_total', stageKind, stageName)),
  ]
)
