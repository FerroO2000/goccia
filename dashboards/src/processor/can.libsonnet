local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'can';

l.row(
  common.getTitle('CAN'),
  common.panels(stageName) +
  [
    p.stat.base('CAN Messages', prometheus.counter('can_messages_total', stageKind, stageName)),
    p.stat.base('CAN Signals', prometheus.counter('can_signals_total', stageKind, stageName)),
  ]
)
