local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = 'egress';

{
  stageKind: stageKind,

  getTitle(stageTitle):
    std.format('Egress - %s', stageTitle),

  panels(stageName): [
    p.stat.base('Delivered Messages', prometheus.counter('delivered_messages_total', stageKind, stageName)),
    p.stat.base('Delivering Errors', prometheus.counter('delivering_errors_total', stageKind, stageName), color='red'),
  ],
}
