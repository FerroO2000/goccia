local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = 'processor';

{
  stageKind: stageKind,

  getTitle(stageTitle):
    std.format('Processor - %s', stageTitle),

  panels(stageName): [
    p.stat.base('Processed Messages', prometheus.counter('processed_messages_total', stageKind, stageName)),
    p.stat.base('Processing Errors', prometheus.counter('processing_errors_total', stageKind, stageName), color='red'),
    p.stat.base('Dropped Messages', prometheus.counter('dropped_messages_total', stageKind, stageName), color='red'),
  ],
}
