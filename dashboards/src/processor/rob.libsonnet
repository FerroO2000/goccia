local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'rob';

l.row(
  common.getTitle('ReOrder Buffer'),
  [
    p.stat.base('Ordered Messages', prometheus.counter('ordered_messages_total', stageKind, stageName)),
    p.stat.base('Primary Enqueued Messages', prometheus.counter('primary_enqueued_messages_total', stageKind, stageName)),
    p.stat.base('Auxiliary Enqueued Messages', prometheus.counter('auxiliary_enqueued_messages_total', stageKind, stageName)),

    p.stat.base('Out of Order Sequence Number Count', prometheus.counter('out_of_order_sequence_number_total', stageKind, stageName), color='red'),
    p.stat.base('Duplicated Sequence Number Count', prometheus.counter('duplicated_sequence_number_total', stageKind, stageName), color='red'),
    p.stat.base('Invalid Sequence Number Count', prometheus.counter('invalid_sequence_number_total', stageKind, stageName), color='red'),

    p.stat.base('Resets', prometheus.counter('resets_total', stageKind, stageName), color='yellow'),
  ]
)
