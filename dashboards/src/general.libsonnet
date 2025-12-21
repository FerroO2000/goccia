local p = import 'lib/panels.libsonnet';
local q = import 'lib/queries.libsonnet';

local prometheus = q.prometheus;

[
  p.gauge.base('Memory Usage', [
    prometheus.base('go_memory_used_bytes{go_memory_type="other",exported_job="$service"}', '{{go_memory_type}}'),
    prometheus.base('go_memory_used_bytes{go_memory_type="stack",exported_job="$service"}', '{{go_memory_type}}'),
  ], unit='decbytes', w=24),

  p.timeSeries.latency(
    'Message Processing Time',
    [
      prometheus.quantile('total_message_processing_time', 99),
      prometheus.quantile('total_message_processing_time', 95),
      prometheus.quantile('total_message_processing_time', 90),
      prometheus.quantile('total_message_processing_time', 75),
      prometheus.quantile('total_message_processing_time', 50),
    ],
  ),

  p.timeSeries.step(
    'Active Workers',
    prometheus.base('worker_pool_active_workers{exported_job="$service"}', '{{goccia_stage_kind}} - {{goccia_stage_name}}')
  ),
]
