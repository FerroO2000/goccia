local g = import 'g.libsonnet';

local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';
local v = import 'variables.libsonnet';

local row = g.panel.row;

local prometheus = q.prometheus;
local tempo = q.tempo;

g.dashboard.new('Goccia Pipeline')
+ g.dashboard.graphTooltip.withSharedCrosshair()
+ g.dashboard.withVariables(
  [
    v.datasource.prometheus,
    v.datasource.tempo,
  ]
)
+ g.dashboard.withPanels(
  g.util.grid.wrapPanels(
    [
      p.gauge.base('Memory Usage', [
        prometheus.filteredCounter('go_memory_used_bytes', 'go_memory_type', 'other'),
        prometheus.filteredCounter('go_memory_used_bytes', 'go_memory_type', 'stack'),
      ], unit='decbytes', w=24),

      p.stat.byteRate('Received Bytes Rate', prometheus.rate('received_bytes_total')),

      p.stat.base('Received Bytes Total', prometheus.counter('received_bytes_total'), unit='decbytes', color='yellow', w=3),

      p.stat.base('Received Messages', prometheus.counter('received_messages_total'), color='purple', w=3),

      p.stat.base(
        'Handled Messages',
        [
          prometheus.filteredCounter('processed_messages_total', 'goccia_stage_name', 'cannelloni'),
          prometheus.filteredCounter('processed_messages_total', 'goccia_stage_name', 'can'),
        ]
      ),

      p.stat.base('Delivered Messages', prometheus.counter('delivered_messages_total'), color='blue', w=3),

      p.stat.base('Inserted Rows', prometheus.counter('inserted_rows_total'), color='purple', w=3),

      p.timeSeries.latency(
        'Message Processing Time',
        [
          prometheus.quantile('total_message_processing_time', 95),
          prometheus.quantile('total_message_processing_time', 90),
          prometheus.quantile('total_message_processing_time', 75),
          prometheus.quantile('total_message_processing_time', 50),
        ]
      ),

      p.timeSeries.step(
        'Active Workers',
        prometheus.counter('worker_pool_active_workers', '{{goccia_stage_kind}} - {{goccia_stage_name}}')
      ),

      p.stat.base('ROB Stage', [
        prometheus.counter('ordered_messages_total', 'ordered_messages'),
        prometheus.counter('primary_enqueued_messages_total', 'primary_enqueued_messages'),
        prometheus.counter('auxiliary_enqueued_messages_total', 'auxiliary_enqueued_messages'),
        prometheus.counter('out_of_order_sequence_number_total', 'out_of_order_sequence_number'),
        prometheus.counter('duplicated_sequence_number_total', 'duplicated_sequence_number'),
        prometheus.counter('invalid_sequence_number_total', 'invalid_sequence_number'),
        prometheus.counter('resets_total', 'resets'),
      ], w=24),

      p.table.base('Client Traces', tempo.duration('can-client-example', '100ms')),
      p.table.base('Server Traces', tempo.duration('can-server-example', '100ms')),
    ],
  ),
)
+ g.dashboard.time.withFrom('now-15m')
+ g.dashboard.withTimezone('browser')
+ g.dashboard.timepicker.withRefreshIntervals(['250ms', '500ms', '1s', '2s', '5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'])
+ g.dashboard.withRefresh('1s')
