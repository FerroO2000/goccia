local g = import 'g.libsonnet';

local p = import 'panels.libsonnet';
local queries = import 'queries.libsonnet';
local v = import 'variables.libsonnet';

local row = g.panel.row;
local q = queries.qdb;

g.dashboard.new('Signals')
+ g.dashboard.graphTooltip.withSharedCrosshair()
+ g.dashboard.withVariables(
  v.datasource.qdb
)
+ g.dashboard.withPanels(
  g.util.grid.wrapPanels(
    [
      p.timeSeries.base(
        'Signal 0',
        q.intSignal('message_0_signal_0'),
        h=8
      ),
      p.timeSeries.base(
        'Signal 1',
        q.intSignal('message_0_signal_1'),
        h=8
      ),
      p.timeSeries.base(
        'Signal 2',
        q.intSignal('message_0_signal_2'),
        h=8
      ),
      p.timeSeries.base(
        'Signal 3',
        q.intSignal('message_0_signal_3'),
        h=8
      ),
      p.timeSeries.base(
        'Signal 4',
        q.intSignal('message_0_signal_4'),
        h=8
      ),
      p.timeSeries.base(
        'Signal 5',
        q.intSignal('message_0_signal_5'),
        h=8
      ),
      p.timeSeries.base(
        'Signal 6',
        q.intSignal('message_0_signal_6'),
        h=8
      ),
      p.timeSeries.base(
        'Signal 7',
        q.intSignal('message_0_signal_7'),
        h=8
      ),
    ],
  ),
)
+ g.dashboard.time.withFrom('now-15m')
+ g.dashboard.withTimezone('browser')
+ g.dashboard.timepicker.withRefreshIntervals(['250ms', '500ms', '1s', '2s', '5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'])
+ g.dashboard.withRefresh('1s')
