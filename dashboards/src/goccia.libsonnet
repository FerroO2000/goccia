local g = import 'lib/g.libsonnet';
local v = import 'lib/variables.libsonnet';

local egress = import 'egress/egress.libsonnet';
local general = import 'general.libsonnet';
local ingress = import 'ingress/ingress.libsonnet';
local processor = import 'processor/processor.libsonnet';
local trace = import 'trace.libsonnet';

g.dashboard.new('Goccia')
+ g.dashboard.graphTooltip.withSharedCrosshair()
+ g.dashboard.withVariables(
  [
    v.datasource.prometheus,
    v.datasource.tempo,
    v.service,
  ]
)
+ g.dashboard.withPanels(
  g.util.grid.makeGrid(
    general +
    trace +
    ingress +
    processor +
    egress
  )
)
+ g.dashboard.time.withFrom('now-15m')
+ g.dashboard.withTimezone('browser')
+ g.dashboard.timepicker.withRefreshIntervals(['250ms', '500ms', '1s', '2s', '5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'])
+ g.dashboard.withRefresh('5s')
