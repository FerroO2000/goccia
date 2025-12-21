local g = import 'g.libsonnet';

{
  gauge: {
    local gauge = g.panel.gauge,
    local opts = gauge.options,
    local stdOpts = gauge.standardOptions,

    base(title, targets, color='green', unit='', w=6, h=6):
      gauge.new(title)
      + gauge.queryOptions.withTargets(targets)
      + gauge.gridPos.withW(w)
      + gauge.gridPos.withH(h)
      + stdOpts.color.withMode('fixed')
      + stdOpts.color.withFixedColor(color)
      + stdOpts.withUnit(unit),
  },

  stat: {
    local stat = g.panel.stat,
    local opts = stat.options,

    base(title, targets, color='green', unit='short', w=6, h=6):
      stat.new(title)
      + stat.queryOptions.withTargets(targets)
      + opts.withShowPercentChange(false)
      + opts.withColorMode('background_solid')
      + stat.standardOptions.color.withMode('fixed')
      + stat.standardOptions.color.withFixedColor(color)
      + stat.gridPos.withW(w)
      + stat.gridPos.withH(h)
      + stat.standardOptions.withUnit(unit)
      + opts.withGraphMode('none'),

    rate(title, targets, color='green', unit='short'):
      self.base(title, targets, color, unit)
      + opts.withColorMode('value')
      + opts.withGraphMode('area'),

    byteRate(title, targets, color='green'):
      self.rate(title, targets, color, unit='Bps'),
  },

  timeSeries: {
    local ts = g.panel.timeSeries,
    local opts = ts.options,
    local custom = ts.fieldConfig.defaults.custom,

    base(title, targets, w=12, h=18):
      ts.new(title)
      + ts.queryOptions.withTargets(targets)
      + ts.standardOptions.color.withMode('palette-classic')
      + ts.gridPos.withW(w)
      + ts.gridPos.withH(h)
      + custom.withLineWidth(2)
      + custom.withLineInterpolation('smooth')
      + custom.withFillOpacity(50),

    latency(title, targets, w=12, h=18):
      self.base(title, targets, w, h)
      + ts.standardOptions.withUnit('ms'),

    step(title, targets, w=12, h=18):
      self.base(title, targets, w, h)
      + custom.withLineInterpolation('stepAfter')
      + custom.withFillOpacity(0),
  },

  table: {
    local t = g.panel.table,

    base(title, targets, w=24, h=8):
      t.new(title)
      + t.queryOptions.withTargets(targets)
      + t.gridPos.withW(w)
      + t.gridPos.withH(h),
  },
}
