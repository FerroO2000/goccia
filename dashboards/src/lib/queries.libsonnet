local g = import 'g.libsonnet';
local v = import 'variables.libsonnet';

local prometheus = g.query.prometheus;
local tempo = g.query.tempo;

{
  prometheus: {
    utils: {
      getField(name, stageKind, stageName):
        std.format('%s{stage_kind="%s",stage_name="%s",exported_job="$service"}', [name, stageKind, stageName]),

      getExpr(field):
        std.format('%s{%s}', [field.name, field.filter]),
    },

    base(expr, legend=''):
      prometheus.new('$' + v.datasource.prometheus.name, expr)
      + prometheus.withInterval('15s') + (
        if legend != '' then prometheus.withLegendFormat(legend) else {}
      ),

    counter(field, stageKind, stageName, legend=''):
      self.base(self.utils.getField(field, stageKind, stageName), legend),

    rate(field, stageKind, stageName):
      self.base(std.format('rate(%s[$__rate_interval])', self.utils.getField(field, stageKind, stageName))),

    quantile(field, quantile):
      local strQuantile = std.format('0.%d', quantile);
      local stdField = std.format('%s_milliseconds_bucket{exported_job="$service"}', field);

      self.base(
        std.format('histogram_quantile(%s, sum by(le) (rate(%s[$__rate_interval])))', [strQuantile, stdField]),
      )
      + prometheus.withLegendFormat(std.format('p%d', quantile)),

    filter: {
      service():
        'exported_job="$service"',

      contains(key, value):
        std.format('%s=~"%s"', [key, value]),
    },

    operation: {
      rate(expr):
        std.format('rate(%s[$__rate_interval])', [expr]),

      sum(expr):
        std.format('sum(%s)', [expr]),

      sumBy(expr, key):
        std.format('sum by (%s) (%s)', [key, expr]),

      histogramQuantile(expr, quantile):
        std.format('histogram_quantile(0.%d, %s)', [quantile, expr]),
    },

    field(name, filters=[], legend=''):
      local l = filters + [self.filter.service()];
      {
        name: name,
        filter: std.join(',', l),
      },

    percentage(fieldA, fieldB, legend=''):
      local aExpr = self.operation.sum(self.operation.rate(self.utils.getExpr(fieldA)));
      local bExpr = self.operation.sum(self.operation.rate(self.utils.getExpr(fieldB)));

      self.base(std.format('100 * %s / %s', [aExpr, bExpr]), legend),

    summed(field, legend=''):
      self.base(self.operation.sum(self.utils.getExpr(field)), legend),

    summedRate(field, key='', legend=''):
      if key == '' then
        self.base(self.operation.sum(self.operation.rate(self.utils.getExpr(field))), legend)
      else
        self.base(self.operation.sumBy(self.operation.rate(self.utils.getExpr(field)), key), legend),

    quant(field, quantile):
      local innerExpr = self.operation.sumBy(self.operation.rate(self.utils.getExpr(field)), 'le');
      local expr = self.operation.histogramQuantile(innerExpr, quantile);

      self.base(expr, std.format("p%d", quantile)),

    heatmap(field, legend=''):
      local innerExpr = self.operation.sumBy(self.operation.rate(self.utils.getExpr(field)), 'le');

      self.base(innerExpr, legend)
      + prometheus.withFormat('heatmap'),
  },

  tempo: {
    local filters = tempo.filters,

    base(query=''):
      local q = if std.isEmpty(query)
      then '{resource.service.name="$service"}'
      else std.format('{resource.service.name="$service" && %s}', query);

      tempo.new(
        '$' + v.datasource.tempo.name, q, []
      )
      + tempo.withLimit(20)
      + tempo.withSpss(10),

    duration(duration, operation='>'):
      self.base(std.format('traceDuration%s%s', [operation, duration])),
  },
}
