local g = import 'g.libsonnet';
local v = import 'variables.libsonnet';

local prometheus = g.query.prometheus;
local tempo = g.query.tempo;

{
  prometheus: {
    utils: {
      getField(name, stageKind, stageName):
        std.format('%s{goccia_stage_kind="%s",goccia_stage_name="%s",exported_job="$service"}', [name, stageKind, stageName]),
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
