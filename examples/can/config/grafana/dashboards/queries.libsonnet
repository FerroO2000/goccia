local g = import 'g.libsonnet';
local var = import 'variables.libsonnet';
local prometheus = g.query.prometheus;
local tempo = g.query.tempo;

{
  prometheus: {
    base(expr):
      prometheus.new('$' + var.datasource.prometheus.name, expr)
      + prometheus.withInterval('15s'),

    counter(field, legend=''):
      self.base(field) + (
        if legend != '' then prometheus.withLegendFormat(legend) else {}
      ),

    filteredCounter(field, label, filter):
      self.base(field + '{' + label + '="' + filter + '"}')
      + prometheus.withLegendFormat('{{' + label + '}}'),

    rate(field):
      self.base('rate(' + field + '[$__rate_interval])'),

    quantile(field, quantile):
      self.base(
        'histogram_quantile(' + std.format('0.%d', quantile)
        + ', sum by(le) (rate(' + field + '_milliseconds_bucket[$__rate_interval])))'
      )
      + prometheus.withLegendFormat(std.format('p%d', quantile)),
  },

  tempo: {
    local filters = tempo.filters,

    base(service, query=''):
      local q = if std.isEmpty(query)
      then std.format('{resource.service.name="%s"}', service)
      else std.format('{resource.service.name="%s" && %s}', [service, query]);

      tempo.new(
        '$' + var.datasource.tempo.name, q, []
      )
      + tempo.withLimit(20)
      + tempo.withSpss(10),

    duration(service, duration, operation='>'):
      self.base(service, std.format('traceDuration%s%s', [operation, duration])),
  },

  qdb: {
    base(): {
      datasource: {
        type: var.datasource.qdb.name,
        uid: '$' + var.datasource.qdb.name,
      },
    },

    intSignal(name):
      self.base()
      + {
        rawSql: 'SELECT timestamp as time,  avg(integer_value) avg_value FROM "int_signals" WHERE $__timeFilter(timestamp) AND name ='
                + " '" + name + "' " + 'SAMPLE BY $__sampleByInterval ALIGN TO CALENDAR',
      },
  },
}
