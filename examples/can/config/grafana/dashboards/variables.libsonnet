local g = import 'g.libsonnet';
local var = g.dashboard.variable;

{
  datasource: {
    prometheus: var.datasource.new('metrics', 'prometheus'),
    tempo: var.datasource.new('traces', 'tempo'),
    qdb: var.datasource.new('datasource', 'questdb-questdb-datasource'),
  },
}
