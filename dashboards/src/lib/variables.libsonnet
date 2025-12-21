local g = import 'g.libsonnet';

local var = g.dashboard.variable;
local q = var.query;

{
  datasource: {
    prometheus: var.datasource.new('metrics', 'prometheus'),
    tempo: var.datasource.new('traces', 'tempo'),
  },


  service:
    q.new('service') +
    q.withDatasourceFromVariable(self.datasource.prometheus) +
    q.queryTypes.withLabelValues('exported_job', 'go_memory_used_bytes'),
}
