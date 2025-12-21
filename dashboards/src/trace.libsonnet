local p = import 'lib/panels.libsonnet';
local q = import 'lib/queries.libsonnet';

local tempo = q.tempo;

[
  p.table.base('Traces Over 100ms', tempo.duration('100ms')),
]
