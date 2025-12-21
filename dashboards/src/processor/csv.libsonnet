local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'csv';

{
  decoder: l.row(
    common.getTitle('CSV Decoder'),
    common.panels(stageName + '_decoder')
  ),

  encoder: l.row(
    common.getTitle('CSV Encoder'),
    common.panels(stageName + '_encoder')
  ),
}
