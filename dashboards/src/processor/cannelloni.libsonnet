local common = import 'common.libsonnet';
local l = import 'layout.libsonnet';
local p = import 'panels.libsonnet';
local q = import 'queries.libsonnet';

local prometheus = q.prometheus;

local stageKind = common.stageKind;
local stageName = 'cannelloni';

{
  decoder: l.row(
    common.getTitle('Cannelloni Decoder'),
    common.panels(stageName + '_decoder')
  ),

  encoder: l.row(
    common.getTitle('Cannelloni Encoder'),
    common.panels(stageName + '_encoder')
  ),
}
