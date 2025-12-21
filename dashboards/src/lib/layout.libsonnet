local g = import 'g.libsonnet';
local r = g.panel.row;

{
  row(title, panels):
    r.new(title)
    + r.withPanels(panels)
    + r.withCollapsed(true),
}
