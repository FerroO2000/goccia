dash-install:
	cd config/grafana/dashboards/ && jb install github.com/grafana/grafonnet/gen/grafonnet-latest@main

dash-gen:
	jsonnet config/grafana/dashboards/server.libsonnet -J config/grafana/dashboards/vendor/ -o config/grafana/dashboards/output/server.json
	jsonnet config/grafana/dashboards/signals.libsonnet -J config/grafana/dashboards/vendor/ -o config/grafana/dashboards/output/signals.json