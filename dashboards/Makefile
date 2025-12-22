dash-install:
	go install github.com/google/go-jsonnet/cmd/jsonnet@latest
	go install github.com/google/go-jsonnet/cmd/jsonnet-lint@latest
	go install github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb@latest
	cd src && jb install github.com/grafana/grafonnet/gen/grafonnet-latest@main

dash-download:
	cd src && jb install github.com/grafana/grafonnet/gen/grafonnet-latest@main

dash-gen:
	jsonnet src/goccia.libsonnet -J src/vendor/ -J src/lib/ -J src/ingress/ -o goccia.json -o ../examples/telemetry/config/grafana/dashboards/goccia.json