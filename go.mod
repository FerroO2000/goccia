module github.com/FerroO2000/goccia

go 1.25.0

require (
	github.com/cilium/ebpf v0.22.0
	github.com/fsnotify/fsnotify v1.9.0
	github.com/questdb/go-questdb-client/v3 v3.2.0
	github.com/segmentio/kafka-go v0.4.49
	github.com/squadracorsepolito/acmelib v1.16.1
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/contrib/bridges/otelslog v0.17.0
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/log v0.19.0
	go.opentelemetry.io/otel/metric v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
	golang.org/x/sys v0.45.0
)

require (
	github.com/FerroO2000/goccia/cmd/metrics-gen v0.0.0-00010101000000-000000000000 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/clipperhouse/displaywidth v0.6.2 // indirect
	github.com/clipperhouse/stringish v0.1.1 // indirect
	github.com/clipperhouse/uax29/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/jaevor/go-nanoid v1.4.0 // indirect
	github.com/karrick/godirwalk v1.17.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.19 // indirect
	github.com/nao1215/markdown v0.13.0 // indirect
	github.com/olekukonko/cat v0.0.0-20250911104152-50322a0618f6 // indirect
	github.com/olekukonko/errors v1.1.0 // indirect
	github.com/olekukonko/ll v0.1.4-0.20260115111900-9e59c2286df0 // indirect
	github.com/olekukonko/tablewriter v1.1.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/tools v0.44.0 // indirect
	google.golang.org/grpc v1.82.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

tool (
	github.com/FerroO2000/goccia/cmd/metrics-gen
	github.com/cilium/ebpf/cmd/bpf2go
)

replace github.com/FerroO2000/goccia/cmd/metrics-gen => ./cmd/metrics-gen
