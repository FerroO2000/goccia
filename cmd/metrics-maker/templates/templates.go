package templates

import (
	_ "embed"
)

//go:embed metrics.go.tmpl
var MetricsTmplSource string
