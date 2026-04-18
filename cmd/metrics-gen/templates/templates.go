// Package templates contains the templates used by the metrics-gen tool.
package templates

import (
	_ "embed"
)

// MetricFileTmplSource contains the template for the metrics file.
//
//go:embed metric_file.go.tmpl
var MetricFileTmplSource string
