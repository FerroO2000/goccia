// Package templates contains the templates used by the metrics-gen tool.
package templates

import (
	"embed"
)

// Templates contains all the templates.
//
//go:embed *.tmpl
var Templates embed.FS
