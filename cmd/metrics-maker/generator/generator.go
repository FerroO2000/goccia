package generator

import (
	"bytes"
	"fmt"
	"go/format"
	"html/template"
	"io"

	"github.com/FerroO2000/goccia/cmd/metrics-maker/loader"
	"github.com/FerroO2000/goccia/cmd/metrics-maker/templates"
)

var tmpl = template.Must(
	template.New("codegen").
		Funcs(template.FuncMap{
			"toUpperCamelCase": toUpperCamelCase,
			"toLowerCamelCase": toLowerCamelCase,
		}).
		Parse(templates.MetricsTmplSource),
)

func Generate(w io.Writer, data *loader.Input) error {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	// Format as valid Go source
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("format source (raw output: %s): %w", buf.String(), err)
	}

	_, err = w.Write(formatted)
	return err
}
