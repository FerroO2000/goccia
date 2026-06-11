package pkg

import (
	"bytes"
	"fmt"
	"go/format"
	"html/template"
	"os"
	"path"

	"github.com/FerroO2000/goccia/cmd/metrics-gen/templates"
)

var metricFileTmpl = template.Must(
	template.New("codegen").
		Funcs(template.FuncMap{
			"toUpperCamelCase": toUpperCamelCase,
			"toLowerCamelCase": toLowerCamelCase,
		}).
		Parse(templates.MetricFileTmplSource),
)

var defaultImports = []string{"github.com/FerroO2000/goccia/internal/telemetry"}

type metricsFile struct {
	Name    string
	Package string
	Imports []string
	Metrics []*Metric
}

// Generator struct defines a metrics generator.
type Generator struct {
	basePath string
}

// NewGenerator returns a new metrics generator instance
// that writes files to the given base path.
func NewGenerator(basePath string) *Generator {
	return &Generator{
		basePath: basePath,
	}
}

func (g *Generator) getImportPackages(metricType *Metric) []string {
	imports := []string{}

	switch metricType.Type {
	case MetricTypeCounter, MetricTypeUpDownCounter:
		imports = append(imports, "sync/atomic")

	case MetricTypeHistogram:
		imports = append(imports, "context")

	}

	if metricType.Unit != "" {
		imports = append(imports, "go.opentelemetry.io/otel/metric")
	}

	return imports
}

func (g *Generator) getImports(metrics []*Metric) []string {
	importsMap := make(map[string]struct{})
	for _, metric := range metrics {
		packages := g.getImportPackages(metric)
		for _, pkg := range packages {
			importsMap[pkg] = struct{}{}
		}
	}

	imports := make([]string, 0, len(importsMap)+len(defaultImports))
	for pkg := range importsMap {
		imports = append(imports, pkg)
	}

	imports = append(imports, defaultImports...)

	return imports
}

func (g *Generator) getMetricsFileName(name string) string {
	fileName := toLowerSnakeCase(name) + ".metrics.go"
	return path.Join(g.basePath, fileName)
}

// Generate generates metrics files from the given spec.
func (g *Generator) Generate(spec *Spec) error {
	for _, group := range spec.Groups {
		metricFile := &metricsFile{
			Name:    group.Name,
			Package: spec.Package,
			Imports: g.getImports(group.Metrics),
			Metrics: group.Metrics,
		}

		if err := g.generateMetricsFile(metricFile); err != nil {
			return err
		}
	}

	return nil
}

func (g *Generator) generateMetricsFile(mf *metricsFile) error {
	var buf bytes.Buffer

	if err := metricFileTmpl.Execute(&buf, mf); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	// Format as valid Go source
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("format source (raw output: %s): %w", buf.String(), err)
	}

	file, err := os.Create(g.getMetricsFileName(mf.Name))
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(formatted)
	if err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	return nil
}
