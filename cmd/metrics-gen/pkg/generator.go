package pkg

import (
	"bytes"
	"fmt"
	"go/format"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"text/template"

	"github.com/FerroO2000/goccia/cmd/metrics-gen/templates"
)

var metricFileTmpl = template.Must(
	template.New("metric_file.go.tmpl").
		Funcs(template.FuncMap{
			"dict":             dict,
			"toUpperCamelCase": toUpperCamelCase,
			"toLowerCamelCase": toLowerCamelCase,
			"getDataType":      getDataType,
		}).
		ParseFS(templates.Templates, "*.tmpl"),
)

var errorTypesFileTmpl = template.Must(
	template.New("error_types_file.go.tmpl").
		Funcs(template.FuncMap{
			"dict":             dict,
			"toUpperCamelCase": toUpperCamelCase,
			"toLowerCamelCase": toLowerCamelCase,
			"getDataType":      getDataType,
		}).
		ParseFS(templates.Templates, "*.tmpl"),
)

var defaultImports = []string{"github.com/FerroO2000/goccia/internal/telemetry"}

type metricsFile struct {
	Name       string
	Package    string
	Imports    []string
	Metrics    []*Metric
	ErrorTypes []*ErrorType
}

type errorTypesFile struct {
	Package    string
	ErrorTypes []*ErrorType
}

// Generator struct defines a metrics generator.
type Generator struct {
	basePath string

	markdown *markdownGenerator
}

// NewGenerator returns a new metrics generator instance
// that writes files to the given base path.
func NewGenerator(basePath string) *Generator {
	mdBasePath := filepath.Join(basePath, "docs")
	if err := os.MkdirAll(mdBasePath, os.ModePerm); err != nil {
		panic(err)
	}

	return &Generator{
		basePath: basePath,

		markdown: newMarkdownGenerator(mdBasePath),
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

	if metricType.Unit != "" || len(metricType.Attributes) > 0 {
		imports = append(imports, "go.opentelemetry.io/otel/metric")
	}

	if len(metricType.Attributes) > 0 || metricType.ErrorTypeRef != nil {
		imports = append(imports, "go.opentelemetry.io/otel/attribute")
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
		errTypes := make(map[string]*ErrorType)
		for _, metric := range group.Metrics {
			if metric.ErrorTypeRef != nil {
				errTypes[metric.ErrorTypeRef.Name] = metric.ErrorTypeRef
			}
		}

		metricFile := &metricsFile{
			Name:       group.Name,
			Package:    spec.Package,
			Imports:    g.getImports(group.Metrics),
			Metrics:    group.Metrics,
			ErrorTypes: slices.Collect(maps.Values(errTypes)),
		}

		if err := g.generateMetricsFile(metricFile); err != nil {
			return err
		}
	}

	if len(spec.ErrorTypes) > 0 {
		errorFile := &errorTypesFile{
			Package:    spec.Package,
			ErrorTypes: spec.ErrorTypes,
		}

		if err := g.generateErrorTypesFile(errorFile); err != nil {
			return err
		}
	}

	if err := g.markdown.generate(spec.Groups); err != nil {
		return err
	}

	return nil
}

func (g *Generator) generateMetricsFile(mf *metricsFile) error {
	var buf bytes.Buffer

	if err := metricFileTmpl.ExecuteTemplate(&buf, "metric_file.go.tmpl", mf); err != nil {
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

func (g *Generator) generateErrorTypesFile(ef *errorTypesFile) error {
	var buf bytes.Buffer

	if err := errorTypesFileTmpl.ExecuteTemplate(&buf, "error_types_file.go.tmpl", ef); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	// Format as valid Go source
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("format source (raw output: %s): %w", buf.String(), err)
	}

	file, err := os.Create("error_types.metrics.go")
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
