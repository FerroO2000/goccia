package pkg

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path"
	"text/template"

	"github.com/FerroO2000/goccia/cmd/metrics-gen/templates"
	md "github.com/nao1215/markdown"
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
	Name    string
	Package string
	Imports []string
	Metrics []*Metric
}

type errorTypesFile struct {
	Package    string
	ErrorTypes []*ErrorType
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

	if metricType.Unit != "" || len(metricType.Attributes) > 0 {
		imports = append(imports, "go.opentelemetry.io/otel/metric")
	}

	if len(metricType.Attributes) > 0 {
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
		metricFile := &metricsFile{
			Name:    group.Name,
			Package: spec.Package,
			Imports: g.getImports(group.Metrics),
			Metrics: group.Metrics,
		}

		if err := g.generateMetricsFile(metricFile); err != nil {
			return err
		}

		if err := g.generateMarkdownFile(metricFile); err != nil {
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

func (g *Generator) getMarkdownFileName(name string) string {
	fileName := toLowerSnakeCase(name) + ".doc.md"
	return path.Join(g.basePath, fileName)
}

func (g *Generator) generateMarkdownFile(mf *metricsFile) error {
	file, err := os.Create(g.getMarkdownFileName(mf.Name))
	if err != nil {
		return err
	}
	defer file.Close()

	rows := make([][]string, 0, len(mf.Metrics))
	for _, metric := range mf.Metrics {
		typ := md.Code(string(metric.Type))
		dataType := md.Code(string(metric.DataType))
		desc := "-"
		if metric.Description != "" {
			desc = metric.Description
		}

		rows = append(rows, []string{
			metric.Name,
			typ,
			dataType,
			desc,
		})
	}

	mdFile := md.NewMarkdown(file).Table(md.TableSet{
		Header: []string{"Name", "Type", "Data Type", "Description"},
		Rows:   rows,
	})

	return mdFile.Build()
}
