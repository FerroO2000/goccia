package pkg

import (
	"bytes"
	"fmt"
	"go/format"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
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

func (g *Generator) getMarkdownAttributeURL(attr *Attribute) string {
	return fmt.Sprintf("#%s", toLowerSnakeCase(attr.Name))
}

func (g *Generator) generateMarkdownFile(mf *metricsFile) error {
	file, err := os.Create(g.getMarkdownFileName(mf.Name))
	if err != nil {
		return err
	}
	defer file.Close()

	attAccumulator := make(map[string]*Attribute)

	rows := make([][]string, 0, len(mf.Metrics))
	for _, metric := range mf.Metrics {
		desc := "-"
		if metric.Description != "" {
			desc = metric.Description
		}
		typ := md.Code(string(metric.Type))
		dataType := md.Code(string(metric.DataType))

		attributes := make([]string, 0, len(metric.Attributes))
		for _, attr := range metric.Attributes {
			attAccumulator[attr.Name] = attr
			attributes = append(attributes, md.Link(attr.Name, g.getMarkdownAttributeURL(attr)))
		}

		attrStr := "-"
		if len(attributes) > 0 {
			attrStr = strings.Join(attributes, ", ")
		}

		rows = append(rows, []string{
			metric.Name,
			typ,
			dataType,
			attrStr,
			desc,
		})
	}

	metricsTable := md.TableSet{
		Header: []string{"Name", "Type", "Data Type", "Attributes", "Description"},
		Rows:   rows,
	}

	attributesRows := make([][]string, 0, len(attAccumulator))
	for _, attr := range attAccumulator {
		attrName := fmt.Sprintf("`%s` {%s}", attr.Name, g.getMarkdownAttributeURL(attr))

		attributesRows = append(attributesRows, []string{
			attrName,
			attr.Type,
		})
	}
	attributesTable := md.TableSet{
		Header: []string{"Name", "Type"},
		Rows:   attributesRows,
	}

	mdFile := md.NewMarkdown(file).Table(metricsTable).Table(attributesTable)

	return mdFile.Build()
}
