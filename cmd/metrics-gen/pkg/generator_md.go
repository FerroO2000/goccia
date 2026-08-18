package pkg

import (
	"fmt"
	"os"
	"path"
	"strings"

	md "github.com/nao1215/markdown"
)

type markdownGenerator struct {
	basePath string
}

func newMarkdownGenerator(basePath string) *markdownGenerator {
	return &markdownGenerator{
		basePath: basePath,
	}
}

func (g *markdownGenerator) getFilename(name, typ string) string {
	filename := fmt.Sprintf("%s.%s.doc.md", toLowerSnakeCase(name), typ)
	return path.Join(g.basePath, filename)
}

func (g *markdownGenerator) generate(groups []*Group) error {
	for _, group := range groups {
		metricsFilename := g.getFilename(group.Name, "metrics")
		if err := g.generateMetricsTable(metricsFilename, group.Metrics); err != nil {
			return err
		}

		attributesFilename := g.getFilename(group.Name, "attributes")
		if err := g.generateAttributesTable(attributesFilename, group.Metrics); err != nil {
			return err
		}

		errorTypesFilename := g.getFilename(group.Name, "error_types")
		if err := g.generateErrorTypeTable(errorTypesFilename, group.Metrics); err != nil {
			return err
		}
	}

	return nil
}

func (g *markdownGenerator) getDescription(desc string) string {
	if len(desc) == 0 {
		return "-"
	}
	return desc
}

func (g *markdownGenerator) getAttributeURL(attr *Attribute) string {
	return fmt.Sprintf("#%s", toLowerSnakeCase(attr.Name))
}

func (g *markdownGenerator) joinItems(items ...string) string {
	if len(items) == 0 {
		return "-"
	}

	strs := make([]string, 0, len(items))
	for _, item := range items {
		strs = append(strs, item)
	}

	return strings.Join(strs, " ")
}

func (g *markdownGenerator) generateMetricsTable(filename string, metrics []*Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	rows := make([][]string, 0, len(metrics))
	for _, metric := range metrics {
		name := metric.Name
		typ := g.joinItems(md.Code(metric.Type), md.Code(metric.DataType))

		attrsCollector := make([]string, 0, len(metric.Attributes))
		for _, attr := range metric.Attributes {
			attrsCollector = append(attrsCollector, md.Link(attr.Name, g.getAttributeURL(attr)))
		}

		if metric.ErrorTypeRef != nil {
			attrsCollector = append(attrsCollector, md.Link("error.type", "#error_type"))
		}

		attrs := g.joinItems(attrsCollector...)

		desc := g.getDescription(metric.Description)

		rows = append(rows, []string{
			name, typ, attrs, desc,
		})
	}

	mdFile := md.NewMarkdown(file).Table(md.TableSet{
		Header: []string{"Name", "Type", "Attributes", "Description"},
		Rows:   rows,
	})

	return mdFile.Build()
}

func (g *markdownGenerator) generateAttributesTable(filename string, metrics []*Metric) error {
	accumulator := make(map[string]*Attribute)
	for _, metric := range metrics {
		for _, attr := range metric.Attributes {
			accumulator[attr.Name] = attr
		}
	}

	totAttributes := len(accumulator)
	if totAttributes == 0 {
		return nil
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	rows := make([][]string, 0, totAttributes)
	for _, attr := range accumulator {
		name := fmt.Sprintf("`%s` {%s}", attr.Name, g.getAttributeURL(attr))
		typ := md.Code(attr.Type)
		desc := g.getDescription(attr.Description)

		rows = append(rows, []string{name, typ, desc})
	}

	mdFile := md.NewMarkdown(file).Table(md.TableSet{
		Header: []string{"Name", "Type", "Description"},
		Rows:   rows,
	})

	return mdFile.Build()
}

func (g *markdownGenerator) generateErrorTypeTable(filename string, metrics []*Metric) error {
	var errorType *ErrorType
	for _, metric := range metrics {
		if metric.ErrorTypeRef != nil {
			errorType = metric.ErrorTypeRef
			break
		}
	}

	if errorType == nil || len(errorType.Errors) == 0 {
		return nil
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	rows := make([][]string, 0, len(errorType.Errors))
	for idx, err := range errorType.Errors {
		name := err.Name
		if idx == 0 {
			name = fmt.Sprintf("`%s` {#error_type}", err.Name)
		}

		value := md.Code(err.Value)
		desc := g.getDescription(err.Description)

		rows = append(rows, []string{name, value, desc})
	}

	mdFile := md.NewMarkdown(file).Table(md.TableSet{
		Header: []string{"Name", "Value", "Description"},
		Rows:   rows,
	})

	return mdFile.Build()
}
