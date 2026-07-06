// Package pkg contains the implementation of the metrics-gen cli tool.
package pkg

import "fmt"

// MetricType defines the type of a metric.
type MetricType string

const (
	// MetricTypeCounter is a counter metric.
	MetricTypeCounter MetricType = "counter"
	// MetricTypeUpDownCounter is an up/down counter metric.
	MetricTypeUpDownCounter MetricType = "upDownCounter"
	// MetricTypeGauge is a gauge metric.
	MetricTypeGauge MetricType = "gauge"
	// MetricTypeHistogram is a histogram metric.
	MetricTypeHistogram MetricType = "histogram"
)

type DataType string

const (
	DataTypeInteger DataType = "integer"
	DataTypeFloat   DataType = "float"
)

// Metric defines a metric.
type Metric struct {
	Name         string     `yaml:"name"`
	Description  string     `yaml:"description"`
	Type         MetricType `yaml:"type"`
	DataType     DataType   `yaml:"data_type"`
	Unit         string     `yaml:"unit"`
	CustomGetter bool       `yaml:"custom_getter"`
}

func (m *Metric) validate() error {
	if m.Name == "" {
		return fmt.Errorf("yaml: 'name' field is required")
	}

	if m.Type == "" {
		return fmt.Errorf("yaml: 'type' field is required")
	}

	if m.DataType == "" {
		m.DataType = DataTypeInteger
	}

	return nil
}

// Group defines a group of metrics.
type Group struct {
	Name    string    `yaml:"name"`
	Metrics []*Metric `yaml:"metrics"`
}

func (g *Group) validate() error {
	if g.Name == "" {
		return fmt.Errorf("yaml: 'name' field is required")
	}

	for _, metric := range g.Metrics {
		if err := metric.validate(); err != nil {
			return err
		}
	}

	return nil
}

// Spec defines a metrics file spec.
type Spec struct {
	Package string   `yaml:"package"`
	Groups  []*Group `yaml:"groups"`
}

func (s *Spec) validate() error {
	if s.Package == "" {
		return fmt.Errorf("yaml: 'package' field is required")
	}

	for _, group := range s.Groups {
		if err := group.validate(); err != nil {
			return err
		}
	}

	return nil
}
