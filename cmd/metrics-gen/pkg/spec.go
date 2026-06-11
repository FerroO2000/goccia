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
	// MetricTypeHistogram is a histogram metric.
	MetricTypeHistogram MetricType = "histogram"
)

// Metric defines a metric.
type Metric struct {
	Name string     `yaml:"name"`
	Type MetricType `yaml:"type"`
	Unit string     `yaml:"unit"`
}

// Group defines a group of metrics.
type Group struct {
	Name    string    `yaml:"name"`
	Metrics []*Metric `yaml:"metrics"`
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

	return nil
}
