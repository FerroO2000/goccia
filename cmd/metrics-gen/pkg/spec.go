// Package pkg contains the implementation of the metrics-gen cli tool.
package pkg

// AttributeType defines the type of an attribute.
type AttributeType = string

const (
	// AttributeTypeString is a string attribute.
	AttributeTypeString AttributeType = "string"
	// AttributeTypeBool is a boolean attribute.
	AttributeTypeBool AttributeType = "bool"
	// AttributeTypeInt is an integer attribute.
	AttributeTypeInt AttributeType = "int"
	// AttributeTypeFloat is a float attribute.
	AttributeTypeFloat AttributeType = "float"
)

// Attribute defines an attribute.
type Attribute struct {
	Name string        `yaml:"name"`
	Type AttributeType `yaml:"type"`
	Arg  string        `yaml:"arg"`
}

func (a *Attribute) getName() string {
	return a.Name
}

// MetricType defines the type of a metric.
type MetricType = string

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

// DataType defines the data type of a metric.
type DataType = string

const (
	// DataTypeInteger is an integer data type.
	DataTypeInteger DataType = "integer"
	// DataTypeFloat is a float data type.
	DataTypeFloat DataType = "float"
)

// Metric defines a metric.
type Metric struct {
	Name            string       `yaml:"name"`
	Description     string       `yaml:"description"`
	Type            MetricType   `yaml:"type"`
	DataType        DataType     `yaml:"data_type"`
	Unit            string       `yaml:"unit"`
	CustomGetter    bool         `yaml:"custom_getter"`
	Attributes      []*Attribute `yaml:"attributes"`
	AttributeSet    string       `yaml:"attribute_set"`
	BucketBounds    []float64    `yaml:"bucket_bounds"`
	BucketBoundsSet string       `yaml:"bucket_bounds_set"`
}

func (m *Metric) getName() string {
	return m.Name
}

// Group defines a group of metrics.
type Group struct {
	Name    string    `yaml:"name"`
	Metrics []*Metric `yaml:"metrics"`
}

func (g *Group) getName() string {
	return g.Name
}

// AttributeSet defines a set of attributes to be referenced
// by other metrics.
type AttributeSet struct {
	Name       string       `yaml:"name"`
	Attributes []*Attribute `yaml:"attributes"`
}

// BucketBoundsSet defines a set of bucket bounds to be referenced
// by other metrics.
type BucketBoundsSet struct {
	Name       string    `yaml:"name"`
	LowerBound float64   `yaml:"lower_bound"`
	UpperBound float64   `yaml:"upper_bound"`
	Bounds     []float64 `yaml:"bounds"`
}

// Spec defines a metrics file spec.
type Spec struct {
	Package          string             `yaml:"package"`
	AttributeSets    []*AttributeSet    `yaml:"attribute_sets"`
	BucketBoundsSets []*BucketBoundsSet `yaml:"bucket_bounds_sets"`
	Groups           []*Group           `yaml:"groups"`
}
