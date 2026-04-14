package loader

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

type MetricType string

const (
	MetricTypeCounter       MetricType = "counter"
	MetricTypeUpDownCounter MetricType = "upDownCounter"
)

type Metric struct {
	Name string     `env:"name"`
	Type MetricType `env:"type"`
}

type Object struct {
	Name    string    `env:"name"`
	Metrics []*Metric `env:"metrics"`
}

type Input struct {
	Package string    `env:"package"`
	Objects []*Object `env:"objects"`
}

func Load(path string) (*Input, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	input := &Input{}
	if err := yaml.NewDecoder(f).Decode(input); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	if input.Package == "" {
		return nil, fmt.Errorf("yaml: 'package' field is required")
	}

	return input, nil
}
