package pkg

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

// LoadSpec loads a spec from a file.
func LoadSpec(specFilePath string) (*Spec, error) {
	file, err := os.Open(specFilePath)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer file.Close()

	spec := &Spec{}
	if err := yaml.NewDecoder(file).Decode(spec); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	if err := spec.validate(); err != nil {
		return nil, err
	}

	return spec, nil
}
