package config

// Empty is a no-op configuration implementing the Config interface.
type Empty struct{}

// NewEmpty returns a new empty configuration.
func NewEmpty() *Empty {
	return &Empty{}
}

// Validate does nothing.
func (c *Empty) Validate(_ *AnomalyCollector) {}
