package config

// Dummy is a dummy configuration implementing the Config interface.
type Dummy struct{}

// Validate does nothing.
func (c *Dummy) Validate(_ *AnomalyCollector) {}
