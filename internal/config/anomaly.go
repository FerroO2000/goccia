package config

import (
	"iter"
	"slices"
)

type anomaly struct {
	field    string
	reason   string
	actual   any
	fallback any
}

// AnomalyCollector is an utility struct for collecting anomalies.
type AnomalyCollector struct {
	anomalies []*anomaly
}

func newAnomalyCollector() *AnomalyCollector {
	return &AnomalyCollector{
		anomalies: []*anomaly{},
	}
}

func (ac *AnomalyCollector) add(field, reason string, actual, fallback any) {
	ac.anomalies = append(ac.anomalies, &anomaly{
		field:    field,
		reason:   reason,
		actual:   actual,
		fallback: fallback,
	})
}

func (ac *AnomalyCollector) iter() iter.Seq[*anomaly] {
	return slices.Values(ac.anomalies)
}
