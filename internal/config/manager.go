package config

import (
	"fmt"

	"github.com/FerroO2000/goccia/internal"
)

type anomaly struct {
	field    string
	reason   string
	actual   any
	fallback any
}

type Anomalies struct {
	list []*anomaly
}

func newAnomalies() *Anomalies {
	return &Anomalies{
		list: []*anomaly{},
	}
}

func (a *Anomalies) Add(field, reason string, actual, fallback any) {
	a.list = append(a.list, &anomaly{
		field:    field,
		reason:   reason,
		actual:   actual,
		fallback: fallback,
	})
}

type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

func CheckNotNegative[T ordered](anomalies *Anomalies, field string, actual, fallback T) {
	if actual < 0 {
		anomalies.Add(field, "cannot be negative", actual, fallback)
	}
}

func CheckNotLowerThan[T ordered](anomalies *Anomalies, field, targetField string, actual, target T) {
	if actual < target {
		anomalies.Add(field, fmt.Sprintf("cannot be lower than %q", targetField), actual, target)
	}
}

func CheckNotGreaterThan[T ordered](anomalies *Anomalies, field, targetField string, actual, target T) {
	if actual > target {
		anomalies.Add(field, fmt.Sprintf("cannot be greater than %q", targetField), actual, target)
	}
}

type Config interface {
	Validate(anomalies *Anomalies)
}

type Manager[T Config] struct {
	tel *internal.Telemetry

	Config T
}

func NewManager[T Config](tel *internal.Telemetry, config T) *Manager[T] {
	return &Manager[T]{
		tel: tel,

		Config: config,
	}
}

func (m *Manager[T]) Init() {
	anomalies := newAnomalies()
	m.Config.Validate(anomalies)

	for _, anomaly := range anomalies.list {
		m.handleAnomaly(anomaly)
	}
}

func (m *Manager[T]) handleAnomaly(an *anomaly) {
	m.tel.LogWarn("config anomaly",
		"field", an.field, "reason", an.reason,
		"actual", an.actual, "fallback", an.fallback)
}
