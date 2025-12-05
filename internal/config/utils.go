package config

import "fmt"

type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// CheckNotNegative checks that the value is not negative.
// If it is, an anomaly is added to the anomaly collector and the value is set to the fallback.
func CheckNotNegative[T ordered](ac *AnomalyCollector, field string, actual *T, fallback T) {
	val := *actual
	if val < 0 {
		ac.add(field, "cannot be negative", val, fallback)
		*actual = fallback
	}
}

// CheckNotZero checks that the value is not zero.
// If it is, an anomaly is added to the anomaly collector and the value is set to the fallback.
func CheckNotZero[T ordered](ac *AnomalyCollector, field string, actual *T, fallback T) {
	val := *actual
	if val == 0 {
		ac.add(field, "cannot be zero", val, fallback)
		*actual = fallback
	}
}

// CheckNotLower checks that the value is not lower than the target.
// If it is, an anomaly is added to the anomaly collector and the value is set to the target.
func CheckNotLower[T ordered](ac *AnomalyCollector, field string, actual *T, target T) {
	val := *actual
	if val < target {
		ac.add(field, fmt.Sprintf("cannot be lower than %v", target), val, target)
		*actual = target
	}
}

// CheckNotLowerThan checks that the value is not lower than the target.
// If it is, an anomaly is added to the anomaly collector and the value is set to the target.
func CheckNotLowerThan[T ordered](ac *AnomalyCollector, field, targetField string, actual *T, target T) {
	val := *actual
	if val < target {
		ac.add(field, fmt.Sprintf("cannot be lower than %q", targetField), val, target)
		*actual = target
	}
}

// CheckNotGreaterThan checks that the value is not greater than the target.
// If it is, an anomaly is added to the anomaly collector and the value is set to the target.
func CheckNotGreaterThan[T ordered](ac *AnomalyCollector, field, targetField string, actual *T, target T) {
	val := *actual
	if val > target {
		ac.add(field, fmt.Sprintf("cannot be greater than %q", targetField), val, target)
		*actual = target
	}
}

// CheckNotEmpty checks that the value is not empty.
// If it is, an anomaly is added to the anomaly collector and the value is set to the fallback.
func CheckNotEmpty(ac *AnomalyCollector, field string, actual *string, fallback string) {
	val := *actual
	if val == "" {
		ac.add(field, "cannot be empty", val, fallback)
		*actual = fallback
	}
}

// CheckLen checks that the value is not empty.
// If it is, an anomaly is added to the anomaly collector and the value is set to the fallback.
func CheckLen[T any](ac *AnomalyCollector, field string, actual *[]T, fallback []T) {
	val := *actual
	if len(val) == 0 {
		ac.add(field, "cannot be empty", val, fallback)
		*actual = fallback
	}
}
