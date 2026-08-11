package pkg

import (
	"reflect"
	"testing"
)

func TestCalculateBucketBounds(t *testing.T) {
	want := []float64{
		0.000001,
		0.0000025,
		0.000005,
		0.00001,
		0.000025,
		0.00005,
		0.0001,
		0.00025,
		0.0005,
		0.001,
		0.0025,
		0.005,
		0.01,
		0.025,
		0.05,
		0.1,
		0.25,
		0.5,
		1,
		2.5,
		5,
		10,
	}

	got := calculateBucketBounds(0.000001, 10)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("calculateBucketBounds() = %#v, want %#v", got, want)
	}
}
