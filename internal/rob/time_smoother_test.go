package rob

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_doubleExponentialEstimator(t *testing.T) {
	assert := assert.New(t)

	// Using the dataset from online calculator:
	// https://planetcalc.com/594/
	testData := []struct {
		value, expected float64
	}{
		{50, 50},
		{52, 52},
		{95, 54},
		{59, 86.75},
		{52, 78.19},
		{45, 63.86},
		{38, 48.48},
		{10, 34.67},
		{47, 7.6},
		{40, 22.41},
	}

	estimator := newDoubleExponentialEstimator(0.5, 0.5)

	for _, data := range testData {
		estimatedValue := estimator.estimate(data.value, 1)
		rounded := math.Round(estimatedValue*100) / 100
		assert.Equal(data.expected, rounded)
	}
}
