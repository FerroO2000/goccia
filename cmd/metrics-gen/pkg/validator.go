package pkg

import (
	"fmt"
	"math"
	"strconv"
)

func isEmpty(field string) bool {
	return field == ""
}

func requiredFieldErr(kind, fieldName string) error {
	return fmt.Errorf("%s: field '%s' is required", kind, fieldName)
}

func calculateBucketBounds(lower, upper float64) []float64 {
	mantissas := [...]float64{1, 2.5, 5}

	exponent := int(math.Floor(math.Log10(lower)))
	var bounds []float64

	for {
		for _, mantissa := range mantissas {
			// Construct the decimal value directly. Multiplying a mantissa by
			// math.Pow can land one ULP away from the intended decimal value
			// (for example, 2.4999999999999998e-06 instead of 2.5e-06).
			literal := strconv.FormatFloat(mantissa, 'f', -1, 64) + "e" + strconv.Itoa(exponent)
			bound, _ := strconv.ParseFloat(literal, 64)

			if bound < lower {
				continue
			}

			if bound > upper {
				return bounds
			}

			bounds = append(bounds, bound)
		}

		exponent++
	}
}

type namedListItem interface {
	getName() string
}

func validateNamedList[T namedListItem](kind string, items []T, validateFn func(T) error) error {
	names := make(map[string]struct{}, len(items))

	for _, item := range items {
		name := item.getName()

		if isEmpty(name) {
			return requiredFieldErr(kind, "name")
		}

		if err := validateFn(item); err != nil {
			return err
		}

		if _, ok := names[name]; ok {
			return fmt.Errorf("%s: duplicated name '%s'", kind, name)
		}

		names[name] = struct{}{}
	}

	return nil
}

type specValidator struct {
	spec *Spec

	attributeSets    map[string]*AttributeSet
	bucketBoundsSets map[string]*BucketBoundsSet
	errorTypeSets    map[string]*ErrorTypeSet
}

func newSpecValidator(spec *Spec) *specValidator {
	return &specValidator{
		spec: spec,

		attributeSets:    make(map[string]*AttributeSet, len(spec.AttributeSets)),
		bucketBoundsSets: make(map[string]*BucketBoundsSet, len(spec.BucketBoundsSets)),
		errorTypeSets:    make(map[string]*ErrorTypeSet, len(spec.ErrorTypeSets)),
	}
}

func (v *specValidator) validate() error {
	if isEmpty(v.spec.Package) {
		return requiredFieldErr("spec", "package")
	}

	for _, attributeSet := range v.spec.AttributeSets {
		if err := v.validateAttributeSet(attributeSet); err != nil {
			return err
		}
	}

	for _, bucketBoundsSet := range v.spec.BucketBoundsSets {
		if err := v.validateBucketBoundsSet(bucketBoundsSet); err != nil {
			return err
		}
	}

	for _, errorSet := range v.spec.ErrorTypeSets {
		if err := v.validateErrorTypeSet(errorSet); err != nil {
			return err
		}
	}

	if err := validateNamedList("group", v.spec.Groups, v.validateGroup); err != nil {
		return err
	}

	return nil
}

func (v *specValidator) validateAttribute(attribute *Attribute) error {
	if isEmpty(attribute.Name) {
		return requiredFieldErr("attribute", "name")
	}

	if isEmpty(attribute.Type) {
		return requiredFieldErr("attribute", "type")
	}

	if isEmpty(attribute.Arg) {
		attribute.Arg = toLowerCamelCase(attribute.Name)
	}

	return nil
}

func (v *specValidator) validateAttributeSet(attributeSet *AttributeSet) error {
	if isEmpty(attributeSet.Name) {
		return requiredFieldErr("attribute_set", "name")
	}

	if _, ok := v.attributeSets[attributeSet.Name]; ok {
		return fmt.Errorf("duplicated attribute set name '%s'", attributeSet.Name)
	}

	if err := validateNamedList("attribute", attributeSet.Attributes, v.validateAttribute); err != nil {
		return err
	}

	v.attributeSets[attributeSet.Name] = attributeSet

	return nil
}

func (v *specValidator) validateBucketBoundsSet(bucketBoundsSet *BucketBoundsSet) error {
	if isEmpty(bucketBoundsSet.Name) {
		return requiredFieldErr("bucket_bounds_set", "name")
	}

	if _, ok := v.bucketBoundsSets[bucketBoundsSet.Name]; ok {
		return fmt.Errorf("duplicated bucket bounds set name '%s'", bucketBoundsSet.Name)
	}

	lower := bucketBoundsSet.LowerBound
	upper := bucketBoundsSet.UpperBound
	if lower != 0 && upper != 0 {
		// Calculate the bounds
		if lower < 0 {
			return fmt.Errorf("invalid lower bound '%f'", lower)
		}

		if upper < 0 {
			return fmt.Errorf("invalid upper bound '%f'", upper)
		}

		if upper < lower {
			return fmt.Errorf("upper bound '%f' is less than lower bound '%f'", upper, lower)
		}

		bucketBoundsSet.Bounds = calculateBucketBounds(lower, upper)
	}

	v.bucketBoundsSets[bucketBoundsSet.Name] = bucketBoundsSet

	return nil
}

func (v *specValidator) validateErrorType(errType *ErrorType) error {
	if isEmpty(errType.Name) {
		return requiredFieldErr("error_type", "name")
	}

	if isEmpty(errType.Value) {
		return requiredFieldErr("error_type", "value")
	}

	return nil
}

func (v *specValidator) validateErrorTypeSet(errorTypeSet *ErrorTypeSet) error {
	if isEmpty(errorTypeSet.Name) {
		return requiredFieldErr("error_type_set", "name")
	}

	if _, ok := v.errorTypeSets[errorTypeSet.Name]; ok {
		return fmt.Errorf("duplicated error type set name '%s'", errorTypeSet.Name)
	}

	if err := validateNamedList("error_type", errorTypeSet.ErrorTypes, v.validateErrorType); err != nil {
		return err
	}

	v.errorTypeSets[errorTypeSet.Name] = errorTypeSet

	return nil
}

func (v *specValidator) validateMetric(metric *Metric) error {
	if isEmpty(metric.Name) {
		return requiredFieldErr("metric", "name")
	}

	if isEmpty(metric.Type) {
		return requiredFieldErr("metric", "type")
	}

	if isEmpty(metric.DataType) {
		metric.DataType = DataTypeInteger
	}

	if !isEmpty(metric.AttributeSet) {
		set, ok := v.attributeSets[metric.AttributeSet]
		if !ok {
			return fmt.Errorf("unknown attribute set '%s'", metric.AttributeSet)
		}

		metric.Attributes = append(metric.Attributes, set.Attributes...)
	}

	if err := validateNamedList("attribute", metric.Attributes, v.validateAttribute); err != nil {
		return err
	}

	if len(metric.BucketBounds) == 0 && !isEmpty(metric.BucketBoundsSet) {
		set, ok := v.bucketBoundsSets[metric.BucketBoundsSet]
		if !ok {
			return fmt.Errorf("unknown bucket bounds set '%s'", metric.BucketBoundsSet)
		}

		metric.BucketBounds = set.Bounds
	}

	if !isEmpty(metric.ErrorTypeSet) {
		set, ok := v.errorTypeSets[metric.ErrorTypeSet]
		if !ok {
			return fmt.Errorf("unknown error type set '%s'", metric.ErrorTypeSet)
		}

		metric.ErrorTypes = append(metric.ErrorTypes, set.ErrorTypes...)
	}

	if err := validateNamedList("error_type", metric.ErrorTypes, v.validateErrorType); err != nil {
		return err
	}

	return nil
}

func (v *specValidator) validateGroup(group *Group) error {
	if isEmpty(group.Name) {
		return requiredFieldErr("group", "name")
	}

	if err := validateNamedList("metric", group.Metrics, v.validateMetric); err != nil {
		return err
	}

	return nil
}
