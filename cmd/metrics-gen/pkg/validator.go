package pkg

import "fmt"

func isEmpty(field string) bool {
	return field == ""
}

func requiredFieldErr(kind, fieldName string) error {
	return fmt.Errorf("%s: field '%s' is required", kind, fieldName)
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

	attributeSets map[string]*AttributeSet
}

func newSpecValidator(spec *Spec) *specValidator {
	return &specValidator{
		spec: spec,

		attributeSets: make(map[string]*AttributeSet, len(spec.AttributeSets)),
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
