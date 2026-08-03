package pkg

import (
	"strings"
	"unicode"
)

// toUpperCamelCase converts "my_field-name" → "MyFieldName" (UpperCamelCase / PascalCase)
func toUpperCamelCase(s string) string {
	return camelCase(s, true)
}

// toLowerCamelCase converts "my_field-name" → "myFieldName" (lowerCamelCase)
func toLowerCamelCase(s string) string {
	return camelCase(s, false)
}

func camelCase(s string, upper bool) string {
	var b strings.Builder
	capitalizeNext := upper

	for _, r := range s {
		if r == '_' || r == '-' || r == '.' {
			capitalizeNext = true
			continue
		}
		if capitalizeNext {
			b.WriteRune(unicode.ToUpper(r))
			capitalizeNext = false
		} else {
			b.WriteRune(unicode.ToLower(r))
		}
	}

	return b.String()
}

func toLowerSnakeCase(s string) string {
	return strings.ToLower(s)
}

func getDataType(dataType DataType) string {
	switch dataType {
	case DataTypeInteger:
		return "int64"
	case DataTypeFloat:
		return "float64"
	default:
		return "int64"
	}
}

func dict(values ...any) map[string]any {
	result := make(map[string]any)
	for i := 0; i < len(values); i += 2 {
		result[values[i].(string)] = values[i+1]
	}
	return result
}
