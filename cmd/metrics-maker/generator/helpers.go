package generator

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
		if r == '_' || r == '-' {
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
