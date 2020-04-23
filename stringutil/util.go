package stringutil

import "fmt"

func Shorten(s string, l int) string {
	if len(s) < l {
		return s
	}
	k := l / 2
	return s[:k] + " [...] " + s[len(s)-k:] + fmt.Sprintf(" [%d]", len(s))
}

// trim string to length.
func Trim(s string, l int, ellipsis string) string {
	if len(s) < l {
		return s
	}
	return s[:l] + ellipsis
}
