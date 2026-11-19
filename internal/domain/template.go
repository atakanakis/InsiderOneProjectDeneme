package domain

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Templates use a deliberately tiny syntax: {{.name}} (spaces inside the
// braces are allowed) is replaced by the variable "name", and nothing else is
// evaluated.
//
// An earlier version used text/template. Template bodies come from API
// callers, and text/template gives them printf, range and define: one small
// request with {{printf "%999999d" 1}} repeated in a template rendered
// gigabytes before validation could reject it. Plain substitution cannot loop
// or pad, and Render refuses before allocating anything larger than the
// caller's limit.
var placeholder = regexp.MustCompile(`\{\{\s*\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

// ParseTemplate checks a template body and returns the variable names it
// uses, in order of first appearance.
func ParseTemplate(body string) ([]string, error) {
	matches := placeholder.FindAllStringSubmatchIndex(body, -1)
	var names []string
	seen := map[string]bool{}
	prev := 0
	for _, m := range matches {
		if err := checkLiteral(body[prev:m[0]]); err != nil {
			return nil, err
		}
		name := body[m[2]:m[3]]
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
		prev = m[1]
	}
	if err := checkLiteral(body[prev:]); err != nil {
		return nil, err
	}
	return names, nil
}

// checkLiteral rejects braces outside a valid placeholder, so a typo such as
// "{{.name" or "{{name}}" is reported when the template is created instead
// of being sent to a customer verbatim.
func checkLiteral(s string) error {
	if i := strings.Index(s, "{{"); i >= 0 {
		return fmt.Errorf("unsupported expression near %q: only {{.name}} placeholders are allowed", snippet(s[i:]))
	}
	if i := strings.Index(s, "}}"); i >= 0 {
		return fmt.Errorf("unmatched }} near %q", snippet(s[max(0, i-10):]))
	}
	return nil
}

func snippet(s string) string {
	if len(s) > 20 {
		return s[:20] + "..."
	}
	return s
}

// Render fills a template body with variables, e.g. "Hi {{.name}}". It fails
// if a variable is missing, rather than sending "Hi " to a customer, and if
// the result would exceed maxBytes. The size is computed before anything is
// allocated.
func Render(body string, vars map[string]string, maxBytes int) (string, error) {
	if _, err := ParseTemplate(body); err != nil {
		return "", err
	}
	matches := placeholder.FindAllStringSubmatchIndex(body, -1)

	size := len(body)
	var missing []string
	for _, m := range matches {
		name := body[m[2]:m[3]]
		v, ok := vars[name]
		if !ok {
			if !slices.Contains(missing, name) {
				missing = append(missing, name)
			}
			continue
		}
		size += len(v) - (m[1] - m[0])
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("missing variables: %s", strings.Join(missing, ", "))
	}
	if size > maxBytes {
		return "", fmt.Errorf("rendered content would exceed %d bytes", maxBytes)
	}

	var b strings.Builder
	b.Grow(size)
	prev := 0
	for _, m := range matches {
		b.WriteString(body[prev:m[0]])
		b.WriteString(vars[body[m[2]:m[3]]])
		prev = m[1]
	}
	b.WriteString(body[prev:])
	return b.String(), nil
}
