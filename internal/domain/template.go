package domain

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

// ParseTemplate compiles a template body. Missing variables are an error at
// render time rather than silently producing "<no value>" in a customer's SMS.
func ParseTemplate(body string) (*template.Template, error) {
	return template.New("notification").Option("missingkey=error").Parse(body)
}

// Render fills a template body with variables, e.g. "Hi {{.name}}".
func Render(body string, vars map[string]string) (string, error) {
	tpl, err := ParseTemplate(body)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	if vars == nil {
		vars = map[string]string{}
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, vars); err != nil {
		// text/template errors are verbose; keep the useful tail for API users.
		msg := err.Error()
		if i := strings.LastIndex(msg, ": "); i >= 0 {
			msg = msg[i+2:]
		}
		return "", fmt.Errorf("render template: %s", msg)
	}
	return buf.String(), nil
}
