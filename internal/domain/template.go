package domain

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Templates use a deliberately tiny syntax: {{.name}} (spaces inside the
// braces are allowed) is replaced by the variable "name", and nothing else is
// evaluated.
//
// An earlier version used text/template. Template bodies come from API
// callers, and text/template gives them printf, range and define: one small
// request with {{printf "%999999d" 1}} repeated in a template rendered
// gigabytes before validation could reject it. Plain substitution cannot loop
// or pad. A template is compiled once per request and Render computes the
// output length from per-variable counts before building anything, so the
// cost of rejecting an oversized result does not depend on how many
// placeholders the body has.
var placeholder = regexp.MustCompile(`\{\{\s*\.([A-Za-z_][A-Za-z0-9_]{0,63})\s*\}\}`)

// CompiledTemplate is a template body split into literal text and
// placeholders.
type CompiledTemplate struct {
	literals     []string       // len(names)+1 pieces of text around the placeholders
	names        []string       // placeholder at each position
	uses         map[string]int // how often each variable appears
	order        []string       // variable names in order of first appearance
	literalRunes int
}

// CompileTemplate checks a template body and prepares it for rendering.
func CompileTemplate(body string) (*CompiledTemplate, error) {
	matches := placeholder.FindAllStringSubmatchIndex(body, -1)
	c := &CompiledTemplate{uses: map[string]int{}}
	prev := 0
	for _, m := range matches {
		lit := body[prev:m[0]]
		if err := checkLiteral(lit); err != nil {
			return nil, err
		}
		name := body[m[2]:m[3]]
		c.literals = append(c.literals, lit)
		c.names = append(c.names, name)
		if c.uses[name] == 0 {
			c.order = append(c.order, name)
		}
		c.uses[name]++
		prev = m[1]
	}
	rest := body[prev:]
	if err := checkLiteral(rest); err != nil {
		return nil, err
	}
	c.literals = append(c.literals, rest)
	for _, l := range c.literals {
		c.literalRunes += utf8.RuneCountInString(l)
	}
	return c, nil
}

// Variables returns the variable names the template uses, in order of first
// appearance.
func (c *CompiledTemplate) Variables() []string { return c.order }

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

// RenderedLength is the length in characters (runes) the template would
// produce with these variables. It fails if a variable is missing, rather
// than sending "Hi " to a customer. Its cost depends on the number of
// distinct variables, not on how often they are used.
func (c *CompiledTemplate) RenderedLength(vars map[string]string) (int, error) {
	n := c.literalRunes
	var missing []string
	for _, name := range c.order {
		v, ok := vars[name]
		if !ok {
			missing = append(missing, name)
			continue
		}
		n += c.uses[name] * utf8.RuneCountInString(v)
	}
	if len(missing) > 0 {
		// Name a few, count the rest: the message must stay small however
		// many variables the template has.
		const show = 5
		if extra := len(missing) - show; extra > 0 {
			return 0, fmt.Errorf("missing variables: %s and %d more", strings.Join(missing[:show], ", "), extra)
		}
		return 0, fmt.Errorf("missing variables: %s", strings.Join(missing, ", "))
	}
	return n, nil
}

// Render fills the template. It refuses, before allocating, any result
// longer than maxRunes characters.
func (c *CompiledTemplate) Render(vars map[string]string, maxRunes int) (string, error) {
	n, err := c.RenderedLength(vars)
	if err != nil {
		return "", err
	}
	if n > maxRunes {
		return "", fmt.Errorf("rendered content would be %d characters, the limit is %d", n, maxRunes)
	}
	var b strings.Builder
	for i, name := range c.names {
		b.WriteString(c.literals[i])
		b.WriteString(vars[name])
	}
	b.WriteString(c.literals[len(c.literals)-1])
	return b.String(), nil
}

// Render compiles body and fills it; for one-off use.
func Render(body string, vars map[string]string, maxRunes int) (string, error) {
	c, err := CompileTemplate(body)
	if err != nil {
		return "", err
	}
	return c.Render(vars, maxRunes)
}
