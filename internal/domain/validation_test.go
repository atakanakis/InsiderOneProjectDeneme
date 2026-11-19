package domain

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateRecipient(t *testing.T) {
	cases := []struct {
		name    string
		channel Channel
		value   string
		ok      bool
	}{
		{"sms e164", ChannelSMS, "+905551234567", true},
		{"sms missing plus", ChannelSMS, "905551234567", false},
		{"sms too short", ChannelSMS, "+9055", false},
		{"sms letters", ChannelSMS, "+90555ABC4567", false},
		{"email plain", ChannelEmail, "user@example.com", true},
		{"email with display name", ChannelEmail, "User <user@example.com>", false},
		{"email garbage", ChannelEmail, "not-an-email", false},
		{"email without a dotted domain", ChannelEmail, "a@b", false},
		{"email domain ends with a dot", ChannelEmail, "a@b.", false},
		{"push token", ChannelPush, "fcm-token-abcdef123456", true},
		{"push too short", ChannelPush, "abc", false},
		{"push whitespace", ChannelPush, "token with spaces", false},
		{"empty", ChannelSMS, "  ", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var v ValidationError
			ValidateRecipient(tc.channel, tc.value, &v)
			assert.Equal(t, tc.ok, v.Err() == nil, "errors: %v", v.Fields)
		})
	}
}

func TestValidateContentLimitsCountCharactersNotBytes(t *testing.T) {
	var v ValidationError
	// 1600 Turkish characters are 3200 bytes but exactly at the SMS limit.
	ValidateContent(ChannelSMS, strings.Repeat("ş", 1600), &v)
	assert.NoError(t, v.Err())

	v = ValidationError{}
	ValidateContent(ChannelSMS, strings.Repeat("a", 1601), &v)
	require.Error(t, v.Err())
	assert.Contains(t, v.Fields[0].Message, "1601")

	v = ValidationError{}
	ValidateContent(ChannelPush, strings.Repeat("a", 1001), &v)
	assert.Error(t, v.Err())

	v = ValidationError{}
	ValidateContent(ChannelEmail, "   ", &v)
	assert.Error(t, v.Err(), "blank content is rejected")
}

func TestValidateSchedule(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	past, soon, tooFar := now.Add(-time.Hour), now.Add(time.Hour), now.Add(31*24*time.Hour)

	for _, tc := range []struct {
		at *time.Time
		ok bool
	}{{nil, true}, {&past, true}, {&soon, true}, {&tooFar, false}} {
		var v ValidationError
		ValidateSchedule(tc.at, now, &v)
		assert.Equal(t, tc.ok, v.Err() == nil)
	}
}

func TestValidationErrorCollectsEveryField(t *testing.T) {
	var v ValidationError
	ValidateRecipient(ChannelSMS, "bad", &v)
	ValidateContent(ChannelSMS, "", &v)
	err := v.Err()
	require.Error(t, err)
	assert.Len(t, v.Fields, 2)
	assert.Contains(t, err.Error(), "recipient")
	assert.Contains(t, err.Error(), "content")
}

func TestStatusRules(t *testing.T) {
	assert.True(t, StatusPending.Cancellable())
	assert.True(t, StatusRetrying.Cancellable())
	assert.False(t, StatusProcessing.Cancellable(), "may already be on the wire")
	assert.False(t, StatusSent.Cancellable())

	for _, s := range []Status{StatusSent, StatusFailed, StatusCancelled} {
		assert.True(t, s.Terminal(), s)
	}
	assert.False(t, StatusRetrying.Terminal())
}

func TestPriorityRankOrdersHighFirst(t *testing.T) {
	assert.Less(t, PriorityHigh.Rank(), PriorityNormal.Rank())
	assert.Less(t, PriorityNormal.Rank(), PriorityLow.Rank())
	for _, p := range Priorities {
		assert.Equal(t, p, PriorityFromRank(p.Rank()))
	}
}

func TestRender(t *testing.T) {
	out, err := Render("Hi {{.name}}, {{ .discount }}% off!", map[string]string{"name": "Ada", "discount": "20"}, 1000)
	require.NoError(t, err)
	assert.Equal(t, "Hi Ada, 20% off!", out)

	_, err = Render("Hi {{.name}} {{.city}}", map[string]string{}, 1000)
	require.Error(t, err, "missing variables must not render as an empty string")
	assert.Contains(t, err.Error(), "name, city")
}

func TestParseTemplateRejectsAnythingButPlaceholders(t *testing.T) {
	c, err := CompileTemplate("{{.a}} and {{.b}} and {{.a}}")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, c.Variables())

	for _, body := range []string{
		"Hi {{.name",
		"Hi {{name}}",
		"Hi }} there",
		`{{printf "%999999d" 1}}`,
		`{{range .x}}loop{{end}}`,
		`{{define "x"}}{{end}}`,
	} {
		_, err := CompileTemplate(body)
		assert.Error(t, err, body)
	}
}

// Regression: with text/template, {{printf "%999999d" 1}} let one request
// render gigabytes. Now the output size is checked before allocating.
func TestRenderRefusesOversizedOutput(t *testing.T) {
	_, err := Render(`{{printf "%999999d" 1}}`, nil, 1000)
	require.Error(t, err)

	big := strings.Repeat("x", 600)
	_, err = Render("{{.v}}{{.v}}", map[string]string{"v": big}, 1000)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1200 characters, the limit is 1000")

	out, err := Render("{{.v}}", map[string]string{"v": big}, 1000)
	require.NoError(t, err)
	assert.Len(t, out, 600)

	// The limit is in characters, not bytes.
	out, err = Render("{{.v}}", map[string]string{"v": strings.Repeat("ş", 1000)}, 1000)
	require.NoError(t, err)
	assert.Len(t, out, 2000)
}

// Regression (review 2): 16,000 placeholders used to be scanned for every
// item of a batch. Length is now computed from per-variable counts.
func TestRenderedLengthDoesNotScaleWithPlaceholderCount(t *testing.T) {
	c, err := CompileTemplate(strings.Repeat("{{.a}}", 16_000))
	require.NoError(t, err)
	start := time.Now()
	for range 1000 {
		n, err := c.RenderedLength(map[string]string{"a": "xxxxxxx"})
		require.NoError(t, err)
		require.Equal(t, 112_000, n)
	}
	assert.Less(t, time.Since(start), 100*time.Millisecond)
}

// Regression (review 3): a template with 9,900 variables and a 1000-item
// batch without variables turned a 99 KB request into a 68 MB response.
func TestValidationOutputIsBounded(t *testing.T) {
	var body strings.Builder
	for i := range 9900 {
		fmt.Fprintf(&body, "{{.v%d}}", i)
	}
	c, err := CompileTemplate(body.String())
	require.NoError(t, err)
	_, err = c.Render(nil, 100_000)
	require.Error(t, err)
	assert.Less(t, len(err.Error()), 200)
	assert.Contains(t, err.Error(), "and 9895 more")

	var v ValidationError
	for i := range 1000 {
		v.Add(fmt.Sprintf("notifications[%d].variables", i), "%s", strings.Repeat("x", 5000))
	}
	assert.Len(t, v.Fields, MaxFieldErrors)
	assert.Equal(t, 900, v.Omitted)
	assert.LessOrEqual(t, len(v.Fields[0].Message), 310)

	_, err = CompileTemplate("{{." + strings.Repeat("a", 65) + "}}")
	assert.Error(t, err, "variable names are capped at 64 characters")
}
