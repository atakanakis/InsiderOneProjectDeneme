package domain

import (
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
	names, err := ParseTemplate("{{.a}} and {{.b}} and {{.a}}")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, names)

	for _, body := range []string{
		"Hi {{.name",
		"Hi {{name}}",
		"Hi }} there",
		`{{printf "%999999d" 1}}`,
		`{{range .x}}loop{{end}}`,
		`{{define "x"}}{{end}}`,
	} {
		_, err := ParseTemplate(body)
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
	assert.Contains(t, err.Error(), "exceed 1000 bytes")

	out, err := Render("{{.v}}", map[string]string{"v": big}, 1000)
	require.NoError(t, err)
	assert.Len(t, out, 600)
}
