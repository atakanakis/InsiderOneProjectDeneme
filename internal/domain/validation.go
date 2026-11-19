package domain

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Content limits per channel, counted in characters (runes), not bytes.
//
//   - SMS: 1600 characters, i.e. up to 10 concatenated 160-character segments.
//   - Email: 100,000 characters of body.
//   - Push: 1,000 characters; APNs/FCM payloads are capped around 4 KB.
var MaxContentLength = map[Channel]int{
	ChannelSMS:   1600,
	ChannelEmail: 100_000,
	ChannelPush:  1000,
}

const (
	MaxIdempotencyKeyLength = 128
	MaxScheduleAhead        = 30 * 24 * time.Hour
	MaxBatchSize            = 1000
)

// E.164: a plus sign, a non-zero country code digit, up to 15 digits total.
var e164 = regexp.MustCompile(`^\+[1-9]\d{7,14}$`)

// FieldError describes one invalid field. Field uses the JSON field name so
// API clients can map it back to their request.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError collects every problem with a request instead of stopping
// at the first one, so clients can fix everything in one round trip.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

func (e *ValidationError) Add(field, format string, args ...any) {
	e.Fields = append(e.Fields, FieldError{Field: field, Message: fmt.Sprintf(format, args...)})
}

// Err returns nil when no field error was recorded.
func (e *ValidationError) Err() error {
	if len(e.Fields) == 0 {
		return nil
	}
	return e
}

// HasNUL reports whether s contains a NUL byte. JSON allows "\u0000" but
// PostgreSQL TEXT does not, so it must be rejected as a 422 here instead of
// surfacing as a 500 from the insert.
func HasNUL(s string) bool { return strings.ContainsRune(s, 0) }

// ValidateRecipient checks the recipient format for the given channel.
func ValidateRecipient(ch Channel, recipient string, v *ValidationError) {
	if strings.TrimSpace(recipient) == "" {
		v.Add("recipient", "is required")
		return
	}
	if HasNUL(recipient) {
		v.Add("recipient", "must not contain NUL characters")
		return
	}
	switch ch {
	case ChannelSMS:
		if !e164.MatchString(recipient) {
			v.Add("recipient", "must be an E.164 phone number such as +905551234567")
		}
	case ChannelEmail:
		addr, err := mail.ParseAddress(recipient)
		if err != nil || addr.Address != recipient {
			v.Add("recipient", "must be a plain email address such as user@example.com")
		}
	case ChannelPush:
		if n := len(recipient); n < 8 || n > 512 || strings.ContainsAny(recipient, " \t\r\n") {
			v.Add("recipient", "must be a device token of 8-512 characters without whitespace")
		}
	}
}

// ValidateContent checks that the (already rendered) content is present and
// within the channel's character limit.
func ValidateContent(ch Channel, content string, v *ValidationError) {
	if strings.TrimSpace(content) == "" {
		v.Add("content", "is required")
		return
	}
	if limit, ok := MaxContentLength[ch]; ok {
		if n := utf8.RuneCountInString(content); n > limit {
			v.Add("content", "is %d characters, the %s limit is %d", n, ch, limit)
		}
	}
	if !utf8.ValidString(content) {
		v.Add("content", "must be valid UTF-8")
	}
	if HasNUL(content) {
		v.Add("content", "must not contain NUL characters")
	}
}

// ValidateSchedule rejects schedules too far in the future. A time in the
// past is accepted and simply means "send as soon as possible".
func ValidateSchedule(at *time.Time, now time.Time, v *ValidationError) {
	if at == nil {
		return
	}
	if at.After(now.Add(MaxScheduleAhead)) {
		v.Add("scheduledAt", "must be within %d days from now", int(MaxScheduleAhead.Hours()/24))
	}
}

func ValidateIdempotencyKey(key *string, v *ValidationError) {
	if key == nil {
		return
	}
	if *key == "" || len(*key) > MaxIdempotencyKeyLength || HasNUL(*key) {
		v.Add("idempotencyKey", "must be 1-%d characters without NUL", MaxIdempotencyKeyLength)
	}
}
