package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultsAreValid(t *testing.T) {
	c, err := Load("test")
	require.NoError(t, err)
	assert.Equal(t, 100, c.RateLimitPerSecond)
	assert.Equal(t, 5, c.MaxAttempts)
}

func TestRejectsSettingsThatWouldPanicOrSpin(t *testing.T) {
	for key, value := range map[string]string{
		"REAP_INTERVAL":             "0s",  // time.NewTicker panics
		"POLL_INTERVAL":             "0s",  // dispatcher busy-loops on an empty queue
		"BREAKER_FAILURE_THRESHOLD": "0",   // breaker trips on the first failure
		"RATE_LIMIT_PER_SECOND":     "0",   // nothing would ever be sent
		"RETRY_MAX_DELAY":           "1ms", // below the base delay
		"LEASE_DURATION":            "1s",  // shorter than the provider timeout
		"MAX_ATTEMPTS":              "abc", // not a number
	} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, value)
			_, err := Load("test")
			assert.ErrorContains(t, err, key)
		})
	}
}
