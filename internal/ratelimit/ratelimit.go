// Package ratelimit implements a distributed token bucket in Redis.
//
// Each channel has one bucket shared by every worker process, so the
// "100 messages per second per channel" limit holds no matter how many
// workers are running. The whole read-refill-take step runs as one Lua
// script, which Redis executes atomically, and uses Redis' own clock so
// workers with skewed clocks cannot cheat.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Bucket semantics: tokens refill continuously at `rate` per second up to
// `burst`. Taking n tokens succeeds partially: the caller gets as many as are
// available right now, plus how long to wait for the next one.
//
// Upper bound: any 1-second window can contain at most rate + burst sends
// (a full bucket plus one second of refill). With burst much smaller than
// rate the steady state is exactly `rate` per second.
var takeScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
  tokens = burst
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(burst, tokens + elapsed * rate / 1000)

local granted = math.min(requested, math.floor(tokens))
tokens = tokens - granted

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', key, 60000)

local wait_ms = 0
if granted < requested then
  wait_ms = math.ceil((1 - (tokens - math.floor(tokens))) * 1000 / rate)
end
return {granted, wait_ms}
`)

// refundScript gives back tokens that were taken but not used (for example
// the queue turned out to be empty), so idle polling does not eat capacity.
var refundScript = redis.NewScript(`
local key = KEYS[1]
local burst = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local tokens = tonumber(redis.call('HGET', key, 'tokens'))
if tokens == nil then return 0 end
tokens = math.min(burst, tokens + n)
redis.call('HSET', key, 'tokens', tostring(tokens))
return 1
`)

type Limiter struct {
	rdb   *redis.Client
	rate  float64
	burst int
}

func New(rdb *redis.Client, ratePerSecond float64, burst int) *Limiter {
	return &Limiter{rdb: rdb, rate: ratePerSecond, burst: burst}
}

// Take asks for up to n tokens for the given key (one key per channel). It
// never blocks: it returns how many tokens were granted and, when fewer than
// requested, roughly how long until the next token is available.
func (l *Limiter) Take(ctx context.Context, key string, n int) (int, time.Duration, error) {
	res, err := takeScript.Run(ctx, l.rdb, []string{"ratelimit:" + key}, l.rate, l.burst, n).Int64Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("rate limit script: %w", err)
	}
	return int(res[0]), time.Duration(res[1]) * time.Millisecond, nil
}

// Refund returns n unused tokens to the bucket (never above burst).
func (l *Limiter) Refund(ctx context.Context, key string, n int) error {
	if n <= 0 {
		return nil
	}
	return refundScript.Run(ctx, l.rdb, []string{"ratelimit:" + key}, l.burst, n).Err()
}
