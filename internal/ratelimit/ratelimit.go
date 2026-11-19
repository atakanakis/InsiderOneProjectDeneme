// Package ratelimit implements a distributed sliding-window rate limiter in
// Redis.
//
// Requirement: at most N messages per second per channel. Each channel has
// one window shared by every worker process, so the limit holds no matter
// how many workers run. The check-and-take step is a Lua script, which Redis
// runs atomically, and it uses Redis' own clock so workers with skewed clocks
// cannot over-send.
//
// Why a sliding window log and not a token bucket: a token bucket with rate
// R and burst B allows up to R+B sends inside some one-second window (a full
// bucket plus a second of refill). The requirement is a hard maximum, so we
// record a timestamp for every permit in a sorted set and only grant a new
// one while fewer than N permits exist in the last second. Memory is
// O(N) per channel (100 entries), which is nothing.
//
// Safety margin: a permit is taken a few milliseconds before the HTTP
// request actually leaves (the worker still has to claim the row). If one
// second's requests leave late and the next second's leave early, the
// provider could observe slightly more than N in a second. The window is
// therefore 1s plus a small margin (default 50ms), trading up to ~5% of
// peak throughput for never exceeding the provider's limit.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var takeScript = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local token = ARGV[4]

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- Forget permits older than the window: the window is (now - window, now].
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window_ms)
local used = redis.call('ZCARD', key)
local granted = math.max(0, math.min(requested, limit - used))
for i = 1, granted do
  redis.call('ZADD', key, now, token .. ':' .. i)
end
redis.call('PEXPIRE', key, window_ms * 2)

-- When short of permits, tell the caller when the oldest one expires.
local wait_ms = 0
if granted < requested then
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  if oldest[2] then
    wait_ms = tonumber(oldest[2]) + window_ms - now
  end
  wait_ms = math.max(1, wait_ms)
end
return {granted, wait_ms}
`)

// Grant is the result of Take. Keep it to Refund permits that went unused.
type Grant struct {
	N     int           // permits granted, 0..requested
	Wait  time.Duration // when N < requested: time until a permit frees up
	token string
}

// keyPrefix is versioned: an older build used a token bucket stored as a
// hash under "ratelimit:<channel>", and a rolling deploy must not feed that
// key to this script (WRONGTYPE). Bump the version when the layout changes.
const keyPrefix = "ratelimit:v2:"

type Limiter struct {
	rdb    *redis.Client
	limit  int
	window time.Duration
}

// New allows at most `perSecond` permits per key in any window of one
// second plus margin.
func New(rdb *redis.Client, perSecond int, margin time.Duration) *Limiter {
	return &Limiter{rdb: rdb, limit: perSecond, window: time.Second + margin}
}

// Take asks for up to n permits for the given key (one key per channel). It
// never blocks: it returns how many were granted right now.
func (l *Limiter) Take(ctx context.Context, key string, n int) (Grant, error) {
	token := uuid.NewString()
	res, err := takeScript.Run(ctx, l.rdb, []string{keyPrefix + key},
		l.limit, l.window.Milliseconds(), n, token).Int64Slice()
	if err != nil {
		return Grant{}, fmt.Errorf("rate limit script: %w", err)
	}
	return Grant{N: int(res[0]), Wait: time.Duration(res[1]) * time.Millisecond, token: token}, nil
}

// Refund gives back the last `unused` permits of a grant (for example when
// the queue turned out to be empty), so idle polling does not eat capacity.
func (l *Limiter) Refund(ctx context.Context, key string, g Grant, unused int) error {
	unused = min(unused, g.N)
	if unused <= 0 {
		return nil
	}
	members := make([]any, 0, unused)
	for i := g.N - unused + 1; i <= g.N; i++ {
		members = append(members, fmt.Sprintf("%s:%d", g.token, i))
	}
	return l.rdb.ZRem(ctx, keyPrefix+key, members...).Err()
}
