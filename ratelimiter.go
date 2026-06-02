package gqueue

import (
	"context"
	"time"
)

const luaAllow = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local max_tokens = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])

local tokens = tonumber(redis.call('HGET', key, 'tokens'))
local last_refill = tonumber(redis.call('HGET', key, 'last_refill'))

if not tokens then
    tokens = max_tokens
end
if not last_refill then
    last_refill = now
end

local elapsed = now - last_refill
local refill = math.floor(elapsed * refill_rate)

tokens = math.min(max_tokens, tokens + refill)

if refill > 0 then
    redis.call('HSET', key, 'last_refill', now)
end

if tokens >= 1 then
    redis.call('HSET', key, 'tokens', tokens - 1)
    return 1
else
    redis.call('HSET', key, 'tokens', tokens)
    return 0
end
`

// Allow checks the token bucket for this queue. Returns true if a job can be processed.
// maxTokens = max jobs allowed in the window, refillRate = tokens per second.
func (q *Queue) Allow(ctx context.Context, maxTokens int, refillRate float64) bool {
	now := float64(time.Now().UnixNano()) / 1e9
	key := "ratelimit:" + q.Name

	result, err := q.client.Eval(ctx, luaAllow, []string{key},
		now,
		maxTokens,
		refillRate,
	).Int()

	if err != nil {
		return true
	}
	return result == 1
}
