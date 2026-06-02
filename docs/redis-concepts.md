# Redis Concepts

## What is Redis?
Redis is an in-memory data store. Data lives in RAM, making reads and writes extremely fast. gqueue uses Redis to store jobs, track their state, and coordinate workers.

## Keys
Everything in Redis is stored under a key. Keys are just strings. You design your key names to avoid collisions.

```
job:abc123       -> hash of job fields
ratelimit:myqueue -> hash for token bucket state
active-jobs      -> sorted set of in-progress jobs
dead-letter      -> list of failed job IDs
```

## Lists (LPUSH / BRPOP)
A Redis list is an ordered sequence of strings. gqueue uses lists as the job queue.

- `LPUSH key value` — push to the left (front) of the list
- `BRPOP key timeout` — pop from the right (back), **blocking** until an item is available

This gives you a FIFO queue. Workers block on `BRPOP` and wake up the moment a job arrives — no polling needed.

## Sorted Sets (ZADD / ZRANGEBYSCORE / ZREM)
A sorted set stores members with a numeric score. Members are always ordered by score.

gqueue uses sorted sets for:
- **Delayed jobs** — score = Unix timestamp when the job should run
- **Active jobs** — score = Unix timestamp when the job started (for stall detection)

```
ZADD delayed 1700000000 "job-id"     // schedule job for that timestamp
ZRANGEBYSCORE delayed 0 <now>        // get all jobs whose time has come
ZREM active-jobs "job-id"            // remove when job finishes
```

## Hashes (HSET / HGET)
A hash stores a map of field-value pairs under one key. gqueue stores job metadata here.

```
HSET job:abc123 status "active" startedAt "2025-01-01T00:00:00Z"
HGET job:abc123 status   -> "active"
```

## Atomicity
Redis executes each command atomically — no two commands can interleave mid-execution. But multiple separate commands are NOT atomic together. That's where Lua scripts come in.

## Lua Scripts (EVAL)
You can send a Lua script to Redis and it runs the entire script atomically — as one uninterruptible operation. This solves race conditions where two workers might read and write the same data simultaneously.

```go
result, err := client.Eval(ctx, luaScript, []string{key}, arg1, arg2).Int()
```

- `KEYS[1]` — the Redis key(s) your script operates on
- `ARGV[1]`, `ARGV[2]` — arguments passed to the script

In gqueue, the token bucket rate limiter uses a Lua script so two workers can't both read "1 token left" and both decrement it — that would allow two jobs through when only one should pass.

## Pub/Sub (PUBLISH / SUBSCRIBE)
Redis pub/sub lets you broadcast messages to any number of listeners instantly. A publisher sends a message to a named channel. Any subscriber on that channel receives it immediately.

```
PUBLISH job:completed "abc123"    // send a message
SUBSCRIBE job:completed           // receive messages on this channel
```

Unlike lists (which store messages until consumed), pub/sub is fire-and-forget — if no one is subscribed when the message is published, it's gone. Use it for real-time notifications, not durable queuing.

In gqueue, the worker publishes the job ID to `job:completed` or `job:failed` after processing. Subscribers can listen to these channels to react to job events in real time.

## Flag Keys (EXISTS / SET / DEL)
Sometimes you just need a boolean stored in Redis — something is either on or off. The pattern is to use key existence as the signal rather than the value.

```
SET queue:default:paused 1    // "on" — key exists
DEL queue:default:paused      // "off" — key gone
EXISTS queue:default:paused   // returns 1 if on, 0 if off
```

The value `"1"` is never read — only whether the key exists matters. gqueue uses this for pause/resume: the key existing means paused, the key missing means running.

## Expiry (TTL)
You can set keys to auto-delete after a time with `EXPIRE`. gqueue doesn't use this directly, but it's useful for cleaning up stale data.

## Persistence
By default Redis is in-memory only — data is lost on restart. Redis supports optional persistence via RDB snapshots or AOF (append-only file) logs. For a job queue in production you'd want one of these enabled.

## Connection
gqueue connects to Redis using the `go-redis` client:

```go
client := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})
```

Port `6379` is Redis's default. In this project Redis runs in a Docker container mapped to that port.
