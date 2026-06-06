# gqueue

A Redis-backed job queue library for Go. Inspired by BullMQ, built to explore distributed systems concepts like job persistence, fault tolerance, and concurrency control.

## Features

- **Priority queues** — high, mid, and low priority lanes; urgent jobs always run first
- **Delayed jobs** — schedule a job to run after a duration
- **Repeatable jobs** — cron-style scheduling (`@every 5s`, `@daily`, standard 6-field expressions)
- **Retry with exponential backoff** — failed jobs are retried automatically with increasing delays
- **Dead-letter queue** — jobs that exhaust retries are moved here for inspection and manual retry
- **Stalled job detection** — if a worker crashes mid-job, the job is reassigned instead of lost
- **Rate limiting** — token bucket algorithm limits how many jobs a worker processes per second
- **Job progress** — workers can report percentage completion
- **Job results** — completed job data (result, timestamps, status) persisted in Redis
- **Pub/sub events** — subscribe to `job:completed` and `job:failed` for real-time updates
- **Pause/resume** — stop a queue without killing workers
- **Concurrent workers** — configurable number of goroutines per worker

## Requirements

- Go 1.21+
- Redis 6+

## Installation

```bash
go get github.com/rcjasub/gqueue
```

## Quick Start

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
queue := gqueue.NewQueue(rdb, []string{"myqueue:high", "myqueue:mid", "myqueue:low"}, "myqueue")
worker := gqueue.NewWorker(queue, 3, nil) // 3 concurrent goroutines, no rate limit

worker.Register("send-email", func(job gqueue.Job, report func(pct int)) (string, error) {
    report(50)
    // do work...
    report(100)
    return "done", nil
})

worker.OnCompleted(func(job gqueue.Job) {
    fmt.Println("completed:", job.Id)
})

ctx := context.Background()
go queue.StartScheduler(ctx)
worker.Start(ctx)

job := gqueue.NewJob("job-1", "send-email", "user@example.com")
queue.Enqueue(ctx, job)

worker.Wait()
```

## API

### Queue

```go
// Create a queue. Names are the Redis list keys per priority (high→mid→low order).
gqueue.NewQueue(client *redis.Client, names []string, name string) *Queue

// Enqueue a job. Uses Delay field if set, otherwise enqueues immediately.
queue.Enqueue(ctx, job) error

// Dequeue blocks until a job is available or context is cancelled.
queue.Dequeue(ctx) (Job, bool)

// Delayed job scheduler — run in a goroutine.
go queue.StartScheduler(ctx)

// Repeatable job scheduler — run in a goroutine.
go queue.StartRepeatableScheduler(ctx)

// Register a repeatable (cron) job.
queue.AddRepeatable(ctx, gqueue.RepeatableJob{
    Name:    "unique-key",
    JobName: "send-email",       // must match a registered handler
    Payload: "user@example.com",
    Cron:    "@every 1m",
})

// Dead-letter inspection.
queue.ListDead(ctx) ([]Job, error)
queue.RetryDead(ctx, jobId) error

// Stalled job detection — reassigns jobs stuck in active state beyond threshold.
go queue.StartStalledDetector(ctx, 5*time.Minute)

// Pause and resume.
queue.Pause(ctx)
queue.Resume(ctx)

// Pub/sub.
pubsub := queue.Subscribe(ctx, "job:completed", "job:failed")
```

### Worker

```go
// Create a worker. Pass *RateLimitConfig to enable rate limiting, nil to disable.
gqueue.NewWorker(queue *Queue, concurrency int, rateLimit *gqueue.RateLimitConfig) *Worker

// Register a handler for a job type.
worker.Register("job-name", func(job gqueue.Job, report func(pct int)) (string, error) {
    report(50)  // report progress 0-100
    return "result", nil
})

// Optional callbacks.
worker.OnCompleted(func(job gqueue.Job) { ... })
worker.OnFailed(func(job gqueue.Job) { ... })

// Start processing. Non-blocking — spawns goroutines internally.
worker.Start(ctx)

// Wait for all goroutines to finish after context is cancelled.
worker.Wait()
```

### Rate Limiting

```go
worker := gqueue.NewWorker(queue, 4, &gqueue.RateLimitConfig{
    MaxTokens:  10,  // burst capacity
    RefillRate: 5.0, // tokens added per second
})
```

Uses a token bucket algorithm backed by a Lua script in Redis, so the limit is enforced atomically.

### Job

```go
job := gqueue.NewJob("unique-id", "handler-name", "payload string")

job.Priority  = gqueue.PriorityHigh  // PriorityHigh, PriorityMid, PriorityLow
job.Delay     = 10 * time.Second     // run after 10s
job.MaxRetries = 5                   // default is 3
```

## How It Works

```
Enqueue → Redis list (by priority)
             ↓
Worker goroutines poll via BRPOP (high → mid → low)
             ↓
Handler runs → success: result stored, job:completed published
             → failure: retry with backoff, or dead-letter after max retries
```

Delayed jobs live in a Redis sorted set scored by Unix timestamp. The scheduler polls every second and moves ready jobs into the priority lists.

Stalled job detection tracks active jobs in a sorted set. Any job active longer than the threshold is assumed to have lost its worker and is re-enqueued.
