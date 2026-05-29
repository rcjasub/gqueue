# Queue Patterns

## Job Queue Basics
A job queue decouples work from the thing requesting it. Instead of doing work immediately, you push a job onto a queue and a worker picks it up asynchronously. This lets you:
- Handle bursts of work without overwhelming your system
- Retry failed work automatically
- Process jobs in parallel with multiple workers

## Priority Queues
Not all jobs are equal. A password reset email should jump ahead of a weekly newsletter.

**How gqueue does it:** Three separate Redis lists — high, mid, low. Workers use `BRPOP` on all three in order, so high priority is always checked first.

```
myqueue:high  <- checked first
myqueue:mid   <- checked second
myqueue:low   <- checked last
```

Jobs carry a `Priority` field. `Enqueue` pushes to the right list based on that value.

## Retry with Exponential Backoff
When a job fails, retrying immediately often fails again (the external service is still down, the database is still overloaded). Exponential backoff increases the wait between retries.

**Formula:** `delay = 2^attempts seconds`

- Attempt 1 fails → wait 2s
- Attempt 2 fails → wait 4s
- Attempt 3 fails → wait 8s

Jobs waiting to retry go into a Redis sorted set (`delayed`) scored by the future Unix timestamp when they should be retried.

## Dead-Letter Queue
After exhausting all retries, a job goes to the dead-letter queue — a separate list where failed jobs are stored for inspection. This prevents jobs from being silently lost.

You can inspect dead-letter jobs with `ListDead` and requeue them with `RetryDead` after fixing the underlying issue.

## Delayed Jobs
Some jobs shouldn't run immediately — send this email in 10 minutes, run this report at midnight.

**How it works:** Jobs are stored in a sorted set scored by their target Unix timestamp. A scheduler goroutine polls the set and moves jobs to the main queue when their time comes.

```
ZADD delayed <unix-timestamp> <job-id>
ZRANGEBYSCORE delayed 0 <now>   // find jobs ready to run
```

## Stalled Job Detection
If a worker crashes mid-job, the job is lost — it was dequeued but never completed. To prevent this, workers register active jobs in a sorted set scored by start time.

A background goroutine (`DetectStalled`) periodically scans for jobs that have been active longer than a threshold and requeues them.

```
ZADD active-jobs <start-timestamp> <job-id>
// if now - start-timestamp > threshold → job is stalled → requeue it
ZREM active-jobs <job-id>
```

## Token Bucket Rate Limiting
Controls how fast jobs are processed. Imagine a bucket that holds tokens:
- The bucket starts full (up to `maxTokens`)
- Each job costs 1 token
- Tokens refill at `refillRate` per second
- If the bucket is empty, the worker waits until tokens refill

This allows short bursts (up to `maxTokens`) while enforcing a sustained rate (`refillRate` jobs/sec).

The check runs atomically in a Lua script in Redis so multiple workers share the same bucket without race conditions.

**Example:** `maxTokens=10, refillRate=2` means you can burst 10 jobs instantly, then process at 2 jobs/second after that.

## Event Callbacks
Hooks that fire when a job completes or fails. Useful for logging, metrics, or triggering follow-up actions.

```go
worker.OnCompleted(func(j Job) {
    log.Printf("job %s done", j.Id)
})
worker.OnFailed(func(j Job) {
    alert.Send("job failed: " + j.Id)
})
```

## Concurrency
Multiple workers can run in parallel by setting `concurrency > 1`. Each worker runs in its own goroutine and pulls from the same queue. This lets you scale throughput without running multiple processes.

The rate limiter is shared across all workers via Redis, so concurrency doesn't bypass the rate limit.
