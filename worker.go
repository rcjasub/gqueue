package gqueue

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math"
	"sync"
	"time"
)

type ProcessFunc func(j Job, report func(pct int)) (string, error)
type Event func(job Job)

type RateLimitConfig struct {
	MaxTokens   int
	RefillRate  float64
}

type Worker struct {
	queue       *Queue
	handlers    map[string]ProcessFunc
	concurrency int
	onCompleted Event
	onFailed    Event
	waitGroup   sync.WaitGroup
	rateLimit   *RateLimitConfig
}



func NewWorker(queue *Queue, concurrency int, rt *RateLimitConfig) *Worker {
	return &Worker{
		queue:       queue,
		handlers:    make(map[string]ProcessFunc),
		concurrency: concurrency,
		rateLimit:   rt,		
	}
}

func (w *Worker) Register(name string, fn ProcessFunc) {
	w.handlers[name] = fn
}

func (w *Worker) Start(ctx context.Context) {
	for i := 0; i < w.concurrency; i++ {
		w.waitGroup.Add(1)
		go func() {
			defer w.waitGroup.Done() // defer means "run this when the function returns"
			for {
				for w.queue.IsPaused(ctx) {
					select {
					case <-ctx.Done():
						return
					case <-time.After(500 * time.Millisecond):
					}
				}
				job, ok := w.queue.Dequeue(ctx)
				if !ok {
					return
				}
				job.StartedAt = time.Now()
				w.queue.client.HSet(ctx, "job:"+job.Id,
					"status", StatusActive.String(),
					"startedAt", job.StartedAt.Format(time.RFC3339),
					"worker", fmt.Sprintf("worker-%d", i),
				)
				w.queue.client.ZAdd(ctx, "active-jobs", redis.Z{
					Score:  float64(job.StartedAt.Unix()),
					Member: job.Id,
				})
				w.processJob(ctx, job)
				w.queue.client.ZRem(ctx, "active-jobs", job.Id)
			}
		}()
	}
}

func (w *Worker) processJob(ctx context.Context, job Job) {

	if w.rateLimit != nil {
		for !w.queue.Allow(ctx, w.rateLimit.MaxTokens, w.rateLimit.RefillRate) {
			time.Sleep(100 * time.Millisecond)  // pause before checking again
		}
	}

	job.Status = StatusActive
	report := func(pct int) {
		w.queue.client.HSet(ctx, "job:"+job.Id, "progress", pct)
	}
	handler, ok := w.handlers[job.Name]
	if !ok {
		handler = func(j Job, report func(pct int)) (string, error) {
			return "", fmt.Errorf("no handler registered for job type: %s", j.Name)
		}
	}
	result, err := handler(job, report)

	if err != nil {
		job.Attempts++

		if job.Attempts < job.MaxRetries {
			job.Delay = time.Duration(math.Pow(2, float64(job.Attempts))) * time.Second
			w.queue.client.HSet(ctx, "job:"+job.Id,
				"status", StatusWaiting.String(),
				"attempts", job.Attempts,
			)
			w.queue.Enqueue(ctx, job)

		} else {
			job.Status = StatusDeadLetter
			job.Error = err.Error()
			job.FailedAt = time.Now()
			w.queue.client.HSet(ctx, "job:"+job.Id,
				"status", job.Status.String(),
				"failedAt", job.FailedAt.Format(time.RFC3339),
				"error", job.Error,
			)
			w.queue.client.LPush(ctx, "dead-letter", job.Id)
			w.queue.client.Publish(ctx, "job:failed", job.Id)
			if w.onFailed != nil {
				w.onFailed(job)
			}
		}
	} else {
		job.Status = StatusCompleted
		job.CompletedAt = time.Now()
		w.queue.client.HSet(ctx, "job:"+job.Id,
			"status", job.Status.String(),
			"completedAt", job.CompletedAt.Format(time.RFC3339),
			"result", result,
		)
		w.queue.client.Publish(ctx, "job:completed", job.Id)
		if w.onCompleted != nil {
			w.onCompleted(job)
		}
	}

}

func (w *Worker) OnCompleted(fn Event) {
	w.onCompleted = fn
}

func (w *Worker) OnFailed(fn Event) {
	w.onFailed = fn
}

func (w *Worker) Wait() {
	w.waitGroup.Wait()
}
