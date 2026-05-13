package main

import (
	"context"
	"fmt"
	"sync"
	"time"
	"math"
)

type ProcessFunc func(j Job) error
type Event func(job Job)

type Worker struct {
	queue       *Queue
	handlers    map[string]ProcessFunc
	concurrency int
	onCompleted Event
	onFailed    Event
	waitGroup   sync.WaitGroup
}

func newWorker(queue *Queue, concurrency int) *Worker {
	return &Worker{
		queue:       queue,
		handlers:    make(map[string]ProcessFunc),
		concurrency: concurrency,
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
				w.processJob(ctx, job)
			}
		}()
	}
}

func (w *Worker) processJob(ctx context.Context, job Job) {

	job.Status = StatusActive
	handler, ok := w.handlers[job.Name]
	if !ok {
		handler = func(j Job) error {
			return fmt.Errorf("no handler registered for job type: %s", j.Name)
		}
	}
	err := handler(job)

	if err != nil {
		job.Attempts++

		if job.Attempts < job.MaxRetries {
			job.Delay = time.Duration(math.Pow(2, float64(job.Attempts))) * time.Second
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
		)
		if w.onCompleted != nil {
			w.onCompleted(job)
		}
	}

	printJob(job)
}

func (w *Worker) OnCompleted(fn Event) {
	w.onCompleted = fn
}

func (w *Worker) OnFailed(fn Event) {
	w.onFailed = fn
}
