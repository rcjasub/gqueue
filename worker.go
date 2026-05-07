package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ProcessFunc func(j Job) error
type Event func(job Job)

type Worker struct {
	queue       *Queue      // where to get jobs from
	process     ProcessFunc // what to do with each job
	concurrency int
	onCompleted Event
	onFailed    Event
	waitGroup   sync.WaitGroup
}

func newWorker(queue *Queue, process ProcessFunc, concurrency int) *Worker {
	return &Worker{
		queue:       queue,
		process:     process,
		concurrency: concurrency}
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
	err := w.process(job)

	if err != nil {
		job.Attempts++

		if job.Attempts < job.MaxRetries {
			w.queue.Enqueue(ctx, job)
		} else {
			job.Status = StatusFailed
			job.Error = err.Error()
			job.FailedAt = time.Now()
			w.queue.client.HSet(ctx, "job:"+job.Id,
				"status", job.Status.String(),
				"failedAt", job.FailedAt.Format(time.RFC3339),
				"error", job.Error,
			)
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
