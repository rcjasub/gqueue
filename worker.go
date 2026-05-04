package main

import (
	"context"
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
				w.processJob(ctx, job)
			}
		}()
	}
}

func (w *Worker) processJob(ctx context.Context, job Job) {

	if job.Delay > 0 {
		time.Sleep(job.Delay)
	}

	job.Status = StatusActive
	err := w.process(job)

	if err != nil {
		job.Attempts++

		if job.Attempts < job.MaxRetries {
			w.queue.Enqueue(ctx, job)
		} else {
			job.Status = StatusFailed
			if w.onFailed != nil {
				w.onFailed(job)
			}
		}
	} else {
		job.Status = StatusCompleted
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
