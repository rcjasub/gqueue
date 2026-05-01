package main

import "context"

type Queue struct {
	jobs chan Job
	Name string
}

func newQueue(size int, name string) *Queue {
	return &Queue{Name: name, jobs: make(chan Job, size)}
}

func (q *Queue) Enqueue(job Job) {
	q.jobs <- job
}

func (q *Queue) Dequeue(ctx context.Context) (Job, bool) {
	select {
	case <-ctx.Done():
		return Job{}, false
	case job := <-q.jobs:
		return job, true
	}
}
