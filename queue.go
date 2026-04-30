package main

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

func (q *Queue) Dequeue() Job {
	return <-q.jobs
}
