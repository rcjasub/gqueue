package main

type ProcessFunc func(j Job) error

type Worker struct {
	queue       *Queue      // where to get jobs from
	process     ProcessFunc // what to do with each job
	concurrency int
}

func newWorker(queue *Queue, process ProcessFunc, concurrency int) *Worker {
	return &Worker{queue, process, concurrency}
}

func (w *Worker) Start() {
	for i := 0; i < w.concurrency; i++ {
		go func() {
			for {
				job := w.queue.Dequeue()
				w.processJob(job)
			}
		}()
	}
}

func (w *Worker) processJob(job Job) {
	job.Status = StatusActive
	err := w.process(job)

	if err != nil {
		job.Attempts++

		if job.Attempts < job.MaxRetries {
			w.queue.Enqueue(job)
		} else {
			job.Status = StatusFailed
		}
	} else {
		job.Status = StatusCompleted
	}

	printJob(job)
}
