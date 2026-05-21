package main

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkEnqueue(b *testing.B) {
	q := newQueue([]string{"bench:high", "bench:mid", "bench:low"})
	ctx := context.Background()
	job := newJob("1", "bench-job", "payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(ctx, job)
	}
}

func BenchmarkDequeue(b *testing.B) {
	q := newQueue([]string{"bench:high", "bench:mid", "bench:low"})
	ctx := context.Background()
	job := newJob("1", "bench-job", "payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q.Enqueue(ctx, job)
		b.StartTimer()
		q.Dequeue(ctx)
	}
}

func TestEnqueue(t *testing.T) {
	q := newQueue([]string{"bench:high", "bench:mid", "bench:low"})
	ctx := context.Background()
	job := newJob("1", "bench-job", "payload")

	q.client.Del(ctx, q.Names...)
	q.Enqueue(ctx, job)

	count := q.client.LLen(ctx, q.Names[1]).Val()
	if count != 1 {
		t.Errorf("expected 1 job in queue, got %d", count)
	}
}

func TestDequeue(t *testing.T) {
	q := newQueue([]string{"bench:high", "bench:mid", "bench:low"})
	ctx := context.Background()
	job := newJob("1", "bench-job", "payload")

	q.client.Del(ctx, q.Names...)
	q.Enqueue(ctx, job)
	q.Dequeue(ctx)

	count := q.client.LLen(ctx, q.Names[1]).Val()
	if count != 0 {
		t.Errorf("expected 0 jobs in queue, got %d", count)
	}
}

func TestRetry(t *testing.T) {
	q := newQueue([]string{"retry:high", "retry:mid", "retry:low"})
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "delayed")

	job := newJob("retry-1", "send-email", "bad@example.com")
	job.MaxRetries = 3

	worker := newWorker(q, 1)
	worker.Register("send-email", func(j Job) error {
		return fmt.Errorf("simulated failure")
	})

	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	delayed := q.client.ZCard(ctx, "delayed").Val()
	if delayed != 1 {
		t.Errorf("expected job to be re-queued in delayed set, got %d", delayed)
	}
}

func TestDeadLetter(t *testing.T) {
	q := newQueue([]string{"dl:high", "dl:mid", "dl:low"})
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "delayed")
	q.client.Del(ctx, "dead-letter")

	job := newJob("retry-1", "send-email", "bad@example.com")
	job.MaxRetries = 3

	worker := newWorker(q, 1)
	worker.Register("send-email", func(j Job) error {
		return fmt.Errorf("simulated failure")
	})

	job.Attempts = job.MaxRetries - 1
	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	count := q.client.LLen(ctx, "dead-letter").Val()
	if count != 1 {
		t.Errorf("expected job in dead-letter, got %d", count)
	}
}

func TestOnCompleted(t *testing.T) {
	q := newQueue([]string{"oc:high", "oc:mid", "oc:low"})
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)

	job := newJob("completed-1", "send-email", "user@example.com")

	worker := newWorker(q, 1)
	worker.Register("send-email", func(j Job) error {
		return nil
	})

	fired := false
	worker.OnCompleted(func(j Job) {
		fired = true
	})

	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	if !fired {
		t.Error("expected OnCompleted callback to fire, but it did not")
	}
}

func TestOnFailed(t *testing.T) {
	q := newQueue([]string{"of:high", "of:mid", "of:low"})
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "dead-letter")

	job := newJob("failed-1", "send-email", "bad@example.com")
	job.Attempts = job.MaxRetries - 1

	worker := newWorker(q, 1)
	worker.Register("send-email", func(j Job) error {
		return fmt.Errorf("simulated failure")
	})

	fired := false
	worker.OnFailed(func(j Job) {
		fired = true
	})

	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	if !fired {
		t.Error("expected OnFailed callback to fire, but it did not")
	}
}

func TestPriority(t *testing.T) {
	q := newQueue([]string{"prio:high", "prio:mid", "prio:low"})
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)

	low := newJob("prio-low", "task", "payload")
	low.Priority = PriorityLow

	mid := newJob("prio-mid", "task", "payload")
	mid.Priority = PriorityMid

	high := newJob("prio-high", "task", "payload")
	high.Priority = PriorityHigh

	q.Enqueue(ctx, low)
	q.Enqueue(ctx, mid)
	q.Enqueue(ctx, high)

	expected := []string{"prio-high", "prio-mid", "prio-low"}
	for _, id := range expected {
		job, _ := q.Dequeue(ctx)
		if job.Id != id {
			t.Errorf("expected job %q, got %q", id, job.Id)
		}
	}
}

func TestNoHandler(t *testing.T) {
	q := newQueue([]string{"nh:high", "nh:mid", "nh:low"})
	ctx := context.Background()

	q.client.Del(ctx, q.Names...)
	q.client.Del(ctx, "delayed")
	q.client.Del(ctx, "dead-letter")

	job := newJob("nohandler-1", "unknown-job-type", "payload")
	job.Attempts = job.MaxRetries - 1

	worker := newWorker(q, 1)

	q.Enqueue(ctx, job)
	dequeued, _ := q.Dequeue(ctx)
	worker.processJob(ctx, dequeued)

	count := q.client.LLen(ctx, "dead-letter").Val()
	if count != 1 {
		t.Errorf("expected unhandled job in dead-letter, got %d", count)
	}
}

