package main

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkEnqueue(b *testing.B) {
	q := newQueue("bench")
	ctx := context.Background()
	job := newJob("1", "bench-job", "payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(ctx, job)
	}
}

func BenchmarkDequeue(b *testing.B) {
	q := newQueue("bench")
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
	q := newQueue("bench")
	ctx := context.Background()
	job := newJob("1", "bench-job", "payload")

	q.client.Del(ctx, q.Name)
	q.Enqueue(ctx, job)

	count := q.client.LLen(ctx, q.Name).Val()
	if count != 1 {
		t.Errorf("expected 1 job in queue, got %d", count)
	}
}

func TestDequeue(t *testing.T) {
	q := newQueue("bench")
	ctx := context.Background()
	job := newJob("1", "bench-job", "payload")

	q.client.Del(ctx, q.Name)
	q.Enqueue(ctx, job)
	q.Dequeue(ctx)

	count := q.client.LLen(ctx, q.Name).Val()
	if count != 0 {
		t.Errorf("expected 0 jobs in queue, got %d", count)
	}
}

func TestRetry(t *testing.T) {
	q := newQueue("test-retry")
	ctx := context.Background()

	q.client.Del(ctx, q.Name)
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
	q := newQueue("dead-letter-test")
	ctx := context.Background()

	q.client.Del(ctx, q.Name)
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

func TestNoHandler(t *testing.T) {
	q := newQueue("test-nohandler")
	ctx := context.Background()

	q.client.Del(ctx, q.Name)
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

