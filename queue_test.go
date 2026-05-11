package main

import (
	"context"
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

