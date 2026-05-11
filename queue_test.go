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
