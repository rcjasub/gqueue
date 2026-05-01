package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
	quit  := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	go func() {
		<-quit
        cancel()
	}()

	job := newJob("1", "send-email", "bad@example.com")
	job2 := newJob("2", "send-email", "send@example.com")
	job2.Delay = 2 * time.Second
	queue := newQueue(10, "email-queue")

	worker := newWorker(queue, func(job Job) error {
		fmt.Println("processing:", job.Payload)

		if job.Payload == "bad@example.com" {
			return fmt.Errorf("invalid email")
		}
		return nil
	}, 3)

	worker.OnCompleted(func(job Job) {
		fmt.Println("Job finished!", job.Id)
	})

	worker.OnFailed(func(job Job) {
		fmt.Println("Job failed", job.Id)
	})

	worker.Start(ctx)
	queue.Enqueue(job)
	queue.Enqueue(job2)

	<-ctx.Done()
}
