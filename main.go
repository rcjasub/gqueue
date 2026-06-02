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
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	go func() {
		<-quit
		cancel()
	}()

	queue := newQueue([]string{"main:high", "main:mid", "main:low"}, "default")
	worker := newWorker(queue, 3)

	worker.Register("send-email", func(job Job, report func(pct int)) (string, error) {
		report(50)
		fmt.Println("sending email to:", job.Payload)
		if job.Payload == "bad@example.com" {
			return "", fmt.Errorf("invalid email address")
		}
		report(100)
		return "email sent to " + job.Payload, nil
	})

	worker.Register("resize-image", func(job Job, report func(pct int)) (string, error) {
		fmt.Println("resizing image:", job.Payload)
		return "resized " + job.Payload, nil
	})

	worker.Register("generate-report", func(job Job, report func(pct int)) (string, error) {
		fmt.Println("generating report for:", job.Payload)
		return "report generated for " + job.Payload, nil
	})

	worker.OnCompleted(func(job Job) {
		fmt.Println("job completed:", job.Id, job.Name)
	})

	worker.OnFailed(func(job Job) {
		fmt.Println("job failed:", job.Id, job.Name)
	})

	job := newJob("1", "send-email", "bad@example.com")
	job2 := newJob("2", "send-email", "user@example.com")
	job2.Delay = 2 * time.Second
	job3 := newJob("3", "resize-image", "photo.jpg")
	job4 := newJob("4", "generate-report", "monthly-sales")

	go queue.StartScheduler(ctx)
	go queue.StartRepeatableScheduler(ctx)
	worker.Start(ctx)
	queue.Enqueue(ctx, job)
	queue.Enqueue(ctx, job2)
	queue.Enqueue(ctx, job3)
	queue.Enqueue(ctx, job4)

	// Repeatable job: fires every 5 seconds until the program is stopped.
	queue.AddRepeatable(ctx, RepeatableJob{
		Name:     "heartbeat-email",
		JobName:  "send-email",
		Payload:  "heartbeat@example.com",
		Priority: PriorityLow,
		Cron:     "@every 5s",
	})

	<-ctx.Done()
	worker.waitGroup.Wait()
}