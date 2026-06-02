package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/rcjasub/gqueue"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	go func() {
		<-quit
		cancel()
	}()

	queue := gqueue.NewQueue([]string{"main:high", "main:mid", "main:low"}, "default")
	worker := gqueue.NewWorker(queue, 3)

	worker.Register("send-email", func(job gqueue.Job, report func(pct int)) (string, error) {
		report(50)
		fmt.Println("sending email to:", job.Payload)
		if job.Payload == "bad@example.com" {
			return "", fmt.Errorf("invalid email address")
		}
		report(100)
		return "email sent to " + job.Payload, nil
	})

	worker.Register("resize-image", func(job gqueue.Job, report func(pct int)) (string, error) {
		fmt.Println("resizing image:", job.Payload)
		return "resized " + job.Payload, nil
	})

	worker.Register("generate-report", func(job gqueue.Job, report func(pct int)) (string, error) {
		fmt.Println("generating report for:", job.Payload)
		return "report generated for " + job.Payload, nil
	})

	worker.OnCompleted(func(job gqueue.Job) {
		fmt.Println("job completed:", job.Id, job.Name)
	})

	worker.OnFailed(func(job gqueue.Job) {
		fmt.Println("job failed:", job.Id, job.Name)
	})

	job := gqueue.NewJob("1", "send-email", "bad@example.com")
	job2 := gqueue.NewJob("2", "send-email", "user@example.com")
	job2.Delay = 2 * time.Second
	job3 := gqueue.NewJob("3", "resize-image", "photo.jpg")
	job4 := gqueue.NewJob("4", "generate-report", "monthly-sales")

	pubsub := queue.Subscribe(ctx, "job:completed", "job:failed")
	go func() {
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				pubsub.Close()
				return
			case msg := <-ch:
				fmt.Printf("[event] channel=%s jobId=%s\n", msg.Channel, msg.Payload)
			}
		}
	}()

	go queue.StartScheduler(ctx)
	go queue.StartRepeatableScheduler(ctx)
	worker.Start(ctx)
	queue.Enqueue(ctx, job)
	queue.Enqueue(ctx, job2)
	queue.Enqueue(ctx, job3)
	queue.Enqueue(ctx, job4)

	queue.AddRepeatable(ctx, gqueue.RepeatableJob{
		Name:     "heartbeat-email",
		JobName:  "send-email",
		Payload:  "heartbeat@example.com",
		Priority: gqueue.PriorityLow,
		Cron:     "@every 5s",
	})

	<-ctx.Done()
	worker.Wait()
}
