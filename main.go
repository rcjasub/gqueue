package main

import (
	"fmt"
	"time"
)

func main() {
	job := newJob("1", "send-email", "bad@example.com")
	job2 := newJob("2", "send-email", "send@example.com")
	queue := newQueue(10)
	worker := newWorker(queue, func(job Job) error {
		fmt.Println("processing:", job.Payload)

		if job.Payload == "bad@example.com" {
			return fmt.Errorf("invalid email")
		}
		return nil
	}, 3)
	worker.Start()
	queue.Enqueue(job)
	queue.Enqueue(job2)

	time.Sleep(2 * time.Second)
}
