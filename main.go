package main

import "fmt"

func main() {
	fmt.Println("gqueue starting..")
	fmt.Println("Jasub Rodriguez")

	job := newJob("1", "send-email", "user@example.com")

	queue := newQueue(3)
	queue.Enqueue(job)
	received := queue.Dequeue()
	printJob(received)
}
