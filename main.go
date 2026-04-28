package main

import "fmt"

func main() {
	fmt.Println("gqueue starting..")
	fmt.Println("Jasub Rodriguez")

	job := Job{Id: "123", Name: "send-email", Payload: "user@example.com"}
	printJob(job)
}
