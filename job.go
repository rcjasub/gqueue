package main

import "fmt"

type Job struct {
	Id      string
	Name    string
	Payload string
}

func printJob(j Job) {
	fmt.Printf("Job[%s]: %s\n %s\n", j.Id, j.Name, j.Payload)
}
