package main

import (
	"fmt"
	"time"
)

type JobStatus int

const (
	StatusWaiting JobStatus = iota
	StatusActive
	StatusCompleted
	StatusFailed
)

type Job struct {
	Id        string
	Name      string
	Payload   string
	Status    JobStatus
	Attempts  int
	CreatedAt time.Time
}

func newJob(id string, name string, payload string) Job {
	job := Job{Id: id, Name: name, Payload: payload, Status: StatusWaiting, CreatedAt: time.Now()}
	return job
}

func printJob(j Job) {
	fmt.Printf("Job[%s] name=%s status=%s attempts=%d\n", j.Id, j.Name, j.Status, j.Attempts)
}
