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
	Id         string
	Name       string
	Payload    string
	Status     JobStatus
	Attempts   int
	MaxRetries int
	CreatedAt  time.Time
}

func newJob(id string, name string, payload string) Job {
	return Job{
		Id:         id,
		Name:       name,
		Payload:    payload,
		Status:     StatusWaiting,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}
}

func (s JobStatus) String() string {
	switch s {
	case StatusWaiting:
		return "waiting"

	case StatusActive:
		return "active"

	case StatusCompleted:
		return "completed"

	case StatusFailed:
		return "failed"

	default:
		return "unknown"
	}
}

func printJob(j Job) {
	fmt.Printf("Job[%s] name=%s status=%s attempts=%d\n", j.Id, j.Name, j.Status, j.Attempts)
}
