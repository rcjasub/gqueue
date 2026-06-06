package gqueue

import (
	"time"
)

type JobStatus int

const (
	StatusWaiting JobStatus = iota
	StatusActive
	StatusCompleted
	StatusFailed
	StatusDeadLetter
)

type JobPriority int

const (
	PriorityHigh JobPriority = iota
	PriorityMid
	PriorityLow
)

type Job struct {
	Id          string
	Name        string
	Payload     string
	Status      JobStatus
	Priority    JobPriority
	Attempts    int
	MaxRetries  int
	Delay       time.Duration
	CreatedAt   time.Time
	StartedAt   time.Time
	FailedAt    time.Time
	CompletedAt time.Time
	Error       string
}

func NewJob(id string, name string, payload string) Job {
	return Job{
		Id:         id,
		Name:       name,
		Payload:    payload,
		Status:     StatusWaiting,
		Priority:   PriorityMid,
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

	case StatusDeadLetter:
		return "dead-letter"

	default:
		return "unknown"
	}
}

// RepeatableJob defines a job that runs on a cron schedule.
// Cron supports standard 6-field expressions (sec min hour dom mon dow)
// and descriptors like @every 5s, @hourly, @daily.
type RepeatableJob struct {
	Name     string      // unique key identifying this repeatable definition
	JobName  string      // matches a registered worker handler
	Payload  string
	Priority JobPriority
	Cron     string
}

