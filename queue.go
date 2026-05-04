package main

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
)

type Queue struct {
	client *redis.Client
	Name   string
}

func newQueue(name string) *Queue {
	return &Queue{client: redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	}), Name: name}
}

func (q *Queue) Enqueue(ctx context.Context, job Job) error {
	jobJSON, err := json.Marshal(job) // converts the struct into a JSON string
	if err != nil {                   // nil = no error
		return err
	}

	return q.client.LPush(ctx, q.Name, jobJSON).Err()
}

func (q *Queue) Dequeue(ctx context.Context) (Job, bool) {
   //next
}
