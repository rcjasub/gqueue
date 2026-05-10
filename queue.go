package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
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

// The ctx gets passed to Redis operations so that if the context is cancelled
// (e.g. Ctrl+C), Redis knows to stop too.
func (q *Queue) Enqueue(ctx context.Context, job Job) error {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return err
	}

	q.client.HSet(ctx, "job:"+job.Id,
		"id", job.Id,
		"status", job.Status.String(),
		"payload", job.Payload,
		"createdAt", job.CreatedAt.Format(time.RFC3339),
	)
	
	if job.Delay > 0 {
		return q.client.ZAdd(ctx, "delayed", redis.Z{
			Score:  float64(time.Now().Add(job.Delay).Unix()),
			Member: jobJSON,
		}).Err()
	}

	return q.client.LPush(ctx, q.Name, jobJSON).Err()
}

func (q *Queue) StartScheduler(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := fmt.Sprintf("%d", time.Now().Unix())
			jobs, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key:     "delayed",
				Start:   "0",
				Stop:    now,
				ByScore: true,
			}).Result()
			if err != nil || len(jobs) == 0 {
				continue
			}
			for _, jobJSON := range jobs {
				q.client.LPush(ctx, q.Name, jobJSON)
				q.client.ZRem(ctx, "delayed", jobJSON)
			}
		}
	}
}

func (q *Queue) Dequeue(ctx context.Context) (Job, bool) {

	for {
		result, err := q.client.BRPop(ctx, 1*time.Second, q.Name).Result()

		//The select checks if context is cancelled
		if err == redis.Nil {
			select {
			case <-ctx.Done():
				return Job{}, false
			default:
				continue
			}
		}

		if err != nil {
			return Job{}, false
		}

		var job Job
		json.Unmarshal([]byte(result[1]), &job)

		return job, true
	}

}
