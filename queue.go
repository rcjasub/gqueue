package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type Queue struct {
	client *redis.Client
	Names []string
}

func newQueue(name []string) *Queue {
	return &Queue{client: redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	}), Names: name}
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
		"name", job.Name,
		"status", job.Status.String(),
		"payload", job.Payload,
		"priority", int(job.Priority),
		"maxRetries", job.MaxRetries,
		"attempts", job.Attempts,
		"createdAt", job.CreatedAt.Format(time.RFC3339),
	)

	if job.Delay > 0 {
		return q.client.ZAdd(ctx, "delayed", redis.Z{
			Score:  float64(time.Now().Add(job.Delay).Unix()),
			Member: jobJSON,
		}).Err()
	}

	return q.client.LPush(ctx, q.Names[job.Priority], jobJSON).Err()
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
				var job Job
				json.Unmarshal([]byte(jobJSON), &job)
				q.client.LPush(ctx, q.Names[job.Priority], jobJSON)
				q.client.ZRem(ctx, "delayed", jobJSON)
			}
		}
	}
}

func (q *Queue) Dequeue(ctx context.Context) (Job, bool) {

	for {
		result, err := q.client.BRPop(ctx, 1*time.Second, q.Names...).Result()

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

func (q *Queue) ListDead(ctx context.Context) ([]Job, error) {
	ids, err := q.client.LRange(ctx, "dead-letter", 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var jobs []Job
	for _, id := range ids {
		data, err := q.client.HGetAll(ctx, "job:"+id).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		jobs = append(jobs, Job{
			Id:      data["id"],
			Status:  StatusDeadLetter,
			Payload: data["payload"],
			Error:   data["error"],
		})
	}

	return jobs, nil
}

func (q *Queue) DetectStalled(ctx context.Context, threshold time.Duration) {
	cutoff := strconv.FormatFloat(float64(time.Now().Add(-threshold).Unix()), 'f', 0, 64)
	ids, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     "active-jobs",
		Start:   "0",
		Stop:    cutoff,
		ByScore: true,
	}).Result()
	if err != nil || len(ids) == 0 {
		return
	}
	for _, id := range ids {
		data, err := q.client.HGetAll(ctx, "job:"+id).Result()
		if err != nil || len(data) == 0 {
			q.client.ZRem(ctx, "active-jobs", id)
			continue
		}
		job := Job{
			Id:         data["id"],
			Name:       data["name"],
			Payload:    data["payload"],
			Status:     StatusWaiting,
			MaxRetries: 3,
		}
		if p, err := strconv.Atoi(data["priority"]); err == nil {
			job.Priority = JobPriority(p)
		}
		if r, err := strconv.Atoi(data["maxRetries"]); err == nil {
			job.MaxRetries = r
		}
		if a, err := strconv.Atoi(data["attempts"]); err == nil {
			job.Attempts = a
		}
		q.client.ZRem(ctx, "active-jobs", id)
		q.Enqueue(ctx, job)
	}
}

func (q *Queue) StartStalledDetector(ctx context.Context, threshold time.Duration) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.DetectStalled(ctx, threshold)
		}
	}
}

func (q *Queue) RetryDead(ctx context.Context, id string) error {
	data, err := q.client.HGetAll(ctx, "job:"+id).Result()
	if err != nil || len(data) == 0 {
		return fmt.Errorf("job not found: %s", id)
	}

	job := Job{
		Id:      data["id"],
		Status:  StatusWaiting,
		Payload: data["payload"],
		Attempts: 0,
	}

	q.client.LRem(ctx, "dead-letter", 1, id)

	return q.Enqueue(ctx, job)
}
