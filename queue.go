package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"strconv"
	"time"
)

type Queue struct {
	client *redis.Client
	Names []string
	Name string
}

func newQueue(name []string, n string) *Queue {
	return &Queue{client: redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	}), Names: name,
        Name: n}
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

// AddRepeatable registers a repeatable job in Redis, scored by its first next-run time.
func (q *Queue) AddRepeatable(ctx context.Context, rj RepeatableJob) error {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(rj.Cron)
	if err != nil {
		return fmt.Errorf("invalid cron %q: %w", rj.Cron, err)
	}
	next := schedule.Next(time.Now())
	data, err := json.Marshal(rj)
	if err != nil {
		return err
	}
	return q.client.ZAdd(ctx, "repeatable", redis.Z{
		Score:  float64(next.Unix()),
		Member: string(data),
	}).Err()
}

// tickRepeatable fires all due repeatable jobs and re-schedules them.
func (q *Queue) tickRepeatable(ctx context.Context, parser cron.Parser) {
	now := fmt.Sprintf("%d", time.Now().Unix())
	members, err := q.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     "repeatable",
		Start:   "0",
		Stop:    now,
		ByScore: true,
	}).Result()
	if err != nil || len(members) == 0 {
		return
	}
	for _, member := range members {
		var rj RepeatableJob
		if err := json.Unmarshal([]byte(member), &rj); err != nil {
			q.client.ZRem(ctx, "repeatable", member)
			continue
		}
		job := newJob(fmt.Sprintf("%s-%d", rj.Name, time.Now().UnixNano()), rj.JobName, rj.Payload)
		job.Priority = rj.Priority
		q.Enqueue(ctx, job)

		q.client.ZRem(ctx, "repeatable", member)
		schedule, err := parser.Parse(rj.Cron)
		if err != nil {
			continue
		}
		next := schedule.Next(time.Now())
		q.client.ZAdd(ctx, "repeatable", redis.Z{
			Score:  float64(next.Unix()),
			Member: member,
		})
	}
}

func (q *Queue) StartRepeatableScheduler(ctx context.Context) {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.tickRepeatable(ctx, parser)
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
