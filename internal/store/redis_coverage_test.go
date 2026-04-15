package store_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/brianbuquoi/overlord/internal/broker"
	redisstore "github.com/brianbuquoi/overlord/internal/store/redis"
)

// TestRedis_EnqueueTask_IndexConsistency verifies that a single EnqueueTask
// populates: the primary (pipeline,stage) index, the flat per-state index,
// and the two-dimensional state×pipeline index — all with the task ID as a
// member.
func TestRedis_EnqueueTask_IndexConsistency(t *testing.T) {
	t.Parallel()
	s, _, client := newRedisTestStore(t, "idxcons:")
	ctx := context.Background()

	pipeline := "pipe-cons-" + uuid.New().String()
	stage := "stage-cons-" + uuid.New().String()

	task := newTask(pipeline, stage)
	if err := s.EnqueueTask(ctx, stage, task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	primary := fmt.Sprintf("idxcons:index:%s:%s", pipeline, stage)
	flatState := fmt.Sprintf("idxcons:tasks:state:%s", broker.TaskStatePending)
	twoD := fmt.Sprintf("idxcons:tasks:state:%s:pipeline:%s", broker.TaskStatePending, pipeline)

	for _, key := range []string{primary, flatState, twoD} {
		score, err := client.ZScore(ctx, key, task.ID).Result()
		if err != nil {
			t.Errorf("task missing from index %s: %v", key, err)
			continue
		}
		if score <= 0 {
			t.Errorf("index %s has non-positive score %v", key, score)
		}
	}
}

// TestRedis_TTL_TaskExpiry uses miniredis fast-forward (no time.Sleep): after
// TTL expiry, GetTask returns ErrTaskNotFound-equivalent and the task does not
// appear in any index MGET (dangling entry is skipped).
func TestRedis_TTL_TaskExpiry(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	s := redisstore.New(client, "ttlexp:", 150*time.Millisecond)
	ctx := context.Background()

	pipeline := "pipe-ttl-" + uuid.New().String()
	stage := "stage-ttl-" + uuid.New().String()

	task := newTask(pipeline, stage)
	if err := s.EnqueueTask(ctx, stage, task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	taskKey := fmt.Sprintf("ttlexp:task:%s", task.ID)
	if !mr.Exists(taskKey) {
		t.Fatal("task key should exist initially")
	}

	// Fast-forward past TTL — no time.Sleep needed.
	mr.FastForward(300 * time.Millisecond)

	if mr.Exists(taskKey) {
		t.Fatal("task key should have expired")
	}

	// GetTask must now fail.
	if _, err := s.GetTask(ctx, task.ID); err == nil {
		t.Error("GetTask on expired task should return an error")
	}

	// ListTasks must skip the dangling index entry silently.
	pipeFilter := pipeline
	stageFilter := stage
	result, err := s.ListTasks(ctx, broker.TaskFilter{PipelineID: &pipeFilter, StageID: &stageFilter})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, got := range result.Tasks {
		if got.ID == task.ID {
			t.Error("expired task should not appear in ListTasks results")
		}
	}
}

// TestRedis_ListTasks_Pagination enqueues 25 tasks and walks three pages of
// size 10, verifying correct counts, no overlap, and coverage.
func TestRedis_ListTasks_Pagination(t *testing.T) {
	t.Parallel()
	s, _, _ := newRedisTestStore(t, "pg:")
	ctx := context.Background()

	pipeline := "pipe-pg-" + uuid.New().String()
	stage := "stage-pg-" + uuid.New().String()

	const total = 25
	expected := make(map[string]bool, total)
	for i := 0; i < total; i++ {
		task := newTask(pipeline, stage)
		// Distinct CreatedAt scores so ordering is deterministic.
		task.CreatedAt = time.Now().Add(time.Duration(i) * time.Millisecond)
		expected[task.ID] = true
		if err := s.EnqueueTask(ctx, stage, task); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	pipeFilter := pipeline
	stageFilter := stage

	pageSize := 10
	seen := make(map[string]bool)
	pages := [][]int{{0, 10}, {10, 10}, {20, 10}}
	wantLens := []int{10, 10, 5}
	for i, p := range pages {
		res, err := s.ListTasks(ctx, broker.TaskFilter{
			PipelineID: &pipeFilter,
			StageID:    &stageFilter,
			Limit:      pageSize,
			Offset:     p[0],
		})
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		if len(res.Tasks) != wantLens[i] {
			t.Errorf("page %d: got %d tasks, want %d", i, len(res.Tasks), wantLens[i])
		}
		if res.Total != total {
			t.Errorf("page %d total: got %d, want %d", i, res.Total, total)
		}
		for _, tk := range res.Tasks {
			if seen[tk.ID] {
				t.Errorf("task %s appeared on multiple pages", tk.ID)
			}
			seen[tk.ID] = true
		}
	}
	if len(seen) != total {
		t.Errorf("paginated coverage: saw %d unique tasks, want %d", len(seen), total)
	}
	for id := range expected {
		if !seen[id] {
			t.Errorf("task %s missing from pagination", id)
		}
	}
}

// TestRedis_ConcurrentEnqueue fires 50 goroutines each enqueueing one task
// and verifies every task is retrievable and all indexes are consistent.
func TestRedis_ConcurrentEnqueue(t *testing.T) {
	t.Parallel()
	s, _, client := newRedisTestStore(t, "cenq:")
	ctx := context.Background()

	pipeline := "pipe-cenq-" + uuid.New().String()
	stage := "stage-cenq-" + uuid.New().String()

	const N = 50
	var wg sync.WaitGroup
	ids := make([]string, N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			task := newTask(pipeline, stage)
			task.CreatedAt = time.Now().Add(time.Duration(i) * time.Microsecond)
			ids[i] = task.ID
			errs[i] = s.EnqueueTask(ctx, stage, task)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("enqueue %d: %v", i, e)
		}
	}

	// Each ID must be individually retrievable.
	for _, id := range ids {
		if _, err := s.GetTask(ctx, id); err != nil {
			t.Errorf("GetTask(%s): %v", id, err)
		}
	}

	// Primary (pipeline,stage) index must contain all N members.
	primary := fmt.Sprintf("cenq:index:%s:%s", pipeline, stage)
	if c, _ := client.ZCard(ctx, primary).Result(); c != N {
		t.Errorf("primary index size: got %d, want %d", c, N)
	}

	// Flat per-state index must contain all N members (they all started PENDING).
	flat := fmt.Sprintf("cenq:tasks:state:%s", broker.TaskStatePending)
	count := 0
	for _, id := range ids {
		if _, err := client.ZScore(ctx, flat, id).Result(); err == nil {
			count++
		}
	}
	if count != N {
		t.Errorf("flat state index: %d of %d tasks present", count, N)
	}

	// Two-dimensional state×pipeline index must contain all N members.
	twoD := fmt.Sprintf("cenq:tasks:state:%s:pipeline:%s", broker.TaskStatePending, pipeline)
	if c, _ := client.ZCard(ctx, twoD).Result(); c != N {
		t.Errorf("two-dimensional index size: got %d, want %d", c, N)
	}

	// ListTasks should report the correct total and return every task when
	// unpaginated.
	pipeFilter := pipeline
	stageFilter := stage
	res, err := s.ListTasks(ctx, broker.TaskFilter{
		PipelineID: &pipeFilter,
		StageID:    &stageFilter,
		Limit:      N + 10,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if res.Total != N {
		t.Errorf("list total: got %d, want %d", res.Total, N)
	}
	if len(res.Tasks) != N {
		t.Errorf("list returned %d tasks, want %d", len(res.Tasks), N)
	}
}
