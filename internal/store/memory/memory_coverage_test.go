package memory

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/brianbuquoi/overlord/internal/broker"
)

// TestMemory_ListTasks_AllFilters exercises every field of broker.TaskFilter
// simultaneously: PipelineID, StageID, State, RoutedToDeadLetter, IncludeDiscarded,
// Limit, Offset. The dataset is crafted so that only a specific subset satisfies
// the combined predicate.
func TestMemory_ListTasks_AllFilters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := New()

	// Seed a mix of tasks across pipelines, stages, states, and dead-letter
	// flags. Six tasks match the target predicate (pipe-1 + stage-1 + FAILED +
	// dead-lettered + not DISCARDED) so pagination can be exercised.
	type seed struct {
		pipeline, stage    string
		state              broker.TaskState
		routedToDeadLetter bool
	}
	seeds := []seed{
		// Matches (6):
		{"pipe-1", "stage-1", broker.TaskStateFailed, true},
		{"pipe-1", "stage-1", broker.TaskStateFailed, true},
		{"pipe-1", "stage-1", broker.TaskStateFailed, true},
		{"pipe-1", "stage-1", broker.TaskStateFailed, true},
		{"pipe-1", "stage-1", broker.TaskStateFailed, true},
		{"pipe-1", "stage-1", broker.TaskStateFailed, true},
		// Wrong pipeline:
		{"pipe-2", "stage-1", broker.TaskStateFailed, true},
		// Wrong stage:
		{"pipe-1", "stage-2", broker.TaskStateFailed, true},
		// Wrong state:
		{"pipe-1", "stage-1", broker.TaskStatePending, true},
		// Not dead-lettered:
		{"pipe-1", "stage-1", broker.TaskStateFailed, false},
		// DISCARDED (should be excluded unless IncludeDiscarded=true):
		{"pipe-1", "stage-1", broker.TaskStateDiscarded, true},
	}

	var matchingIDs []string
	for i, sd := range seeds {
		task := newTask(sd.pipeline, sd.stage)
		task.State = sd.state
		task.RoutedToDeadLetter = sd.routedToDeadLetter
		if err := s.EnqueueTask(ctx, sd.stage, task); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		// EnqueueTask overwrites State/RoutedToDeadLetter with whatever
		// was set on the task — but some memory stores reset. Apply
		// explicitly via UpdateTask to guarantee the observable state.
		if err := s.UpdateTask(ctx, task.ID, broker.TaskUpdate{
			State:              &sd.state,
			RoutedToDeadLetter: &sd.routedToDeadLetter,
		}); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
		if i < 6 {
			matchingIDs = append(matchingIDs, task.ID)
		}
	}

	pipe := "pipe-1"
	stage := "stage-1"
	state := broker.TaskStateFailed
	dlTrue := true

	// Full filter (no pagination): expect 6 matches, no DISCARDED.
	res, err := s.ListTasks(ctx, broker.TaskFilter{
		PipelineID:         &pipe,
		StageID:            &stage,
		State:              &state,
		RoutedToDeadLetter: &dlTrue,
		IncludeDiscarded:   false,
	})
	if err != nil {
		t.Fatalf("list all filters: %v", err)
	}
	if len(res.Tasks) != 6 {
		t.Fatalf("got %d tasks, want 6", len(res.Tasks))
	}
	got := make(map[string]bool)
	for _, tk := range res.Tasks {
		got[tk.ID] = true
		if tk.PipelineID != pipe || tk.StageID != stage ||
			tk.State != state || !tk.RoutedToDeadLetter ||
			tk.State == broker.TaskStateDiscarded {
			t.Errorf("task %s violates filter: pipe=%s stage=%s state=%s dl=%v",
				tk.ID, tk.PipelineID, tk.StageID, tk.State, tk.RoutedToDeadLetter)
		}
	}
	for _, id := range matchingIDs {
		if !got[id] {
			t.Errorf("expected matching task %s not returned", id)
		}
	}

	// Now exercise Limit + Offset on the same filter: offset=2, limit=2
	// should return 2 items, none overlapping with offset=0,limit=2.
	resA, err := s.ListTasks(ctx, broker.TaskFilter{
		PipelineID:         &pipe,
		StageID:            &stage,
		State:              &state,
		RoutedToDeadLetter: &dlTrue,
		Limit:              2,
		Offset:             0,
	})
	if err != nil {
		t.Fatalf("list page A: %v", err)
	}
	resB, err := s.ListTasks(ctx, broker.TaskFilter{
		PipelineID:         &pipe,
		StageID:            &stage,
		State:              &state,
		RoutedToDeadLetter: &dlTrue,
		Limit:              2,
		Offset:             2,
	})
	if err != nil {
		t.Fatalf("list page B: %v", err)
	}
	if len(resA.Tasks) != 2 || len(resB.Tasks) != 2 {
		t.Fatalf("pagination lengths: A=%d B=%d, want 2/2", len(resA.Tasks), len(resB.Tasks))
	}
	seen := make(map[string]bool)
	for _, tk := range resA.Tasks {
		seen[tk.ID] = true
	}
	for _, tk := range resB.Tasks {
		if seen[tk.ID] {
			t.Errorf("task %s overlaps between offset=0 and offset=2 pages", tk.ID)
		}
	}

	// IncludeDiscarded must surface the DISCARDED task when the remaining
	// filter still accommodates it. Drop the state filter so the DISCARDED
	// task can pass; keep pipeline/stage/dl so we isolate one task.
	resDisc, err := s.ListTasks(ctx, broker.TaskFilter{
		PipelineID:         &pipe,
		StageID:            &stage,
		RoutedToDeadLetter: &dlTrue,
		IncludeDiscarded:   true,
	})
	if err != nil {
		t.Fatalf("list include discarded: %v", err)
	}
	sawDiscarded := false
	for _, tk := range resDisc.Tasks {
		if tk.State == broker.TaskStateDiscarded {
			sawDiscarded = true
		}
	}
	if !sawDiscarded {
		t.Error("IncludeDiscarded=true should surface DISCARDED tasks")
	}

	// And IncludeDiscarded=false should suppress them.
	resNoDisc, err := s.ListTasks(ctx, broker.TaskFilter{
		PipelineID:         &pipe,
		StageID:            &stage,
		RoutedToDeadLetter: &dlTrue,
		IncludeDiscarded:   false,
	})
	if err != nil {
		t.Fatalf("list exclude discarded: %v", err)
	}
	for _, tk := range resNoDisc.Tasks {
		if tk.State == broker.TaskStateDiscarded {
			t.Errorf("IncludeDiscarded=false still returned DISCARDED task %s", tk.ID)
		}
	}
}

// TestMemory_ConcurrentUpdateTask fires 20 goroutines performing UpdateTask
// on the same task. Each writes a distinct metadata key. After all goroutines
// complete, the store must not panic (checked via t.Fail path) and the final
// task state must be valid and consistent.
func TestMemory_ConcurrentUpdateTask(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := New()

	task := newTask("pipe-cu", "stage-cu")
	if err := s.EnqueueTask(ctx, "stage-cu", task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	const N = 20
	var wg sync.WaitGroup
	var failures atomic.Int64
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			md := map[string]any{}
			// Unique key per goroutine so we can detect lost writes if any.
			md[keyForIndex(i)] = i
			if err := s.UpdateTask(ctx, task.ID, broker.TaskUpdate{
				Metadata: md,
			}); err != nil {
				failures.Add(1)
				t.Errorf("goroutine %d update: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if failures.Load() != 0 {
		t.Fatalf("%d update failures under concurrency", failures.Load())
	}

	got, err := s.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("get after concurrent updates: %v", err)
	}

	// The task must remain valid: ID unchanged, state still PENDING (no one
	// wrote state), UpdatedAt advanced.
	if got.ID != task.ID {
		t.Errorf("ID changed: got %s, want %s", got.ID, task.ID)
	}
	if got.State != broker.TaskStatePending {
		t.Errorf("state drifted to %s despite no state writes", got.State)
	}
	if !got.UpdatedAt.After(task.UpdatedAt) {
		t.Error("UpdatedAt did not advance after concurrent updates")
	}

	// Memory store applies metadata as a per-key merge (see TestUpdateTaskFields).
	// Every key from every goroutine must be present — no lost writes for
	// disjoint-key updates, since each UpdateTask is mutex-serialized.
	for i := 0; i < N; i++ {
		if _, ok := got.Metadata[keyForIndex(i)]; !ok {
			t.Errorf("metadata key %s missing — concurrent disjoint-key update lost",
				keyForIndex(i))
		}
	}
}

func keyForIndex(i int) string {
	return "k_" + strconv.Itoa(i)
}
