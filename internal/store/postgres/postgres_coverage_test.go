//go:build integration

package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/brianbuquoi/overlord/internal/broker"
)

// TestPostgres_Migration_003 creates a table representing the pre-003 schema
// (the state before migrations/003_store_parity.sql was applied), runs the
// migration SQL, and verifies that the two new columns (routed_to_dead_letter,
// cross_stage_transitions) exist with the expected defaults and types. Also
// checks the partial index is created.
func TestPostgres_Migration_003(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	// Migration 003 targets a fixed table name ("overlord_tasks"). Use a
	// session-local replica: create a dedicated test table, run the
	// migration SQL rewritten against our table name, and then inspect the
	// information_schema for the column presence and defaults.
	table := "overlord_tasks_mig003_" + sanitizeID(uuid.New().String()[:8])

	// Pre-003 schema — matches migrations/001_initial.sql layout (no
	// routed_to_dead_letter, no cross_stage_transitions).
	_, err = pool.Exec(ctx, `CREATE TABLE `+table+` (
		id                    TEXT PRIMARY KEY,
		pipeline_id           TEXT NOT NULL,
		stage_id              TEXT NOT NULL,
		input_schema_name     TEXT NOT NULL DEFAULT '',
		input_schema_version  TEXT NOT NULL DEFAULT '',
		output_schema_name    TEXT NOT NULL DEFAULT '',
		output_schema_version TEXT NOT NULL DEFAULT '',
		payload               JSONB,
		metadata              JSONB,
		state                 TEXT NOT NULL DEFAULT 'PENDING',
		attempts              INTEGER NOT NULL DEFAULT 0,
		max_attempts          INTEGER NOT NULL DEFAULT 1,
		created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
		expires_at            TIMESTAMPTZ
	)`)
	if err != nil {
		t.Fatalf("create pre-003 table: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	})

	// Apply migration 003 SQL against our test table.
	migrationSQL := `
	ALTER TABLE ` + table + `
		ADD COLUMN IF NOT EXISTS routed_to_dead_letter   BOOLEAN NOT NULL DEFAULT FALSE,
		ADD COLUMN IF NOT EXISTS cross_stage_transitions INTEGER NOT NULL DEFAULT 0;
	CREATE INDEX IF NOT EXISTS idx_` + table + `_dead_letter
		ON ` + table + ` (state, routed_to_dead_letter)
		WHERE routed_to_dead_letter = TRUE;
	`
	if _, err := pool.Exec(ctx, migrationSQL); err != nil {
		t.Fatalf("apply migration 003: %v", err)
	}

	// Inspect information_schema for the two new columns.
	type col struct {
		name     string
		dataType string
		dflt     *string
		nullable string
	}
	wantDefaults := map[string]string{
		"routed_to_dead_letter":   "false",
		"cross_stage_transitions": "0",
	}
	wantType := map[string]string{
		"routed_to_dead_letter":   "boolean",
		"cross_stage_transitions": "integer",
	}
	for colName := range wantDefaults {
		var c col
		c.name = colName
		err := pool.QueryRow(ctx, `SELECT data_type, column_default, is_nullable
			FROM information_schema.columns
			WHERE table_name = $1 AND column_name = $2`, table, colName).Scan(
			&c.dataType, &c.dflt, &c.nullable,
		)
		if err != nil {
			t.Errorf("column %s not found after migration: %v", colName, err)
			continue
		}
		if c.dataType != wantType[colName] {
			t.Errorf("column %s: data_type=%q, want %q", colName, c.dataType, wantType[colName])
		}
		if c.nullable != "NO" {
			t.Errorf("column %s: is_nullable=%q, want NO", colName, c.nullable)
		}
		if c.dflt == nil || !strings.Contains(*c.dflt, wantDefaults[colName]) {
			var got string
			if c.dflt != nil {
				got = *c.dflt
			}
			t.Errorf("column %s: default=%q, want to contain %q", colName, got, wantDefaults[colName])
		}
	}

	// Verify the defaults actually apply to a plain INSERT that omits the
	// new columns.
	id := uuid.New().String()
	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s
		(id, pipeline_id, stage_id, payload, metadata)
		VALUES ($1, $2, $3, $4, $5)`, table),
		id, "p", "s", []byte(`{}`), []byte(`{}`))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	var dl bool
	var cst int
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT routed_to_dead_letter, cross_stage_transitions
			FROM %s WHERE id = $1`, table), id,
	).Scan(&dl, &cst); err != nil {
		t.Fatalf("select defaults: %v", err)
	}
	if dl || cst != 0 {
		t.Errorf("defaults: routed_to_dead_letter=%v cross_stage_transitions=%d, want false/0", dl, cst)
	}

	// Verify the partial index exists.
	var idxCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM pg_indexes
		 WHERE tablename = $1 AND indexname = $2`,
		table, "idx_"+table+"_dead_letter",
	).Scan(&idxCount); err != nil {
		t.Fatalf("pg_indexes query: %v", err)
	}
	if idxCount != 1 {
		t.Errorf("dead-letter partial index: got %d, want 1", idxCount)
	}

	// Re-running the migration must be idempotent (IF NOT EXISTS guards).
	if _, err := pool.Exec(ctx, migrationSQL); err != nil {
		t.Fatalf("migration re-apply: %v", err)
	}
}

// TestPostgres_ListTasks_LargeResult inserts 200 tasks into a single
// (pipeline,stage), paginates them with Limit=25 across 8 pages, and verifies:
//   - every task is returned exactly once;
//   - Total matches across every page;
//   - pages are ordered by created_at ASC.
func TestPostgres_ListTasks_LargeResult(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	table := "overlord_list_large_" + sanitizeID(uuid.New().String()[:8])
	_, err = pool.Exec(ctx, `CREATE TABLE `+table+` (
		id                      TEXT PRIMARY KEY,
		pipeline_id             TEXT NOT NULL,
		stage_id                TEXT NOT NULL,
		input_schema_name       TEXT NOT NULL DEFAULT '',
		input_schema_version    TEXT NOT NULL DEFAULT '',
		output_schema_name      TEXT NOT NULL DEFAULT '',
		output_schema_version   TEXT NOT NULL DEFAULT '',
		payload                 JSONB,
		metadata                JSONB,
		state                   TEXT NOT NULL DEFAULT 'PENDING',
		attempts                INTEGER NOT NULL DEFAULT 0,
		max_attempts            INTEGER NOT NULL DEFAULT 1,
		created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
		expires_at              TIMESTAMPTZ,
		routed_to_dead_letter   BOOLEAN NOT NULL DEFAULT FALSE,
		cross_stage_transitions INTEGER NOT NULL DEFAULT 0
	)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	})

	s, err := New(pool, table)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	const total = 200
	expected := make(map[string]bool, total)
	for i := 0; i < total; i++ {
		task := parityTask(uuid.New().String(), "pipe-large", "stage-large", broker.TaskStatePending)
		// Stagger CreatedAt so the ASC ordering is deterministic across pages.
		task.CreatedAt = task.CreatedAt.Add(-1 * (200 - time.Duration(i)) * time.Millisecond)
		expected[task.ID] = true
		if err := s.EnqueueTask(ctx, "stage-large", task); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	pipe := "pipe-large"
	stage := "stage-large"
	pageSize := 25
	seen := make(map[string]bool, total)
	var prevCreatedAtUnix int64

	for page := 0; page*pageSize < total; page++ {
		res, err := s.ListTasks(ctx, broker.TaskFilter{
			PipelineID: &pipe,
			StageID:    &stage,
			Limit:      pageSize,
			Offset:     page * pageSize,
		})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if res.Total != total {
			t.Errorf("page %d total: got %d, want %d", page, res.Total, total)
		}
		wantLen := pageSize
		if (page+1)*pageSize > total {
			wantLen = total - page*pageSize
		}
		if len(res.Tasks) != wantLen {
			t.Errorf("page %d length: got %d, want %d", page, len(res.Tasks), wantLen)
		}
		for _, tk := range res.Tasks {
			if seen[tk.ID] {
				t.Errorf("task %s returned on multiple pages", tk.ID)
			}
			seen[tk.ID] = true
			createdUnix := tk.CreatedAt.UnixNano()
			if createdUnix < prevCreatedAtUnix {
				t.Errorf("ordering broken on page %d: createdAt %d < previous %d",
					page, createdUnix, prevCreatedAtUnix)
			}
			prevCreatedAtUnix = createdUnix
		}
	}

	if len(seen) != total {
		t.Errorf("saw %d unique tasks across all pages, want %d", len(seen), total)
	}
	for id := range expected {
		if !seen[id] {
			t.Errorf("task %s missing from paginated results", id)
		}
	}
}
