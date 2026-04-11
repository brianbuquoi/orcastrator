-- Migration 002: Rename table from orcastrator_tasks to overlord_tasks
-- Run this before starting Overlord v0.3.0 against an existing database.
ALTER TABLE orcastrator_tasks RENAME TO overlord_tasks;
ALTER INDEX IF EXISTS orcastrator_tasks_pkey
  RENAME TO overlord_tasks_pkey;
ALTER INDEX IF EXISTS orcastrator_tasks_stage_state_created_idx
  RENAME TO overlord_tasks_stage_state_created_idx;
