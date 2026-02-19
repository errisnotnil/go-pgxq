package pgxq

import (
	"context"
	"fmt"
	"strings"
)

func createTableQuery(table string) string {
	allStates := strings.Join([]string{
		"'" + string(JobStateAvailable) + "'",
		"'" + string(JobStateRunning) + "'",
		"'" + string(JobStateCompleted) + "'",
		"'" + string(JobStateFailed) + "'",
		"'" + string(JobStateDiscarded) + "'",
	}, ", ")

	return strings.NewReplacer(
		"{t}", table,
		"{available}", string(JobStateAvailable),
		"{all_states}", allStates,
		"{max_attempts}", fmt.Sprintf("%d", defaultMaxAttempts),
	).Replace(`CREATE TABLE IF NOT EXISTS {t} (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind          TEXT        NOT NULL,
    args          JSONB       NOT NULL DEFAULT '{}',
    state         TEXT        NOT NULL DEFAULT '{available}',
    queue         TEXT        NOT NULL DEFAULT 'default',
    priority      SMALLINT    NOT NULL DEFAULT 0,
    attempt       SMALLINT    NOT NULL DEFAULT 0,
    max_attempts  SMALLINT    NOT NULL DEFAULT {max_attempts},
    scheduled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempted_at  TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    error         TEXT,
    CHECK (state IN ({all_states}))
)`)
}

func createIndexQuery(table, suffix, columns, where string) string {
	idx := "idx_" + strings.ReplaceAll(table, ".", "_") + "_" + suffix
	return fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s) WHERE %s", idx, table, columns, where)
}

func createFetchIndexQuery(table string) string {
	return createIndexQuery(table, "fetch", "queue, priority, scheduled_at", "state = '"+string(JobStateAvailable)+"'")
}

func createRescueIndexQuery(table string) string {
	return createIndexQuery(table, "rescue", "attempted_at", "state = '"+string(JobStateRunning)+"'")
}

// Schema returns SQL statements to create the job table and indexes.
func Schema(table string) (string, error) {
	if err := validateTable(table); err != nil {
		return "", err
	}
	return createTableQuery(table) + ";\n" + createFetchIndexQuery(table) + ";\n" + createRescueIndexQuery(table) + ";\n", nil
}

// Migrate creates the job table and indexes if they don't exist.
func Migrate(ctx context.Context, db DBTX, table string) error {
	if err := validateTable(table); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, createTableQuery(table)); err != nil {
		return fmt.Errorf("pgxq: create table: %w", err)
	}
	if _, err := db.Exec(ctx, createFetchIndexQuery(table)); err != nil {
		return fmt.Errorf("pgxq: create fetch index: %w", err)
	}
	if _, err := db.Exec(ctx, createRescueIndexQuery(table)); err != nil {
		return fmt.Errorf("pgxq: create rescue index: %w", err)
	}
	return nil
}
