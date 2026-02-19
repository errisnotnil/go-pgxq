// Package pgxq provides a PostgreSQL-backed job queue for pgx/v5.
//
// Jobs are enqueued with [Insert] and processed by a [Client] worker pool.
// Enqueue works with any [DBTX] — pass a pgx.Tx for transactional guarantees
// or a pgxpool.Pool for standalone inserts.
package pgxq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DefaultTable is the default table name used when none is specified.
const DefaultTable = "pgxq_job"

// DBTX accepts pgxpool.Pool, pgx.Tx, and pgx.Conn.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// HandlerFunc processes a single job within a transaction.
//
// On success (nil return), the transaction is committed — any database work
// done through tx is atomic with the job completion.
//
// On error, the transaction is rolled back and the job is retried or failed.
// Wrap with [Discard] to skip retries and mark the job as discarded.
type HandlerFunc func(ctx context.Context, tx pgx.Tx, job *Job) error

// Job represents a row in the job table.
type Job struct {
	ID          int64
	Kind        string
	Args        json.RawMessage
	State       JobState
	Queue       string
	Priority    int16
	Attempt     int16
	MaxAttempts int16
	ScheduledAt time.Time
	CreatedAt   time.Time
	AttemptedAt *time.Time
	CompletedAt *time.Time
	Error       *string
}

// JobState represents the lifecycle state of a job.
type JobState string

const (
	// JobStateAvailable means the job is ready to be picked up by a worker.
	JobStateAvailable JobState = "available"
	// JobStateRunning means a worker is currently processing the job.
	JobStateRunning JobState = "running"
	// JobStateCompleted means the job finished successfully.
	JobStateCompleted JobState = "completed"
	// JobStateFailed means the job exhausted all retry attempts.
	JobStateFailed JobState = "failed"
	// JobStateDiscarded means the job was explicitly discarded via [Discard].
	JobStateDiscarded JobState = "discarded"
)

func scanJob(row pgx.Row) (*Job, error) {
	var j Job
	err := row.Scan(
		&j.ID,
		&j.Kind,
		&j.Args,
		&j.State,
		&j.Queue,
		&j.Priority,
		&j.Attempt,
		&j.MaxAttempts,
		&j.ScheduledAt,
		&j.CreatedAt,
		&j.AttemptedAt,
		&j.CompletedAt,
		&j.Error,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// validateTable checks that the table name is a valid identifier ([schema.]table).
func validateTable(table string) error {
	if table == "" {
		return fmt.Errorf("pgxq: table name must not be empty")
	}
	dots := 0
	for _, r := range table {
		if r == '.' {
			dots++
			if dots > 1 {
				return fmt.Errorf("pgxq: invalid table name %q (expected [schema.]table)", table)
			}
			continue
		}
		if !isIdentChar(r) {
			return fmt.Errorf("pgxq: invalid character %q in table name %q", r, table)
		}
	}
	for _, component := range strings.Split(table, ".") {
		if len(component) > 0 && component[0] >= '0' && component[0] <= '9' {
			return fmt.Errorf("pgxq: invalid table name %q (identifier cannot start with a digit)", table)
		}
	}
	if table[0] == '.' || table[len(table)-1] == '.' {
		return fmt.Errorf("pgxq: invalid table name %q (empty component)", table)
	}
	return nil
}

// UnmarshalArgs decodes a job's JSON args into a value of type T.
func UnmarshalArgs[T any](job *Job) (T, error) {
	var v T
	if err := json.Unmarshal(job.Args, &v); err != nil {
		return v, fmt.Errorf("pgxq: unmarshal args: %w", err)
	}
	return v, nil
}

func isIdentChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
}
