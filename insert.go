package pgxq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type insertOpts struct {
	queue       string
	priority    int16
	scheduledAt *time.Time
	maxAttempts int16
	table       string
}

// InsertOption configures a job at insert time.
type InsertOption func(*insertOpts)

// WithQueue sets the queue name. Default: "default".
func WithQueue(q string) InsertOption {
	return func(o *insertOpts) { o.queue = q }
}

// WithPriority sets the job priority. Lower values run first. Default: 0.
func WithPriority(p int16) InsertOption {
	return func(o *insertOpts) { o.priority = p }
}

// WithScheduledAt schedules the job for a future time. Default: now.
func WithScheduledAt(t time.Time) InsertOption {
	return func(o *insertOpts) { o.scheduledAt = &t }
}

// WithMaxAttempts overrides the maximum number of attempts. Default: 5.
func WithMaxAttempts(n int16) InsertOption {
	return func(o *insertOpts) { o.maxAttempts = n }
}

// WithTable overrides the table name for this insert. Default: [DefaultTable].
// Must match the table used by the [Client] that will process this job.
func WithTable(t string) InsertOption {
	return func(o *insertOpts) { o.table = t }
}

func insertQuery(table string) string {
	return strings.ReplaceAll(`INSERT INTO {t} (kind, args, queue, priority, scheduled_at, max_attempts)
VALUES ($1, $2, $3, $4, COALESCE($5, now()), $6)
RETURNING id, kind, args, state, queue, priority, attempt, max_attempts,
          scheduled_at, created_at, attempted_at, completed_at, error`, "{t}", table)
}

// Insert enqueues a job. Pass a pgx.Tx for transactional insert or a pgxpool.Pool for standalone.
// Args can be any JSON-marshalable value (struct, map, string, nil).
func Insert(ctx context.Context, db DBTX, kind string, args any, opts ...InsertOption) (*Job, error) {
	if kind == "" {
		return nil, fmt.Errorf("pgxq: kind must not be empty")
	}

	o := insertOpts{
		queue:       defaultQueue,
		table:       DefaultTable,
		maxAttempts: defaultMaxAttempts,
	}
	for _, opt := range opts {
		opt(&o)
	}

	if err := validateTable(o.table); err != nil {
		return nil, err
	}
	if o.maxAttempts < 1 {
		return nil, fmt.Errorf("pgxq: max_attempts must be at least 1, got %d", o.maxAttempts)
	}

	if args == nil {
		args = json.RawMessage("{}")
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("pgxq: marshal args: %w", err)
	}

	row := db.QueryRow(ctx, insertQuery(o.table),
		kind,
		payload,
		o.queue,
		o.priority,
		o.scheduledAt,
		o.maxAttempts,
	)

	return scanJob(row)
}
