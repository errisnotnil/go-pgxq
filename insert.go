package pgxq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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

var insertQueryCache sync.Map // table name → query string

func insertQuery(table string) string {
	if q, ok := insertQueryCache.Load(table); ok {
		return q.(string)
	}
	q := strings.ReplaceAll(`INSERT INTO {t} (kind, args, queue, priority, scheduled_at, max_attempts)
VALUES ($1, $2, $3, $4, COALESCE($5, now()), $6)
RETURNING id, kind, args, state, queue, priority, attempt, max_attempts,
          scheduled_at, created_at, attempted_at, completed_at, error`, "{t}", table)
	insertQueryCache.Store(table, q)
	return q
}

// Insert enqueues a job. Pass a pgx.Tx for transactional insert or a pgxpool.Pool for standalone.
// Args can be any JSON-marshalable value (struct, map, string, nil).
func Insert(ctx context.Context, db DBTX, kind string, args any, opts ...InsertOption) (*Job, error) {
	if kind == "" {
		return nil, errors.New("pgxq: kind must not be empty")
	}

	o := insertOpts{
		queue:       defaultQueue,
		priority:    0,
		scheduledAt: nil,
		maxAttempts: defaultMaxAttempts,
		table:       DefaultTable,
	}
	for _, opt := range opts {
		opt(&o)
	}

	if err := validateTable(o.table); err != nil {
		return nil, err
	}
	if o.queue == "" {
		return nil, errors.New("pgxq: queue must not be empty")
	}
	if o.maxAttempts < 1 {
		return nil, fmt.Errorf("pgxq: max_attempts must be at least 1, got %d", o.maxAttempts)
	}

	payload, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("pgxq: marshal args: %w", err)
	}
	if string(payload) == "null" {
		payload = []byte("{}")
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
