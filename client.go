package pgxq

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ClientConfig configures a [Client] worker pool.
type ClientConfig struct {
	// Pool is the pgx connection pool. Required.
	Pool *pgxpool.Pool
	// Table is the job table name. Default: [DefaultTable].
	Table string
	// Queue is the queue name to poll. Default: "default".
	Queue string
	// PollInterval is the delay between polls when no jobs are found. Default: 1s.
	PollInterval time.Duration
	// BatchSize is the max number of jobs fetched per poll. Default: 10.
	BatchSize int
	// MaxWorkers is the max concurrent job goroutines. Default: 100.
	MaxWorkers int
	// RescueAfter is the duration after which a running job is considered orphaned. Default: 1h.
	RescueAfter time.Duration
	// Backoff returns the delay before the next retry. Default: [DefaultBackoff].
	Backoff BackoffFunc
	// Logger for operational messages. Default: slog.Default().
	Logger *slog.Logger
}

const (
	defaultQueue        = "default"
	defaultPollInterval = time.Second
	defaultBatchSize    = 10
	defaultMaxWorkers   = 100
	defaultMaxAttempts  = 5
	defaultRescueAfter  = time.Hour
)

func (c *ClientConfig) setDefaults() {
	if c.Table == "" {
		c.Table = DefaultTable
	}
	if c.Queue == "" {
		c.Queue = defaultQueue
	}
	if c.PollInterval == 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.BatchSize == 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.MaxWorkers == 0 {
		c.MaxWorkers = defaultMaxWorkers
	}
	if c.RescueAfter == 0 {
		c.RescueAfter = defaultRescueAfter
	}
	if c.Backoff == nil {
		c.Backoff = DefaultBackoff
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// clientQueries holds pre-built SQL for a specific table name.
type clientQueries struct {
	fetch   string
	complete string
	retry    string
	fail     string
	discard  string
	rescue   string
	release  string
}

func buildQueries(table string) clientQueries {
	r := strings.NewReplacer(
		"{t}", table,
		"{available}", string(JobStateAvailable),
		"{running}", string(JobStateRunning),
		"{completed}", string(JobStateCompleted),
		"{failed}", string(JobStateFailed),
		"{discarded}", string(JobStateDiscarded),
	).Replace

	return clientQueries{
		fetch: r(`WITH locked AS (
    SELECT id FROM {t}
    WHERE queue = $1
      AND state = '{available}'
      AND scheduled_at <= now()
      AND kind = ANY($3)
    ORDER BY priority, scheduled_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE {t}
SET state = '{running}', attempted_at = now(), attempt = attempt + 1
FROM locked WHERE {t}.id = locked.id
RETURNING {t}.id, {t}.kind, {t}.args, {t}.state,
          {t}.queue, {t}.priority, {t}.attempt, {t}.max_attempts,
          {t}.scheduled_at, {t}.created_at, {t}.attempted_at,
          {t}.completed_at, {t}.error`),

		complete: r(`UPDATE {t} SET state = '{completed}', completed_at = now() WHERE id = $1`),
		retry:    r(`UPDATE {t} SET state = '{available}', scheduled_at = now() + $2 * interval '1 second', error = $3 WHERE id = $1`),
		fail:     r(`UPDATE {t} SET state = '{failed}', completed_at = now(), error = $2 WHERE id = $1`),
		discard:  r(`UPDATE {t} SET state = '{discarded}', completed_at = now(), error = $2 WHERE id = $1`),
		rescue:   r(`UPDATE {t} SET state = '{available}', scheduled_at = now() WHERE state = '{running}' AND queue = $2 AND attempted_at < now() - $1 * interval '1 second'`),
		release:  r(`UPDATE {t} SET state = '{available}', attempt = attempt - 1, attempted_at = NULL WHERE id = ANY($1) AND state = '{running}'`),
	}
}

// Client polls for jobs and dispatches them to registered handlers.
//
// Handle must be called before Start. The client cannot be reused after Stop —
// create a new one if needed.
type Client struct {
	cfg      ClientConfig
	queries  clientQueries
	handlers map[string]HandlerFunc
	kinds    []string // registered handler kinds for fetch filter
	mu       sync.Mutex
	started  bool

	pollCtx    context.Context
	pollCancel context.CancelFunc
	jobCtx     context.Context
	jobCancel  context.CancelFunc
	done       chan struct{}
}

// NewClient creates a worker pool client.
func NewClient(cfg ClientConfig) (*Client, error) {
	cfg.setDefaults()
	if cfg.Pool == nil {
		return nil, fmt.Errorf("pgxq: Pool is required")
	}
	if err := validateTable(cfg.Table); err != nil {
		return nil, err
	}
	if cfg.PollInterval < 0 {
		return nil, fmt.Errorf("pgxq: PollInterval must not be negative")
	}
	if cfg.BatchSize < 0 {
		return nil, fmt.Errorf("pgxq: BatchSize must not be negative")
	}
	if cfg.MaxWorkers < 0 {
		return nil, fmt.Errorf("pgxq: MaxWorkers must not be negative")
	}
	if cfg.RescueAfter < 0 {
		return nil, fmt.Errorf("pgxq: RescueAfter must not be negative")
	}
	pollCtx, pollCancel := context.WithCancel(context.Background())
	jobCtx, jobCancel := context.WithCancel(context.Background())
	return &Client{
		cfg:        cfg,
		queries:    buildQueries(cfg.Table),
		handlers:   make(map[string]HandlerFunc),
		done:       make(chan struct{}),
		pollCtx:    pollCtx,
		pollCancel: pollCancel,
		jobCtx:     jobCtx,
		jobCancel:  jobCancel,
	}, nil
}

// Handle registers a handler for the given job kind.
// Must be called before [Client.Start]. Panics if called after Start.
func (c *Client) Handle(kind string, fn HandlerFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		panic("pgxq: Handle called after Start")
	}
	if _, exists := c.handlers[kind]; !exists {
		c.kinds = append(c.kinds, kind)
	}
	c.handlers[kind] = fn
}

// Start begins polling and processing jobs. It blocks until [Client.Stop]
// is called and all active jobs complete.
// Returns an error if called more than once.
//
// Handlers must respect ctx.Done() for graceful shutdown. If a handler
// blocks indefinitely ignoring context cancellation, [Client.Stop] will
// return a timeout error and the Start goroutine will leak.
func (c *Client) Start() error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return fmt.Errorf("pgxq: client already started")
	}
	c.started = true
	c.mu.Unlock()

	defer close(c.done)
	defer c.jobCancel()

	if len(c.handlers) == 0 {
		c.cfg.Logger.Warn("pgxq: starting with no handlers registered")
	}

	c.cfg.Logger.Info("pgxq: starting", "queue", c.cfg.Queue, "workers", c.cfg.MaxWorkers)

	var wg sync.WaitGroup
	sem := make(chan struct{}, c.cfg.MaxWorkers)

	// Rescue orphaned jobs periodically.
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.rescueLoop(c.pollCtx)
	}()

	// Poll loop.
	for {
		select {
		case <-c.pollCtx.Done():
			goto shutdown
		default:
		}

		jobs, err := c.fetchBatch(c.pollCtx)
		if err != nil {
			if c.pollCtx.Err() != nil {
				continue // shutting down
			}
			c.cfg.Logger.Error("pgxq: fetch error", "err", err)
			sleepCtx(c.pollCtx, c.cfg.PollInterval)
			continue
		}

		if len(jobs) == 0 {
			sleepCtx(c.pollCtx, c.cfg.PollInterval)
			continue
		}

		for i, job := range jobs {
			select {
			case sem <- struct{}{}:
			case <-c.pollCtx.Done():
				c.releaseJobs(jobs[i:])
				goto shutdown
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				c.processJob(c.jobCtx, job)
			}()
		}
	}

shutdown:
	c.cfg.Logger.Info("pgxq: shutting down, waiting for active jobs")
	wg.Wait()
	c.cfg.Logger.Info("pgxq: shutdown complete")
	return nil
}

// Stop gracefully stops the client. It stops polling for new jobs and waits
// for active jobs to complete. If ctx expires before all jobs finish,
// active job contexts are cancelled and Stop returns ctx.Err().
//
// Safe to call before Start — cancels the poll context so a subsequent
// Start returns immediately.
func (c *Client) Stop(ctx context.Context) error {
	c.pollCancel()
	c.mu.Lock()
	started := c.started
	c.mu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		c.jobCancel()
		return ctx.Err()
	}
}

func (c *Client) fetchBatch(ctx context.Context) ([]*Job, error) {
	rows, err := c.cfg.Pool.Query(ctx, c.queries.fetch, c.cfg.Queue, c.cfg.BatchSize, c.kinds)
	if err != nil {
		return nil, fmt.Errorf("pgxq: fetch: %w", err)
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		j, scanErr := scanJob(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("pgxq: scan: %w", scanErr)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (c *Client) processJob(ctx context.Context, job *Job) {
	handler, ok := c.handlers[job.Kind]
	if !ok {
		errMsg := fmt.Sprintf("no handler registered for kind %q", job.Kind)
		c.cfg.Logger.Error("pgxq: "+errMsg, "job_id", job.ID)
		if _, err := c.cfg.Pool.Exec(ctx, c.queries.discard, job.ID, errMsg); err != nil {
			c.cfg.Logger.Error("pgxq: discard (no handler)", "job_id", job.ID, "err", err)
		}
		return
	}

	tx, err := c.cfg.Pool.Begin(ctx)
	if err != nil {
		c.cfg.Logger.Error("pgxq: begin tx", "job_id", job.ID, "err", err)
		if _, execErr := c.cfg.Pool.Exec(ctx, c.queries.release, []int64{job.ID}); execErr != nil {
			c.cfg.Logger.Error("pgxq: release after begin failure", "job_id", job.ID, "err", execErr)
		}
		return
	}

	// Recover from handler panics: rollback tx, discard the job.
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback(ctx)
			errMsg := fmt.Sprintf("panic: %v", r)
			c.cfg.Logger.Error("pgxq: handler panic", "job_id", job.ID, "kind", job.Kind, "panic", r, "stack", string(debug.Stack()))
			if _, execErr := c.cfg.Pool.Exec(ctx, c.queries.discard, job.ID, errMsg); execErr != nil {
				c.cfg.Logger.Error("pgxq: discard after panic", "job_id", job.ID, "err", execErr)
			}
		}
	}()

	handlerErr := handler(ctx, tx, job)

	if handlerErr == nil {
		if _, err := tx.Exec(ctx, c.queries.complete, job.ID); err != nil {
			c.cfg.Logger.Error("pgxq: complete", "job_id", job.ID, "err", err)
			_ = tx.Rollback(ctx)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			c.cfg.Logger.Error("pgxq: commit", "job_id", job.ID, "err", err)
			_ = tx.Rollback(ctx)
		}
		return
	}

	// Error: rollback handler's work, update state separately.
	_ = tx.Rollback(ctx)
	c.handleError(ctx, job, handlerErr)
}

func (c *Client) handleError(ctx context.Context, job *Job, err error) {
	errMsg := err.Error()

	if isDiscard(err) {
		c.cfg.Logger.Warn("pgxq: job discarded", "job_id", job.ID, "kind", job.Kind, "err", errMsg)
		if _, execErr := c.cfg.Pool.Exec(ctx, c.queries.discard, job.ID, errMsg); execErr != nil {
			c.cfg.Logger.Error("pgxq: discard", "job_id", job.ID, "err", execErr)
		}
		return
	}

	if job.Attempt >= job.MaxAttempts {
		c.cfg.Logger.Error("pgxq: job failed (max attempts)", "job_id", job.ID, "kind", job.Kind, "attempts", job.Attempt, "err", errMsg)
		if _, execErr := c.cfg.Pool.Exec(ctx, c.queries.fail, job.ID, errMsg); execErr != nil {
			c.cfg.Logger.Error("pgxq: fail", "job_id", job.ID, "err", execErr)
		}
		return
	}

	delay := c.cfg.Backoff(int(job.Attempt))
	c.cfg.Logger.Warn("pgxq: job retry", "job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt, "delay", delay, "err", errMsg)
	if _, execErr := c.cfg.Pool.Exec(ctx, c.queries.retry, job.ID, delay.Seconds(), errMsg); execErr != nil {
		c.cfg.Logger.Error("pgxq: retry", "job_id", job.ID, "err", execErr)
	}
}

func (c *Client) releaseJobs(jobs []*Job) {
	if len(jobs) == 0 {
		return
	}
	ids := make([]int64, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	// Use Background — pollCtx is already cancelled at this point.
	if _, err := c.cfg.Pool.Exec(context.Background(), c.queries.release, ids); err != nil {
		c.cfg.Logger.Error("pgxq: release undispatched jobs", "count", len(jobs), "err", err)
		return
	}
	c.cfg.Logger.Info("pgxq: released undispatched jobs", "count", len(jobs))
}

func (c *Client) rescueLoop(ctx context.Context) {
	interval := c.cfg.RescueAfter / 2
	for {
		sleepCtx(ctx, interval)
		if ctx.Err() != nil {
			return
		}
		rescued, err := c.cfg.Pool.Exec(ctx, c.queries.rescue, c.cfg.RescueAfter.Seconds(), c.cfg.Queue)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.cfg.Logger.Error("pgxq: rescue", "err", err)
			continue
		}
		if rescued.RowsAffected() > 0 {
			c.cfg.Logger.Warn("pgxq: rescued orphaned jobs", "count", rescued.RowsAffected())
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
