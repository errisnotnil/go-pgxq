# pgxq

PostgreSQL job queue for [pgx/v5](https://github.com/jackc/pgx). One table, one dependency, no magic.

- **Transactional enqueue** — insert jobs in the same transaction as your business data
- **`SELECT FOR UPDATE SKIP LOCKED`** — efficient concurrent polling without advisory locks
- **Retry with backoff** — exponential, constant, or custom
- **Rescue orphaned jobs** — automatic recovery after worker crashes
- **Graceful shutdown** — waits for active jobs to complete

## Install

```
go get github.com/errisnotnil/go-pgxq
```

Requires Go 1.24+ and PostgreSQL 10+.

## Schema

Create the job table (or call `pgxq.Migrate`):

```sql
CREATE TABLE IF NOT EXISTS pgxq_job (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind          TEXT        NOT NULL,
    args          JSONB       NOT NULL DEFAULT '{}',
    state         TEXT        NOT NULL DEFAULT 'available',
    queue         TEXT        NOT NULL DEFAULT 'default',
    priority      SMALLINT    NOT NULL DEFAULT 0,
    attempt       SMALLINT    NOT NULL DEFAULT 0,
    max_attempts  SMALLINT    NOT NULL DEFAULT 5,
    scheduled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempted_at  TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    error         TEXT,
    CHECK (state IN ('available', 'running', 'completed', 'failed', 'discarded'))
);

CREATE INDEX IF NOT EXISTS idx_pgxq_job_fetch
    ON pgxq_job (queue, priority, scheduled_at)
    WHERE state = 'available';

CREATE INDEX IF NOT EXISTS idx_pgxq_job_rescue
    ON pgxq_job (attempted_at)
    WHERE state = 'running';
```

Or programmatically:

```go
pgxq.Migrate(ctx, pool, pgxq.DefaultTable)
```

Custom table name (including `schema.table`):

```go
pgxq.Migrate(ctx, pool, "myapp.jobs")
```

## Usage

### Enqueue

```go
// Standalone
pgxq.Insert(ctx, pool, "send_email", SendEmail{To: "user@example.com", Subject: "Hello", Body: "..."})

// In a transaction — job commits or rolls back with your data
tx, _ := pool.Begin(ctx)
queries.CreateAccount(ctx, tx, ...)
pgxq.Insert(ctx, tx, "send_email", SendEmail{To: "user@example.com", Subject: "Welcome", Body: "..."})
tx.Commit(ctx)
```

`Insert` accepts any `pgxq.DBTX` — works with `pgxpool.Pool`, `pgx.Tx`, and `pgx.Conn`.
Args can be any JSON-marshalable value: struct, map, string, or nil.

Options:

```go
pgxq.Insert(ctx, tx, "send_email", args,
    pgxq.WithQueue("emails"),       // default: "default"
    pgxq.WithPriority(-1),          // lower = runs first, default: 0
    pgxq.WithScheduledAt(tomorrow), // default: now
    pgxq.WithMaxAttempts(10),       // default: 5
)
```

### Process

Each handler receives a `pgx.Tx`. On success, the transaction is committed — any
database work done through `tx` is atomic with the job completion. On error, the
transaction is rolled back and the job is retried.

```go
client, err := pgxq.NewClient(pgxq.ClientConfig{
    Pool: pool,
})
if err != nil {
    log.Fatal(err)
}

client.Handle("send_email", func(ctx context.Context, _ pgx.Tx, job *pgxq.Job) error {
    args, err := pgxq.UnmarshalArgs[SendEmail](job)
    if err != nil {
        return pgxq.Discard(err) // bad payload, don't retry
    }
    return emailService.Send(ctx, args.To, args.Subject, args.Body)
})

go client.Start()
// on shutdown:
client.Stop(shutdownCtx)
```

Use `tx` when you need atomicity with the job completion:

```go
client.Handle("process_payment", func(ctx context.Context, tx pgx.Tx, job *pgxq.Job) error {
    args, err := pgxq.UnmarshalArgs[ProcessPayment](job)
    if err != nil {
        return pgxq.Discard(err)
    }
    // Business logic and job completion commit together.
    // If commit fails, everything rolls back and the job is retried.
    _, err = tx.Exec(ctx, "UPDATE account SET balance = balance - $1 WHERE id = $2", args.Amount, args.AccountID)
    return err
})
```

### Retry and discard

- Return `nil` — transaction committed, job completed
- Return `error` — transaction rolled back, job retried with backoff (up to `max_attempts`)
- Return `pgxq.Discard(err)` — transaction rolled back, job discarded immediately

### Configuration

```go
pgxq.ClientConfig{
    Pool:         pool,
    Table:        pgxq.DefaultTable, // custom table name, e.g. "myapp.jobs"
    Queue:        "default",         // queue to poll
    PollInterval: time.Second,       // delay between polls when idle
    BatchSize:    10,                // jobs fetched per poll
    MaxWorkers:   100,               // max concurrent handlers
    RescueAfter:  time.Hour,         // recover orphaned running jobs after this
    Backoff:      pgxq.DefaultBackoff,
    Logger:       slog.Default(),
}
```

`Pool` accepts `*pgxpool.Pool` specifically — the client needs connection pooling for concurrent workers.

`Logger` accepts `*slog.Logger`. Use `slog.New(handler)` to integrate with any logging library (zap, zerolog, etc.) via its `slog.Handler` implementation.

### Custom backoff

```go
// Exponential: base * factor^(attempt-1), capped, with 20% jitter
pgxq.ExponentialBackoff(time.Second, 2.0, time.Hour)

// Constant
pgxq.ConstantBackoff(5 * time.Second)

// Custom
func myBackoff(attempt int) time.Duration {
    return time.Duration(attempt) * 10 * time.Second
}
```

## Job lifecycle

```
available → running → completed
                ↓
            (error) → available (retry)
                ↓
            (max attempts) → failed
                ↓
            (Discard) → discarded
```

Orphaned jobs (worker crashed while processing) are automatically returned to `available` after `RescueAfter`.

## Notes

- **Handler context**: handlers must respect `ctx.Done()` for graceful shutdown. If a handler blocks ignoring context cancellation, `Stop` will return a timeout error and the `Start` goroutine will leak.
- **Cleanup**: completed, failed, and discarded jobs stay in the table. Periodically run `DELETE FROM pgxq_job WHERE state IN ('completed', 'failed', 'discarded') AND completed_at < now() - interval '7 days'` (or similar) to keep the table small.
- **Client reuse**: a `Client` cannot be reused after `Stop` — create a new one if needed.

## License

MIT
