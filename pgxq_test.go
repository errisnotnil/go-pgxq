package pgxq_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/errisnotnil/go-pgxq"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Allow override via env var (CI with external DB, local dev, etc.).
	connURL := os.Getenv("PGXQ_TEST_DATABASE_URL")
	var pgContainer *postgres.PostgresContainer

	if connURL == "" {
		var err error
		pgContainer, err = postgres.Run(ctx, "postgres:17",
			postgres.WithDatabase("pgxq_test"),
			postgres.WithUsername("test"),
			postgres.WithPassword("test"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(30*time.Second),
			),
		)
		if err != nil {
			log.Fatalf("start postgres container: %v", err)
		}

		connURL, err = pgContainer.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			log.Fatalf("connection string: %v", err)
		}
	}

	pool, err := pgxpool.New(ctx, connURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	testPool = pool

	if err := pgxq.Migrate(ctx, pool, pgxq.DefaultTable); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	code := m.Run()

	pool.Close()
	if pgContainer != nil {
		_ = pgContainer.Terminate(ctx)
	}
	os.Exit(code)
}

// --- helpers ---

func truncate(t *testing.T) {
	t.Helper()
	_, err := testPool.Exec(context.Background(), "TRUNCATE "+pgxq.DefaultTable)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func jobState(t *testing.T, id int64) pgxq.JobState {
	t.Helper()
	var state pgxq.JobState
	err := testPool.QueryRow(context.Background(),
		"SELECT state FROM "+pgxq.DefaultTable+" WHERE id = $1", id).Scan(&state)
	if err != nil {
		t.Fatalf("query job state: %v", err)
	}
	return state
}

func waitForState(t *testing.T, id int64, want pgxq.JobState) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for job %d to reach state %q", id, want)
		default:
		}
		if s := jobState(t, id); s == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func jobCount(t *testing.T) int {
	t.Helper()
	var n int
	err := testPool.QueryRow(context.Background(),
		"SELECT count(*) FROM "+pgxq.DefaultTable).Scan(&n)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func newClient(t *testing.T, opts ...func(*pgxq.ClientConfig)) *pgxq.Client {
	t.Helper()
	cfg := pgxq.ClientConfig{
		Pool:         testPool,
		PollInterval: 50 * time.Millisecond,
		Logger:       silentLogger(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	client, err := pgxq.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

// stopClient is a test helper: stops client with a generous timeout.
func stopClient(t *testing.T, client *pgxq.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Stop(ctx); err != nil {
		t.Errorf("stop: %v", err)
	}
}

// --- test job type ---

type testArgs struct {
	Value string `json:"value"`
}

// --- insert tests ---

func TestInsert(t *testing.T) {
	truncate(t)

	job, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "hello"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	if job.Kind != "test" {
		t.Errorf("kind: got %q, want %q", job.Kind, "test")
	}
	if job.State != pgxq.JobStateAvailable {
		t.Errorf("state: got %q, want %q", job.State, pgxq.JobStateAvailable)
	}
	if job.Queue != "default" {
		t.Errorf("queue: got %q, want %q", job.Queue, "default")
	}
	if job.Priority != 0 {
		t.Errorf("priority: got %d, want 0", job.Priority)
	}
	if job.MaxAttempts != 5 {
		t.Errorf("max_attempts: got %d, want 5", job.MaxAttempts)
	}

	args, err := pgxq.UnmarshalArgs[testArgs](job)
	if err != nil {
		t.Fatalf("unmarshal args: %v", err)
	}
	if args.Value != "hello" {
		t.Errorf("args.Value: got %q, want %q", args.Value, "hello")
	}
}

func TestInsertOptions(t *testing.T) {
	truncate(t)
	scheduled := time.Now().Add(time.Hour).Truncate(time.Microsecond)

	job, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "opts"},
		pgxq.WithQueue("emails"),
		pgxq.WithPriority(-5),
		pgxq.WithScheduledAt(scheduled),
		pgxq.WithMaxAttempts(10),
	)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	if job.Queue != "emails" {
		t.Errorf("queue: got %q, want %q", job.Queue, "emails")
	}
	if job.Priority != -5 {
		t.Errorf("priority: got %d, want -5", job.Priority)
	}
	if !job.ScheduledAt.Equal(scheduled) {
		t.Errorf("scheduled_at: got %v, want %v", job.ScheduledAt, scheduled)
	}
	if job.MaxAttempts != 10 {
		t.Errorf("max_attempts: got %d, want 10", job.MaxAttempts)
	}
}

func TestInsertTransactional(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	// Commit: job should exist.
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err = pgxq.Insert(ctx, tx, "test", testArgs{Value: "committed"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := jobCount(t); n != 1 {
		t.Fatalf("after commit: got %d jobs, want 1", n)
	}

	// Rollback: job should not exist.
	tx2, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err = pgxq.Insert(ctx, tx2, "test", testArgs{Value: "rolled_back"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if n := jobCount(t); n != 1 {
		t.Fatalf("after rollback: got %d jobs, want 1", n)
	}
}

func TestInsertNilArgs(t *testing.T) {
	truncate(t)

	job, err := pgxq.Insert(context.Background(), testPool, "test", nil)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if string(job.Args) != "{}" {
		t.Errorf("args: got %s, want {}", job.Args)
	}
}

func TestInsertEmptyKind(t *testing.T) {
	truncate(t)

	_, err := pgxq.Insert(context.Background(), testPool, "", nil)
	if err == nil {
		t.Error("expected error for empty kind")
	}
}

func TestInsertInvalidMaxAttempts(t *testing.T) {
	truncate(t)

	_, err := pgxq.Insert(context.Background(), testPool, "test", nil, pgxq.WithMaxAttempts(0))
	if err == nil {
		t.Error("expected error for max_attempts=0")
	}

	_, err = pgxq.Insert(context.Background(), testPool, "test", nil, pgxq.WithMaxAttempts(-1))
	if err == nil {
		t.Error("expected error for max_attempts=-1")
	}
}

// --- client tests ---

func TestClientProcess(t *testing.T) {
	truncate(t)

	_, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "process_me"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	received := make(chan testArgs, 1)
	client := newClient(t)
	client.Handle("test", func(_ context.Context, _ pgx.Tx, job *pgxq.Job) error {
		a, err := pgxq.UnmarshalArgs[testArgs](job)
		if err != nil {
			return err
		}
		received <- a
		return nil
	})

	go client.Start() //nolint:errcheck

	select {
	case a := <-received:
		if a.Value != "process_me" {
			t.Errorf("args.Value: got %q, want %q", a.Value, "process_me")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for job")
	}

	stopClient(t, client)

	var state pgxq.JobState
	err = testPool.QueryRow(context.Background(),
		"SELECT state FROM "+pgxq.DefaultTable+" LIMIT 1").Scan(&state)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if state != pgxq.JobStateCompleted {
		t.Errorf("state: got %q, want %q", state, pgxq.JobStateCompleted)
	}
}

func TestClientHandlerTx(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	_, err := testPool.Exec(ctx, "CREATE TABLE IF NOT EXISTS pgxq_test_scratch (value TEXT)")
	if err != nil {
		t.Fatalf("create scratch: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), "DROP TABLE IF EXISTS pgxq_test_scratch") //nolint:errcheck
	})

	_, err = pgxq.Insert(ctx, testPool, "test", testArgs{Value: "atomic"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	done := make(chan struct{})
	client := newClient(t)
	client.Handle("test", func(ctx context.Context, tx pgx.Tx, _ *pgxq.Job) error {
		_, err := tx.Exec(ctx, "INSERT INTO pgxq_test_scratch (value) VALUES ($1)", "from_handler")
		if err != nil {
			return err
		}
		close(done)
		return nil
	})

	go client.Start() //nolint:errcheck

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	stopClient(t, client)

	var value string
	err = testPool.QueryRow(context.Background(),
		"SELECT value FROM pgxq_test_scratch LIMIT 1").Scan(&value)
	if err != nil {
		t.Fatalf("query scratch: %v", err)
	}
	if value != "from_handler" {
		t.Errorf("scratch value: got %q, want %q", value, "from_handler")
	}
}

func TestClientRetry(t *testing.T) {
	truncate(t)

	job, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "retry_me"}, pgxq.WithMaxAttempts(3))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var attempts atomic.Int32
	done := make(chan struct{})

	client := newClient(t, func(c *pgxq.ClientConfig) {
		c.Backoff = pgxq.ConstantBackoff(0)
	})
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("attempt %d failed", n)
		}
		close(done)
		return nil
	})

	go client.Start() //nolint:errcheck

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	stopClient(t, client)

	if n := attempts.Load(); n != 3 {
		t.Errorf("attempts: got %d, want 3", n)
	}
	if s := jobState(t, job.ID); s != pgxq.JobStateCompleted {
		t.Errorf("state: got %q, want %q", s, pgxq.JobStateCompleted)
	}
}

func TestClientDiscard(t *testing.T) {
	truncate(t)

	job, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "discard_me"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	done := make(chan struct{})
	client := newClient(t)
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error {
		close(done)
		return pgxq.Discard(fmt.Errorf("bad payload"))
	})

	go client.Start() //nolint:errcheck

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	stopClient(t, client)

	if s := jobState(t, job.ID); s != pgxq.JobStateDiscarded {
		t.Errorf("state: got %q, want %q", s, pgxq.JobStateDiscarded)
	}
}

func TestClientMaxAttempts(t *testing.T) {
	truncate(t)

	job, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "fail_me"}, pgxq.WithMaxAttempts(2))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var attempts atomic.Int32

	client := newClient(t, func(c *pgxq.ClientConfig) {
		c.Backoff = pgxq.ConstantBackoff(0)
	})
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error {
		attempts.Add(1)
		return fmt.Errorf("always fail")
	})

	go client.Start() //nolint:errcheck

	waitForState(t, job.ID, pgxq.JobStateFailed)
	stopClient(t, client)

	if n := attempts.Load(); n != 2 {
		t.Errorf("attempts: got %d, want 2", n)
	}
}

func TestClientUnknownKind(t *testing.T) {
	truncate(t)

	job, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "unknown"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Client only handles "other_kind" — should not fetch "test" jobs.
	client := newClient(t)
	client.Handle("other_kind", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error { return nil })
	go client.Start() //nolint:errcheck

	// Wait a bit and verify the job is still available (not fetched).
	time.Sleep(300 * time.Millisecond)
	if s := jobState(t, job.ID); s != pgxq.JobStateAvailable {
		t.Errorf("state: got %q, want %q", s, pgxq.JobStateAvailable)
	}
	stopClient(t, client)
}

func TestClientHandlerPanic(t *testing.T) {
	truncate(t)

	job, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "panic"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	client := newClient(t)
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error {
		panic("handler exploded")
	})

	go client.Start() //nolint:errcheck

	waitForState(t, job.ID, pgxq.JobStateDiscarded)
	stopClient(t, client)

	// Verify error message contains panic info.
	var errMsg *string
	err = testPool.QueryRow(context.Background(),
		"SELECT error FROM "+pgxq.DefaultTable+" WHERE id = $1", job.ID).Scan(&errMsg)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if errMsg == nil || !strings.Contains(*errMsg, "panic") {
		t.Errorf("error: got %v, want containing 'panic'", errMsg)
	}
}

func TestConcurrentWorkers(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	const numJobs = 20
	for i := 0; i < numJobs; i++ {
		_, err := pgxq.Insert(ctx, testPool, "test", testArgs{Value: fmt.Sprintf("job_%d", i)})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	var processed atomic.Int32
	done := make(chan struct{})

	client := newClient(t, func(c *pgxq.ClientConfig) {
		c.MaxWorkers = 5
		c.BatchSize = 10
	})
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error {
		time.Sleep(50 * time.Millisecond) // simulate work
		if n := processed.Add(1); n == numJobs {
			close(done)
		}
		return nil
	})

	go client.Start() //nolint:errcheck

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}

	stopClient(t, client)

	var completed int
	err := testPool.QueryRow(ctx,
		"SELECT count(*) FROM "+pgxq.DefaultTable+" WHERE state = 'completed'").Scan(&completed)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if completed != numJobs {
		t.Errorf("completed: got %d, want %d", completed, numJobs)
	}
}

func TestGracefulShutdown(t *testing.T) {
	truncate(t)

	_, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "slow"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})

	client := newClient(t)
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error {
		close(handlerStarted)
		time.Sleep(500 * time.Millisecond)
		close(handlerDone)
		return nil
	})

	clientDone := make(chan error, 1)
	go func() { clientDone <- client.Start() }()

	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for handler start")
	}

	// Stop with enough time for the handler to finish.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := client.Stop(stopCtx); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}

	select {
	case <-handlerDone:
	default:
		t.Error("handler did not complete before Stop returned")
	}

	select {
	case err := <-clientDone:
		if err != nil {
			t.Errorf("Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
}

// --- table tests ---

func TestCustomTable(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	table := "pgxq_custom_test"

	if err := pgxq.Migrate(ctx, testPool, table); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) //nolint:errcheck
	})

	job, err := pgxq.Insert(ctx, testPool, "test", testArgs{Value: "custom"}, pgxq.WithTable(table))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	if n := jobCount(t); n != 0 {
		t.Errorf("default table: got %d jobs, want 0", n)
	}

	var state pgxq.JobState
	err = testPool.QueryRow(ctx, "SELECT state FROM "+table+" WHERE id = $1", job.ID).Scan(&state)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if state != pgxq.JobStateAvailable {
		t.Errorf("state: got %q, want %q", state, pgxq.JobStateAvailable)
	}
}

func TestValidateTable(t *testing.T) {
	ctx := context.Background()

	_, err := testPool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS myschema")
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), "DROP SCHEMA myschema CASCADE") //nolint:errcheck
	})

	for _, name := range []string{"pgxq_valid", "myschema.jobs", "Jobs123"} {
		if err := pgxq.Migrate(ctx, testPool, name); err != nil {
			t.Errorf("valid name %q: %v", name, err)
		}
		testPool.Exec(ctx, "DROP TABLE IF EXISTS "+name) //nolint:errcheck
	}

	for _, name := range []string{"", "drop table;", "my-jobs", "foo bar", "a.b.c", ".foo", "foo.", "123table", "schema.123t"} {
		if err := pgxq.Migrate(ctx, testPool, name); err == nil {
			t.Errorf("invalid name %q: expected error", name)
			testPool.Exec(ctx, "DROP TABLE IF EXISTS "+name) //nolint:errcheck
		}
	}
}

// --- rescue tests ---

func TestRescueOrphanedJobs(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	// Insert a job, then manually set it to "running" with old attempted_at.
	job, err := pgxq.Insert(ctx, testPool, "test", testArgs{Value: "orphaned"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = testPool.Exec(ctx,
		"UPDATE "+pgxq.DefaultTable+" SET state = 'running', attempted_at = now() - interval '2 hours', attempt = 1 WHERE id = $1",
		job.ID)
	if err != nil {
		t.Fatalf("update to running: %v", err)
	}

	// Start client with short RescueAfter so rescue fires quickly.
	processed := make(chan struct{})

	client := newClient(t, func(c *pgxq.ClientConfig) {
		c.RescueAfter = 200 * time.Millisecond // rescue check every 100ms
	})
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error {
		close(processed)
		return nil
	})

	go client.Start() //nolint:errcheck

	select {
	case <-processed:
		// Job was rescued (returned to available) and then picked up.
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for rescued job to be processed")
	}
	stopClient(t, client)
}

// --- schema tests ---

func TestSchema(t *testing.T) {
	sql, err := pgxq.Schema(pgxq.DefaultTable)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if !strings.Contains(sql, "CREATE TABLE") {
		t.Error("Schema does not contain CREATE TABLE")
	}
	if !strings.Contains(sql, "CREATE INDEX") {
		t.Error("Schema does not contain CREATE INDEX")
	}
	if !strings.Contains(sql, pgxq.DefaultTable) {
		t.Errorf("Schema does not contain table name %q", pgxq.DefaultTable)
	}
	if !strings.Contains(sql, "CHECK") {
		t.Error("Schema does not contain CHECK constraint")
	}
	if !strings.Contains(sql, "idx_pgxq_job_rescue") {
		t.Error("Schema does not contain rescue index")
	}
}

// --- error tests ---

func TestDiscardError(t *testing.T) {
	orig := fmt.Errorf("bad payload")
	wrapped := pgxq.Discard(orig)

	// Error message includes "discard:".
	if !strings.Contains(wrapped.Error(), "discard") {
		t.Errorf("Error(): got %q, want containing 'discard'", wrapped.Error())
	}

	// Unwrap returns the original error.
	var de *pgxq.DiscardError
	if !errors.As(wrapped, &de) {
		t.Fatal("errors.As failed for DiscardError")
	}
	if de.Unwrap() != orig {
		t.Errorf("Unwrap: got %v, want %v", de.Unwrap(), orig)
	}

	// Nil inner error.
	nilDiscard := pgxq.Discard(nil)
	if nilDiscard.Error() != "discard" {
		t.Errorf("nil Discard Error(): got %q, want %q", nilDiscard.Error(), "discard")
	}
}

func TestStartAlreadyStarted(t *testing.T) {
	truncate(t)

	client := newClient(t)
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error { return nil })

	started := make(chan struct{})
	go func() {
		close(started)
		client.Start() //nolint:errcheck
	}()
	<-started
	// Give Start a moment to set the flag.
	time.Sleep(50 * time.Millisecond)

	err := client.Start()
	if err == nil || !strings.Contains(err.Error(), "already started") {
		t.Errorf("second Start: got %v, want 'already started' error", err)
	}
	stopClient(t, client)
}

func TestHandleAfterStartPanics(t *testing.T) {
	truncate(t)

	client := newClient(t)
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error { return nil })

	go client.Start() //nolint:errcheck
	time.Sleep(50 * time.Millisecond)

	defer stopClient(t, client)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Handle after Start did not panic")
		}
	}()
	client.Handle("late", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error { return nil })
}

func TestUnmarshalArgs(t *testing.T) {
	job := &pgxq.Job{Args: json.RawMessage(`{"value":"hello"}`)}

	args, err := pgxq.UnmarshalArgs[testArgs](job)
	if err != nil {
		t.Fatalf("UnmarshalArgs: %v", err)
	}
	if args.Value != "hello" {
		t.Errorf("got %q, want %q", args.Value, "hello")
	}
}

func TestUnmarshalArgsInvalid(t *testing.T) {
	job := &pgxq.Job{Args: json.RawMessage(`not json`)}

	_, err := pgxq.UnmarshalArgs[testArgs](job)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestNewClientInvalidTable(t *testing.T) {
	_, err := pgxq.NewClient(pgxq.ClientConfig{
		Pool:  testPool,
		Table: "drop table;",
	})
	if err == nil {
		t.Error("NewClient should return error on invalid table name")
	}
}

func TestNewClientNilPool(t *testing.T) {
	_, err := pgxq.NewClient(pgxq.ClientConfig{})
	if err == nil {
		t.Error("NewClient should return error when Pool is nil")
	}
}

func TestStopBeforeStart(t *testing.T) {
	client := newClient(t)
	client.Handle("test", func(_ context.Context, _ pgx.Tx, _ *pgxq.Job) error { return nil })

	// Stop before Start should not panic.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Stop(ctx); err != nil {
		t.Errorf("Stop before Start: %v", err)
	}
}

// --- priority and scheduling tests ---

func TestPriorityOrdering(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	// Insert jobs with different priorities (lower = runs first).
	for _, p := range []int16{10, -5, 0, 5} {
		_, err := pgxq.Insert(ctx, testPool, "test", testArgs{Value: fmt.Sprintf("p%d", p)},
			pgxq.WithPriority(p))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	var order []string
	done := make(chan struct{})

	client := newClient(t, func(c *pgxq.ClientConfig) {
		c.MaxWorkers = 1 // sequential processing to observe order
		c.BatchSize = 4
	})
	client.Handle("test", func(_ context.Context, _ pgx.Tx, job *pgxq.Job) error {
		a, err := pgxq.UnmarshalArgs[testArgs](job)
		if err != nil {
			return err
		}
		order = append(order, a.Value)
		if len(order) == 4 {
			close(done)
		}
		return nil
	})

	go client.Start() //nolint:errcheck

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
	stopClient(t, client)

	expected := []string{"p-5", "p0", "p5", "p10"}
	if len(order) != len(expected) {
		t.Fatalf("got %d jobs, want %d", len(order), len(expected))
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("order[%d]: got %q, want %q", i, order[i], v)
		}
	}
}

func TestScheduledAtFuture(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	// Insert a job scheduled 1 hour from now — should NOT be picked up.
	_, err := pgxq.Insert(ctx, testPool, "test", testArgs{Value: "future"},
		pgxq.WithScheduledAt(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Insert a job scheduled now — should be picked up.
	_, err = pgxq.Insert(ctx, testPool, "test", testArgs{Value: "now"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	received := make(chan string, 2)
	client := newClient(t)
	client.Handle("test", func(_ context.Context, _ pgx.Tx, job *pgxq.Job) error {
		a, err := pgxq.UnmarshalArgs[testArgs](job)
		if err != nil {
			return err
		}
		received <- a.Value
		return nil
	})

	go client.Start() //nolint:errcheck

	select {
	case v := <-received:
		if v != "now" {
			t.Errorf("got %q, want %q", v, "now")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	// Wait a bit to make sure the future job is NOT picked up.
	time.Sleep(200 * time.Millisecond)
	select {
	case v := <-received:
		t.Errorf("future job was picked up: %q", v)
	default:
		// Good — future job not processed.
	}
	stopClient(t, client)
}

func TestQueueIsolation(t *testing.T) {
	truncate(t)
	ctx := context.Background()

	// Insert jobs into different queues.
	_, err := pgxq.Insert(ctx, testPool, "test", testArgs{Value: "email"}, pgxq.WithQueue("emails"))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defaultJob, err := pgxq.Insert(ctx, testPool, "test", testArgs{Value: "default"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	received := make(chan string, 2)

	// Client only polls "emails" queue.
	client := newClient(t, func(c *pgxq.ClientConfig) {
		c.Queue = "emails"
	})
	client.Handle("test", func(_ context.Context, _ pgx.Tx, job *pgxq.Job) error {
		a, err := pgxq.UnmarshalArgs[testArgs](job)
		if err != nil {
			return err
		}
		received <- a.Value
		return nil
	})

	go client.Start() //nolint:errcheck

	select {
	case v := <-received:
		if v != "email" {
			t.Errorf("got %q, want %q", v, "email")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	// Default queue job should NOT be picked up.
	time.Sleep(200 * time.Millisecond)
	select {
	case v := <-received:
		t.Errorf("wrong queue job picked up: %q", v)
	default:
	}
	stopClient(t, client)

	// Verify default job is still available.
	var state pgxq.JobState
	err = testPool.QueryRow(context.Background(),
		"SELECT state FROM "+pgxq.DefaultTable+" WHERE id = $1", defaultJob.ID).Scan(&state)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if state != pgxq.JobStateAvailable {
		t.Errorf("default job state: got %q, want %q", state, pgxq.JobStateAvailable)
	}
}

func TestShutdownTimeout(t *testing.T) {
	truncate(t)

	_, err := pgxq.Insert(context.Background(), testPool, "test", testArgs{Value: "blocking"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	handlerStarted := make(chan struct{})
	handlerCtx, handlerCancel := context.WithCancel(context.Background())
	defer handlerCancel()

	client := newClient(t)
	client.Handle("test", func(ctx context.Context, _ pgx.Tx, _ *pgxq.Job) error {
		close(handlerStarted)
		// Block until job context is cancelled (by Stop timeout).
		select {
		case <-ctx.Done():
		case <-handlerCtx.Done():
		}
		return nil
	})

	startDone := make(chan error, 1)
	go func() { startDone <- client.Start() }()

	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for handler start")
	}

	// Stop with a short timeout — should return ctx.Err().
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stopCancel()
	err = client.Stop(stopCtx)
	if err == nil {
		t.Error("Stop should return error on timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error should be DeadlineExceeded, got: %v", err)
	}

	// Wait for Start to return so goroutines don't leak.
	select {
	case <-startDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Stop cancelled job context")
	}
}
