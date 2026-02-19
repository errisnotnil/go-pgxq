// A minimal worker that enqueues and processes a job.
//
// Usage:
//
//	DATABASE_URL=postgres://localhost:5432/mydb go run ./examples
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/errisnotnil/go-pgxq"
)

type SendEmail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := pgxq.Migrate(ctx, pool, pgxq.DefaultTable); err != nil {
		log.Fatal(err)
	}

	// Enqueue a job.
	job, err := pgxq.Insert(ctx, pool, "send_email", SendEmail{
		To:      "user@example.com",
		Subject: "Hello from pgxq",
		Body:    "Your first job queue message.",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued job %d (%s)\n", job.ID, job.Kind)

	// Process jobs.
	client, err := pgxq.NewClient(pgxq.ClientConfig{Pool: pool})
	if err != nil {
		log.Fatal(err)
	}
	client.Handle("send_email", func(_ context.Context, _ pgx.Tx, job *pgxq.Job) error {
		args, err := pgxq.UnmarshalArgs[SendEmail](job)
		if err != nil {
			return pgxq.Discard(err)
		}
		fmt.Printf("sending email to %s: %s\n", args.To, args.Subject)
		return nil
	})

	startErrCh := make(chan error, 1)
	go func() { startErrCh <- client.Start() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	select {
	case err := <-startErrCh:
		log.Fatalf("client stopped unexpectedly: %v", err)
	case <-sigCh:
	}

	fmt.Println("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Stop(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}
