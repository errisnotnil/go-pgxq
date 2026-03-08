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

const shutdownTimeout = 10 * time.Second

// SendEmail is a sample job payload.
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

	if migrateErr := pgxq.Migrate(ctx, pool, pgxq.DefaultTable); migrateErr != nil {
		pool.Close()
		log.Fatal(migrateErr)
	}

	// Enqueue a job.
	job, err := pgxq.Insert(ctx, pool, "send_email", SendEmail{
		To:      "user@example.com",
		Subject: "Hello from pgxq",
		Body:    "Your first job queue message.",
	})
	if err != nil {
		pool.Close()
		log.Fatal(err)
	}
	fmt.Printf("enqueued job %d (%s)\n", job.ID, job.Kind)

	// Process jobs.
	client, err := pgxq.NewClient(pgxq.ClientConfig{Pool: pool})
	if err != nil {
		pool.Close()
		log.Fatal(err)
	}
	client.Handle("send_email", func(_ context.Context, _ pgx.Tx, j *pgxq.Job) error {
		args, unmarshalErr := pgxq.UnmarshalArgs[SendEmail](j)
		if unmarshalErr != nil {
			return pgxq.Discard(unmarshalErr)
		}
		fmt.Printf("sending email to %s: %s\n", args.To, args.Subject)
		return nil
	})

	go func() {
		if startErr := client.Start(); startErr != nil {
			log.Fatalf("client stopped unexpectedly: %v", startErr)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	fmt.Println("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	stopErr := client.Stop(shutdownCtx)
	cancel()
	pool.Close()
	if stopErr != nil {
		log.Fatal(stopErr)
	}
}
