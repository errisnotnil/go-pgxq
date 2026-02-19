package pgxq_test

import (
	"testing"
	"time"

	"github.com/errisnotnil/go-pgxq"
)

func TestExponentialBackoff(t *testing.T) {
	backoff := pgxq.ExponentialBackoff(time.Second, 2.0, time.Hour)

	// Attempt 1: ~1s (base).
	d1 := backoff(1)
	if d1 < 800*time.Millisecond || d1 > 1200*time.Millisecond {
		t.Errorf("attempt 1: got %v, want ~1s", d1)
	}

	// Attempt 5: ~16s (1 * 2^4).
	d5 := backoff(5)
	if d5 < 12*time.Second || d5 > 20*time.Second {
		t.Errorf("attempt 5: got %v, want ~16s", d5)
	}

	// Large attempt: capped at max.
	d100 := backoff(100)
	if d100 > time.Hour+12*time.Minute {
		t.Errorf("attempt 100: got %v, want <= ~1h (with jitter)", d100)
	}
}

func TestConstantBackoff(t *testing.T) {
	backoff := pgxq.ConstantBackoff(5 * time.Second)

	for _, attempt := range []int{1, 2, 10, 100} {
		d := backoff(attempt)
		if d != 5*time.Second {
			t.Errorf("attempt %d: got %v, want 5s", attempt, d)
		}
	}
}
