package pgxq

import (
	"math"
	"math/rand/v2"
	"time"
)

// BackoffFunc returns the delay before the next retry based on the attempt number (1-based).
type BackoffFunc func(attempt int) time.Duration

const (
	jitterMin   = 0.8
	jitterRange = 0.4
)

// DefaultBackoff is exponential backoff: 1s base, 2x factor, 1h max, 20% jitter.
var DefaultBackoff = ExponentialBackoff(time.Second, 2.0, time.Hour)

// ExponentialBackoff returns a [BackoffFunc] with exponential delay, capped at maxDelay, with 20% jitter.
//
//	delay = min(base * factor^(attempt-1), maxDelay) * (0.8 + rand*0.4)
func ExponentialBackoff(base time.Duration, factor float64, maxDelay time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		delay := float64(base) * math.Pow(factor, float64(attempt-1))
		if math.IsInf(delay, 0) || math.IsNaN(delay) || delay > float64(maxDelay) {
			delay = float64(maxDelay)
		}
		jitter := jitterMin + rand.Float64()*jitterRange
		return time.Duration(delay * jitter)
	}
}

// ConstantBackoff returns a [BackoffFunc] that always returns the same delay.
func ConstantBackoff(d time.Duration) BackoffFunc {
	return func(_ int) time.Duration {
		return d
	}
}
