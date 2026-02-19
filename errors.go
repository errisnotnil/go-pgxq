package pgxq

import "errors"

// DiscardError wraps an error to signal that the job should be discarded
// immediately without further retries.
type DiscardError struct {
	Err error
}

// Discard wraps err so that the worker discards the job instead of retrying.
//
//	return pgxq.Discard(fmt.Errorf("invalid payload: %w", err))
func Discard(err error) error {
	return &DiscardError{Err: err}
}

func (e *DiscardError) Error() string {
	if e.Err == nil {
		return "discard"
	}
	return "discard: " + e.Err.Error()
}

func (e *DiscardError) Unwrap() error {
	return e.Err
}

func isDiscard(err error) bool {
	var de *DiscardError
	return errors.As(err, &de)
}
