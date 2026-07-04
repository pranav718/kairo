package main

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

type retryablefunc func(ctx context.Context) error

type retryconfig struct {
	maxattempts int
	basedelay   time.Duration
	maxdelay    time.Duration
}

var defaultretryconfig = retryconfig{
	maxattempts: 3,
	basedelay:   500 * time.Millisecond,
	maxdelay:    30 * time.Second,
}

type transienterror struct {
	err error
}

func (e *transienterror) Error() string {
	return e.err.Error()
}

func (e *transienterror) Unwrap() error {
	return e.err
}

func doretry(ctx context.Context, cfg retryconfig, fn retryablefunc) error {
	var lasterr error

	for attempt := 0; attempt < cfg.maxattempts; attempt++ {
		lasterr = fn(ctx)
		if lasterr == nil {
			return nil
		}

		var transient *transienterror
		if !errors.As(lasterr, &transient) {
			return lasterr
		}

		if attempt < cfg.maxattempts-1 {
			delay := calculatebackoff(attempt, cfg.basedelay, cfg.maxdelay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return lasterr
}

func calculatebackoff(attempt int, base, max time.Duration) time.Duration {
	backoff := float64(base) * math.Pow(2, float64(attempt))
	jitter := rand.Float64() * float64(base)
	delay := time.Duration(backoff + jitter)
	if delay > max {
		delay = max
	}
	return delay
}
