package main

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type providerlimiter struct {
	mu       sync.RWMutex
	limiters map[string]*rate.Limiter
}

func newproviderlimiter() *providerlimiter {
	return &providerlimiter{
		limiters: map[string]*rate.Limiter{
			"openai":    rate.NewLimiter(rate.Every(time.Second), 50),
			"anthropic": rate.NewLimiter(rate.Every(time.Second), 30),
			"local":     rate.NewLimiter(rate.Every(time.Second), 100),
		},
	}
}

func (pl *providerlimiter) wait(ctx context.Context, provider string) error {
	pl.mu.RLock()
	l, ok := pl.limiters[provider]
	pl.mu.RUnlock()

	if !ok {
		return nil
	}

	return l.Wait(ctx)
}
