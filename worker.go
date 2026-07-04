package main

import (
	"context"
	"log/slog"
	"sync"
)

type job struct {
	id      string
	jobtype string
	payload []byte
}

type handler func(ctx context.Context, j job) error

type pool struct {
	jobs   chan job
	h      handler
	wg     sync.WaitGroup
	logger *slog.Logger
}

func newpool(size int, h handler, logger *slog.Logger) *pool {
	return &pool{
		jobs:   make(chan job, size*2),
		h:      h,
		logger: logger,
	}
}

func (p *pool) start(ctx context.Context, workers int) {
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.work(ctx, i)
	}
}

func (p *pool) work(ctx context.Context, id int) {
	defer p.wg.Done()
	p.logger.Info("worker started", "worker_id", id)

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("worker stopping", "worker_id", id)
			return
		case j, ok := <-p.jobs:
			if !ok {
				return
			}
			if err := p.h(ctx, j); err != nil {
				p.logger.Error("job failed",
					"worker_id", id,
					"job_id", j.id,
					"error", err,
				)
			}
		}
	}
}

func (p *pool) submit(j job) {
	p.jobs <- j
}

func (p *pool) shutdown() {
	close(p.jobs)
	p.wg.Wait()
}
