package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	h := func(ctx context.Context, j job) error {
		logger.Info("processing job", "job_id", j.id, "type", j.jobtype)
		if j.id == "job-fail" {
			return &transienterror{err: errors.New("temporary database connection drop")}
		}
		return nil
	}

	p := newpool(10, h, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.start(ctx, 3)

	p.submit(job{id: "job-1", jobtype: "outreach.generate"})
	p.submit(job{id: "job-fail", jobtype: "analytics.aggregate"})

	lim := newproviderlimiter()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := lim.wait(ctx, "openai"); err != nil {
					logger.Error("rate limiter wait failed", "error", err)
					return
				}
				logger.Info("rate limit check passed for openai")
				time.Sleep(2 * time.Second)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logger.Info("http status server starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("shutdown signal received", "signal", sig.String())
	cancel()

	shutdownctx, shutdowncancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdowncancel()

	if err := srv.Shutdown(shutdownctx); err != nil {
		logger.Error("forced http server shutdown", "error", err)
	}

	p.shutdown()
	logger.Info("kairo server gracefully stopped")
}
