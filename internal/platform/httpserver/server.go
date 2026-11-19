package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

func Run(ctx context.Context, log *slog.Logger, addr string, handler http.Handler, shutdownTimeout time.Duration) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	errs := make(chan error, 1)
	go func() {
		log.Info("http: ouvindo", "addr", addr)
		errs <- server.ListenAndServe()
	}()
	select {
	case err := <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	log.Info("http: encerrando")
	return server.Shutdown(shutdownCtx)
}
