// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/errors"
)

const (
	ShutdownTimeout = 30 * time.Second
)

type App struct {
	api      *Api
	logLevel string
	logger   *utils.Logger
	wg       sync.WaitGroup
	services []services.IService
}

// NewApp creates a new App instance with the provided options
func NewApp(options ...IAppOption) (*App, error) {

	a := &App{
		services: []services.IService{},
		api: &Api{
			port:        DefaultPort,
			metricsPort: DefaultMetricsPort,
		},
	}

	for _, option := range options {
		option.apply(a)
	}

	// Set up logger
	a.logger = utils.NewLogger(a.logLevel)

	// api.Init inits the gin engine, sets up server-wide middlewares
	// and adds the controllers endpoints and the Prometheus metrics endpoint
	a.api.Init(a.logger)

	return a, nil
}

// Start starts all the application components:
// - all services
// - the API server
// - the signal handler for graceful shutdown
func (a *App) Start(ctx context.Context) error {

	ctx, cancelMainCtx := context.WithCancel(ctx)
	defer cancelMainCtx()

	errChan := make(chan error)

	// Init all services
	a.logger.Info("Starting app services")
	for _, service := range a.services {
		a.logger.Debug(
			"Initializing service",
			slog.String("service", fmt.Sprintf("%T", service)),
		)
		err := service.StartBackgroundWork(ctx, a.logger, errChan)
		if err != nil {
			return err
		}
	}

	// Start the API server
	a.logger.Info("Starting app API server")
	err := a.api.Start(ctx, errChan)
	if err != nil {
		return err
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		// We make a channel that listens to SIGINT and SIGTERM and gracefully
		// shutdown
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		for {
			select {
			case err := <-errChan:
				switch {
				case errors.HasType(err, (*utils.CriticalError)(nil)):
					a.logger.Error(
						"Critical error encountered in service, shutting down",
						slog.Any("error", err),
					)
					err = a.Shutdown()
					if err != nil {
						a.logger.Error("Error shutting down app", slog.Any("error", err))
					}
					return
				default:
					a.logger.Error(
						"Error encountered in service",
						slog.Any("error", err),
					)
				}
			case <-sigs:
				a.logger.Info("Received SIGINT or SIGTERM, shutting down")
				err = a.Shutdown()
				if err != nil {
					a.logger.Error("Error shutting down app", slog.Any("error", err))
				}
				return
			}
		}
	}()

	// Just wait for processes to complete
	a.wg.Wait()

	return nil
}

// Shutdown shuts down the application gracefully
func (a *App) Shutdown() error {

	// Start by shutting down the API server to stop accepting new requests
	err := a.api.Shutdown()
	if err != nil {
		a.logger.Error("Error shutting down API server", slog.Any("error", err))
		return err
	}

	// Shutdown all services
	for _, service := range a.services {
		err = func() error {
			serviceShutdownCtx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
			defer cancel()

			a.logger.Info("Shutting down service", slog.String("service", fmt.Sprintf("%T", service)))
			err := service.Shutdown(serviceShutdownCtx)
			if err != nil {
				a.logger.Error("Error shutting down service", slog.Any("error", err))
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *App) GetApi() *Api {
	return a.api
}
