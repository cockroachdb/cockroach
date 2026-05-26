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
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	ShutdownTimeout = 30 * time.Second
)

type App struct {
	api      *Api
	logLevel string
	logger   *logger.Logger
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
	if a.logger == nil {
		a.logger = logger.NewLogger(a.logLevel)
	}

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

	// Phase 1: Register all tasks from all services
	a.logger.Info("Registering tasks for all services")
	for _, service := range a.services {
		a.logger.Debug(
			"Registering tasks",
			slog.String("service", fmt.Sprintf("%T", service)),
		)
		err := service.RegisterTasks(ctx)
		if err != nil {
			return errors.Wrapf(err, "failed to register tasks for service %T", service)
		}
	}

	// Phase 2: Start task service first (so it can process tasks)
	a.logger.Info("Starting task service")
	var taskService services.IService
	for _, service := range a.services {
		// Check if this is the task service by checking the type
		if _, ok := service.(*stasks.Service); ok {
			taskService = service
			err := service.StartService(ctx, a.logger)
			if err != nil {
				return errors.Wrapf(err, "failed to start task service")
			}
			err = service.StartBackgroundWork(ctx, a.logger, errChan)
			if err != nil {
				return errors.Wrap(err, "failed to start task service background work")
			}
			break
		}
	}

	// Phase 3: Start other services (which may schedule and wait for tasks)
	a.logger.Info("Starting other services")
	for _, service := range a.services {
		// Skip task service as it's already started
		if service == taskService {
			continue
		}
		a.logger.Debug(
			"Starting service",
			slog.String("service", fmt.Sprintf("%T", service)),
		)
		err := service.StartService(ctx, a.logger)
		if err != nil {
			return errors.Wrapf(err, "failed to start service %T", service)
		}
		err = service.StartBackgroundWork(ctx, a.logger, errChan)
		if err != nil {
			return errors.Wrapf(err, "failed to start background work for service %T", service)
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

	var shutdownErrors []error

	// Start by shutting down the API server to stop accepting new requests
	err := a.api.Shutdown()
	if err != nil {
		a.logger.Error("Error shutting down API server", slog.Any("error", err))
		shutdownErrors = append(shutdownErrors, err)
	}

	// Shutdown all services concurrently so that a slow service (e.g. one
	// draining long-running tasks) does not delay the shutdown of others.
	var mu syncutil.Mutex
	var wg sync.WaitGroup
	for _, service := range a.services {
		wg.Add(1)
		go func() {
			defer wg.Done()

			timeout := ShutdownTimeout
			if s, ok := service.(services.IServiceWithShutdownTimeout); ok {
				timeout = s.GetShutdownTimeout()
			}
			serviceShutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			a.logger.Info(
				"Shutting down service",
				slog.String("service", fmt.Sprintf("%T", service)),
				slog.Duration("timeout", timeout),
			)
			if err := service.Shutdown(serviceShutdownCtx); err != nil {
				a.logger.Error(
					"Error shutting down service",
					slog.String("service", fmt.Sprintf("%T", service)),
					slog.Any("error", err),
				)

				mu.Lock()
				defer mu.Unlock()
				shutdownErrors = append(shutdownErrors, err)
				return
			}

			a.logger.Info(
				"Service shut down successfully",
				slog.String("service", fmt.Sprintf("%T", service)),
			)
		}()
	}
	wg.Wait()

	return errors.Join(shutdownErrors...)
}

func (a *App) GetApi() *Api {
	return a.api
}
