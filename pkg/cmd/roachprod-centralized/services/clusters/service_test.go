// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"testing"
	"time"

	clustersrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	healthmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/mocks"
	tasksmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test helpers and shared test utilities for the clusters service

// createTestService creates a service with mocked dependencies for testing
func createTestService(
	t *testing.T, opts Options,
) (
	*Service,
	*clustersrepomock.IClustersRepository,
	*tasksmock.IService,
	*healthmock.IHealthService,
) {
	store := clustersrepomock.NewIClustersRepository(t)
	taskService := tasksmock.NewIService(t)
	healthService := healthmock.NewIHealthService(t)

	service, err := NewService(store, taskService, healthService, opts)
	if err != nil {
		t.Fatalf("failed to create test service: %v", err)
	}

	return service, store, taskService, healthService
}

// Lifecycle tests

func TestService_Shutdown(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func(*Service)
		ctxTimeout  time.Duration
		wantErr     error
		wantTimeout bool
	}{
		{
			name:       "clean shutdown",
			ctxTimeout: 1 * time.Second,
		},
		{
			name: "shutdown timeout",
			setupFunc: func(s *Service) {
				s.backgroundJobsWg.Add(1)
				go func() {
					time.Sleep(50 * time.Millisecond)
					s.backgroundJobsWg.Done()
				}()
			},
			ctxTimeout: 5 * time.Millisecond,
			wantErr:    types.ErrShutdownTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			healthService := healthmock.NewIHealthService(t)
			s, err := NewService(store, nil, healthService, Options{})
			assert.NoError(t, err)

			if tt.setupFunc != nil {
				tt.setupFunc(s)
			}

			ctx, cancel := context.WithTimeout(context.Background(), tt.ctxTimeout)
			defer cancel()

			err = s.Shutdown(ctx)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)

			store.AssertExpectations(t)
			store.ExpectedCalls = nil
		})
	}
}

func TestService_StartBackgroundWork(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		wantErr bool
	}{
		{
			name: "success with periodic refresh enabled",
			options: Options{
				NoInitialSync:           true,
				PeriodicRefreshEnabled:  true,
				PeriodicRefreshInterval: 50 * time.Millisecond,
			},
		},
		{
			name: "success with periodic refresh disabled",
			options: Options{
				NoInitialSync:          true,
				PeriodicRefreshEnabled: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			if !tt.options.NoInitialSync {
				store.On("StoreClusters", mock.Anything, mock.Anything).Return(nil)
			}

			taskService := tasksmock.NewIService(t)
			// Don't expect RegisterTasksService since the test doesn't go through full app lifecycle

			healthService := healthmock.NewIHealthService(t)
			service, err := NewService(store, taskService, healthService, tt.options)
			assert.NoError(t, err)

			errChan := make(chan error, 1)
			err = service.StartBackgroundWork(context.Background(), logger.DefaultLogger, errChan)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			store.AssertExpectations(t)
			store.ExpectedCalls = nil

			// No task service expectations to assert since we don't call RegisterTasks

			// Cleanup
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_ = service.Shutdown(ctx)
		})
	}
}
