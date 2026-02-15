// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package services

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// IService defines the lifecycle interface for all services in the roachprod-centralized system.
// Services are responsible for business logic operations and coordinate with repositories for data access.
// All services must implement this interface to ensure proper startup, background work management, and shutdown.
type IService interface {
	// RegisterTasks registers any background tasks that this service needs to process.
	// This is called during application initialization before the service starts.
	RegisterTasks(ctx context.Context) error
	// StartService initializes the service and prepares it for operation.
	// This is called after all dependencies are initialized but before background work starts.
	StartService(ctx context.Context, l *logger.Logger) error
	// StartBackgroundWork starts any background processing routines for this service.
	// Errors should be sent to the provided error channel for centralized error handling.
	StartBackgroundWork(ctx context.Context, l *logger.Logger, errChan chan<- error) error
	// Shutdown gracefully terminates the service and cleans up any resources.
	// This is called during application shutdown to ensure clean termination.
	Shutdown(ctx context.Context) error
}

// IServiceWithShutdownTimeout is an optional interface that services can
// implement to override the default shutdown timeout. Services that don't
// implement this interface use the app-level default.
type IServiceWithShutdownTimeout interface {
	GetShutdownTimeout() time.Duration
}
