// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

const (
	TaskServiceName = "health"
)

// IHealthService defines the health-specific interface.
type IHealthService interface {
	// Health-specific methods only
	RegisterInstance(ctx context.Context, l *logger.Logger, instanceID, hostname string) error
	IsInstanceHealthy(ctx context.Context, l *logger.Logger, instanceID string) (bool, error)
	GetHealthyInstances(ctx context.Context, l *logger.Logger) ([]health.InstanceInfo, error)
	GetInstanceID() string
	GetInstanceTimeout() time.Duration
	CleanupDeadInstances(ctx context.Context, l *logger.Logger, instanceTimeout, cleanupRetention time.Duration) (int, error)
}
