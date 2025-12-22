// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package health

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

var (
	// ErrInstanceNotFound is returned when an instance is not found.
	ErrInstanceNotFound = fmt.Errorf("instance not found")
)

// IHealthRepository defines the interface for health data persistence.
type IHealthRepository interface {
	// Instance management
	RegisterInstance(ctx context.Context, l *logger.Logger, instance health.InstanceInfo) error
	UpdateHeartbeat(ctx context.Context, l *logger.Logger, instanceID string) error
	GetInstance(ctx context.Context, l *logger.Logger, instanceID string) (*health.InstanceInfo, error)
	GetHealthyInstances(ctx context.Context, l *logger.Logger, timeout time.Duration) ([]health.InstanceInfo, error)
	IsInstanceHealthy(ctx context.Context, l *logger.Logger, instanceID string, timeout time.Duration) (bool, error)

	// Cleanup operations
	CleanupDeadInstances(ctx context.Context, l *logger.Logger, timeout time.Duration, retentionPeriod time.Duration) (int, error)
}
