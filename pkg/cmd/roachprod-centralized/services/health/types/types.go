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

// FileInfo contains information about a single file.
type FileInfo struct {
	Path         string
	SizeBytes    int64
	ModifiedTime string // ISO 8601 format
	IsDir        bool
}

// DiskUsageDiagnostics contains disk usage information for a directory.
type DiskUsageDiagnostics struct {
	Path        string
	TotalBytes  uint64
	UsedBytes   uint64
	AvailBytes  uint64
	UsedPercent float64
	FileCount   int
	Files       []*FileInfo
	Error       string
}

// TempDirsDiagnostics contains diagnostics for temporary directories.
type TempDirsDiagnostics struct {
	Directories []*DiskUsageDiagnostics
}
