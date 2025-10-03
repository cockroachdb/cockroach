// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
)

var (
	// ErrClusterNotFound is returned when a cluster is not found.
	ErrClusterNotFound = fmt.Errorf("cluster not found")
	// ErrSyncLockHeld is returned when another instance holds the sync lock.
	ErrSyncLockHeld = fmt.Errorf("sync lock held by another instance")
)

// IClustersRepository defines the interface for cluster data persistence and distributed coordination.
// This repository manages roachprod cluster metadata, handles distributed synchronization across
// multiple instances, and provides operations queuing for eventual consistency in cluster state management.
type IClustersRepository interface {
	// GetClusters retrieves multiple clusters based on the provided filter criteria.
	GetClusters(ctx context.Context, l *logger.Logger, filters filtertypes.FilterSet) (cloudcluster.Clusters, error)
	// GetCluster retrieves a single cluster by its name.
	GetCluster(ctx context.Context, l *logger.Logger, name string) (cloudcluster.Cluster, error)
	// StoreClusters persists multiple clusters in a batch operation for efficiency.
	StoreClusters(ctx context.Context, l *logger.Logger, clusters cloudcluster.Clusters) error
	// StoreCluster persists a single cluster to the repository.
	StoreCluster(ctx context.Context, l *logger.Logger, cluster cloudcluster.Cluster) error
	// DeleteCluster removes a cluster from the repository.
	DeleteCluster(ctx context.Context, l *logger.Logger, cluster cloudcluster.Cluster) error

	// AcquireSyncLock attempts to acquire a distributed lock for cluster synchronization.
	// Returns true if the lock was acquired, false if another instance holds it.
	AcquireSyncLock(ctx context.Context, l *logger.Logger, instanceID string) (bool, error)
	// AcquireSyncLockWithHealthCheck atomically checks if the lock is available or held by a stale instance,
	// then acquires it. This eliminates race conditions from separate health check + lock acquisition.
	// healthTimeout specifies how old a heartbeat can be before considering the owner stale.
	// Returns true if the lock was acquired, false if held by a healthy owner.
	AcquireSyncLockWithHealthCheck(ctx context.Context, l *logger.Logger, instanceID string, healthTimeout time.Duration) (bool, error)
	// ReleaseSyncLock releases the distributed sync lock held by the given instance.
	ReleaseSyncLock(ctx context.Context, l *logger.Logger, instanceID string) error
	// GetSyncStatus returns the current synchronization status across all instances.
	GetSyncStatus(ctx context.Context, l *logger.Logger) (*SyncStatus, error)

	// EnqueueOperation adds a cluster operation to the pending operations queue.
	EnqueueOperation(ctx context.Context, l *logger.Logger, operation OperationData) error
	// ConditionalEnqueueOperation atomically adds an operation only if sync is in progress
	// and the syncing instance is healthy. This combines sync status check, health check,
	// and operation enqueueing in a single atomic database operation to eliminate race conditions.
	// healthTimeout specifies how old a heartbeat can be before considering an instance unhealthy.
	// Returns true if the operation was enqueued, false if sync is not in progress or instance is unhealthy.
	ConditionalEnqueueOperation(ctx context.Context, l *logger.Logger, operation OperationData, healthTimeout time.Duration) (bool, error)
	// GetPendingOperations retrieves all operations waiting to be processed.
	GetPendingOperations(ctx context.Context, l *logger.Logger) ([]OperationData, error)
	// GetPendingOperationsWithTimestamp retrieves all pending operations and returns the timestamp
	// of the most recent operation (or current time if no operations exist).
	// The timestamp can be used with ClearPendingOperationsBefore to ensure only the fetched
	// operations are cleared, preventing accidental deletion of operations added during processing.
	GetPendingOperationsWithTimestamp(ctx context.Context, l *logger.Logger) ([]OperationData, time.Time, error)
	// ClearPendingOperations removes all pending operations from the queue.
	// Returns the number of operations that were deleted.
	ClearPendingOperations(ctx context.Context, l *logger.Logger) (int64, error)
	// ClearPendingOperationsBefore removes operations created before the given timestamp.
	// This allows selective clearing of operations that have been processed while preserving
	// newer operations that arrived during processing.
	// Returns the number of operations that were deleted.
	ClearPendingOperationsBefore(ctx context.Context, l *logger.Logger, timestamp time.Time) (int64, error)

	// StoreClustersAndReleaseSyncLock atomically stores clusters and releases the sync lock.
	// This ensures no operations can be enqueued between storing the final cluster state
	// and releasing the lock, eliminating a race condition window.
	StoreClustersAndReleaseSyncLock(ctx context.Context, l *logger.Logger, clusters cloudcluster.Clusters, instanceID string) error
}

// SyncStatus represents the current distributed synchronization state across all instances.
// This helps coordinate cluster discovery activities to prevent conflicts and duplicate work.
type SyncStatus struct {
	InProgress bool      `json:"in_progress"`
	InstanceID string    `json:"instance_id,omitempty"`
	StartedAt  time.Time `json:"started_at,omitempty"`
}

// OperationData represents a serializable cluster operation in the pending operations queue.
// These operations ensure eventual consistency when cluster changes cannot be applied immediately.
type OperationData struct {
	ID          string          `json:"id"`
	Type        OperationType   `json:"type"`
	ClusterName string          `json:"cluster_name"`
	ClusterData json.RawMessage `json:"cluster_data"`
	Timestamp   time.Time       `json:"timestamp"`
}

// OperationType defines the types of cluster operations that can be queued for processing.
type OperationType string

const (
	OperationTypeCreate OperationType = "create"
	OperationTypeDelete OperationType = "delete"
	OperationTypeUpdate OperationType = "update"
)
