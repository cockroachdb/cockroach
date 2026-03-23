// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// CRDBClustersRepo is a CockroachDB implementation of the clusters repository.
type CRDBClustersRepo struct {
	db *gosql.DB
}

// NewClustersRepository creates a new CockroachDB clusters repository.
func NewClustersRepository(db *gosql.DB) *CRDBClustersRepo {
	repo := &CRDBClustersRepo{db: db}

	// Initialize sync state if it doesn't exist
	// This is safe to call multiple times
	_, _ = db.Exec(`
		INSERT INTO cluster_sync_state (id, in_progress) 
		SELECT 1, FALSE 
		WHERE NOT EXISTS (SELECT 1 FROM cluster_sync_state WHERE id = 1)
	`)

	return repo
}

// GetClusters returns all clusters from the database.
func (r *CRDBClustersRepo) GetClusters(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) (cloudcluster.Clusters, error) {
	baseQuery := `SELECT name, data FROM clusters`

	// Build WHERE clause using the filtering framework
	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(cloudcluster.Cluster{}),
	)
	whereClause, args, err := qb.BuildWhere(&filterSet)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build query filters")
	}

	// Construct final query
	query := baseQuery
	if whereClause != "" {
		query += " " + whereClause
	}
	query += " ORDER BY name"

	l.Debug("querying clusters from database",
		slog.String("query", query),
		slog.Int("args_count", len(args)),
		slog.Any("args", args),
	)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query clusters")
	}
	defer func() {
		if err := rows.Close(); err != nil {
			l.Warn("failed to close database rows",
				slog.String("operation", "GetClusters"),
				slog.Any("error", err))
		}
	}()

	clusters := make(cloudcluster.Clusters)
	for rows.Next() {
		var name string
		var data []byte
		if err := rows.Scan(&name, &data); err != nil {
			return nil, errors.Wrap(err, "failed to scan cluster row")
		}

		var cluster cloudcluster.Cluster
		if err := json.Unmarshal(data, &cluster); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal cluster data")
		}

		clusters[name] = &cluster
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating cluster rows")
	}

	l.Debug("successfully retrieved clusters from database",
		slog.Int("cluster_count", len(clusters)),
	)

	return clusters, nil
}

// GetCluster returns a specific cluster by name.
func (r *CRDBClustersRepo) GetCluster(
	ctx context.Context, l *logger.Logger, name string,
) (cloudcluster.Cluster, error) {
	query := `SELECT data FROM clusters WHERE name = $1`
	var data []byte
	err := r.db.QueryRowContext(ctx, query, name).Scan(&data)

	if errors.Is(err, gosql.ErrNoRows) {
		return cloudcluster.Cluster{}, clusters.ErrClusterNotFound
	}
	if err != nil {
		return cloudcluster.Cluster{}, errors.Wrap(err, "failed to query cluster")
	}

	var cluster cloudcluster.Cluster
	if err := json.Unmarshal(data, &cluster); err != nil {
		return cloudcluster.Cluster{}, errors.Wrap(err, "failed to unmarshal cluster data")
	}

	return cluster, nil
}

// StoreClusters stores all clusters in the database (replaces existing).
func (r *CRDBClustersRepo) StoreClusters(
	ctx context.Context, l *logger.Logger, clusters cloudcluster.Clusters,
) (retErr error) {
	l.Debug("storing clusters in database",
		slog.Int("cluster_count", len(clusters)),
	)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				if retErr != nil {
					retErr = errors.CombineErrors(retErr, errors.Wrap(rollbackErr, "rollback error"))
				} else {
					retErr = errors.Wrap(rollbackErr, "rollback error")
				}
			}
		}
	}()

	// Clear existing clusters
	if _, err := tx.ExecContext(ctx, `DELETE FROM clusters`); err != nil {
		return errors.Wrap(err, "failed to clear existing clusters")
	}

	// Insert all clusters
	for name, cluster := range clusters {
		data, err := json.Marshal(cluster)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal cluster %s", name)
		}

		query := `INSERT INTO clusters (name, data) VALUES ($1, $2)`
		if _, err := tx.ExecContext(ctx, query, name, data); err != nil {
			return errors.Wrapf(err, "failed to insert cluster %s", name)
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	l.Debug("successfully stored clusters in database",
		slog.Int("cluster_count", len(clusters)),
	)

	committed = true
	return nil
}

// StoreClustersAndReleaseSyncLock atomically stores clusters and releases the sync lock.
func (r *CRDBClustersRepo) StoreClustersAndReleaseSyncLock(
	ctx context.Context, l *logger.Logger, clusters cloudcluster.Clusters, instanceID string,
) (retErr error) {
	l.Debug("atomically storing clusters and releasing sync lock",
		slog.Int("cluster_count", len(clusters)),
		slog.String("instance_id", instanceID))

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				if retErr != nil {
					retErr = errors.CombineErrors(retErr, errors.Wrap(rollbackErr, "rollback error"))
				} else {
					retErr = errors.Wrap(rollbackErr, "rollback error")
				}
			}
		}
	}()

	// Clear existing clusters
	if _, err := tx.ExecContext(ctx, `DELETE FROM clusters`); err != nil {
		return errors.Wrap(err, "failed to clear existing clusters")
	}

	// Insert all clusters
	for name, cluster := range clusters {
		data, err := json.Marshal(cluster)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal cluster %s", name)
		}

		query := `INSERT INTO clusters (name, data) VALUES ($1, $2)`
		if _, err := tx.ExecContext(ctx, query, name, data); err != nil {
			return errors.Wrapf(err, "failed to insert cluster %s", name)
		}
	}

	// Release sync lock
	releaseQuery := `
		UPDATE cluster_sync_state
		SET in_progress = false, instance_id = NULL, started_at = NULL
		WHERE id = 1 AND instance_id = $1
	`
	result, err := tx.ExecContext(ctx, releaseQuery, instanceID)
	if err != nil {
		return errors.Wrap(err, "failed to release sync lock")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get affected rows for lock release")
	}

	if rowsAffected == 0 {
		return errors.New("failed to release sync lock: instance no longer owns the lock")
	}

	// Commit transaction (atomic)
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	l.Debug("successfully stored clusters and released sync lock",
		slog.Int("cluster_count", len(clusters)),
		slog.String("instance_id", instanceID))

	committed = true
	return nil
}

// StoreCluster stores a single cluster.
func (r *CRDBClustersRepo) StoreCluster(
	ctx context.Context, l *logger.Logger, cluster cloudcluster.Cluster,
) error {
	data, err := json.Marshal(cluster)
	if err != nil {
		return errors.Wrap(err, "failed to marshal cluster")
	}

	query := `
		INSERT INTO clusters (name, data, updated_at) VALUES ($1, $2, now())
		ON CONFLICT (name) DO UPDATE SET data = $2, updated_at = now()
	`
	if _, err := r.db.ExecContext(ctx, query, cluster.Name, data); err != nil {
		return errors.Wrapf(err, "failed to store cluster %s", cluster.Name)
	}

	return nil
}

// DeleteCluster deletes a cluster.
func (r *CRDBClustersRepo) DeleteCluster(
	ctx context.Context, l *logger.Logger, cluster cloudcluster.Cluster,
) error {
	query := `DELETE FROM clusters WHERE name = $1`
	result, err := r.db.ExecContext(ctx, query, cluster.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to delete cluster %s", cluster.Name)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get affected rows")
	}

	if rowsAffected == 0 {
		return clusters.ErrClusterNotFound
	}

	return nil
}

// Distributed sync state management

// AcquireSyncLock attempts to acquire the sync lock for the given instance.
func (r *CRDBClustersRepo) AcquireSyncLock(
	ctx context.Context, l *logger.Logger, instanceID string,
) (bool, error) {
	l.Debug("attempting to acquire sync lock",
		slog.String("instance_id", instanceID))

	// Try to acquire the lock
	query := `
		UPDATE cluster_sync_state 
		SET in_progress = true, instance_id = $1, started_at = now()
		WHERE id = 1 AND (in_progress = false OR instance_id = $1)
	`
	result, err := r.db.ExecContext(ctx, query, instanceID)
	if err != nil {
		return false, errors.Wrap(err, "failed to acquire sync lock")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, errors.Wrap(err, "failed to get affected rows")
	}

	acquired := rowsAffected > 0
	l.Debug("sync lock acquisition result",
		slog.String("instance_id", instanceID),
		slog.Bool("acquired", acquired),
		slog.Int64("rows_affected", rowsAffected))

	return acquired, nil
}

// AcquireSyncLockWithHealthCheck atomically checks lock availability and owner health, then acquires the lock.
// This eliminates race conditions from separate health check + lock acquisition operations.
// The lock state row is automatically initialized if it doesn't exist, enabling easier manual recovery.
func (r *CRDBClustersRepo) AcquireSyncLockWithHealthCheck(
	ctx context.Context, l *logger.Logger, instanceID string, healthTimeout time.Duration,
) (bool, error) {
	l.Debug(
		"attempting to acquire sync lock with health check",
		slog.String("instance_id", instanceID),
		slog.String("health_timeout", healthTimeout.String()),
	)

	// Ensure the sync state row exists (idempotent initialization).
	// Using INSERT ... ON CONFLICT DO NOTHING is safe as a separate statement.
	initQuery := `
		INSERT INTO cluster_sync_state (id, in_progress, instance_id, started_at)
		VALUES (1, false, NULL, NULL)
		ON CONFLICT (id) DO NOTHING
	`
	_, err := r.db.ExecContext(ctx, initQuery)
	if err != nil {
		return false, errors.Wrap(err, "failed to initialize sync lock state")
	}

	// Now try to acquire the lock with health check.
	// CockroachDB doesn't allow mixing INSERT ON CONFLICT with UPDATE in same statement,
	// so we split initialization and acquisition into separate operations.
	query := `
		UPDATE cluster_sync_state
		SET in_progress = true, instance_id = $1, started_at = now()
		WHERE id = 1
		AND (
			-- Lock is available
			in_progress = false
			OR
			-- Lock owner is stale (not heartbeating)
			NOT EXISTS (
				SELECT 1
				FROM instance_health
				WHERE instance_id = cluster_sync_state.instance_id
				AND last_heartbeat > now() - $2::INTERVAL
			)
		)
	`

	result, err := r.db.ExecContext(ctx, query, instanceID, fmt.Sprintf("%.0f seconds", healthTimeout.Seconds()))
	if err != nil {
		return false, errors.Wrap(err, "failed to acquire sync lock with health check")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, errors.Wrap(err, "failed to get affected rows")
	}

	acquired := rowsAffected > 0
	l.Debug("sync lock acquisition with health check result",
		slog.String("instance_id", instanceID),
		slog.Bool("acquired", acquired),
		slog.Int64("rows_affected", rowsAffected))

	return acquired, nil
}

// ReleaseSyncLock releases the sync lock for the given instance.
func (r *CRDBClustersRepo) ReleaseSyncLock(
	ctx context.Context, l *logger.Logger, instanceID string,
) error {
	query := `
		UPDATE cluster_sync_state 
		SET in_progress = false, instance_id = NULL, started_at = NULL
		WHERE id = 1 AND instance_id = $1
	`
	if _, err := r.db.ExecContext(ctx, query, instanceID); err != nil {
		return errors.Wrap(err, "failed to release sync lock")
	}

	return nil
}

// GetSyncStatus returns the current sync status.
func (r *CRDBClustersRepo) GetSyncStatus(
	ctx context.Context, l *logger.Logger,
) (*clusters.SyncStatus, error) {
	query := `SELECT in_progress, instance_id, started_at FROM cluster_sync_state WHERE id = 1`
	var status clusters.SyncStatus
	var instanceID gosql.NullString
	var startedAt gosql.NullTime

	err := r.db.QueryRowContext(ctx, query).Scan(&status.InProgress, &instanceID, &startedAt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query sync status")
	}

	if instanceID.Valid {
		status.InstanceID = instanceID.String
	}
	if startedAt.Valid {
		status.StartedAt = startedAt.Time
	}

	return &status, nil
}

// Operations queue management

// EnqueueOperation adds an operation to the queue.
func (r *CRDBClustersRepo) EnqueueOperation(
	ctx context.Context, l *logger.Logger, operation clusters.OperationData,
) error {
	// Set ID and timestamp if not provided
	if operation.ID == "" {
		operation.ID = uuid.MakeV4().String()
	}
	if operation.Timestamp.IsZero() {
		operation.Timestamp = timeutil.Now()
	}

	query := `
		INSERT INTO cluster_operations (id, operation_type, cluster_name, cluster_data, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.db.ExecContext(ctx, query,
		operation.ID,
		string(operation.Type),
		operation.ClusterName,
		operation.ClusterData,
		operation.Timestamp,
	)
	if err != nil {
		return errors.Wrap(err, "failed to enqueue operation")
	}

	return nil
}

// ConditionalEnqueueOperation atomically adds an operation only if sync is in progress
// and the syncing instance is healthy. This combines sync status check, health check,
// and operation enqueueing in a single atomic database operation to eliminate race conditions.
// Returns true if the operation was enqueued, false if sync is not in progress or instance is unhealthy.
func (r *CRDBClustersRepo) ConditionalEnqueueOperation(
	ctx context.Context,
	l *logger.Logger,
	operation clusters.OperationData,
	healthTimeout time.Duration,
) (bool, error) {
	// Set ID and timestamp if not provided
	if operation.ID == "" {
		operation.ID = uuid.MakeV4().String()
	}
	if operation.Timestamp.IsZero() {
		operation.Timestamp = timeutil.Now()
	}

	// Atomic INSERT that verifies:
	// 1. Sync is in progress
	// 2. Syncing instance is healthy (heartbeating)
	// This eliminates race conditions and reduces 3 queries to 1
	query := `
		INSERT INTO cluster_operations (id, operation_type, cluster_name, cluster_data, created_at)
		SELECT $1, $2, $3, $4, $5
		FROM cluster_sync_state
		WHERE id = 1
		  AND in_progress = true
		  AND instance_id IS NOT NULL
		  -- Only enqueue if syncing instance is healthy (inline health check)
		  AND EXISTS (
			SELECT 1
			FROM instance_health
			WHERE instance_id = cluster_sync_state.instance_id
			  AND last_heartbeat > now() - $6::INTERVAL
		  )
	`
	result, err := r.db.ExecContext(ctx, query,
		operation.ID,
		string(operation.Type),
		operation.ClusterName,
		operation.ClusterData,
		operation.Timestamp,
		fmt.Sprintf("%.0f seconds", healthTimeout.Seconds()),
	)
	if err != nil {
		return false, errors.Wrap(err, "failed to conditionally enqueue operation")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, errors.Wrap(err, "failed to get affected rows")
	}

	enqueued := rowsAffected > 0
	if enqueued {
		l.Debug("operation enqueued (sync in progress with healthy instance)",
			slog.String("operation_id", operation.ID),
			slog.String("cluster_name", operation.ClusterName))
	}

	return enqueued, nil
}

// GetPendingOperations returns all pending operations ordered by creation time.
func (r *CRDBClustersRepo) GetPendingOperations(
	ctx context.Context, l *logger.Logger,
) ([]clusters.OperationData, error) {
	query := `
		SELECT id, operation_type, cluster_name, cluster_data, created_at
		FROM cluster_operations
		ORDER BY created_at ASC
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query pending operations")
	}
	defer func() {
		if err := rows.Close(); err != nil {
			l.Warn("failed to close database rows",
				slog.String("operation", "GetPendingOperations"),
				slog.Any("error", err))
		}
	}()

	var operations []clusters.OperationData
	for rows.Next() {
		var op clusters.OperationData
		var opType string
		err := rows.Scan(&op.ID, &opType, &op.ClusterName, &op.ClusterData, &op.Timestamp)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan operation row")
		}
		op.Type = clusters.OperationType(opType)
		operations = append(operations, op)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating operation rows")
	}

	return operations, nil
}

// GetPendingOperationsWithTimestamp returns all pending operations and the timestamp
// of the most recent operation (or current time if no operations exist).
func (r *CRDBClustersRepo) GetPendingOperationsWithTimestamp(
	ctx context.Context, l *logger.Logger,
) ([]clusters.OperationData, time.Time, error) {
	query := `
		SELECT id, operation_type, cluster_name, cluster_data, created_at
		FROM cluster_operations
		ORDER BY created_at ASC
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, time.Time{}, errors.Wrap(err, "failed to query pending operations")
	}
	defer func() {
		if err := rows.Close(); err != nil {
			l.Warn("failed to close database rows",
				slog.String("operation", "GetPendingOperationsWithTimestamp"),
				slog.Any("error", err))
		}
	}()

	var operations []clusters.OperationData
	var maxTimestamp time.Time
	for rows.Next() {
		var op clusters.OperationData
		var opType string
		err := rows.Scan(&op.ID, &opType, &op.ClusterName, &op.ClusterData, &op.Timestamp)
		if err != nil {
			return nil, time.Time{}, errors.Wrap(err, "failed to scan operation row")
		}
		op.Type = clusters.OperationType(opType)
		operations = append(operations, op)

		// Track the most recent timestamp
		if op.Timestamp.After(maxTimestamp) {
			maxTimestamp = op.Timestamp
		}
	}

	if err := rows.Err(); err != nil {
		return nil, time.Time{}, errors.Wrap(err, "error iterating operation rows")
	}

	// If no operations, return current time as the cutoff
	if len(operations) == 0 {
		maxTimestamp = timeutil.Now()
	}

	return operations, maxTimestamp, nil
}

// ClearPendingOperations removes all pending operations.
func (r *CRDBClustersRepo) ClearPendingOperations(
	ctx context.Context, l *logger.Logger,
) (int64, error) {
	query := `DELETE FROM cluster_operations`
	result, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return 0, errors.Wrap(err, "failed to clear pending operations")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "failed to get affected rows")
	}

	return rowsAffected, nil
}

// ClearPendingOperationsBefore removes operations created before the given timestamp.
func (r *CRDBClustersRepo) ClearPendingOperationsBefore(
	ctx context.Context, l *logger.Logger, timestamp time.Time,
) (int64, error) {
	query := `DELETE FROM cluster_operations WHERE created_at <= $1`
	result, err := r.db.ExecContext(ctx, query, timestamp)
	if err != nil {
		return 0, errors.Wrap(err, "failed to clear pending operations before timestamp")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "failed to get affected rows")
	}

	return rowsAffected, nil
}
