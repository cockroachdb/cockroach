// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	sclustertasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	rplogger "github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// Sync contains the logic to sync the clusters and store them in the repository.
func (s *Service) Sync(ctx context.Context, l *logger.Logger) (cloudcluster.Clusters, error) {
	instanceID := s._healthService.GetInstanceID()

	// Try to acquire the distributed sync lock.
	acquired, err := s.acquireSyncLockWithHealthCheck(ctx, l, instanceID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to acquire sync lock")
	}

	if !acquired {
		l.Info("another instance is already syncing, skipping sync")
		return s._store.GetClusters(ctx, l, *filters.NewFilterSet())
	}

	l.Debug("acquired sync lock, starting cluster sync", slog.String("instance_id", instanceID))

	lockReleased := false
	defer func() {
		if !lockReleased {
			// Use background context for cleanup to ensure lock is always released,
			// even when the main context has been cancelled due to timeout.
			// We give this operation a generous timeout to handle transient DB issues.
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer releaseCancel()

			if err := s._store.ReleaseSyncLock(releaseCtx, l, instanceID); err != nil {
				l.Error("failed to release sync lock (defer)",
					slog.Any("error", err),
					slog.String("instance_id", instanceID),
				)
			} else {
				l.Debug("released sync lock (defer)")
			}
		}
	}()

	// Clear potential stale operations from previous crashed syncs.
	staleCount, err := s._store.ClearPendingOperations(ctx, l)
	if err != nil {
		l.Warn("failed to clear stale operations", slog.Any("error", err))
	} else if staleCount > 0 {
		l.Warn("cleared stale operations from previous crashed syncs",
			slog.Int64("count", staleCount))
	}

	// Step 2: Sync from cloud.
	crlLogger, err := (&rplogger.Config{Stdout: l, Stderr: l}).NewLogger("")
	if err != nil {
		return nil, err
	}

	l.Info("syncing clusters")
	err = s.roachprodCloud.ListCloud(ctx, crlLogger, vm.ListOptions{
		IncludeVolumes:       true,
		ComputeEstimatedCost: false,
		BailOnProviderError:  true,
	})
	if err != nil {
		return nil, err
	}

	// Loop to apply all operations on staging clusters.
	// Continue until no more operations remain to minimize stale window.
	totalOpsApplied := 0
	for {
		pendingOps, fetchTimestamp, err := s._store.GetPendingOperationsWithTimestamp(ctx, l)
		if err != nil {
			l.Error("failed to get pending operations",
				slog.Any("error", err),
				slog.String("instance_id", instanceID),
			)
			break
		}

		if len(pendingOps) == 0 {
			break
		}

		l.Debug("replaying pending operations",
			slog.Int("batch_size", len(pendingOps)),
			slog.Int("total_applied", totalOpsApplied),
		)

		for _, opData := range pendingOps {
			op, err := s.operationDataToOperation(opData)
			if err != nil {
				l.Error("failed to convert operation data",
					slog.Any("error", err),
					slog.String("operation_id", opData.ID),
					slog.String("operation_type", string(opData.Type)),
				)
				continue
			}

			err = op.ApplyOnStagingClusters(ctx, l, s.roachprodCloud.Clusters)
			if err != nil {
				l.Error("unable to replay operation",
					slog.Any("error", err),
					slog.String("operation_id", opData.ID),
					slog.String("operation_type", string(opData.Type)),
					slog.String("cluster_name", opData.ClusterName),
				)
			}
		}

		totalOpsApplied += len(pendingOps)

		// Clear the operations we just applied
		clearedCount, err := s._store.ClearPendingOperationsBefore(ctx, l, fetchTimestamp)
		if err != nil {
			l.Error("failed to clear applied operations",
				slog.Any("error", err),
				slog.Time("timestamp", fetchTimestamp),
			)
		} else if clearedCount > 0 {
			l.Debug("cleared applied operations",
				slog.Int64("count", clearedCount),
				slog.Time("timestamp", fetchTimestamp))
		}
	}

	if totalOpsApplied > 0 {
		l.Debug("finished replaying operations", slog.Int("total", totalOpsApplied))
	}

	// Verify context is still valid before storing clusters.
	// This prevents leaked goroutines from writing stale data after timeout.
	if err := ctx.Err(); err != nil {
		return nil, errors.Wrap(err, "context cancelled before storing clusters")
	}

	// Atomically store clusters and release the sync lock.
	l.Debug("storing clusters and releasing sync lock")
	err = s._store.StoreClustersAndReleaseSyncLock(ctx, l, s.roachprodCloud.Clusters, instanceID)
	if err != nil {
		return nil, err
	}
	lockReleased = true
	l.Debug("stored clusters and released sync lock")

	// Final cleanup: clear any remaining operations that arrived in the tiny window
	// between last fetch and atomic store+release.
	remainingCount, err := s._store.ClearPendingOperations(ctx, l)
	if err != nil {
		l.Error("failed to clear pending operations",
			slog.Any("error", err),
			slog.String("instance_id", instanceID),
		)
	} else if remainingCount > 0 {
		l.Warn("cleared operations that arrived during final replay window",
			slog.Int64("count", remainingCount),
			slog.String("instance_id", instanceID))
	}

	// Trigger DNS sync.
	err = s.maybeEnqueuePublicDNSSyncTaskService(ctx, l)
	if err != nil {
		l.Error("failed to enqueue public DNS sync task",
			slog.Any("error", err),
			slog.String("trigger_operation", "sync"),
			slog.String("instance_id", instanceID),
		)
	}

	return s.roachprodCloud.Clusters, nil
}

// ScheduleSyncTaskIfNeeded creates a sync task if no recent one exists.
// This prevents duplicate sync tasks when multiple instances are running.
func (s *Service) ScheduleSyncTaskIfNeeded(
	ctx context.Context, l *logger.Logger,
) (tasks.ITask, error) {
	if s._taskService == nil {
		return nil, nil
	}

	// Create sync task using the helper that prevents duplicates
	return s._taskService.CreateTaskIfNotRecentlyScheduled(
		ctx, l,
		sclustertasks.NewTaskSync(),
		s.options.PeriodicRefreshInterval,
	)
}

// acquireSyncLockWithHealthCheck attempts to acquire the sync lock with atomic health verification.
// This uses the repository's atomic method to eliminate race conditions.
func (s *Service) acquireSyncLockWithHealthCheck(
	ctx context.Context, l *logger.Logger, instanceID string,
) (bool, error) {
	// Use atomic repository method that checks lock state and owner health in single transaction
	healthTimeout := s._healthService.GetInstanceTimeout()
	return s._store.AcquireSyncLockWithHealthCheck(ctx, l, instanceID, healthTimeout)
}
