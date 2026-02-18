// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"log/slog"
	"time"

	mtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// operations.go contains business operations that are called by task implementations.
// These are public methods that tasks can invoke to perform their work.

// PurgeTasks purges old completed and failed tasks from the database.
// Returns the number of done and failed tasks purged.
func (s *Service) PurgeTasks(ctx context.Context, l *logger.Logger) (int, int, error) {
	delDone, err := s.purgeTasksInState(ctx, l, s.options.PurgeDoneTaskOlderThan, mtasks.TaskStateDone)
	if err != nil {
		return 0, 0, errors.Wrapf(
			err, "unable to purge tasks in %s state", string(mtasks.TaskStateDone),
		)
	}

	delFailed, err := s.purgeTasksInState(
		ctx, l, s.options.PurgeFailedTaskOlderThan, mtasks.TaskStateFailed,
	)
	if err != nil {
		return delDone, 0, errors.Wrapf(
			err, "unable to purge tasks in %s state", string(mtasks.TaskStateFailed),
		)
	}

	return delDone, delFailed, nil
}

// purgeTasksInState purges tasks in a given state that are older
// than a given interval.
func (s *Service) purgeTasksInState(
	ctx context.Context, l *logger.Logger, interval time.Duration, state mtasks.TaskState,
) (int, error) {
	purgedIDs, err := s.store.PurgeTasks(ctx, l, interval, state)
	if err != nil {
		return 0, err
	}

	// Delete logs for purged tasks (best-effort).
	if s.logStore != nil {
		for _, id := range purgedIDs {
			if delErr := s.logStore.DeleteLogs(ctx, id); delErr != nil {
				l.Warn("failed to delete task logs",
					slog.String("task_id", id.String()),
					slog.Any("error", delErr),
				)
			}
		}
	}

	return len(purgedIDs), nil
}
