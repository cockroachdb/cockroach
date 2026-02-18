// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// ILogStore persists and retrieves task log entries.
// Implementations must be safe for concurrent use.
type ILogStore interface {
	// NewSink creates a LogSink that writes log entries for the given task.
	// The caller is responsible for calling Close() on the returned sink.
	NewSink(taskID uuid.UUID) logger.LogSink

	// ReadLogs returns log entries for a task starting from the given offset
	// (0-based entry index). Returns the entries and the next offset for
	// subsequent reads. If all data has been written (sink closed) and no
	// more entries exist past nextOffset, done is true.
	ReadLogs(ctx context.Context, taskID uuid.UUID, offset int) (
		entries []logger.LogEntry, nextOffset int, done bool, err error,
	)

	// DeleteLogs removes all stored log data for the given task.
	// Returns nil if no logs exist for the task.
	DeleteLogs(ctx context.Context, taskID uuid.UUID) error
}
