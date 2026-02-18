// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	mtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	tasksrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// terminalStallLimit is the number of consecutive polls with no new entries
// after the task reaches a terminal state before the stream is closed. This
// handles the case where the done marker was never written (e.g.,
// gcsSink.Close() failed). With a 1-second poll interval this gives
// ~5 seconds for in-flight GCS writes to land.
const terminalStallLimit = 5

// StreamTaskLogs returns a reader that streams JSONL log entries for a task.
// It first verifies the task exists (returning ErrTaskNotFound if not), then
// starts a background goroutine that polls the log store and writes JSONL
// to the pipe. The reader closes with io.EOF when the task reaches a terminal
// state and all logs are flushed.
//
// Stream termination has two paths:
//   - Primary: the log store's done marker is present and the task is terminal.
//   - Fallback: the task is terminal and no new entries have arrived for
//     terminalStallLimit consecutive polls (covers missing done marker).
//
// The caller must close the returned reader.
func (s *Service) StreamTaskLogs(
	ctx context.Context, l *logger.Logger, taskID uuid.UUID, offset int,
) (io.ReadCloser, error) {
	// Verify the task exists before starting the stream.
	_, err := s.store.GetTask(ctx, l, taskID)
	if err != nil {
		if errors.Is(err, tasksrepo.ErrTaskNotFound) {
			return nil, types.ErrTaskNotFound
		}
		return nil, err
	}

	if s.logStore == nil {
		return io.NopCloser(strings.NewReader("")), nil
	}

	pr, pw := io.Pipe()

	go func() {
		defer func() { _ = pw.Close() }()
		enc := json.NewEncoder(pw)
		currentOffset := offset
		terminalStallCount := 0

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			entries, nextOffset, done, readErr := s.logStore.ReadLogs(ctx, taskID, currentOffset)
			if readErr != nil {
				pw.CloseWithError(readErr)
				return
			}

			for _, entry := range entries {
				if encErr := enc.Encode(entry); encErr != nil {
					pw.CloseWithError(encErr)
					return
				}
			}
			currentOffset = nextOffset

			// Check if the task has reached a terminal state and all logs
			// have been flushed.
			task, getErr := s.store.GetTask(ctx, l, taskID)
			if getErr == nil {
				state := task.GetState()
				if state == mtasks.TaskStateDone || state == mtasks.TaskStateFailed {
					// Primary: done marker present and task is terminal.
					if done {
						return
					}
					// Fallback: task is terminal but done marker is missing
					// (e.g., gcsSink.Close() failed). Close the stream after
					// terminalStallLimit consecutive polls with no new entries.
					if len(entries) == 0 {
						terminalStallCount++
						if terminalStallCount >= terminalStallLimit {
							return
						}
					} else {
						terminalStallCount = 0
					}
				}
			}

			select {
			case <-ctx.Done():
				pw.CloseWithError(ctx.Err())
				return
			case <-ticker.C:
				// continue polling
			}
		}
	}()

	return pr, nil
}
