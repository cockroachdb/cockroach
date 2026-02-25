// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// MemoryLogStore stores log entries in memory. Safe for concurrent use.
type MemoryLogStore struct {
	mu       syncutil.Mutex
	logs     map[uuid.UUID][]logger.LogEntry
	complete map[uuid.UUID]bool
}

// NewMemoryLogStore creates a new in-memory log store.
func NewMemoryLogStore() *MemoryLogStore {
	return &MemoryLogStore{
		logs:     make(map[uuid.UUID][]logger.LogEntry),
		complete: make(map[uuid.UUID]bool),
	}
}

var _ ILogStore = (*MemoryLogStore)(nil)

// NewSink creates a LogSink that writes entries to the in-memory store.
func (s *MemoryLogStore) NewSink(taskID uuid.UUID) logger.LogSink {
	return &memorySink{store: s, taskID: taskID}
}

// ReadLogs returns log entries for a task starting from the given offset.
func (s *MemoryLogStore) ReadLogs(
	ctx context.Context, taskID uuid.UUID, offset int,
) ([]logger.LogEntry, int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.logs[taskID]
	if offset >= len(entries) {
		return nil, offset, s.complete[taskID], nil
	}
	result := make([]logger.LogEntry, len(entries)-offset)
	copy(result, entries[offset:])
	return result, len(entries), s.complete[taskID], nil
}

// DeleteLogs removes all stored log data for the given task.
func (s *MemoryLogStore) DeleteLogs(ctx context.Context, taskID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.logs, taskID)
	delete(s.complete, taskID)
	return nil
}

func (s *MemoryLogStore) append(taskID uuid.UUID, entry logger.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs[taskID] = append(s.logs[taskID], entry)
}

func (s *MemoryLogStore) markComplete(taskID uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.complete[taskID] = true
}

type memorySink struct {
	store  *MemoryLogStore
	taskID uuid.UUID
}

func (s *memorySink) WriteEntry(entry logger.LogEntry) error {
	s.store.append(s.taskID, entry)
	return nil
}

func (s *memorySink) Close() error {
	s.store.markComplete(s.taskID)
	return nil
}
