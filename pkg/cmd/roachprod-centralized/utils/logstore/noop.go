// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// NoopLogStore is a no-op log store that discards all entries.
type NoopLogStore struct{}

var _ ILogStore = (*NoopLogStore)(nil)

// NewNoopLogStore creates a new no-op log store.
func NewNoopLogStore() *NoopLogStore { return &NoopLogStore{} }

// NewSink returns a no-op sink.
func (s *NoopLogStore) NewSink(uuid.UUID) logger.LogSink { return &noopSink{} }

// ReadLogs always returns empty results with done=true.
func (s *NoopLogStore) ReadLogs(
	_ context.Context, _ uuid.UUID, offset int,
) ([]logger.LogEntry, int, bool, error) {
	return nil, offset, true, nil
}

// DeleteLogs is a no-op.
func (s *NoopLogStore) DeleteLogs(_ context.Context, _ uuid.UUID) error { return nil }

type noopSink struct{}

func (s *noopSink) WriteEntry(logger.LogEntry) error { return nil }
func (s *noopSink) Close() error                     { return nil }
