// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// indexusagestats is a subsystem that is responsible for collecting index usage
// statistics.

package indexusagestats

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// IndexUsageType is the enum specifying the type of usage of an index.
type IndexUsageType int8

const (
	// ReadOp indicates that a read operation has occurred for an index.
	ReadOp IndexUsageType = iota

	// WriteOp indicates that a write operation has occurred for an index.
	WriteOp
)

// MetaData is the payload struct that records all the metadata we want to
// record for a single index usage.
type MetaData struct {
	// Key is what specify a particular index. It's a tuple of
	// (table_id, index_id).
	Key roachpb.IndexUsageKey

	// UsageType specifies how this index is being used.
	UsageType IndexUsageType
}

// Writer provides interface to record index usage statistics.
type Writer interface {
	// Record records the MetaData if this subsystem is enabled through cluster
	// setting.
	Record(ctx context.Context, payload MetaData)
}

// Sink provides interfaces to batch insert index usage statistics.
type Sink interface {
	// IngestStats is the method to call to insert a batch of
	// roachpb.CollectedIndexUsageStatistics into the Sink.
	IngestStats(stats []roachpb.CollectedIndexUsageStatistics)
}

// IteratorOptions provides knobs to change the iterating behavior when
// calling IterateIndexUsageStats.
type IteratorOptions struct {
	SortedTableID bool
	SortedIndexID bool
	Max           *uint64
}

// StatsVisitor is the callback invoked when calling IterateIndexUsageStats.
type StatsVisitor func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error

// Reader provides interfaces to retrieve index usage statistics from the
// subsystem.
type Reader interface {
	// GetIndexUsageStats returns the index usage statistics for a given key.
	GetIndexUsageStats(key roachpb.IndexUsageKey) roachpb.IndexUsageStatistics

	// IterateIndexUsageStats iterates through all stored index usage statistics
	// based on the options specified in IteratorOptions. If an error is
	// encountered when calling StatsVisitor, the iteration is aborted.
	IterateIndexUsageStats(options IteratorOptions, visitor StatsVisitor) error

	// GetLastReset returns the time where the subsystem is last reset.
	GetLastReset() time.Time
}

// Storage provides interfaces to read/write to/from this subsystem.
type Storage interface {
	Reader
	Writer
	Sink

	// Clear clears the index usage stats stored by the current subsystem and
	// update GetLastReset() timestamp.
	Clear()
}

// Provider provides interfaces to control the overall lifetime of the entire
// subsystem.
type Provider interface {
	Storage

	// Start starts the subsystem. This is necessary for enabling the subsystem
	// to collect node-local index usage stats. It is not necessary if the
	// subsystem is only used as a Reader.
	Start(ctx context.Context, stopper *stop.Stopper)
}
