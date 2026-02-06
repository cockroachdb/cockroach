// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// BulkSink abstracts the destination for bulk writes during index backfills
// and imports. Implementations can route KVs to legacy BulkAdder ingestion or
// to SST-based distributed merge pipelines.
//
// All sinks share a common lifecycle:
//  1. Add() is called to enqueue KV pairs
//  2. Flush() is called to persist buffered data
//  3. SetOnFlush() callback fires after each successful flush
//  4. ConsumeFlushManifests() retrieves SST metadata (if applicable)
//  5. Close() releases resources
type BulkSink interface {
	// Add enqueues a single KV pair for eventual persistence in the sink-specific
	// backing store. The sink may buffer data in memory until Flush is called or
	// an internal buffer limit is reached.
	Add(ctx context.Context, key roachpb.Key, value []byte) error

	// Flush forces any buffered state to be persisted. For BulkAdder-based sinks,
	// this sends AddSSTable requests to KV. For SST-based sinks, this writes SST
	// files to external storage.
	Flush(ctx context.Context) error

	// Close releases resources owned by the sink. Implementations should be
	// idempotent and safe to call even if Flush returns an error.
	Close(ctx context.Context)

	// SetOnFlush installs a callback that is invoked after the sink successfully
	// writes a batch. The callback receives a summary of the work performed
	// (rows written, bytes processed, etc.). This is used to coordinate progress
	// reporting and span tracking.
	SetOnFlush(func(summary kvpb.BulkOpSummary))

	// ConsumeFlushManifests returns SST manifests produced since the last call.
	// For distributed-merge sinks, this returns metadata about SST files written
	// to external storage. For legacy BulkAdder sinks, this always returns nil.
	// Calling this method clears the internal manifest buffer.
	ConsumeFlushManifests() []jobspb.BulkSSTManifest
}

// BulkAdderSink wraps a kvserverbase.BulkAdder to implement the BulkSink
// interface. This provides the legacy ingestion path where SSTs are sent
// directly to KV via AddSSTable requests rather than staged in external storage.
type BulkAdderSink struct {
	kvserverbase.BulkAdder
}

var _ BulkSink = (*BulkAdderSink)(nil)

// ConsumeFlushManifests implements BulkSink. BulkAdder ingests directly to KV
// and does not produce SST manifests, so this always returns nil.
func (b *BulkAdderSink) ConsumeFlushManifests() []jobspb.BulkSSTManifest {
	return nil
}
