// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

// SSTSink writes bulk data to sorted SST files backed by ExternalStorage. Each
// flush returns metadata for newly produced files so coordinators can persist
// them for downstream merge/ingest stages. This sink is used by both index
// backfill and IMPORT when running with distributed merge enabled.
type SSTSink struct {
	writer           *Writer
	allocator        FileAllocator
	es               cloud.ExternalStorage
	writeTS          hlc.Timestamp
	emittedFileCount int
	onFlush          func(summary kvpb.BulkOpSummary)
	pendingManifests []jobspb.BulkSSTManifest
}

var _ BulkSink = (*SSTSink)(nil)

// NewSSTSink creates a new SST-based bulk sink that writes sorted SSTs to
// external storage. This sink is used by both index backfill and IMPORT when
// distributed merge is enabled.
//
// Parameters:
//   - distributedMergeFilePrefix: Base URI for SST files (e.g., "nodelocal://1/job/123/map")
//   - processorID: Unique ID for this processor, used to create per-processor subdirectories
//   - writeAsOf: Timestamp to write on all KV pairs
//   - checkDuplicates: Whether to detect and reject duplicate keys during flush
func NewSSTSink(
	ctx context.Context,
	settings *cluster.Settings,
	externalStorageFromURI cloud.ExternalStorageFromURIFactory,
	clock *hlc.Clock,
	distributedMergeFilePrefix string,
	writeAsOf hlc.Timestamp,
	processorID int32,
	checkDuplicates bool,
) (BulkSink, error) {
	if distributedMergeFilePrefix == "" {
		return nil, errors.AssertionFailedf("distributed merge sink requires file prefix")
	}
	if externalStorageFromURI == nil {
		return nil, errors.AssertionFailedf("external storage factory must be configured")
	}

	prefix := fmt.Sprintf("%s/proc-%d/", strings.TrimRight(distributedMergeFilePrefix, "/"), processorID)

	es, err := externalStorageFromURI(ctx, prefix, username.NodeUserName())
	if err != nil {
		return nil, err
	}

	fileAllocator := NewExternalFileAllocator(es, prefix, clock)
	writer := NewUnsortedSSTBatcher(settings, fileAllocator)
	writer.SetWriteTS(writeAsOf)
	writer.SetCheckDuplicates(checkDuplicates)

	return &SSTSink{
		writer:    writer,
		allocator: fileAllocator,
		es:        es,
		writeTS:   writeAsOf,
	}, nil
}

// Add implements the BulkSink interface.
func (s *SSTSink) Add(ctx context.Context, key roachpb.Key, value []byte) error {
	return s.writer.Add(ctx, key, value)
}

// Flush implements the BulkSink interface.
func (s *SSTSink) Flush(ctx context.Context) error {
	// Callers must use SetOnFlush before calling Flush in order to properly track
	// the pending manifests.
	if s.onFlush == nil {
		return errors.AssertionFailedf("SetOnFlush must be called before Flush")
	}
	return s.writer.Flush(ctx)
}

// SetOnFlush implements the BulkSink interface.
func (s *SSTSink) SetOnFlush(fn func(summary kvpb.BulkOpSummary)) {
	s.onFlush = fn
	s.writer.SetOnFlush(func(summary kvpb.BulkOpSummary) {
		s.pendingManifests = append(s.pendingManifests, s.collectNewManifests()...)
		if s.onFlush != nil {
			s.onFlush(summary)
		}
	})
}

// ConsumeFlushManifests implements the BulkSink interface.
func (s *SSTSink) ConsumeFlushManifests() []jobspb.BulkSSTManifest {
	if len(s.pendingManifests) == 0 {
		return nil
	}
	out := s.pendingManifests
	s.pendingManifests = nil
	return out
}

func (s *SSTSink) collectNewManifests() []jobspb.BulkSSTManifest {
	files := s.allocator.GetFileList()
	if len(files.SST) <= s.emittedFileCount {
		return nil
	}
	// Extract only the new files since last collection.
	newFiles := &SSTFiles{
		SST: files.SST[s.emittedFileCount:],
	}
	s.emittedFileCount = len(files.SST)

	// Use shared conversion function, passing writeTS for the timestamp.
	return SSTFilesToManifests(newFiles, &s.writeTS)
}

// Close implements the BulkSink interface.
func (s *SSTSink) Close(ctx context.Context) {
	if err := s.writer.CloseWithError(ctx); err != nil {
		log.Dev.Warningf(ctx, "closing SST sink writer: %v", err)
	}
	if err := s.es.Close(); err != nil {
		log.Dev.Warningf(ctx, "closing SST sink storage: %v", err)
	}
}
