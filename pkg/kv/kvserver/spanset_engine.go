// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/metrics"
)

// spanSetEngine wraps an Engine and intercepts NewBatch() to wrap the batch
// with spanset.NewBatch.
type spanSetEngine struct {
	storage.Reader
	storage.Writer
	e     storage.EngineWithoutRW
	spans *spanset.SpanSet
}

var _ storage.Engine = &spanSetEngine{}

// NewSpanSetEngine creates a new Engine wrapper that wraps batches returned
// from NewBatch() with spanset.NewBatch for span access verification.
func NewSpanSetEngine(engine storage.Engine) storage.Engine {
	spans := &spanset.SpanSet{}
	spans.DisableUndeclaredAccessAssertions()
	return &spanSetEngine{
		e:      engine,
		Reader: spanset.NewReader(engine, spans, hlc.Timestamp{}),
		Writer: spanset.NewReadWriterAt(engine, spans, hlc.Timestamp{}),
		spans:  spans,
	}
}

// NewBatch wraps the underlying engine's batch with spanset.NewBatch.
func (s *spanSetEngine) AddForbiddenMatcher(matcher func(spanset.TrickySpan) error) {
	s.spans.AddForbiddenMatcher(matcher)
}

// NewBatch wraps the underlying engine's batch with spanset.NewBatch.
func (s *spanSetEngine) NewBatch() storage.Batch {
	return spanset.NewBatch(s.e.NewBatch(), s.spans)
}

// NewBatch wraps the underlying engine's batch with spanset.NewBatch.
func (s *spanSetEngine) NewBatchWithSpans(spans *spanset.SpanSet) storage.Batch {
	return spanset.NewBatch(s.e.NewBatch(), spans)
}

func (s *spanSetEngine) NewReader(durability storage.DurabilityRequirement) storage.Reader {
	return spanset.NewReader(s.e.NewReader(durability), s.spans, hlc.Timestamp{})
}

func (s *spanSetEngine) NewReadOnly(durability storage.DurabilityRequirement) storage.ReadWriter {
	return spanset.NewReadWriterAt(s.e.NewReadOnly(durability), s.spans, hlc.Timestamp{})
}

func (s *spanSetEngine) NewUnindexedBatch() storage.Batch {
	return spanset.NewBatch(s.e.NewUnindexedBatch(), s.spans)
}

func (s *spanSetEngine) Excise(ctx context.Context, span roachpb.Span) error {
	if err := s.spans.CheckAllowed(spanset.SpanReadWrite, spanset.TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
		return err
	}
	return s.e.Excise(ctx, span)
}

func (s *spanSetEngine) Download(ctx context.Context, span roachpb.Span, copy bool) error {
	if err := s.spans.CheckAllowed(spanset.SpanReadWrite, spanset.TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
		return err
	}
	return s.e.Download(ctx, span, copy)
}

func (s *spanSetEngine) CreateCheckpoint(dir string, spans []roachpb.Span) error {
	for _, span := range spans {
		if err := s.spans.CheckAllowed(spanset.SpanReadOnly, spanset.TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
			return err
		}
	}
	return s.e.CreateCheckpoint(dir, spans)
}

func (s *spanSetEngine) GetTableMetrics(
	start, end roachpb.Key,
) ([]enginepb.SSTableMetricsInfo, error) {
	if err := s.spans.CheckAllowed(spanset.SpanReadOnly, spanset.TrickySpan{Key: start, EndKey: end}); err != nil {
		return nil, err
	}
	return s.e.GetTableMetrics(start, end)
}

func (s *spanSetEngine) CompactRange(ctx context.Context, start, end roachpb.Key) error {
	if err := s.spans.CheckAllowed(spanset.SpanReadWrite, spanset.TrickySpan{Key: start, EndKey: end}); err != nil {
		return err
	}
	return s.e.CompactRange(ctx, start, end)
}

func (s *spanSetEngine) ApproximateDiskBytes(
	from, to roachpb.Key,
) (total, remote, external uint64, _ error) {
	if err := s.spans.CheckAllowed(spanset.SpanReadOnly, spanset.TrickySpan{Key: from, EndKey: to}); err != nil {
		return 0, 0, 0, err
	}
	return s.e.ApproximateDiskBytes(from, to)
}

func (s *spanSetEngine) NewSnapshot(keyRanges ...roachpb.Span) storage.Reader {
	return spanset.NewReader(s.e.NewSnapshot(keyRanges...), s.spans, hlc.Timestamp{})
}

func (s *spanSetEngine) NewWriteBatch() storage.WriteBatch {
	wb := s.e.NewWriteBatch()
	return spanset.NewWriteBatch(wb, s.spans)
}

func (s *spanSetEngine) Attrs() roachpb.Attributes {
	return s.e.Attrs()
}

func (s *spanSetEngine) Capacity() (roachpb.StoreCapacity, error) {
	return s.e.Capacity()
}

func (s *spanSetEngine) Properties() roachpb.StoreProperties {
	return s.e.Properties()
}

func (s *spanSetEngine) ProfileSeparatedValueRetrievals(
	ctx context.Context,
) (*metrics.ValueRetrievalProfile, error) {
	return s.e.ProfileSeparatedValueRetrievals(ctx)
}

func (s *spanSetEngine) Compact(ctx context.Context) error {
	return s.e.Compact(ctx)
}

func (s *spanSetEngine) Env() *fs.Env {
	return s.e.Env()
}

func (s *spanSetEngine) Flush() error {
	return s.e.Flush()
}

func (s *spanSetEngine) GetMetrics() storage.Metrics {
	return s.e.GetMetrics()
}

func (s *spanSetEngine) GetEncryptionRegistries() (*fs.EncryptionRegistries, error) {
	return s.e.GetEncryptionRegistries()
}

func (s *spanSetEngine) GetEnvStats() (*fs.EnvStats, error) {
	return s.e.GetEnvStats()
}

func (s *spanSetEngine) GetAuxiliaryDir() string {
	return s.e.GetAuxiliaryDir()
}

func (s *spanSetEngine) IngestLocalFiles(ctx context.Context, paths []string) error {
	return s.e.IngestLocalFiles(ctx, paths)
}

func (s *spanSetEngine) IngestLocalFilesWithStats(
	ctx context.Context, paths []string,
) (pebble.IngestOperationStats, error) {
	return s.e.IngestLocalFilesWithStats(ctx, paths)
}

func (s *spanSetEngine) IngestAndExciseFiles(
	ctx context.Context,
	paths []string,
	shared []pebble.SharedSSTMeta,
	external []pebble.ExternalFile,
	exciseSpan roachpb.Span,
) (pebble.IngestOperationStats, error) {
	return s.e.IngestAndExciseFiles(ctx, paths, shared, external, exciseSpan)
}

func (s *spanSetEngine) IngestExternalFiles(
	ctx context.Context, external []pebble.ExternalFile,
) (pebble.IngestOperationStats, error) {
	return s.e.IngestExternalFiles(ctx, external)
}

func (s *spanSetEngine) IngestLocalFilesToWriter(
	ctx context.Context, paths []string, clearedSpans []roachpb.Span, writer storage.Writer,
) error {
	return s.e.IngestLocalFilesToWriter(ctx, paths, clearedSpans, writer)
}

func (s *spanSetEngine) ScanStorageInternalKeys(
	start, end roachpb.Key, megabytesPerSecond int64,
) ([]enginepb.StorageInternalKeysMetrics, error) {
	return s.e.ScanStorageInternalKeys(start, end, megabytesPerSecond)
}

func (s *spanSetEngine) RegisterFlushCompletedCallback(cb func()) {
	s.e.RegisterFlushCompletedCallback(cb)
}

func (s *spanSetEngine) MinVersion() roachpb.Version {
	return s.e.MinVersion()
}

func (s *spanSetEngine) SetMinVersion(version roachpb.Version) error {
	return s.e.SetMinVersion(version)
}

func (s *spanSetEngine) SetCompactionConcurrency(n uint64) {
	s.e.SetCompactionConcurrency(n)
}

func (s *spanSetEngine) SetStoreID(ctx context.Context, storeID int32) error {
	return s.e.SetStoreID(ctx, storeID)
}

func (s *spanSetEngine) GetStoreID() (int32, error) {
	return s.e.GetStoreID()
}

func (s *spanSetEngine) RegisterDiskSlowCallback(cb func(info pebble.DiskSlowInfo)) {
	s.e.RegisterDiskSlowCallback(cb)
}

func (s *spanSetEngine) RegisterLowDiskSpaceCallback(cb func(info pebble.LowDiskSpaceInfo)) {
	s.e.RegisterLowDiskSpaceCallback(cb)
}

func (s *spanSetEngine) GetPebbleOptions() *pebble.Options {
	return s.e.GetPebbleOptions()
}

func (s *spanSetEngine) GetDiskUnhealthy() bool {
	return s.e.GetDiskUnhealthy()
}
