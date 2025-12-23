// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/metrics"
)

// SpanSetEngine wraps an Engine and asserts that it does not access spans that
// do not belong to it.
type SpanSetEngine struct {
	storage.ReadWriter
	e     storage.Engine
	spans *SpanSet
}

var _ storage.Engine = &SpanSetEngine{}

// NewSpanSetEngine creates a new SpanSetEngine wrapper.
func NewSpanSetEngine(engine storage.Engine) storage.Engine {
	spans := &SpanSet{}
	// For engines, we disable undeclared access assertions as we only care about
	// preventing access to spans that don't belong to this engine.
	spans.DisableUndeclaredAccessAssertions()
	return &SpanSetEngine{
		e:          engine,
		ReadWriter: NewReadWriter(engine, spans),
		spans:      spans,
	}
}

// AddForbiddenMatcher adds a forbidden matcher to the underlying SpanSet.
func (s *SpanSetEngine) AddForbiddenMatcher(matcher func(TrickySpan) error) {
	s.spans.AddForbiddenMatcher(matcher)
}

// NewBatch implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) NewBatch() storage.Batch {
	return NewBatch(s.e.NewBatch(), s.spans)
}

// NewBatch implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) NewReader(durability storage.DurabilityRequirement) storage.Reader {
	return NewReader(s.e.NewReader(durability), s.spans, hlc.Timestamp{})
}

// NewReadOnly implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) NewReadOnly(durability storage.DurabilityRequirement) storage.ReadWriter {
	return NewReadWriterAt(s.e.NewReadOnly(durability), s.spans, hlc.Timestamp{})
}

// NewUnindexedBatch implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) NewUnindexedBatch() storage.Batch {
	return NewBatch(s.e.NewUnindexedBatch(), s.spans)
}

// Excise implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) Excise(ctx context.Context, span roachpb.Span) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
		return err
	}
	return s.e.Excise(ctx, span)
}

// Download implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) Download(ctx context.Context, span roachpb.Span, copy bool) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
		return err
	}
	return s.e.Download(ctx, span, copy)
}

// CreateCheckpoint implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) CreateCheckpoint(dir string, spans []roachpb.Span) error {
	for _, span := range spans {
		if err := s.spans.CheckAllowed(SpanReadOnly, TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
			return err
		}
	}
	return s.e.CreateCheckpoint(dir, spans)
}

// GetTableMetrics implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) GetTableMetrics(
	start, end roachpb.Key,
) ([]enginepb.SSTableMetricsInfo, error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, TrickySpan{Key: start, EndKey: end}); err != nil {
		return nil, err
	}
	return s.e.GetTableMetrics(start, end)
}

// CompactRange implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) CompactRange(ctx context.Context, start, end roachpb.Key) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, TrickySpan{Key: start, EndKey: end}); err != nil {
		return err
	}
	return s.e.CompactRange(ctx, start, end)
}

// ApproximateDiskBytes implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) ApproximateDiskBytes(
	from, to roachpb.Key,
) (total, remote, external uint64, _ error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, TrickySpan{Key: from, EndKey: to}); err != nil {
		return 0, 0, 0, err
	}
	return s.e.ApproximateDiskBytes(from, to)
}

// NewSnapshot implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) NewSnapshot(keyRanges ...roachpb.Span) storage.Reader {
	return NewReader(s.e.NewSnapshot(keyRanges...), s.spans, hlc.Timestamp{})
}

// NewWriteBatch implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) NewWriteBatch() storage.WriteBatch {
	wb := s.e.NewWriteBatch()
	return &spanSetWriteBatch{
		spanSetWriter: spanSetWriter{w: wb, spans: s.spans, spansOnly: true},
		wb:            wb,
	}
}

// Attrs implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) Attrs() roachpb.Attributes {
	return s.e.Attrs()
}

// Capacity implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) Capacity() (roachpb.StoreCapacity, error) {
	return s.e.Capacity()
}

// Properties implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) Properties() roachpb.StoreProperties {
	return s.e.Properties()
}

// ProfileSeparatedValueRetrievals implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) ProfileSeparatedValueRetrievals(
	ctx context.Context,
) (*metrics.ValueRetrievalProfile, error) {
	return s.e.ProfileSeparatedValueRetrievals(ctx)
}

// Compact implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) Compact(ctx context.Context) error {
	return s.e.Compact(ctx)
}

// Env implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) Env() *fs.Env {
	return s.e.Env()
}

// Flush implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) Flush() error {
	return s.e.Flush()
}

// GetMetrics implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) GetMetrics() storage.Metrics {
	return s.e.GetMetrics()
}

// GetEncryptionRegistries implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) GetEncryptionRegistries() (*fs.EncryptionRegistries, error) {
	return s.e.GetEncryptionRegistries()
}

// GetEnvStats implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) GetEnvStats() (*fs.EnvStats, error) {
	return s.e.GetEnvStats()
}

// GetAuxiliaryDir implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) GetAuxiliaryDir() string {
	return s.e.GetAuxiliaryDir()
}

// IngestLocalFiles implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) IngestLocalFiles(ctx context.Context, paths []string) error {
	return s.e.IngestLocalFiles(ctx, paths)
}

// IngestLocalFilesWithStats implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) IngestLocalFilesWithStats(
	ctx context.Context, paths []string,
) (pebble.IngestOperationStats, error) {
	return s.e.IngestLocalFilesWithStats(ctx, paths)
}

// IngestAndExciseFiles implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) IngestAndExciseFiles(
	ctx context.Context,
	paths []string,
	shared []pebble.SharedSSTMeta,
	external []pebble.ExternalFile,
	exciseSpan roachpb.Span,
) (pebble.IngestOperationStats, error) {
	return s.e.IngestAndExciseFiles(ctx, paths, shared, external, exciseSpan)
}

// IngestExternalFiles implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) IngestExternalFiles(
	ctx context.Context, external []pebble.ExternalFile,
) (pebble.IngestOperationStats, error) {
	return s.e.IngestExternalFiles(ctx, external)
}

// IngestLocalFilesToWriter implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) IngestLocalFilesToWriter(
	ctx context.Context, paths []string, clearedSpans []roachpb.Span, writer storage.Writer,
) error {
	return s.e.IngestLocalFilesToWriter(ctx, paths, clearedSpans, writer)
}

// ScanStorageInternalKeys implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) ScanStorageInternalKeys(
	start, end roachpb.Key, megabytesPerSecond int64,
) ([]enginepb.StorageInternalKeysMetrics, error) {
	return s.e.ScanStorageInternalKeys(start, end, megabytesPerSecond)
}

// RegisterFlushCompletedCallback implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) RegisterFlushCompletedCallback(cb func()) {
	s.e.RegisterFlushCompletedCallback(cb)
}

// MinVersion implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) MinVersion() roachpb.Version {
	return s.e.MinVersion()
}

// SetMinVersion implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) SetMinVersion(version roachpb.Version) error {
	return s.e.SetMinVersion(version)
}

// SetCompactionConcurrency implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) SetCompactionConcurrency(n uint64) {
	s.e.SetCompactionConcurrency(n)
}

// SetStoreID implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) SetStoreID(ctx context.Context, storeID int32) error {
	return s.e.SetStoreID(ctx, storeID)
}

// GetStoreID implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) GetStoreID() (int32, error) {
	return s.e.GetStoreID()
}

// RegisterDiskSlowCallback implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) RegisterDiskSlowCallback(cb func(info pebble.DiskSlowInfo)) {
	s.e.RegisterDiskSlowCallback(cb)
}

// RegisterLowDiskSpaceCallback implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) RegisterLowDiskSpaceCallback(cb func(info pebble.LowDiskSpaceInfo)) {
	s.e.RegisterLowDiskSpaceCallback(cb)
}

// GetPebbleOptions implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) GetPebbleOptions() *pebble.Options {
	return s.e.GetPebbleOptions()
}

// GetDiskUnhealthy implements the storage.EngineWithoutRW interface.
func (s *SpanSetEngine) GetDiskUnhealthy() bool {
	return s.e.GetDiskUnhealthy()
}
