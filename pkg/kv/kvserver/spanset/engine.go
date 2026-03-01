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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/metrics"
)

// EnableAssertions is a convenient entry point to enable/disable spanset
// assertions in all places that wrap storage types into spanset types. It also
// helps discover all those places (users of this constant).
const EnableAssertions = util.RaceEnabled

// spanSetEngine wraps an Engine and asserts that it does not access spans that
// do not belong to it.
type spanSetEngine struct {
	ReadWriter
	e     storage.Engine
	spans *SpanSet
}

var _ storage.Engine = &spanSetEngine{}

// NewEngine creates a new spanSetEngine wrapper.
func NewEngine(engine storage.Engine, forbiddenMatcher func(TrickySpan) error) storage.Engine {
	spans := &SpanSet{}
	// For engines, we disable undeclared access assertions as we only care about
	// preventing access to spans that don't belong to this engine.
	spans.DisableUndeclaredAccessAssertions()
	// Set the forbidden matcher that asserts against access to disallowed keys.
	spans.SetForbiddenMatcher(forbiddenMatcher)
	return &spanSetEngine{
		ReadWriter: makeSpanSetReadWriter(engine, spans),
		e:          engine,
		spans:      spans,
	}
}

// NewBatch implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) NewBatch() storage.Batch {
	return NewBatch(s.e.NewBatch(), s.spans)
}

// NewBatch implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) NewReader(durability storage.DurabilityRequirement) storage.Reader {
	return NewReader(s.e.NewReader(durability), s.spans, hlc.Timestamp{})
}

// NewReadOnly implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) NewReadOnly(durability storage.DurabilityRequirement) storage.ReadWriter {
	return NewReadWriterAt(s.e.NewReadOnly(durability), s.spans, hlc.Timestamp{})
}

// NewUnindexedBatch implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) NewUnindexedBatch() storage.Batch {
	return NewBatch(s.e.NewUnindexedBatch(), s.spans)
}

// Excise implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) Excise(ctx context.Context, span roachpb.Span) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
		return err
	}
	return s.e.Excise(ctx, span)
}

// Download implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) Download(ctx context.Context, span roachpb.Span, copy bool) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
		return err
	}
	return s.e.Download(ctx, span, copy)
}

// CreateCheckpoint implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) CreateCheckpoint(dir string, spans []roachpb.Span) error {
	for _, span := range spans {
		if err := s.spans.CheckAllowed(SpanReadOnly, TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
			return err
		}
	}
	return s.e.CreateCheckpoint(dir, spans)
}

// GetTableMetrics implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) GetTableMetrics(
	start, end roachpb.Key,
) ([]enginepb.SSTableMetricsInfo, error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, TrickySpan{Key: start, EndKey: end}); err != nil {
		return nil, err
	}
	return s.e.GetTableMetrics(start, end)
}

// CompactRange implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) CompactRange(ctx context.Context, start, end roachpb.Key) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, TrickySpan{Key: start, EndKey: end}); err != nil {
		return err
	}
	return s.e.CompactRange(ctx, start, end)
}

// ApproximateDiskBytes implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) ApproximateDiskBytes(
	from, to roachpb.Key,
) (total, remote, external uint64, _ error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, TrickySpan{Key: from, EndKey: to}); err != nil {
		return 0, 0, 0, err
	}
	return s.e.ApproximateDiskBytes(from, to)
}

// NewSnapshot implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) NewSnapshot(keyRanges ...roachpb.Span) storage.Reader {
	for _, span := range keyRanges {
		if err := s.spans.CheckAllowed(SpanReadOnly, TrickySpan{Key: span.Key, EndKey: span.EndKey}); err != nil {
			panic(err)
		}
	}
	return NewReader(s.e.NewSnapshot(keyRanges...), s.spans, hlc.Timestamp{})
}

// NewWriteBatch implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) NewWriteBatch() storage.WriteBatch {
	return s.WrapWriteBatch(s.e.NewWriteBatch())
}

// WrapWriteBatch associates the given WriteBatch with this Engine, and returns
// a wrapped WriteBatch. This enables the corresponding assertions, e.g.
// checking that the batch accesses only the keys allowed by this Engine.
func (s *spanSetEngine) WrapWriteBatch(wb storage.WriteBatch) storage.WriteBatch {
	return &spanSetWriteBatch{
		spanSetWriter: spanSetWriter{w: wb, spans: s.spans, spansOnly: true},
		wb:            wb,
	}
}

// Attrs implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) Attrs() roachpb.Attributes {
	return s.e.Attrs()
}

// Capacity implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) Capacity() (roachpb.StoreCapacity, error) {
	return s.e.Capacity()
}

// Properties implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) Properties() roachpb.StoreProperties {
	return s.e.Properties()
}

// ProfileSeparatedValueRetrievals implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) ProfileSeparatedValueRetrievals(
	ctx context.Context,
) (*metrics.ValueRetrievalProfile, error) {
	return s.e.ProfileSeparatedValueRetrievals(ctx)
}

// Compact implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) Compact(ctx context.Context) error {
	return s.e.Compact(ctx)
}

// Env implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) Env() *fs.Env {
	return s.e.Env()
}

// Flush implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) Flush() error {
	return s.e.Flush()
}

// GetMetrics implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) GetMetrics() storage.Metrics {
	return s.e.GetMetrics()
}

// GetEncryptionRegistries implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) GetEncryptionRegistries() (*fs.EncryptionRegistries, error) {
	return s.e.GetEncryptionRegistries()
}

// GetEnvStats implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) GetEnvStats() (*fs.EnvStats, error) {
	return s.e.GetEnvStats()
}

// GetAuxiliaryDir implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) GetAuxiliaryDir() string {
	return s.e.GetAuxiliaryDir()
}

// IngestLocalFiles implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) IngestLocalFiles(ctx context.Context, paths []string) error {
	return s.e.IngestLocalFiles(ctx, paths)
}

// IngestLocalFilesWithStats implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) IngestLocalFilesWithStats(
	ctx context.Context, paths []string,
) (pebble.IngestOperationStats, error) {
	return s.e.IngestLocalFilesWithStats(ctx, paths)
}

// IngestAndExciseFiles implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) IngestAndExciseFiles(
	ctx context.Context,
	paths []string,
	shared []pebble.SharedSSTMeta,
	external []pebble.ExternalFile,
	exciseSpan roachpb.Span,
) (pebble.IngestOperationStats, error) {
	if err := s.spans.CheckAllowed(SpanReadWrite, TrickySpan{Key: exciseSpan.Key, EndKey: exciseSpan.EndKey}); err != nil {
		return pebble.IngestOperationStats{}, err
	}
	return s.e.IngestAndExciseFiles(ctx, paths, shared, external, exciseSpan)
}

// IngestExternalFiles implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) IngestExternalFiles(
	ctx context.Context, external []pebble.ExternalFile,
) (pebble.IngestOperationStats, error) {
	return s.e.IngestExternalFiles(ctx, external)
}

// IngestLocalFilesToWriter implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) IngestLocalFilesToWriter(
	ctx context.Context, paths []string, clearedSpans []roachpb.Span, writer storage.Writer,
) error {
	return s.e.IngestLocalFilesToWriter(ctx, paths, clearedSpans, writer)
}

// ScanStorageInternalKeys implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) ScanStorageInternalKeys(
	start, end roachpb.Key, megabytesPerSecond int64,
) ([]enginepb.StorageInternalKeysMetrics, error) {
	if err := s.spans.CheckAllowed(SpanReadWrite, TrickySpan{Key: start, EndKey: end}); err != nil {
		return nil, err
	}
	return s.e.ScanStorageInternalKeys(start, end, megabytesPerSecond)
}

// RegisterFlushCompletedCallback implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) RegisterFlushCompletedCallback(cb func()) {
	s.e.RegisterFlushCompletedCallback(cb)
}

// MinVersion implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) MinVersion() roachpb.Version {
	return s.e.MinVersion()
}

// SetMinVersion implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) SetMinVersion(version roachpb.Version) error {
	return s.e.SetMinVersion(version)
}

// SetCompactionConcurrency implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) SetCompactionConcurrency(n uint64) {
	s.e.SetCompactionConcurrency(n)
}

// SetStoreID implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) SetStoreID(ctx context.Context, storeID int32) error {
	return s.e.SetStoreID(ctx, storeID)
}

// GetStoreID implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) GetStoreID() (int32, error) {
	return s.e.GetStoreID()
}

// RegisterDiskSlowCallback implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) RegisterDiskSlowCallback(cb func(info pebble.DiskSlowInfo)) {
	s.e.RegisterDiskSlowCallback(cb)
}

// RegisterLowDiskSpaceCallback implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) RegisterLowDiskSpaceCallback(cb func(info pebble.LowDiskSpaceInfo)) {
	s.e.RegisterLowDiskSpaceCallback(cb)
}

// GetPebbleOptions implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) GetPebbleOptions() *pebble.Options {
	return s.e.GetPebbleOptions()
}

// GetDiskUnhealthy implements the storage.EngineWithoutRW interface.
func (s *spanSetEngine) GetDiskUnhealthy() bool {
	return s.e.GetDiskUnhealthy()
}
