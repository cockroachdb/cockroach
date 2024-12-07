// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/sstable"
)

// IngestionValueBlocksEnabled controls whether older versions of MVCC keys in
// the same ingested sstable will have their values written to value blocks.
// This configuration ability was motivated by a case of > 130GB sstables,
// caused by snapshot ingestion. Writing value blocks requires in-memory
// buffering of compressed value blocks, which caused OOMs in the above case.
var IngestionValueBlocksEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"storage.ingestion.value_blocks.enabled",
	"set to true to enable writing of value blocks in ingestion sstables",
	metamorphic.ConstantWithTestBool(
		"storage.ingestion.value_blocks.enabled", true),
	settings.WithPublic)

// SSTWriter writes SSTables.
type SSTWriter struct {
	fw *sstable.Writer
	// DataSize tracks the total key and value bytes added so far.
	DataSize int64
	scratch  []byte

	Meta *sstable.WriterMetadata
}

var _ Writer = &SSTWriter{}
var _ ExportWriter = &SSTWriter{}
var _ InternalWriter = &SSTWriter{}

// NoopFinishAbortWritable wraps an io.Writer to make a objstorage.Writable that
// will ignore Finish and Abort calls.
func NoopFinishAbortWritable(w io.Writer) objstorage.Writable {
	return &noopFinishAbort{Writer: w}
}

// noopFinishAbort is used to wrap io.Writers for sstable.Writer.
type noopFinishAbort struct {
	io.Writer
}

var _ objstorage.Writable = (*noopFinishAbort)(nil)

// Write is part of the objstorage.Writable interface.
func (n *noopFinishAbort) Write(p []byte) error {
	// An io.Writer always returns an error if it can't write the entire slice.
	_, err := n.Writer.Write(p)
	return err
}

// Finish is part of the objstorage.Writable interface.
func (*noopFinishAbort) Finish() error {
	return nil
}

// Abort is part of the objstorage.Writable interface.
func (*noopFinishAbort) Abort() {}

// MakeIngestionWriterOptions returns writer options suitable for writing SSTs
// that will subsequently be ingested (e.g. with AddSSTable). These options are
// also used when constructing sstables for backups (because these sstables may
// ultimately be ingested during online restore).
func MakeIngestionWriterOptions(ctx context.Context, cs *cluster.Settings) sstable.WriterOptions {
	// All supported versions understand TableFormatPebblev4. If columnar blocks
	// are enabled and the active cluster version is at least 24.3, use
	// TableFormatPebblev5.
	format := sstable.TableFormatPebblev4
	if ColumnarBlocksEnabled.Get(&cs.SV) {
		format = sstable.TableFormatPebblev5
	}

	opts := DefaultPebbleOptions().MakeWriterOptions(0, format)
	// By default, compress with the algorithm used for storage in a Pebble store.
	// There are other, more specific, use cases that may call for a different
	// algorithm, which can be set by overriding the default (see
	// MakeIngestionSSTWriterWithOverrides).
	opts.Compression = getCompressionAlgorithm(ctx, cs, CompressionAlgorithmStorage)
	opts.MergerName = "nullptr"
	if !IngestionValueBlocksEnabled.Get(&cs.SV) {
		opts.DisableValueBlocks = true
	}
	return opts
}

// makeSSTRewriteOptions should be used instead of MakeIngestionWriterOptions
// when we are going to rewrite ssts. It additionally returns the minimum
// table format that we accept, since sst rewriting will often preserve the
// input table format.
func makeSSTRewriteOptions(
	ctx context.Context, cs *cluster.Settings,
) (opts sstable.WriterOptions, minTableFormat sstable.TableFormat) {
	// v22.2 clusters use sstable.TableFormatPebblev2.
	return MakeIngestionWriterOptions(ctx, cs), sstable.TableFormatPebblev2
}

// MakeTransportSSTWriter creates a new SSTWriter tailored for sstables
// constructed exclusively for transport, which are typically only ever iterated
// in their entirety and not durably persisted. At the time of writing, this is
// used by export requests. During a backup, the export requests will construct
// sstables using this writer, those sstables will be sent over the network,
// scanned and their keys inserted into new sstables (NB: constructed using
// MakeIngestionSSTWriter) that ultimately are uploaded to object storage.
func MakeTransportSSTWriter(ctx context.Context, cs *cluster.Settings, f io.Writer) SSTWriter {
	// By default, take a conservative approach and assume we don't have newer
	// table features available. Upgrade to an appropriate version only if the
	// cluster supports it.
	format := sstable.TableFormatPebblev4
	if ColumnarBlocksEnabled.Get(&cs.SV) {
		format = sstable.TableFormatPebblev5
	}

	opts := DefaultPebbleOptions().MakeWriterOptions(0, format)

	// Don't need value blocks.
	opts.DisableValueBlocks = true
	// Don't need BlockPropertyCollectors for backups.
	opts.BlockPropertyCollectors = nil
	// Disable bloom filters since we only ever iterate backups.
	opts.FilterPolicy = nil
	// Bump up block size, since we almost never seek or do point lookups, so more
	// block checksums and more index entries are just overhead and smaller blocks
	// reduce compression ratio.
	opts.BlockSize = 128 << 10
	opts.Compression = getCompressionAlgorithm(ctx, cs, CompressionAlgorithmBackupTransport)
	opts.MergerName = "nullptr"
	return SSTWriter{
		fw: sstable.NewWriter(&noopFinishAbort{f}, opts),
	}
}

// MakeIngestionSSTWriter creates a new SSTWriter tailored for ingestion SSTs.
// These SSTs have bloom filters enabled (as set in DefaultPebbleOptions). If
// the cluster settings permit value blocks, the SST may contain value blocks.
// This writer is used when constructing sstables for backups too, because
// backup sstables may ultimately be ingested during online restore.
func MakeIngestionSSTWriter(
	ctx context.Context, cs *cluster.Settings, w objstorage.Writable,
) SSTWriter {
	return MakeIngestionSSTWriterWithOverrides(ctx, cs, w)
}

// SSTWriterOption augments one or more sstable.WriterOptions.
type SSTWriterOption func(opts *sstable.WriterOptions)

// WithValueBlocksDisabled disables the use of value blocks in an SSTable.
var WithValueBlocksDisabled SSTWriterOption = func(opts *sstable.WriterOptions) {
	opts.DisableValueBlocks = true
}

// WithCompressionFromClusterSetting sets the compression algorithm for an
// SSTable based on the value of the given cluster setting.
func WithCompressionFromClusterSetting(
	ctx context.Context, cs *cluster.Settings, setting *settings.EnumSetting[compressionAlgorithm],
) SSTWriterOption {
	return func(opts *sstable.WriterOptions) {
		opts.Compression = getCompressionAlgorithm(ctx, cs, setting)
	}
}

// MakeIngestionSSTWriterWithOverrides creates a new SSTWriter tailored for
// ingestion SSTs. Note that writer is used when constructing sstables for
// backups, because backup sstables may ultimately be ingested during online
// restore.
//
// These SSTs have bloom filters enabled (as set in DefaultPebbleOptions) and
// format set to the highest permissible by the cluster settings. Callers that
// expect to write huge SSTs, say 200+MB, which could contain multiple versions
// for the same key, should pass in a WithValueBlocksDisabled option. This is
// because value blocks are buffered in-memory while writing the SST (see
// https://github.com/cockroachdb/cockroach/issues/117113).
func MakeIngestionSSTWriterWithOverrides(
	ctx context.Context, cs *cluster.Settings, w objstorage.Writable, overrides ...SSTWriterOption,
) SSTWriter {
	opts := MakeIngestionWriterOptions(ctx, cs)
	for _, o := range overrides {
		o(&opts)
	}
	return SSTWriter{
		fw: sstable.NewWriter(w, opts),
	}
}

// Finish finalizes the writer and returns the constructed file's contents,
// since the last call to Truncate (if any). At least one kv entry must have been added.
func (fw *SSTWriter) Finish() error {
	if fw.fw == nil {
		return errors.New("cannot call Finish on a closed writer")
	}
	if err := fw.fw.Close(); err != nil {
		return err
	}
	var err error
	fw.Meta, err = fw.fw.Raw().Metadata()
	fw.fw = nil
	return err
}

// ClearRawRange implements the Engine interface.
func (fw *SSTWriter) ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	fw.scratch = EngineKey{Key: start}.EncodeToBuf(fw.scratch[:0])
	endRaw := EngineKey{Key: end}.Encode()
	if pointKeys {
		fw.DataSize += int64(len(start)) + int64(len(end))
		if err := fw.fw.DeleteRange(fw.scratch, endRaw); err != nil {
			return err
		}
	}
	if rangeKeys {
		fw.DataSize += int64(len(start)) + int64(len(end))
		if err := fw.fw.RangeKeyDelete(fw.scratch, endRaw); err != nil {
			return err
		}
	}
	return nil
}

// ClearMVCCRange implements the Writer interface.
func (fw *SSTWriter) ClearMVCCRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	panic("not implemented")
}

// ClearMVCCVersions implements the Writer interface.
func (fw *SSTWriter) ClearMVCCVersions(start, end MVCCKey) error {
	return fw.clearRange(start, end)
}

// PutMVCCRangeKey implements the Writer interface.
func (fw *SSTWriter) PutMVCCRangeKey(rangeKey MVCCRangeKey, value MVCCValue) error {
	// NB: all MVCC APIs currently assume all range keys are range tombstones.
	if !value.IsTombstone() {
		return errors.New("range keys can only be MVCC range tombstones")
	}
	valueRaw, err := EncodeMVCCValue(value)
	if err != nil {
		return errors.Wrapf(err, "failed to encode MVCC value for range key %s", rangeKey)
	}
	return fw.PutRawMVCCRangeKey(rangeKey, valueRaw)
}

// PutRawMVCCRangeKey implements the Writer interface.
func (fw *SSTWriter) PutRawMVCCRangeKey(rangeKey MVCCRangeKey, value []byte) error {
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	return fw.PutEngineRangeKey(
		rangeKey.StartKey, rangeKey.EndKey, EncodeMVCCTimestampSuffix(rangeKey.Timestamp), value)
}

// ClearMVCCRangeKey implements the Writer interface.
func (fw *SSTWriter) ClearMVCCRangeKey(rangeKey MVCCRangeKey) error {
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	// If the range key holds an encoded timestamp as it was read from storage,
	// write the tombstone to clear it using the same encoding of the timestamp.
	// See #129592.
	if len(rangeKey.EncodedTimestampSuffix) > 0 {
		return fw.ClearEngineRangeKey(rangeKey.StartKey, rangeKey.EndKey,
			rangeKey.EncodedTimestampSuffix)
	}
	return fw.ClearEngineRangeKey(rangeKey.StartKey, rangeKey.EndKey,
		EncodeMVCCTimestampSuffix(rangeKey.Timestamp))
}

// PutEngineRangeKey implements the Writer interface.
func (fw *SSTWriter) PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte) error {
	// MVCC values don't account for the timestamp, so we don't account
	// for the suffix here.
	fw.DataSize += int64(len(start)) + int64(len(end)) + int64(len(value))
	return fw.fw.RangeKeySet(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, value)
}

// ClearEngineRangeKey implements the Writer interface.
func (fw *SSTWriter) ClearEngineRangeKey(start, end roachpb.Key, suffix []byte) error {
	// MVCC values don't account for the timestamp, so we don't account for the
	// suffix here.
	fw.DataSize += int64(len(start)) + int64(len(end))
	return fw.fw.RangeKeyUnset(EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix)
}

// ClearEngineRange clears point keys in the specified EngineKey range.
func (fw *SSTWriter) ClearEngineRange(start, end EngineKey) error {
	fw.scratch = start.EncodeToBuf(fw.scratch[:0])
	endRaw := end.Encode()
	fw.DataSize += int64(len(start.Key)) + int64(len(end.Key))
	if err := fw.fw.DeleteRange(fw.scratch, endRaw); err != nil {
		return err
	}
	return nil
}

// ClearRawEncodedRange implements the InternalWriter interface.
func (fw *SSTWriter) ClearRawEncodedRange(start, end []byte) error {
	startEngine, ok := DecodeEngineKey(start)
	if !ok {
		return errors.New("cannot decode start engine key")
	}
	endEngine, ok := DecodeEngineKey(end)
	if !ok {
		return errors.New("cannot decode end engine key")
	}
	fw.DataSize += int64(len(startEngine.Key)) + int64(len(endEngine.Key))
	return fw.fw.DeleteRange(start, end)
}

// PutInternalRangeKey implements the InternalWriter interface.
func (fw *SSTWriter) PutInternalRangeKey(start, end []byte, key rangekey.Key) error {
	startEngine, ok := DecodeEngineKey(start)
	if !ok {
		return errors.New("cannot decode engine key")
	}
	endEngine, ok := DecodeEngineKey(end)
	if !ok {
		return errors.New("cannot decode engine key")
	}
	fw.DataSize += int64(len(startEngine.Key)) + int64(len(endEngine.Key)) + int64(len(key.Value))
	switch key.Kind() {
	case pebble.InternalKeyKindRangeKeyUnset:
		return fw.fw.RangeKeyUnset(start, end, key.Suffix)
	case pebble.InternalKeyKindRangeKeySet:
		return fw.fw.RangeKeySet(start, end, key.Suffix, key.Value)
	case pebble.InternalKeyKindRangeKeyDelete:
		return fw.fw.RangeKeyDelete(start, end)
	default:
		panic("unexpected range key kind")
	}
}

// PutInternalPointKey implements the InternalWriter interface.
func (fw *SSTWriter) PutInternalPointKey(key *pebble.InternalKey, value []byte) error {
	ek, ok := DecodeEngineKey(key.UserKey)
	if !ok {
		return errors.New("cannot decode engine key")
	}
	fw.DataSize += int64(len(ek.Key)) + int64(len(value))
	return fw.fw.Raw().AddWithForceObsolete(*key, value, false /* forceObsolete */)
}

// clearRange clears all point keys in the given range by dropping a Pebble
// range tombstone.
//
// NB: Does not clear range keys.
func (fw *SSTWriter) clearRange(start, end MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call ClearRange on a closed writer")
	}
	fw.DataSize += int64(len(start.Key)) + int64(len(end.Key))
	fw.scratch = EncodeMVCCKeyToBuf(fw.scratch[:0], start)
	return fw.fw.DeleteRange(fw.scratch, EncodeMVCCKey(end))
}

// Put puts a kv entry into the sstable being built. An error is returned if it
// is not greater than any previously added entry (according to the comparator
// configured during writer creation). `Close` cannot have been called.
//
// TODO(sumeer): Put has been removed from the Writer interface, but there
// are many callers of this SSTWriter method. Fix those callers and remove.
func (fw *SSTWriter) Put(key MVCCKey, value []byte) error {
	if fw.fw == nil {
		return errors.New("cannot call Put on a closed writer")
	}
	fw.DataSize += int64(len(key.Key)) + int64(len(value))
	fw.scratch = EncodeMVCCKeyToBuf(fw.scratch[:0], key)
	return fw.fw.Set(fw.scratch, value)
}

// PutMVCC implements the Writer interface.
// An error is returned if it is not greater than any previously added entry
// (according to the comparator configured during writer creation). `Close`
// cannot have been called.
func (fw *SSTWriter) PutMVCC(key MVCCKey, value MVCCValue) error {
	if key.Timestamp.IsEmpty() {
		panic("PutMVCC timestamp is empty")
	}
	encValue, err := EncodeMVCCValue(value)
	if err != nil {
		return err
	}
	return fw.put(key, encValue)
}

// PutRawMVCC implements the Writer interface.
// An error is returned if it is not greater than any previously added entry
// (according to the comparator configured during writer creation). `Close`
// cannot have been called.
func (fw *SSTWriter) PutRawMVCC(key MVCCKey, value []byte) error {
	if key.Timestamp.IsEmpty() {
		panic("PutRawMVCC timestamp is empty")
	}
	return fw.put(key, value)
}

// PutUnversioned implements the Writer interface.
// An error is returned if it is not greater than any previously added entry
// (according to the comparator configured during writer creation). `Close`
// cannot have been called.
func (fw *SSTWriter) PutUnversioned(key roachpb.Key, value []byte) error {
	return fw.put(MVCCKey{Key: key}, value)
}

// PutEngineKey implements the Writer interface.
// An error is returned if it is not greater than any previously added entry
// (according to the comparator configured during writer creation). `Close`
// cannot have been called.
func (fw *SSTWriter) PutEngineKey(key EngineKey, value []byte) error {
	if fw.fw == nil {
		return errors.New("cannot call Put on a closed writer")
	}
	fw.DataSize += int64(len(key.Key)) + int64(len(value))
	fw.scratch = key.EncodeToBuf(fw.scratch[:0])
	return fw.fw.Set(fw.scratch, value)
}

// put puts a kv entry into the sstable being built. An error is returned if it
// is not greater than any previously added entry (according to the comparator
// configured during writer creation). `Close` cannot have been called.
func (fw *SSTWriter) put(key MVCCKey, value []byte) error {
	if fw.fw == nil {
		return errors.New("cannot call Put on a closed writer")
	}
	fw.DataSize += int64(len(key.Key)) + int64(len(value))
	fw.scratch = EncodeMVCCKeyToBuf(fw.scratch[:0], key)
	return fw.fw.Set(fw.scratch, value)
}

// ApplyBatchRepr implements the Writer interface.
func (fw *SSTWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("unimplemented")
}

// ClearMVCC implements the Writer interface. An error is returned if it is
// not greater than any previous point key passed to this Writer (according to
// the comparator configured during writer creation). `Close` cannot have been
// called.
func (fw *SSTWriter) ClearMVCC(key MVCCKey, opts ClearOptions) error {
	if key.Timestamp.IsEmpty() {
		panic("ClearMVCC timestamp is empty")
	}
	return fw.clear(key, opts)
}

// ClearUnversioned implements the Writer interface. An error is returned if
// it is not greater than any previous point key passed to this Writer
// (according to the comparator configured during writer creation). `Close`
// cannot have been called.
func (fw *SSTWriter) ClearUnversioned(key roachpb.Key, opts ClearOptions) error {
	return fw.clear(MVCCKey{Key: key}, opts)
}

// ClearEngineKey implements the Writer interface. An error is returned if it is
// not greater than any previous point key passed to this Writer (according to
// the comparator configured during writer creation). `Close` cannot have been
// called.
func (fw *SSTWriter) ClearEngineKey(key EngineKey, opts ClearOptions) error {
	if fw.fw == nil {
		return errors.New("cannot call Clear on a closed writer")
	}
	fw.scratch = key.EncodeToBuf(fw.scratch[:0])
	fw.DataSize += int64(len(key.Key))
	// TODO(jackson): We could use opts.ValueSize if known, but it would require
	// additional logic around ensuring the cluster version is at least
	// V23_2_UseSizedPebblePointTombstones. It's probably not worth it until we
	// can unconditionally use it; I don't believe we ever write point
	// tombstones to sstables constructed within Cockroach.
	return fw.fw.Delete(fw.scratch)
}

// An error is returned if it is not greater than any previous point key
// passed to this Writer (according to the comparator configured during writer
// creation). `Close` cannot have been called.
func (fw *SSTWriter) clear(key MVCCKey, opts ClearOptions) error {
	if fw.fw == nil {
		return errors.New("cannot call Clear on a closed writer")
	}
	fw.scratch = EncodeMVCCKeyToBuf(fw.scratch[:0], key)
	fw.DataSize += int64(len(key.Key))
	// TODO(jackson): We could use opts.ValueSize if known, but it would require
	// additional logic around ensuring the cluster version is at least
	// V23_2_UseSizedPebblePointTombstones. It's probably not worth it until we
	// can unconditionally use it; I don't believe we ever write point
	// tombstones to sstables constructed within Cockroach.
	return fw.fw.Delete(fw.scratch)
}

// SingleClearEngineKey implements the Writer interface.
func (fw *SSTWriter) SingleClearEngineKey(key EngineKey) error {
	panic("unimplemented")
}

// ClearMVCCIteratorRange implements the Writer interface.
func (fw *SSTWriter) ClearMVCCIteratorRange(_, _ roachpb.Key, _, _ bool) error {
	panic("not implemented")
}

// Merge implements the Writer interface.
func (fw *SSTWriter) Merge(key MVCCKey, value []byte) error {
	if fw.fw == nil {
		return errors.New("cannot call Merge on a closed writer")
	}
	fw.DataSize += int64(len(key.Key)) + int64(len(value))
	fw.scratch = EncodeMVCCKeyToBuf(fw.scratch[:0], key)
	return fw.fw.Merge(fw.scratch, value)
}

// LogData implements the Writer interface.
func (fw *SSTWriter) LogData(data []byte) error {
	// No-op.
	return nil
}

// LogLogicalOp implements the Writer interface.
func (fw *SSTWriter) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op.
}

// Close finishes and frees memory and other resources. Close is idempotent.
func (fw *SSTWriter) Close() {
	if fw.fw == nil {
		return
	}
	// pebble.Writer *does* return interesting errors from Close... but normally
	// we already called its Close() in Finish() and we no-op here. Thus the only
	// time we expect to be here is in a deferred Close(), in which case the caller
	// probably is already returning some other error, so returning one from this
	// method just makes for messy defers.
	_ = fw.fw.Close()
	fw.fw = nil
}

// ShouldWriteLocalTimestamps implements the Writer interface.
func (fw *SSTWriter) ShouldWriteLocalTimestamps(context.Context) bool {
	return false
}

// BufferedSize implements the Writer interface.
func (fw *SSTWriter) BufferedSize() int {
	return 0
}

// EstimatedSize returns the underlying RawWriter's estimated size. Note that
// this size is an estimate as if the writer were to be closed at the time of
// calling.
func (fw *SSTWriter) EstimatedSize() uint64 {
	return fw.fw.Raw().EstimatedSize()
}

// MemObject is an in-memory implementation of objstorage.Writable, intended
// use with SSTWriter.
type MemObject struct {
	bytes.Buffer
}

var _ objstorage.Writable = (*MemObject)(nil)

// Write is part of the objstorage.Writable interface.
func (f *MemObject) Write(p []byte) error {
	_, err := f.Buffer.Write(p)
	return err
}

// Finish is part of the objstorage.Writable interface.
func (*MemObject) Finish() error {
	return nil
}

// Abort is part of the objstorage.Writable interface.
func (*MemObject) Abort() {}

// Close implements the writeCloseSyncer interface.
func (*MemObject) Close() error {
	return nil
}

// Data returns the in-memory buffer behind this MemObject.
func (f *MemObject) Data() []byte {
	return f.Bytes()
}
