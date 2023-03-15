// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
)

// SSTWriter writes SSTables.
type SSTWriter struct {
	fw *sstable.Writer
	// DataSize tracks the total key and value bytes added so far.
	DataSize int64
	scratch  []byte

	supportsRangeKeys bool // TODO(erikgrinaker): remove after 22.2
}

var _ Writer = &SSTWriter{}
var _ ExportWriter = &SSTWriter{}

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
// that will subsequently be ingested (e.g. with AddSSTable).
func MakeIngestionWriterOptions(ctx context.Context, cs *cluster.Settings) sstable.WriterOptions {
	// By default, take a conservative approach and assume we don't have newer
	// table features available. Upgrade to an appropriate version only if the
	// cluster supports it.
	format := sstable.TableFormatPebblev2
	if cs.Version.IsActive(ctx, clusterversion.V23_1EnablePebbleFormatSSTableValueBlocks) &&
		ValueBlocksEnabled.Get(&cs.SV) {
		format = sstable.TableFormatPebblev3
	}
	opts := DefaultPebbleOptions().MakeWriterOptions(0, format)
	opts.MergerName = "nullptr"
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

// MakeBackupSSTWriter creates a new SSTWriter tailored for backup SSTs which
// are typically only ever iterated in their entirety.
func MakeBackupSSTWriter(ctx context.Context, cs *cluster.Settings, f io.Writer) SSTWriter {
	// By default, take a conservative approach and assume we don't have newer
	// table features available. Upgrade to an appropriate version only if the
	// cluster supports it.
	format := sstable.TableFormatPebblev2

	// TODO(sumeer): add code to use TableFormatPebblev3 after confirming that
	// we won't run afoul of any stale tooling that reads backup ssts.
	opts := DefaultPebbleOptions().MakeWriterOptions(0, format)

	// Don't need BlockPropertyCollectors for backups.
	opts.BlockPropertyCollectors = nil
	// Disable bloom filters since we only ever iterate backups.
	opts.FilterPolicy = nil
	// Bump up block size, since we almost never seek or do point lookups, so more
	// block checksums and more index entries are just overhead and smaller blocks
	// reduce compression ratio.
	opts.BlockSize = 128 << 10
	opts.MergerName = "nullptr"
	return SSTWriter{
		fw:                sstable.NewWriter(&noopFinishAbort{f}, opts),
		supportsRangeKeys: opts.TableFormat >= sstable.TableFormatPebblev2,
	}
}

// MakeIngestionSSTWriter creates a new SSTWriter tailored for ingestion SSTs.
// These SSTs have bloom filters enabled (as set in DefaultPebbleOptions) and
// format set to RocksDBv2.
func MakeIngestionSSTWriter(
	ctx context.Context, cs *cluster.Settings, w objstorage.Writable,
) SSTWriter {
	opts := MakeIngestionWriterOptions(ctx, cs)
	return SSTWriter{
		fw:                sstable.NewWriter(w, opts),
		supportsRangeKeys: opts.TableFormat >= sstable.TableFormatPebblev2,
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
	fw.fw = nil
	return nil
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
	if rangeKeys && fw.supportsRangeKeys {
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
	if !fw.supportsRangeKeys {
		return nil // noop
	}
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	return fw.ClearEngineRangeKey(rangeKey.StartKey, rangeKey.EndKey,
		EncodeMVCCTimestampSuffix(rangeKey.Timestamp))
}

// PutEngineRangeKey implements the Writer interface.
func (fw *SSTWriter) PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte) error {
	if !fw.supportsRangeKeys {
		return errors.New("range keys not supported by SST writer")
	}
	// MVCC values don't account for the timestamp, so we don't account
	// for the suffix here.
	fw.DataSize += int64(len(start)) + int64(len(end)) + int64(len(value))
	return fw.fw.RangeKeySet(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, value)
}

// ClearEngineRangeKey implements the Writer interface.
func (fw *SSTWriter) ClearEngineRangeKey(start, end roachpb.Key, suffix []byte) error {
	if !fw.supportsRangeKeys {
		return nil // noop
	}
	// MVCC values don't account for the timestamp, so we don't account for the
	// suffix here.
	fw.DataSize += int64(len(start)) + int64(len(end))
	return fw.fw.RangeKeyUnset(EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix)
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

// PutIntent implements the Writer interface.
// An error is returned if it is not greater than any previously added entry
// (according to the comparator configured during writer creation). `Close`
// cannot have been called.
func (fw *SSTWriter) PutIntent(
	ctx context.Context, key roachpb.Key, value []byte, txnUUID uuid.UUID,
) error {
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
func (fw *SSTWriter) ClearMVCC(key MVCCKey) error {
	if key.Timestamp.IsEmpty() {
		panic("ClearMVCC timestamp is empty")
	}
	return fw.clear(key)
}

// ClearUnversioned implements the Writer interface. An error is returned if
// it is not greater than any previous point key passed to this Writer
// (according to the comparator configured during writer creation). `Close`
// cannot have been called.
func (fw *SSTWriter) ClearUnversioned(key roachpb.Key) error {
	return fw.clear(MVCCKey{Key: key})
}

// ClearIntent implements the Writer interface. An error is returned if it is
// not greater than any previous point key passed to this Writer (according to
// the comparator configured during writer creation). `Close` cannot have been
// called.
func (fw *SSTWriter) ClearIntent(
	key roachpb.Key, txnDidNotUpdateMeta bool, txnUUID uuid.UUID,
) error {
	panic("ClearIntent is unsupported")
}

// ClearEngineKey implements the Writer interface. An error is returned if it is
// not greater than any previous point key passed to this Writer (according to
// the comparator configured during writer creation). `Close` cannot have been
// called.
func (fw *SSTWriter) ClearEngineKey(key EngineKey) error {
	if fw.fw == nil {
		return errors.New("cannot call Clear on a closed writer")
	}
	fw.scratch = key.EncodeToBuf(fw.scratch[:0])
	fw.DataSize += int64(len(key.Key))
	return fw.fw.Delete(fw.scratch)
}

// An error is returned if it is not greater than any previous point key
// passed to this Writer (according to the comparator configured during writer
// creation). `Close` cannot have been called.
func (fw *SSTWriter) clear(key MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call Clear on a closed writer")
	}
	fw.scratch = EncodeMVCCKeyToBuf(fw.scratch[:0], key)
	fw.DataSize += int64(len(key.Key))
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
