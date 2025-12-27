// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage"
)

// mergeWriter is an interface for writing merged key-value pairs.
// Implementations can either write to SST files or directly ingest via KV.
type mergeWriter interface {
	// Add writes a key-value pair. Returns true if the writer wants to
	// complete the current output unit (e.g., due to size threshold).
	Add(ctx context.Context, key storage.MVCCKey, val []byte) (shouldSplit bool, err error)

	// Complete finishes the current output unit and prepares for the next.
	// The endKey is the exclusive upper bound for the current unit.
	// Returns metadata about what was completed.
	Complete(ctx context.Context, endKey roachpb.Key) (execinfrapb.BulkMergeSpec_SST, error)

	// Finish completes all writes and returns final output.
	// The endKey is the exclusive upper bound for the final unit.
	Finish(ctx context.Context, endKey roachpb.Key) (execinfrapb.BulkMergeSpec_Output, error)

	// Close releases any resources held by the writer.
	Close(ctx context.Context)
}

// externalStorageWriter writes merged data to SST files in external storage,
// splitting into multiple files when the size threshold is exceeded.
type externalStorageWriter struct {
	settings       *cluster.Settings
	allocator      bulksst.FileAllocator
	targetSize     int64
	writer         storage.SSTWriter
	currentFile    objstorage.Writable
	currentFileURI string
	firstKey       roachpb.Key
	currentSize    int64
	output         execinfrapb.BulkMergeSpec_Output
}

var _ mergeWriter = &externalStorageWriter{}

func newExternalStorageWriter(
	ctx context.Context,
	settings *cluster.Settings,
	allocator bulksst.FileAllocator,
	targetSize int64,
) (*externalStorageWriter, error) {
	w := &externalStorageWriter{
		settings:   settings,
		allocator:  allocator,
		targetSize: targetSize,
	}

	// Create the first file.
	if err := w.createNewFile(ctx); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *externalStorageWriter) createNewFile(ctx context.Context) error {
	var err error
	w.currentFile, w.currentFileURI, err = w.allocator.AddFile(ctx)
	if err != nil {
		return err
	}
	w.writer = storage.MakeIngestionSSTWriter(ctx, w.settings, w.currentFile)
	return nil
}

// Add implements the mergeWriter interface.
func (w *externalStorageWriter) Add(
	ctx context.Context, key storage.MVCCKey, val []byte,
) (bool, error) {
	if w.firstKey == nil {
		w.firstKey = key.Key.Clone()
	}

	if err := w.writer.PutRawMVCC(key, val); err != nil {
		return false, err
	}
	w.currentSize += int64(len(key.Key) + len(val))

	// Signal that we should split if we've exceeded the target size.
	return w.currentSize >= w.targetSize, nil
}

// Complete implements the mergeWriter interface.
func (w *externalStorageWriter) Complete(
	ctx context.Context, endKey roachpb.Key,
) (execinfrapb.BulkMergeSpec_SST, error) {
	sst := execinfrapb.BulkMergeSpec_SST{
		StartKey: w.firstKey,
		EndKey:   endKey,
		URI:      w.currentFileURI,
	}
	w.output.SSTs = append(w.output.SSTs, sst)

	if err := w.writer.Finish(); err != nil {
		return execinfrapb.BulkMergeSpec_SST{}, err
	}

	// Finish closed currentFile. Create a new file for the next batch.
	if err := w.createNewFile(ctx); err != nil {
		return execinfrapb.BulkMergeSpec_SST{}, err
	}

	// Next SST starts where this one ended to maintain contiguous
	// non-overlapping ranges.
	w.firstKey = endKey
	w.currentSize = 0

	return sst, nil
}

// Finish implements the mergeWriter interface.
func (w *externalStorageWriter) Finish(
	ctx context.Context, endKey roachpb.Key,
) (execinfrapb.BulkMergeSpec_Output, error) {
	if err := w.writer.Finish(); err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}

	if w.currentSize > 0 {
		// Iterator finished with some SST data written.
		w.output.SSTs = append(w.output.SSTs, execinfrapb.BulkMergeSpec_SST{
			StartKey: w.firstKey,
			EndKey:   endKey,
			URI:      w.currentFileURI,
		})
	}

	return w.output, nil
}

// Close implements the mergeWriter interface.
func (w *externalStorageWriter) Close(ctx context.Context) {
	w.writer.Close()
}

// kvStorageWriter writes merged data directly to the KV layer using a
// SSTBatcher. It does not split the data into multiple files.
type kvStorageWriter struct {
	batcher *bulk.SSTBatcher
	writeTS hlc.Timestamp
}

var _ mergeWriter = &kvStorageWriter{}

func newKVStorageWriter(batcher *bulk.SSTBatcher, writeTS hlc.Timestamp) *kvStorageWriter {
	return &kvStorageWriter{
		batcher: batcher,
		writeTS: writeTS,
	}
}

// Add implements the mergeWriter interface.
func (w *kvStorageWriter) Add(ctx context.Context, key storage.MVCCKey, val []byte) (bool, error) {
	// Write the user key with the writer's timestamp, replacing the incoming
	// timestamp. This rewrites all merged data to a single MVCC timestamp for
	// the final ingestion. The data is already sorted, so SSTBatcher can write
	// directly without sorting.
	key.Timestamp = w.writeTS
	if err := w.batcher.AddMVCCKey(ctx, key, val); err != nil {
		return false, err
	}
	// Never signal to split - KV storage writer doesn't split.
	return false, nil
}

// Complete implements the mergeWriter interface.
func (w *kvStorageWriter) Complete(
	ctx context.Context, endKey roachpb.Key,
) (execinfrapb.BulkMergeSpec_SST, error) {
	// KV storage writer doesn't split, so this isn't implemented.
	return execinfrapb.BulkMergeSpec_SST{}, errors.AssertionFailedf("not implemented")
}

// Finish implements the mergeWriter interface.
func (w *kvStorageWriter) Finish(
	ctx context.Context, endKey roachpb.Key,
) (execinfrapb.BulkMergeSpec_Output, error) {
	if err := w.batcher.Flush(ctx); err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}
	// KV storage writer returns empty output.
	return execinfrapb.BulkMergeSpec_Output{}, nil
}

// Close implements the mergeWriter interface.
func (w *kvStorageWriter) Close(ctx context.Context) {
	if w.batcher != nil {
		w.batcher.Close(ctx)
	}
}
