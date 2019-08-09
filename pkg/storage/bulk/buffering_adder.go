// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// BufferingAdder is a wrapper for an SSTBatcher that allows out-of-order calls
// to Add, buffering them up and then sorting them before then passing them in
// order into an SSTBatcher
type BufferingAdder struct {
	sink SSTBatcher
	// timestamp applied to mvcc keys created from keys during SST construction.
	timestamp hlc.Timestamp

	// threshold at which buffered entries will be flushed to SSTBatcher.
	flushSize int

	// currently buffered kvs.
	curBuf kvBuf

	flushCounts struct {
		total      int
		bufferSize int
	}

	// name of the BufferingAdder for the purpose of logging only.
	name string
}

// MakeBulkAdder makes a storagebase.BulkAdder that buffers and sorts K/Vs
// passed to add into SSTs that are then ingested. rangeCache if set is
// consulted to avoid generating an SST that will span a range boundary and thus
// encounter an error and need to be split and retired to be applied.
func MakeBulkAdder(
	db sender,
	rangeCache *kv.RangeDescriptorCache,
	timestamp hlc.Timestamp,
	opts storagebase.BulkAdderOptions,
) (*BufferingAdder, error) {
	if opts.BufferSize == 0 {
		opts.BufferSize = 32 << 20
	}
	if opts.SSTSize == 0 {
		opts.SSTSize = 32 << 20
	}
	b := &BufferingAdder{
		name: opts.Name,
		sink: SSTBatcher{
			db:                db,
			maxSize:           opts.SSTSize,
			rc:                rangeCache,
			skipDuplicates:    opts.SkipDuplicates,
			disallowShadowing: opts.DisallowShadowing,
		},
		timestamp: timestamp,
		flushSize: int(opts.BufferSize),
	}
	return b, nil
}

// Close closes the underlying SST builder.
func (b *BufferingAdder) Close(ctx context.Context) {
	log.VEventf(ctx, 2,
		"bulk adder %s ingested %s, flushed %d times, %d due to buffer size. Flushed %d files, %d due to ranges, %d due to sst size",
		b.name,
		sz(b.sink.totalRows.DataSize),
		b.flushCounts.total, b.flushCounts.bufferSize,
		b.sink.flushCounts.total, b.sink.flushCounts.split, b.sink.flushCounts.sstSize,
	)
	b.sink.Close()
}

// Add adds a key to the buffer and checks if it needs to flush.
func (b *BufferingAdder) Add(ctx context.Context, key roachpb.Key, value []byte) error {
	if err := b.curBuf.append(key, value); err != nil {
		return err
	}

	if b.curBuf.MemSize > b.flushSize {
		b.flushCounts.bufferSize++
		log.VEventf(ctx, 3, "buffer size triggering flush of %s buffer", sz(b.curBuf.MemSize))
		return b.Flush(ctx)
	}
	return nil
}

// CurrentBufferFill returns the current buffer fill percentage.
func (b *BufferingAdder) CurrentBufferFill() float32 {
	return float32(b.curBuf.MemSize) / float32(b.flushSize)
}

// Flush flushes any buffered kvs to the batcher.
func (b *BufferingAdder) Flush(ctx context.Context) error {
	if b.curBuf.Len() == 0 {
		return nil
	}
	if err := b.sink.Reset(); err != nil {
		return err
	}
	b.flushCounts.total++

	before := b.sink.flushCounts
	beforeSize := b.sink.totalRows.DataSize

	sort.Sort(&b.curBuf)
	mvccKey := engine.MVCCKey{Timestamp: b.timestamp}

	for i := range b.curBuf.entries {
		mvccKey.Key = b.curBuf.Key(i)
		if err := b.sink.AddMVCCKey(ctx, mvccKey, b.curBuf.Value(i)); err != nil {
			return err
		}
	}
	if err := b.sink.Flush(ctx); err != nil {
		return err
	}

	if log.V(3) {
		written := b.sink.totalRows.DataSize - beforeSize
		files := b.sink.flushCounts.total - before.total
		dueToSplits := b.sink.flushCounts.split - before.split
		dueToSize := b.sink.flushCounts.sstSize - before.sstSize

		log.Infof(ctx,
			"flushing %s buffer wrote %d SSTs (avg: %s) with %d for splits, %d for size",
			sz(b.curBuf.MemSize), files, sz(written/int64(files)), dueToSplits, dueToSize,
		)
	}

	b.curBuf.Reset()
	return nil
}

// GetSummary returns this batcher's total added rows/bytes/etc.
func (b *BufferingAdder) GetSummary() roachpb.BulkOpSummary {
	return b.sink.GetSummary()
}
