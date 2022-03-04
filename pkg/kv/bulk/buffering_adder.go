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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// BufferingAdder is a wrapper for an SSTBatcher that allows out-of-order calls
// to Add, buffering them up and then sorting them before then passing them in
// order into an SSTBatcher
type BufferingAdder struct {
	sink SSTBatcher
	// timestamp applied to mvcc keys created from keys during SST construction.
	timestamp hlc.Timestamp

	// threshold at which buffered entries will be flushed to SSTBatcher.
	curBufferSize int64

	// ceiling till which we can grow curBufferSize if bulkMon permits.
	maxBufferSize func() int64

	// unit by which we increment the curBufferSize.
	incrementBufferSize int64

	// currently buffered kvs.
	curBuf kvBuf

	sorted bool

	initialSplits int

	flushCounts struct {
		total      int
		bufferSize int
		totalSort  time.Duration
		totalFlush time.Duration
	}

	// name of the BufferingAdder for the purpose of logging only.
	name string

	bulkMon *mon.BytesMonitor
	memAcc  mon.BoundAccount

	onFlush func(summary roachpb.BulkOpSummary)
}

var _ kvserverbase.BulkAdder = &BufferingAdder{}

// MakeBulkAdder makes a kvserverbase.BulkAdder that buffers and sorts K/Vs
// passed to add into SSTs that are then ingested. rangeCache if set is
// consulted to avoid generating an SST that will span a range boundary and thus
// encounter an error and need to be split and retired to be applied.
func MakeBulkAdder(
	ctx context.Context,
	db *kv.DB,
	rangeCache *rangecache.RangeCache,
	settings *cluster.Settings,
	timestamp hlc.Timestamp,
	opts kvserverbase.BulkAdderOptions,
	bulkMon *mon.BytesMonitor,
) (*BufferingAdder, error) {
	if opts.MinBufferSize == 0 {
		opts.MinBufferSize = 32 << 20
	}
	if opts.MaxBufferSize == nil {
		opts.MaxBufferSize = func() int64 { return 128 << 20 }
	}
	if opts.StepBufferSize == 0 {
		opts.StepBufferSize = 32 << 20
	}

	b := &BufferingAdder{
		name: opts.Name,
		sink: SSTBatcher{
			db:                     db,
			rc:                     rangeCache,
			settings:               settings,
			skipDuplicates:         opts.SkipDuplicates,
			disallowShadowingBelow: opts.DisallowShadowingBelow,
			batchTS:                opts.BatchTimestamp,
			writeAtBatchTS:         opts.WriteAtBatchTimestamp,
		},
		timestamp:           timestamp,
		curBufferSize:       opts.MinBufferSize,
		maxBufferSize:       opts.MaxBufferSize,
		incrementBufferSize: opts.StepBufferSize,
		bulkMon:             bulkMon,
		sorted:              true,
		initialSplits:       opts.InitialSplitsIfUnordered,
	}

	// If no monitor is attached to the instance of a bulk adder, we do not
	// control its memory usage.
	if bulkMon == nil {
		return b, nil
	}

	// At minimum a bulk adder needs enough space to store a buffer of
	// curBufferSize, and a subsequent SST of SSTSize in-memory. If the memory
	// account is unable to reserve this minimum threshold we cannot continue.
	//
	// TODO(adityamaru): IMPORT should also reserve memory for a single SST which
	// it will store in-memory before sending it to RocksDB.
	b.memAcc = bulkMon.MakeBoundAccount()
	if err := b.memAcc.Grow(ctx, b.curBufferSize); err != nil {
		return nil, errors.WithHint(
			errors.Wrap(err, "not enough memory available to create a BulkAdder"),
			"Try setting a higher --max-sql-memory.")
	}

	return b, nil
}

// SetOnFlush sets a callback to run after the buffering adder flushes.
func (b *BufferingAdder) SetOnFlush(fn func(summary roachpb.BulkOpSummary)) {
	b.onFlush = fn
}

// Close closes the underlying SST builder.
func (b *BufferingAdder) Close(ctx context.Context) {
	log.VEventf(ctx, 2,
		"bulk adder %s ingested %s, spent %v sorting and %v flushing (%v sending, %v splitting, %v scattering %v). Flushed %d due to buffer (%s) size. Flushed chunked as %d files (%d after split-retries), %d due to ranges, %d due to sst size.",
		b.name,
		sz(b.sink.totalRows.DataSize),
		b.flushCounts.totalFlush,
		b.flushCounts.totalSort,
		b.sink.flushCounts.sendWait,
		b.sink.flushCounts.splitWait,
		b.sink.flushCounts.scatterWait,
		sz(b.sink.flushCounts.scatterMoved),
		b.flushCounts.bufferSize,
		sz(b.memAcc.Used()),
		b.sink.flushCounts.total, b.sink.flushCounts.files,
		b.sink.flushCounts.split, b.sink.flushCounts.sstSize,
	)
	b.sink.Close()

	if b.bulkMon != nil {
		b.memAcc.Close(ctx)
		b.bulkMon.Stop(ctx)
	}
}

// Add adds a key to the buffer and checks if it needs to flush.
func (b *BufferingAdder) Add(ctx context.Context, key roachpb.Key, value []byte) error {
	if b.sorted {
		if l := len(b.curBuf.entries); l > 0 && key.Compare(b.curBuf.Key(l-1)) < 0 {
			b.sorted = false
		}
	}
	if err := b.curBuf.append(key, value); err != nil {
		return err
	}

	if b.curBuf.MemSize > int(b.curBufferSize) {
		// This is an optimization to try and increase the current buffer size if
		// our memory account permits it. This would lead to creation of a fewer
		// number of SSTs.
		//
		// To prevent a single import from growing its buffer indefinitely we check
		// if it has exceeded its upper bound.
		if b.bulkMon != nil && b.curBufferSize < b.maxBufferSize() {
			if err := b.memAcc.Grow(ctx, b.incrementBufferSize); err != nil {
				// If we are unable to reserve the additional memory then flush the
				// buffer, and continue as normal.
				return b.sizeFlush(ctx)
			}
			b.curBufferSize += b.incrementBufferSize
		} else {
			return b.sizeFlush(ctx)
		}
	}
	return nil
}

// CurrentBufferFill returns the current buffer fill percentage.
func (b *BufferingAdder) CurrentBufferFill() float32 {
	return float32(b.curBuf.MemSize) / float32(b.curBufferSize)
}

// IsEmpty returns true if the adder has no un-flushed data in its buffer.
func (b *BufferingAdder) IsEmpty() bool {
	return b.curBuf.Len() == 0
}

func (b *BufferingAdder) sizeFlush(ctx context.Context) error {
	b.flushCounts.bufferSize++
	log.VEventf(ctx, 3, "buffer size triggering flush of %s buffer", sz(b.curBuf.MemSize))
	return b.doFlush(ctx, true)
}

// Flush flushes any buffered kvs to the batcher.
func (b *BufferingAdder) Flush(ctx context.Context) error {
	return b.doFlush(ctx, false)
}

func (b *BufferingAdder) doFlush(ctx context.Context, forSize bool) error {
	if b.curBuf.Len() == 0 {
		if b.onFlush != nil {
			b.onFlush(b.sink.GetBatchSummary())
		}
		return nil
	}
	if err := b.sink.Reset(ctx); err != nil {
		return err
	}
	b.flushCounts.total++

	before := b.sink.flushCounts
	beforeSize := b.sink.totalRows.DataSize

	beforeSort := timeutil.Now()

	if !b.sorted {
		sort.Sort(&b.curBuf)
	}
	mvccKey := storage.MVCCKey{Timestamp: b.timestamp}

	beforeFlush := timeutil.Now()
	b.flushCounts.totalSort += beforeFlush.Sub(beforeSort)

	// If this is the first flush and is due to size, if it was unsorted then
	// create initial splits if requested before flushing.
	if b.flushCounts.total == 1 && forSize && b.initialSplits != 0 && !b.sorted {
		if err := b.createInitialSplits(ctx); err != nil {
			return err
		}
	}

	for i := range b.curBuf.entries {
		mvccKey.Key = b.curBuf.Key(i)
		if err := b.sink.AddMVCCKey(ctx, mvccKey, b.curBuf.Value(i)); err != nil {
			return err
		}
	}
	if err := b.sink.Flush(ctx); err != nil {
		return err
	}
	b.flushCounts.totalFlush += timeutil.Since(beforeFlush)

	if log.V(3) {
		written := b.sink.totalRows.DataSize - beforeSize
		files := b.sink.flushCounts.total - before.total
		dueToSplits := b.sink.flushCounts.split - before.split
		dueToSize := b.sink.flushCounts.sstSize - before.sstSize

		log.Infof(ctx,
			"flushing %s buffer wrote %d SSTs (avg: %s) with %d for splits, %d for size, took %v",
			sz(b.curBuf.MemSize), files, sz(written/int64(files)), dueToSplits, dueToSize, timeutil.Since(beforeSort),
		)
	}
	if log.V(4) {
		log.Infof(ctx,
			"bulk adder %s has ingested %s, spent %v sorting and %v flushing (%v sending, %v splitting, %v scattering %v). Flushed %d times due to buffer (%s) size. Flushed chunked as %d files (%d after split-retries), %d due to ranges, %d due to sst size.",
			b.name,
			sz(b.sink.totalRows.DataSize),
			b.flushCounts.totalSort,
			b.flushCounts.totalFlush,
			b.sink.flushCounts.sendWait,
			b.sink.flushCounts.splitWait,
			b.sink.flushCounts.scatterWait,
			sz(b.sink.flushCounts.scatterMoved),
			b.flushCounts.bufferSize,
			sz(b.memAcc.Used()),
			b.sink.flushCounts.total, b.sink.flushCounts.files,
			b.sink.flushCounts.split, b.sink.flushCounts.sstSize,
		)
	}
	if b.onFlush != nil {
		b.onFlush(b.sink.GetBatchSummary())
	}
	b.curBuf.Reset()
	return nil
}

func (b *BufferingAdder) createInitialSplits(ctx context.Context) error {
	targetSize := b.curBuf.Len() / b.initialSplits
	log.Infof(ctx, "%s creating up to %d initial splits from %d keys in %s buffer", b.name, b.initialSplits, b.curBuf.Len(), sz(b.curBuf.MemSize))

	hour := hlc.Timestamp{WallTime: timeutil.Now().Add(time.Hour).UnixNano()}

	before := timeutil.Now()

	created := 0
	for i := targetSize; i < b.curBuf.Len(); i += targetSize {
		k := b.curBuf.Key(i)
		prev := b.curBuf.Key(i - targetSize)
		log.VEventf(ctx, 1, "splitting at key %d / %d: %s", i, b.curBuf.Len(), k)
		if _, err := b.sink.db.SplitAndScatter(ctx, k, hour, prev); err != nil {
			// TODO(dt): a typed error would be nice here.
			if strings.Contains(err.Error(), "predicate") {
				log.VEventf(ctx, 1, "split at %s rejected, had previously split and no longer included %s", k, prev)
				continue
			}
			return err
		}
		created++
	}
	log.Infof(ctx, "%s created %d initial splits in %v from %d keys in %s buffer",
		b.name, created, timeutil.Since(before), b.curBuf.Len(), sz(b.curBuf.MemSize))

	b.sink.initialSplitDone = true
	return nil
}

// GetSummary returns this batcher's total added rows/bytes/etc.
func (b *BufferingAdder) GetSummary() roachpb.BulkOpSummary {
	return b.sink.GetSummary()
}
