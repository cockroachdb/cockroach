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

	"github.com/cockroachdb/cockroach/pkg/keys"
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
	kvSize sz

	sorted bool

	initialSplits int

	lastFlush   time.Time
	flushCounts struct {
		total        int
		bufferSize   int
		totalSort    time.Duration
		totalFlush   time.Duration
		totalFilling time.Duration
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
		lastFlush:           timeutil.Now(),
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
	log.VEventf(ctx, 1,
		"%s adder ingested %s (%s); spent %s filling, %v sorting, %v flushing (%v sink, %v sending, %v splitting, %v scattering %v)",
		b.name,
		sz(b.sink.totalRows.DataSize),
		sorted(b.sorted),
		timing(b.flushCounts.totalFilling),
		timing(b.flushCounts.totalSort),
		timing(b.flushCounts.totalFlush),
		timing(b.sink.flushCounts.flushWait),
		timing(b.sink.flushCounts.sendWait),
		timing(b.sink.flushCounts.splitWait),
		timing(b.sink.flushCounts.scatterWait),
		b.sink.flushCounts.scatterMoved,
	)
	log.VEventf(ctx, 2, "%s adder flushed %d times, %d due to buffer size (%s); flushing chunked into %d files (%d for ranges, %d for sst size, +%d after split-retries)",
		b.name,
		b.flushCounts.total,
		b.flushCounts.bufferSize,
		sz(b.memAcc.Used()),
		b.sink.flushCounts.total,
		b.sink.flushCounts.split,
		b.sink.flushCounts.sstSize,
		b.sink.flushCounts.files-b.sink.flushCounts.total,
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
	b.kvSize += sz(len(key) + len(value))

	if b.curBuf.MemSize > sz(b.curBufferSize) {
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

func (b *BufferingAdder) bufferedKeys() int {
	return len(b.curBuf.entries)
}

func (b *BufferingAdder) bufferedMemSize() sz {
	return b.curBuf.MemSize
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
	log.VEventf(ctx, 2, "%s adder triggering flush of %s of KVs in %s buffer",
		b.name, b.kvSize, b.bufferedMemSize())
	return b.doFlush(ctx, true)
}

// Flush flushes any buffered kvs to the batcher.
func (b *BufferingAdder) Flush(ctx context.Context) error {
	return b.doFlush(ctx, false)
}

func (b *BufferingAdder) doFlush(ctx context.Context, forSize bool) error {
	b.flushCounts.totalFilling += timeutil.Since(b.lastFlush)

	if b.bufferedKeys() == 0 {
		if b.onFlush != nil {
			b.onFlush(b.sink.GetBatchSummary())
		}
		b.lastFlush = timeutil.Now()
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
	if b.initialSplits > 0 {
		if forSize && !b.sorted {
			if err := b.createInitialSplits(ctx); err != nil {
				return err
			}
		}
		// Disable doing initial splits going forward.
		b.initialSplits = 0
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
			"%s adder flushing %s (%s buffered/%0.2gx) wrote %d SSTs (avg: %s) with %d for splits, %d for size, took %v",
			b.name,
			b.kvSize,
			b.curBuf.MemSize,
			float64(b.kvSize)/float64(b.curBuf.MemSize),
			files,
			sz(written/int64(files)),
			dueToSplits,
			dueToSize,
			timing(timeutil.Since(beforeSort)),
		)
	}

	if log.V(4) {
		log.Infof(ctx,
			"%s adder has ingested %s (%s); spent %s filling, %v sorting, %v flushing (%v sink, %v sending, %v splitting, %v scattering %v)",
			b.name,
			sz(b.sink.totalRows.DataSize),
			sorted(b.sorted),
			timing(b.flushCounts.totalFilling),
			timing(b.flushCounts.totalSort),
			timing(b.flushCounts.totalFlush),
			timing(b.sink.flushCounts.flushWait),
			timing(b.sink.flushCounts.sendWait),
			timing(b.sink.flushCounts.splitWait),
			timing(b.sink.flushCounts.scatterWait),
			b.sink.flushCounts.scatterMoved,
		)
	}

	if log.V(5) {
		log.Infof(ctx,
			"%s adder has flushed %d times due to buffer size (%s), chunked as %d files (%d for ranges, %d for sst size, +%d for split-retries)",
			b.name,
			b.flushCounts.bufferSize,
			sz(b.memAcc.Used()),
			b.sink.flushCounts.total,
			b.sink.flushCounts.split,
			b.sink.flushCounts.sstSize,
			b.sink.flushCounts.files-b.sink.flushCounts.total,
		)
	}

	if b.onFlush != nil {
		b.onFlush(b.sink.GetBatchSummary())
	}
	b.curBuf.Reset()
	b.kvSize = 0
	b.lastFlush = timeutil.Now()
	return nil
}

func (b *BufferingAdder) createInitialSplits(ctx context.Context) error {
	log.Infof(ctx, "%s adder creating up to %d initial splits from %d KVs in %s buffer",
		b.name, b.initialSplits, b.curBuf.Len(), b.curBuf.MemSize)

	hour := hlc.Timestamp{WallTime: timeutil.Now().Add(time.Hour).UnixNano()}
	before := timeutil.Now()

	created := 0
	width := len(b.curBuf.entries) / b.initialSplits
	for i := 0; i < b.initialSplits; i++ {
		expire := hour
		if i == 0 {
			// If we over-split because our input is loosely ordered and we're just
			// seeing a sample of the first span here vs a sample of all of it, then
			// we may not fill enough for these splits to remain on their own. In that
			// case we'd really prefer the other splits be merged away first rather
			// than the first split, as it serves the important purpose of segregating
			// this processor's span from the one below it when is being constantly
			// re-scattered by that processor, so give the first split an extra hour.
			expire = hour.Add(time.Hour.Nanoseconds(), 0)
		}

		splitAt := i * width
		if splitAt >= len(b.curBuf.entries) {
			break
		}
		// Typically we split at splitAt if, and only if, its range still includes
		// the prior split, indicating no other processor is also splitting this
		// span. However, for the first split, there is no prior split, so we can
		// use the next split instead, as it too proves the range that need to be
		// split still has enough "width" (i.e. between two splits) to indicate that
		// another processor hasn't already split it.
		predicateAt := splitAt - width
		if predicateAt < 0 {
			next := splitAt + width
			if next > len(b.curBuf.entries)-1 {
				next = len(b.curBuf.entries) - 1
			}
			predicateAt = next
		}
		splitKey, err := keys.EnsureSafeSplitKey(b.curBuf.Key(splitAt))
		if err != nil {
			log.Warningf(ctx, "failed to generate pre-split key for key %s", b.curBuf.Key(splitAt))
			continue
		}
		predicateKey := b.curBuf.Key(predicateAt)
		log.VEventf(ctx, 1, "pre-splitting span %d of %d at %s", i, b.initialSplits, splitKey)
		resp, err := b.sink.db.SplitAndScatter(ctx, splitKey, expire, predicateKey)
		if err != nil {
			// TODO(dt): a typed error would be nice here.
			if strings.Contains(err.Error(), "predicate") {
				log.VEventf(ctx, 1, "%s adder split at %s rejected, had previously split and no longer included %s",
					b.name, splitKey, predicateKey)
				continue
			}
			return err
		}

		b.sink.flushCounts.splitWait += resp.Timing.Split
		b.sink.flushCounts.scatterWait += resp.Timing.Scatter
		if resp.ScatteredStats != nil {
			moved := sz(resp.ScatteredStats.Total())
			b.sink.flushCounts.scatterMoved += moved
			if resp.ScatteredStats.Total() > 0 {
				log.VEventf(ctx, 1, "pre-split scattered %s in non-empty range %s",
					moved, resp.ScatteredSpan)
			}
		}
		created++
	}

	log.Infof(ctx, "%s adder created %d initial splits in %v from %d keys in %s buffer",
		b.name, created, timing(timeutil.Since(before)), b.curBuf.Len(), b.curBuf.MemSize)

	b.sink.initialSplitDone = true
	return nil
}

// GetSummary returns this batcher's total added rows/bytes/etc.
func (b *BufferingAdder) GetSummary() roachpb.BulkOpSummary {
	return b.sink.GetSummary()
}
