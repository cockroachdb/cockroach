// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk/bulkpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
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

	// maxBufferLimit returns the size above which we will not request increases
	// to curBufferLimit from the monitor.
	maxBufferLimit func() int64

	// currently buffered kvs.
	curBuf kvBuf

	// curBufSummary is a summary of the currently buffered kvs that is populated
	// as and when the kvs are ingested by the underlying sink. This field is
	// cleared after each flush of the BufferingAdder.
	curBufSummary kvpb.BulkOpSummary

	sorted bool

	initialSplits int

	lastFlush time.Time

	// name of the BufferingAdder for the purpose of logging only.
	name string

	// importEpoch specifies the ImportEpoch of the table the BufferingAdder
	// is ingesting data as part of an IMPORT INTO job. If specified, the Bulk
	// Adder's SSTBatcher will write the import epoch to each versioned value's
	// metadata.
	importEpoch uint32

	bulkMon *mon.BytesMonitor
	memAcc  mon.EarmarkedBoundAccount

	onFlush func(summary kvpb.BulkOpSummary)
	// underfill tracks how much capacity was remaining in curBuf when it was
	// flushed due to size, e.g. how much its mis-allocated entries vs slab.
	underfill sz
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
	sendLimiter limit.ConcurrentRequestLimiter,
) (*BufferingAdder, error) {
	if opts.MinBufferSize == 0 {
		opts.MinBufferSize = 32 << 20
	}
	if opts.MaxBufferSize == nil {
		opts.MaxBufferSize = func() int64 { return 128 << 20 }
	}

	b := &BufferingAdder{
		name:        opts.Name,
		importEpoch: opts.ImportEpoch,
		sink: SSTBatcher{
			name:                   opts.Name,
			db:                     db,
			rc:                     rangeCache,
			settings:               settings,
			skipDuplicates:         opts.SkipDuplicates,
			disallowShadowingBelow: opts.DisallowShadowingBelow,
			batchTS:                opts.BatchTimestamp,
			writeAtBatchTS:         opts.WriteAtBatchTimestamp,
			mem:                    bulkMon.MakeConcurrentBoundAccount(),
			limiter:                sendLimiter,
			priority:               admissionpb.BulkNormalPri,
		},
		timestamp:      timestamp,
		maxBufferLimit: opts.MaxBufferSize,
		bulkMon:        bulkMon,
		sorted:         true,
		initialSplits:  opts.InitialSplitsIfUnordered,
		lastFlush:      timeutil.Now(),
		curBufSummary:  kvpb.BulkOpSummary{},
	}

	// Register a callback with the underlying sink to accumulate the summary for
	// the current buffered KVs. The curBufSummary is reset when the buffering
	// adder, and therefore the underlying sink, has completed ingested all the
	// currently buffered kvs.
	b.sink.mu.onFlush = func(batchSummary kvpb.BulkOpSummary) {
		b.curBufSummary.Add(batchSummary)
	}
	// At minimum a bulk adder needs enough space to store a buffer of
	// curBufferSize, and a subsequent SST of SSTSize in-memory. If the memory
	// account is unable to reserve this minimum threshold we cannot continue.
	//
	// TODO(adityamaru): IMPORT should also reserve memory for a single SST which
	// it will store in-memory before sending it to RocksDB.
	b.memAcc = bulkMon.MakeEarmarkedBoundAccount()
	if opts.MinBufferSize > 0 {
		if err := b.memAcc.Reserve(ctx, opts.MinBufferSize); err != nil {
			return nil, errors.WithHint(
				errors.Wrap(err, "not enough memory available to create a BulkAdder"),
				"Try setting a higher --max-sql-memory.")
		}
	}
	return b, nil
}

// SetOnFlush sets a callback to run after the buffering adder flushes.
func (b *BufferingAdder) SetOnFlush(fn func(summary kvpb.BulkOpSummary)) {
	b.onFlush = fn
}

// Close closes the underlying SST builder.
func (b *BufferingAdder) Close(ctx context.Context) {
	if log.V(1) {
		b.sink.mu.Lock()
		if b.sink.mu.totalStats.BufferFlushes > 0 {
			b.sink.mu.totalStats.LogTimings(ctx, b.name, "closing")
			if log.V(3) {
				b.sink.mu.totalStats.LogPerStoreTimings(ctx, b.name)
			}
			b.sink.mu.totalStats.LogFlushes(ctx, b.name, "closing", b.memAcc.Used(), b.sink.span)
		} else {
			log.Infof(ctx, "%s adder closing; ingested nothing", b.name)
		}
		b.sink.mu.Unlock()
	}
	b.sink.Close(ctx)

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

	need := sz(len(key) + len(value))
	// Check if this KV can fit in the buffer resizing it if needed and able.
	if b.curBuf.fits(ctx, need, sz(b.maxBufferLimit()), &b.memAcc) {
		return b.curBuf.append(key, value)
	}

	b.sink.currentStats.FlushesDueToSize++
	log.VEventf(ctx, 3, "%s adder triggering flush of %s of KVs in %s buffer",
		b.name, b.curBuf.KVSize(), b.bufferedMemSize())

	unusedEntries, unusedSlab := b.curBuf.unusedCap()
	b.underfill += unusedEntries + unusedSlab

	if err := b.doFlush(ctx, true); err != nil {
		return err
	}

	// If the budget allocation between the slab and entries capacity is skewed vs
	// actual usage -- say, if early keys were tiny so we grew entries cap, but
	// now the keys are big, so we need slab cap but our budget is still all used
	// by unused entries cap -- reset both. We'll take a slight hit re-alloc'ing
	// but will hopefully waste less buffer space.
	if b.underfill > 1<<30 {
		b.memAcc.Shrink(ctx, int64(b.curBuf.MemSize()))
		b.curBuf.entries = nil
		b.curBuf.slab = nil
		b.underfill = 0
	}

	// At this point we've flushed the buffer which implies that the slab and
	// entries slices have been emptied out. Furthermore, we may have nil'ed out
	// the slab and entries slices if we identified the allocation as skewed in
	// the condition above. In both cases we should always be able to fit the key
	// and value, so this call is to ensure proper reallocation and memory
	// monitoring.
	if b.curBuf.fits(ctx, need, sz(b.maxBufferLimit()), &b.memAcc) {
		return b.curBuf.append(key, value)
	}
	// If we are unable to fit this key and value even after flushing then we
	// should error since we do not have enough headroom to buffer this single
	// key+value.
	return errors.Newf("failed to fit KV of size %d in the buffering adder, "+
		"current max buffer limit is %d", need, b.maxBufferLimit())
}

func (b *BufferingAdder) bufferedKeys() int {
	return len(b.curBuf.entries)
}

func (b *BufferingAdder) bufferedMemSize() sz {
	return b.curBuf.MemSize()
}

// CurrentBufferFill returns the current buffer fill percentage.
func (b *BufferingAdder) CurrentBufferFill() float32 {
	return float32(b.curBuf.KVSize()) / float32(b.curBuf.MemSize())
}

// IsEmpty returns true if the adder has no un-flushed data in its buffer.
func (b *BufferingAdder) IsEmpty() bool {
	return b.curBuf.Len() == 0
}

// Flush flushes any buffered kvs to the batcher.
func (b *BufferingAdder) Flush(ctx context.Context) error {
	return b.doFlush(ctx, false)
}

func (b *BufferingAdder) doFlush(ctx context.Context, forSize bool) error {
	b.sink.currentStats.FillWait += timeutil.Since(b.lastFlush)

	if b.bufferedKeys() == 0 {
		if b.onFlush != nil {
			b.onFlush(b.curBufSummary)
		}
		b.lastFlush = timeutil.Now()
		b.curBufSummary.Reset()
		return nil
	}
	b.sink.Reset(ctx)
	b.sink.currentStats.BufferFlushes++

	var before *bulkpb.IngestionPerformanceStats
	var beforeSize int64
	// Get the stats before flush by summing totalStats and currentStats
	if log.V(3) {
		b.sink.mu.Lock()
		before = b.sink.mu.totalStats.Identity().(*bulkpb.IngestionPerformanceStats)
		before.Combine(&b.sink.mu.totalStats)
		before.Combine(&b.sink.currentStats)
		beforeSize = b.sink.mu.totalBulkOpSummary.DataSize
		b.sink.mu.Unlock()
	}

	beforeSort := timeutil.Now()

	if !b.sorted {
		sort.Sort(&b.curBuf)
	}
	mvccKey := storage.MVCCKey{Timestamp: b.timestamp}

	beforeFlush := timeutil.Now()
	b.sink.currentStats.SortWait += beforeFlush.Sub(beforeSort)

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

	if log.V(1) {
		if len(b.sink.span.Key) == 0 || b.curBuf.Key(0).Compare(b.sink.span.Key) < 0 {
			b.sink.span.Key = b.curBuf.Key(0).Clone()
		}
	}

	for i := range b.curBuf.entries {
		mvccKey.Key = b.curBuf.Key(i)
		if b.importEpoch != 0 {
			if err := b.sink.AddMVCCKeyWithImportEpoch(ctx, mvccKey, b.curBuf.Value(i),
				b.importEpoch); err != nil {
				return err
			}
		} else {
			if err := b.sink.AddMVCCKey(ctx, mvccKey, b.curBuf.Value(i)); err != nil {
				return err
			}
		}
	}
	if err := b.sink.Flush(ctx); err != nil {
		return err
	}

	if log.V(1) {
		if b.sink.span.EndKey.Compare(mvccKey.Key) < 0 {
			b.sink.span.EndKey = mvccKey.Key.Clone()
		}
	}

	b.sink.currentStats.FlushWait += timeutil.Since(beforeFlush)

	if log.V(3) && before != nil {
		b.sink.mu.Lock()
		written := b.sink.mu.totalBulkOpSummary.DataSize - beforeSize
		afterStats := b.sink.mu.totalStats.Identity().(*bulkpb.IngestionPerformanceStats)
		afterStats.Combine(&b.sink.mu.totalStats)
		afterStats.Combine(&b.sink.currentStats)
		b.sink.mu.Unlock()

		files := afterStats.Batches - before.Batches
		dueToSplits := afterStats.BatchesDueToRange - before.BatchesDueToRange
		dueToSize := afterStats.BatchesDueToSize - before.BatchesDueToSize

		log.Infof(ctx,
			"%s adder flushing %s (%s buffered/%0.2gx) wrote %d SSTs (avg: %s) with %d for splits, %d for size, took %v",
			b.name,
			b.curBuf.KVSize(),
			b.curBuf.MemSize(),
			float64(b.curBuf.KVSize())/float64(b.curBuf.MemSize()),
			files,
			sz(written/files),
			dueToSplits,
			dueToSize,
			timing(timeutil.Since(beforeSort)),
		)
	}

	if log.V(2) {
		b.sink.mu.Lock()
		b.sink.mu.totalStats.LogTimings(ctx, b.name, "flushed")
		if log.V(3) {
			b.sink.mu.totalStats.LogPerStoreTimings(ctx, b.name)
		}
		b.sink.mu.Unlock()
	}

	if log.V(3) {
		b.sink.mu.Lock()
		b.sink.mu.totalStats.LogFlushes(ctx, b.name, "flushed", b.memAcc.Used(), b.sink.span)
		b.sink.mu.Unlock()
	}

	if b.onFlush != nil {
		b.onFlush(b.curBufSummary)
	}
	b.curBuf.Reset()
	b.curBufSummary.Reset()
	b.lastFlush = timeutil.Now()
	return nil
}

func (b *BufferingAdder) createInitialSplits(ctx context.Context) error {
	log.Infof(ctx, "%s adder creating up to %d initial splits from %d KVs in %s buffer",
		b.name, b.initialSplits, b.curBuf.Len(), b.curBuf.KVSize())

	// First make all the splits, then go back and scatter them, so that those
	// scatters only move the narrower, post-split spans.
	beforeSplits := timeutil.Now()
	hour := hlc.Timestamp{WallTime: beforeSplits.Add(time.Hour).UnixNano()}
	width := len(b.curBuf.entries) / b.initialSplits
	var toScatter []roachpb.Key
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
		if err := b.sink.db.AdminSplit(ctx, splitKey, expire, predicateKey); err != nil {
			// TODO(dt): a typed error would be nice here.
			if strings.Contains(err.Error(), "predicate") {
				log.VEventf(ctx, 1, "%s adder split at %s rejected, had previously split and no longer included %s",
					b.name, splitKey, predicateKey)
			} else {
				log.Warningf(ctx, "failed to create initial split %s: %s", splitKey, err)
			}
			continue
		}
		toScatter = append(toScatter, splitKey)
	}

	beforeScatters := timeutil.Now()
	splitsWait := beforeScatters.Sub(beforeSplits)
	log.Infof(ctx, "%s adder created %d initial splits in %v from %d keys in %s buffer",
		b.name, len(toScatter), timing(splitsWait), b.curBuf.Len(), b.curBuf.MemSize())
	b.sink.currentStats.Splits += int64(len(toScatter))
	b.sink.currentStats.SplitWait += splitsWait

	for _, splitKey := range toScatter {
		resp, err := b.sink.db.AdminScatter(ctx, splitKey, 0 /* maxSize */)
		if err != nil {
			log.Warningf(ctx, "failed to scatter: %v", err)
			continue
		}
		b.sink.currentStats.Scatters++
		b.sink.currentStats.ScatterMoved += resp.ReplicasScatteredBytes
		if resp.ReplicasScatteredBytes > 0 {
			log.VEventf(ctx, 1, "pre-split scattered %s in non-empty range %s",
				sz(resp.ReplicasScatteredBytes), resp.RangeInfos[0].Desc.KeySpan().AsRawSpanWithNoLocals())
		}
	}
	scattersWait := timeutil.Since(beforeScatters)
	b.sink.currentStats.ScatterWait += scattersWait
	log.Infof(ctx, "%s adder scattered %d initial split spans in %v",
		b.name, len(toScatter), timing(scattersWait))

	b.sink.initialSplitDone = true
	return nil
}

// GetSummary returns this batcher's total added rows/bytes/etc.
func (b *BufferingAdder) GetSummary() kvpb.BulkOpSummary {
	return b.sink.GetSummary()
}
