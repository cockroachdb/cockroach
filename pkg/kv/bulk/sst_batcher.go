// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"bytes"
	"context"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk/bulkpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// maxScatterSize is the size limit included in scatters sent for as-we-write
// splits which expect to just move empty spans to balance ingestion, to avoid
// them becoming expensive moves of existing data if sent to a non-empty range.
const maxScatterSize = 4 << 20

var (
	tooSmallSSTSize = settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"kv.bulk_io_write.small_write_size",
		"size below which a 'bulk' write will be performed as a normal write instead",
		400*1<<10, // 400 Kib
	)

	ingestDelay = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"bulkio.ingest.flush_delay",
		"amount of time to wait before sending a file to the KV/Storage layer to ingest",
		0,
	)

	senderConcurrency = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"bulkio.ingest.sender_concurrency_limit",
		"maximum number of concurrent bulk ingest requests sent by any one sender, such as a processor in an IMPORT, index creation or RESTORE, etc (0 = no limit)",
		0,
		settings.NonNegativeInt,
	)

	computeStatsDiffInStreamBatcher = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"bulkio.ingest.compute_stats_diff_in_stream_batcher.enabled",
		"if set, kvserver will compute an accurate stats diff for every addsstable request",
		metamorphic.ConstantWithTestBool("computeStatsDiffInStreamBatcher", true),
	)

	sstBatcherElasticCPUControlEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"bulkio.ingest.sst_batcher_elastic_control.enabled",
		"determines whether the sst batcher integrates with elastic CPU control",
		false, // TODO(dt): enable this by default.
	)
)

// MakeAndRegisterConcurrencyLimiter makes a concurrency limiter and registers it
// with the setting on-change hook; it should be called only once during server
// setup due to the side-effects of the on-change registration.
func MakeAndRegisterConcurrencyLimiter(sv *settings.Values) limit.ConcurrentRequestLimiter {
	newLimit := int(senderConcurrency.Get(sv))
	if newLimit == 0 {
		newLimit = math.MaxInt
	}
	l := limit.MakeConcurrentRequestLimiter("bulk-send-limit", newLimit)
	senderConcurrency.SetOnChange(sv, func(ctx context.Context) {
		newLimit := int(senderConcurrency.Get(sv))
		if newLimit == 0 {
			newLimit = math.MaxInt
		}
		l.SetLimit(newLimit)
	})
	return l
}

type batch struct {
	sstWriter    storage.SSTWriter
	sstFile      *storage.MemObject
	startKey     []byte
	endKey       []byte
	endValue     []byte
	endTimestamp hlc.Timestamp

	// ts is set for mvcc complient ingestion. It is set to the current hlc time
	// when the first kv is added to the batch. If ts is set, it must be used as the
	// timestamp for every key in the sst and as the request timestamp for the add sst
	// request.
	//
	// If ts is not set, the batcher client is supposed to set the a timestamp on each
	// key. That is used by non-mvcc compliant ingestion such as PCR, which is allowed
	// to write data in the past and may be ingesting multiple KVs with different timestamps
	// or even multiple values with the same key, but different timestamps.
	ts hlc.Timestamp

	flushKeyChecked bool
	flushKey        roachpb.Key

	// stores on-the-fly stats for the SST if disallowShadowingBelow is set.
	ms enginepb.MVCCStats

	// Summary of the rows written in the current batch.
	//
	// NB: It is not advisable to use this field directly to consume per batch
	// summaries. This field is reset when the batcher is reset after each flush.
	// Under certain conditions the batcher can internally trigger a flush and a
	// reset while adding KVs i.e. without the caller explicilty calling Flush.
	// Furthermore, the batcher may reset the row counter before the async flush
	// has actually completed.
	//
	// If the caller requires a BulkOpSummary after each flush, they must register
	// a callback `onFlush`.
	rowCounter storage.RowCounter

	// stats contain the stats since the last flush. After each flush,
	// batch.stats is reset back to the empty value after being combined into
	// totalStats.
	stats bulkpb.IngestionPerformanceStats
}

func (b *batch) reset(ctx context.Context, settings *cluster.Settings, resetTS bool) {
	b.sstWriter.Close()

	b.sstFile = &storage.MemObject{}
	// Create sstables intended for ingestion using the newest format that all
	// nodes can support. MakeIngestionSSTWriter will handle cluster version
	// gating using b.settings.
	b.sstWriter = storage.MakeIngestionSSTWriter(ctx, settings, b.sstFile)

	b.startKey = b.startKey[:0]
	b.endKey = b.endKey[:0]
	b.endValue = b.endValue[:0]
	b.endTimestamp = hlc.Timestamp{}

	if resetTS {
		b.ts = hlc.Timestamp{}
	}

	b.flushKey = nil
	b.flushKeyChecked = false

	b.ms.Reset()
	b.rowCounter.BulkOpSummary.Reset()
	if b.stats.SendWaitByStore == nil {
		b.stats.SendWaitByStore = make(map[roachpb.StoreID]time.Duration)
	}
	b.stats.Reset()
}

// copyStats constructs a copy of the `stats` field in the batch.
func (b *batch) copyStats() *bulkpb.IngestionPerformanceStats {
	stats := b.stats.Identity().(*bulkpb.IngestionPerformanceStats)
	stats.Combine(&b.stats)
	return stats
}

func (b *batch) close() {
	b.sstWriter.Close()
}

func (b *batch) updateMVCCStats(key storage.MVCCKey, value []byte) {
	metaKeySize := int64(len(key.Key)) + 1
	metaValSize := int64(0)
	b.ms.LiveBytes += metaKeySize
	b.ms.LiveCount++
	b.ms.KeyBytes += metaKeySize
	b.ms.ValBytes += metaValSize
	b.ms.KeyCount++

	totalBytes := int64(len(value)) + storage.MVCCVersionTimestampSize
	b.ms.LiveBytes += totalBytes
	b.ms.KeyBytes += storage.MVCCVersionTimestampSize
	b.ms.ValBytes += int64(len(value))
	b.ms.ValCount++
}

// SSTBatcher is a helper for bulk-adding many KVs in chunks via AddSSTable. An
// SSTBatcher can be handed KVs repeatedly and will make them into SSTs that are
// added when they reach the configured size, tracking the total added rows,
// bytes, etc. If configured with a non-nil, populated range cache, it will use
// it to attempt to flush SSTs before they cross range boundaries to minimize
// expensive on-split retries.
//
// Note: the SSTBatcher currently cannot bulk add range keys.
type SSTBatcher struct {
	name     string
	db       *kv.DB
	adder    *sstAdder
	rc       *rangecache.RangeCache
	settings *cluster.Settings
	mem      *mon.ConcurrentBoundAccount
	limiter  limit.ConcurrentRequestLimiter

	// disallowShadowingBelow is described on kvpb.AddSSTableRequest.
	disallowShadowingBelow hlc.Timestamp

	// pacer for admission control during SST ingestion
	pacer CPUPacer

	// skips duplicate keys (iff they are buffered together). This is true when
	// used to backfill an inverted index. An array in JSONB with multiple values
	// which are the same, will all correspond to the same kv in the inverted
	// index. The method which generates these kvs does not dedup, thus we rely on
	// the SSTBatcher to dedup them (by skipping), rather than throwing a
	// DuplicateKeyError.
	// This is also true when used with IMPORT. Import
	// generally prohibits the ingestion of KVs which will shadow existing data,
	// with the exception of duplicates having the same value and timestamp. To
	// maintain uniform behavior, duplicates in the same batch with equal values
	// will not raise a DuplicateKeyError.
	skipDuplicates bool
	// ingestAll can only be set when disallowShadowingBelow is empty and
	// skipDuplicates is false.
	//
	// It will only ever return a duplicateKey error if the key and timestamp are
	// the same as en existing key but the value differs.
	ingestAll bool

	// writeAtBatchTS is passed to the writeAtBatchTs argument to db.AddSStable.
	writeAtBatchTS bool

	// disableScatters controls scatters of the as-we-fill split ranges.
	disableScatters bool

	// span tracks the total span into which this batcher has flushed. It is
	// only maintained if log.V(1), so if vmodule is upped mid-ingest it may be
	// incomplete.
	span roachpb.Span

	// batch stores state for the unflushed batch.
	batch batch

	// lastRange is the span and remaining capacity of the last range added to,
	// for checking if the next addition would overfill it.
	lastRange struct {
		span            roachpb.Span
		remaining       sz
		nextExistingKey roachpb.Key
	}

	// mustSyncBeforeFlush is set to true if the caller must sync in fight flushes
	// before starting a new flush.
	mustSyncBeforeFlush bool

	// asyncAddSSTs is a group that tracks the async flushes of SSTs.
	asyncAddSSTs ctxgroup.Group

	// flushSpan must be finished after waiting for the context group to finish.
	flushSpan *tracing.Span

	// cancelFlush is called by `Close` to cancel any in-flight flushes. Before
	// waiting for the flushes to complete.
	cancelFlush func()

	valueScratch []byte

	mu struct {
		syncutil.Mutex

		maxWriteTS         hlc.Timestamp
		totalBulkOpSummary kvpb.BulkOpSummary

		// totalStats contain the stats over the entire lifetime of the SST Batcher.
		// As rows accumulate, the corresponding stats initially start out in
		// batch.stats. After each flush, the contents of batch.stats are combined
		// into totalStats, and batch.stats is reset back to the empty value.
		totalStats  bulkpb.IngestionPerformanceStats
		lastFlush   time.Time
		tracingSpan *tracing.Span

		// onFlush is the callback called after the current batch has been
		// successfully ingested.
		onFlush func(summary kvpb.BulkOpSummary)
	}
}

// MakeSSTBatcher makes a ready-to-use SSTBatcher.
func MakeSSTBatcher(
	ctx context.Context,
	name string,
	db *kv.DB,
	settings *cluster.Settings,
	disallowShadowingBelow hlc.Timestamp,
	writeAtBatchTs bool,
	scatterSplitRanges bool,
	mem *mon.ConcurrentBoundAccount,
	sendLimiter limit.ConcurrentRequestLimiter,
	rc *rangecache.RangeCache,
) (*SSTBatcher, error) {
	b := &SSTBatcher{
		name:                   name,
		db:                     db,
		adder:                  newSSTAdder(db, settings, writeAtBatchTs, disallowShadowingBelow, admissionpb.BulkNormalPri, false),
		settings:               settings,
		disallowShadowingBelow: disallowShadowingBelow,
		writeAtBatchTS:         writeAtBatchTs,
		disableScatters:        !scatterSplitRanges,
		mem:                    mem,
		limiter:                sendLimiter,
		rc:                     rc,
		pacer:                  NewCPUPacer(ctx, db, sstBatcherElasticCPUControlEnabled),
	}
	b.mu.lastFlush = timeutil.Now()
	b.mu.tracingSpan = tracing.SpanFromContext(ctx)
	b.init(ctx)
	return b, nil
}

// MakeStreamSSTBatcher creates a batcher configured to ingest duplicate keys
// that might be received from a cluster to cluster stream.
func MakeStreamSSTBatcher(
	ctx context.Context,
	db *kv.DB,
	rc *rangecache.RangeCache,
	settings *cluster.Settings,
	mem *mon.ConcurrentBoundAccount,
	sendLimiter limit.ConcurrentRequestLimiter,
	onFlush func(summary kvpb.BulkOpSummary),
) (*SSTBatcher, error) {
	b := &SSTBatcher{
		db: db,
		rc: rc,
		// We use NormalPri since anything lower than normal priority is assumed to
		// be able to handle reduced throughput. We are OK with his for now since
		// the consuming cluster of a replication stream does not have a latency
		// sensitive workload running against it.
		adder:     newSSTAdder(db, settings, false /*writeAtBatchTS*/, hlc.Timestamp{}, admissionpb.BulkNormalPri, computeStatsDiffInStreamBatcher.Get(&settings.SV)),
		settings:  settings,
		ingestAll: true,
		mem:       mem,
		limiter:   sendLimiter,
		// disableScatters is set to true to disable scattering as-we-fill. The
		// replication job already pre-splits and pre-scatters its target ranges to
		// distribute the ingestion load.
		//
		// If the batcher knows that it is about to overfill a range, it always
		// makes sense to split it before adding to it, rather than overfill it. It
		// does not however make sense to scatter that range as the RHS maybe
		// non-empty.
		disableScatters: true,
		pacer:           NewCPUPacer(ctx, db, sstBatcherElasticCPUControlEnabled),
	}
	b.mu.lastFlush = timeutil.Now()
	b.mu.tracingSpan = tracing.SpanFromContext(ctx)
	b.SetOnFlush(onFlush)
	b.init(ctx)
	return b, nil
}

// MakeTestingSSTBatcher creates a batcher for testing, allowing setting options
// that are typically only set when constructing a batcher in BufferingAdder.
func MakeTestingSSTBatcher(
	ctx context.Context,
	db *kv.DB,
	settings *cluster.Settings,
	skipDuplicates bool,
	ingestAll bool,
	mem *mon.ConcurrentBoundAccount,
	sendLimiter limit.ConcurrentRequestLimiter,
) (*SSTBatcher, error) {
	b := &SSTBatcher{
		db:             db,
		adder:          newSSTAdder(db, settings, false, hlc.Timestamp{}, admissionpb.BulkNormalPri, false),
		settings:       settings,
		skipDuplicates: skipDuplicates,
		ingestAll:      ingestAll,
		mem:            mem,
		limiter:        sendLimiter,
		pacer:          NewCPUPacer(ctx, db, sstBatcherElasticCPUControlEnabled),
	}
	b.init(ctx)
	return b, nil
}

// SetOnFlush sets a callback to run after the SSTBatcher flushes.
func (b *SSTBatcher) SetOnFlush(onFlush func(summary kvpb.BulkOpSummary)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.onFlush = onFlush
}

func (b *SSTBatcher) AddMVCCKeyWithImportEpoch(
	ctx context.Context, key storage.MVCCKey, value []byte, importEpoch uint32,
) error {
	mvccVal, err := storage.DecodeMVCCValue(value)
	if err != nil {
		return err
	}
	mvccVal.MVCCValueHeader.ImportEpoch = importEpoch
	buf, canRetainBuffer, err := storage.EncodeMVCCValueToBuf(mvccVal, b.valueScratch[:0])
	if canRetainBuffer {
		b.valueScratch = buf
	}
	if err != nil {
		return err
	}
	return b.AddMVCCKey(ctx, key, b.valueScratch)
}

func (b *SSTBatcher) AddMVCCKeyLDR(ctx context.Context, key storage.MVCCKey, value []byte) error {
	mvccVal, err := storage.DecodeMVCCValue(value)
	if err != nil {
		return err
	}
	mvccVal.MVCCValueHeader.OriginTimestamp = key.Timestamp
	mvccVal.OriginID = 1
	// NOTE: since we are setting header values, EncodeMVCCValueToBuf will
	// always use the sctarch buffer or return an error.
	b.valueScratch, _, err = storage.EncodeMVCCValueToBuf(mvccVal, b.valueScratch[:0])
	if err != nil {
		return err
	}
	return b.AddMVCCKey(ctx, key, b.valueScratch)
}

// AddMVCCKey adds a key+timestamp/value pair to the batch (flushing if needed).
// This is only for callers that want to control the timestamp on individual
// keys -- like RESTORE where we want the restored data to look like the backup.
// Keys must be added in order.
func (b *SSTBatcher) AddMVCCKey(ctx context.Context, key storage.MVCCKey, value []byte) error {
	// Pace based on admission control before adding the key.
	b.pacer.Pace(ctx)

	if len(b.batch.endKey) > 0 && bytes.Equal(b.batch.endKey, key.Key) {
		if b.ingestAll && key.Timestamp.Equal(b.batch.endTimestamp) {
			if bytes.Equal(b.batch.endValue, value) {
				// If ingestAll is set, we allow and skip (key, timestamp, value)
				// matches. We expect this from callers who may need to deal with
				// duplicates caused by retransmission.
				return nil
			}
			// Despite ingestAll, we raise an error in the case of a because a new value
			// at the exact key and timestamp is unexpected and one of the two values
			// would be completely lost.
			return kvserverbase.NewDuplicateKeyError(key.Key, value)
		} else if b.skipDuplicates && bytes.Equal(b.batch.endValue, value) {
			// If skipDuplicates is set, we allow and skip (key, value) matches.
			return nil
		}

		if !b.ingestAll {
			return kvserverbase.NewDuplicateKeyError(key.Key, value)
		}
	}
	// Check if we need to flush current batch *before* adding the next k/v --
	// the batcher may want to flush the keys it already has, either because it
	// is full or because it wants this key in a separate batch due to splits.
	if err := b.flushIfNeeded(ctx, key.Key); err != nil {
		return err
	}

	if b.writeAtBatchTS {
		if b.batch.ts.IsEmpty() {
			b.batch.ts = b.db.Clock().Now()
		}
		key.Timestamp = b.batch.ts
	}

	// Update the range currently represented in this batch, as necessary.
	if len(b.batch.startKey) == 0 {
		b.batch.startKey = append(b.batch.startKey[:0], key.Key...)
	}

	b.batch.endTimestamp = key.Timestamp
	b.batch.endKey = append(b.batch.endKey[:0], key.Key...)
	b.batch.endValue = append(b.batch.endValue[:0], value...)

	if err := b.batch.rowCounter.Count(key.Key); err != nil {
		return err
	}

	// If we do not allow shadowing of keys when ingesting an SST via AddSSTable,
	// then we can update the MVCCStats on the fly because we are guaranteed to
	// ingest unique keys. This saves us an extra iteration in AddSSTable which
	// has been identified as a significant performance regression for IMPORT.
	if !b.disallowShadowingBelow.IsEmpty() {
		b.batch.updateMVCCStats(key, value)
	}
	return b.batch.sstWriter.PutRawMVCC(key, value)
}

func (b *SSTBatcher) init(ctx context.Context) {
	b.batch.reset(ctx, b.settings, b.writeAtBatchTS)
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.mu.totalStats.SendWaitByStore == nil {
		b.mu.totalStats.SendWaitByStore = make(map[roachpb.StoreID]time.Duration)
	}
}

// Reset clears all state in the batcher and prepares it for reuse.
func (b *SSTBatcher) Reset(ctx context.Context) error {
	// TODO(jeffswenson): clean up callers of Reset. It is no longer necessary now that
	// `Flush` always leaves the batcher in a quiescent state.
	if err := b.syncFlush(); err != nil {
		return errors.Wrap(err, "failed to sync flush before reset")
	}
	b.batch.reset(ctx, b.settings, b.writeAtBatchTS)
	b.valueScratch = b.valueScratch[:0]
	return nil
}

const (
	manualFlush = iota
	sizeFlush
	rangeFlush
)

func (b *SSTBatcher) flushIfNeeded(ctx context.Context, nextKey roachpb.Key) error {
	// If this is the first key we have seen (since being reset), attempt to find
	// the end of the range it is in so we can flush the SST before crossing it,
	// because AddSSTable cannot span ranges and will need to be split and retried
	// from scratch if we generate an SST that ends up doing so.
	if !b.batch.flushKeyChecked && b.rc != nil {
		b.batch.flushKeyChecked = true
		if k, err := keys.Addr(nextKey); err != nil {
			log.Warningf(ctx, "failed to get RKey for flush key lookup: %v", err)
		} else {
			if r, err := b.rc.Lookup(ctx, k); err != nil {
				log.Warningf(ctx, "failed to lookup range cache entry for key %v: %v", k, err)
			} else {
				k := r.Desc.EndKey.AsRawKey()
				b.batch.flushKey = k
				log.VEventf(ctx, 3, "%s building sstable that will flush before %v", b.name, k)
			}
		}
	}

	shouldFlushDueToRange := b.batch.flushKey != nil && b.batch.flushKey.Compare(nextKey) <= 0

	if shouldFlushDueToRange {
		if b.mustSyncBeforeFlush {
			err := b.syncFlush()
			if err != nil {
				return err
			}
		}
		b.startFlush(ctx, rangeFlush)
		return nil
	}

	if b.batch.sstWriter.DataSize >= ingestFileSize(b.settings) {
		// We're at/over size target, so we want to flush, but first check if we are
		// at a new row boundary. Having row-aligned boundaries is not actually
		// required by anything, but has the nice property of meaning a split will
		// fall between files. This is particularly useful mid-IMPORT when we split
		// at the beginning of the next file, as that split is at the row boundary
		// at *or before* that file's start. If the file boundary is not aligned,
		// the prior file overhangs beginning of the row in which the next file
		// starts, so when we split at that row, that overhang into the RHS that we
		// just wrote will be rewritten by the subsequent scatter. By waiting for a
		// row boundary, we ensure any split is actually between files.
		prevRow, prevErr := keys.EnsureSafeSplitKey(b.batch.endKey)
		nextRow, nextErr := keys.EnsureSafeSplitKey(nextKey)
		if prevErr == nil && nextErr == nil && bytes.Equal(prevRow, nextRow) {
			// An error decoding either key implies it is not a valid row key and thus
			// not the same row for our purposes; we don't care what the error is.
			return nil // keep going to row boundary.
		}
		if b.mustSyncBeforeFlush {
			err := b.syncFlush()
			if err != nil {
				return err
			}
		}
		b.startFlush(ctx, sizeFlush)
		return nil
	}
	return nil
}

// Flush sends the current batch and waits for all in-flight flushes to complete. Flush will
// always leave the batcher in a `quiescent` state, meaning there are no in flight flushes.
func (b *SSTBatcher) Flush(ctx context.Context) error {
	if b.mustSyncBeforeFlush {
		err := b.syncFlush()
		if err != nil {
			return err
		}
	}

	b.startFlush(ctx, manualFlush)

	if err := b.syncFlush(); err != nil {
		return err
	}

	// no need to lock b.mu since we just wait()'ed.
	if !b.mu.maxWriteTS.IsEmpty() {
		if now := b.db.Clock().Now(); now.Less(b.mu.maxWriteTS) {
			guess := timing(b.mu.maxWriteTS.WallTime - now.WallTime)
			log.VEventf(ctx, 1, "%s batcher waiting %s until max write time %s", b.name, guess, b.mu.maxWriteTS)
			if err := b.db.Clock().SleepUntil(ctx, b.mu.maxWriteTS); err != nil {
				return err
			}
			b.batch.stats.CommitWait += timeutil.Since(now.GoTime())
		}
		b.mu.maxWriteTS.Reset()
	}

	return nil
}

// syncFlush waits for all in-flight flushes to complete. Once this returns,
// the batcher is guaranteed to be in a state where there are no async goroutines.
func (b *SSTBatcher) syncFlush() error {
	if b.cancelFlush == nil {
		return nil
	}

	flushErr := b.asyncAddSSTs.Wait()

	// We already waited for the flush to complete, but we must call cancel to
	// avoid leaking the context.
	b.cancelFlush()
	b.cancelFlush = nil

	b.flushSpan.Finish()
	b.flushSpan = nil

	b.asyncAddSSTs = ctxgroup.Group{}
	b.mustSyncBeforeFlush = false

	return flushErr
}

var debugDropSSTOnFlush = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"bulkio.ingest.unsafe_debug.drop_sst_on_flush.enabled",
	"if set, the SSTBatcher will simply discard data instead of flushing it (destroys data; for performance debugging experiments only)",
	false,
	settings.WithUnsafe,
)

// startFlush starts a flush of the current batch. If it encounters any errors
// the errors are reported by the call to `syncFlush`.
//
// if `mustSyncBeforeFlush` is true, the caller must call `syncFlush` before
// starting another flush.
//
// The flush is always asynchronous. This allows the caller to start constructing
// the next batch while the current one is being flushed. Multiple flushes may
// be pipelined if the flush reason is `rangeFlush`. We can't pipeline more than
// one flush to a single range because the request includes the current range size
// and we use that as a signal to split and scatter the range.
func (b *SSTBatcher) startFlush(ctx context.Context, reason int) {
	if buildutil.CrdbTestBuild && b.mustSyncBeforeFlush {
		panic(errors.AssertionFailedf("mustSyncBeforeFlush is set, but startFlush was called"))
	}

	defer b.batch.reset(ctx, b.settings, b.writeAtBatchTS)

	if b.batch.sstWriter.DataSize == 0 {
		return
	}
	beforeFlush := timeutil.Now()

	b.batch.stats.Batches++

	if b.cancelFlush == nil {
		// TODO(jeffswenson): clean up context handling. This works well if the flush is
		// triggered by `Flush`. But its a little weird if the flush is triggered by
		// adding a kv that triggers an automatic flush.

		// We create a new span for the flush since we can't guarantee that the span
		// attached to the context will live as long as the SSTBatcher.
		var flushCtx context.Context
		flushCtx, b.cancelFlush = context.WithCancel(ctx)
		flushCtx, b.flushSpan = tracing.ChildSpan(flushCtx, "sstbatcher-flush")
		b.asyncAddSSTs = ctxgroup.WithContext(flushCtx)
	}

	if err := b.batch.sstWriter.Finish(); err != nil {
		// Create a goroutine to push the error to `syncFlush`.
		b.mustSyncBeforeFlush = true
		b.asyncAddSSTs.Go(func() error {
			return err
		})
		return
	}

	start := roachpb.Key(append([]byte(nil), b.batch.startKey...))
	// The end key of the WriteBatch request is exclusive, but batchEndKey is
	// currently the largest key in the batch. Increment it.
	end := roachpb.Key(append([]byte(nil), b.batch.endKey...)).Next()

	size := sz(b.batch.sstWriter.DataSize)

	if reason == sizeFlush {
		log.VEventf(ctx, 3, "%s flushing %s SST due to size > %s", b.name, size, sz(ingestFileSize(b.settings)))
		b.batch.stats.BatchesDueToSize++
	} else if reason == rangeFlush {
		log.VEventf(ctx, 3, "%s flushing %s SST due to range boundary", b.name, size)
		b.batch.stats.BatchesDueToRange++
	}

	// If this file is starting in the same span we last added to and is bigger
	// than the size that range had when we last added to it, then we should split
	// off the suffix of that range where this file starts and add it to that new
	// range after scattering it.
	if b.lastRange.span.ContainsKey(start) && size >= b.lastRange.remaining {
		log.VEventf(ctx, 2, "%s batcher splitting full range before adding file starting at %s",
			b.name, start)

		expire := hlc.Timestamp{WallTime: timeutil.Now().Add(time.Minute * 10).UnixNano()}

		nextKey := b.lastRange.nextExistingKey

		// If there was existing data as of last add that is above the file we are
		// about to add, split there first, both since that could be why the range
		// is full as well as because if we do not, that existing data will need to
		// be moved when we scatter the span we're splitting for ingestion.
		if len(nextKey) > 0 && end.Compare(nextKey) < 0 {
			log.VEventf(ctx, 2, "%s splitting above file span %s at existing key %s",
				b.name, roachpb.Span{Key: start, EndKey: end}, nextKey)

			splitAbove, err := keys.EnsureSafeSplitKey(nextKey)
			if err != nil {
				log.Warningf(ctx, "%s failed to generate split-above key: %v", b.name, err)
			} else {
				beforeSplit := timeutil.Now()
				err := b.db.AdminSplit(ctx, splitAbove, expire)
				b.batch.stats.SplitWait += timeutil.Since(beforeSplit)
				if err != nil {
					log.Warningf(ctx, "%s failed to split-above: %v", b.name, err)
				} else {
					b.batch.stats.Splits++
				}
			}
		}

		splitAt, err := keys.EnsureSafeSplitKey(start)
		if err != nil {
			log.Warningf(ctx, "%s failed to generate split key: %v", b.name, err)
		} else {
			beforeSplit := timeutil.Now()
			err := b.db.AdminSplit(ctx, splitAt, expire)
			b.batch.stats.SplitWait += timeutil.Since(beforeSplit)
			if err != nil {
				log.Warningf(ctx, "%s failed to split: %v", b.name, err)
			} else {
				b.batch.stats.Splits++

				if !b.disableScatters {
					// Now scatter the RHS before we proceed to ingest into it. We know it
					// should be empty since we split above if there was a nextExistingKey.
					beforeScatter := timeutil.Now()
					resp, err := b.db.AdminScatter(ctx, splitAt, maxScatterSize)
					b.batch.stats.ScatterWait += timeutil.Since(beforeScatter)
					if err != nil {
						// TODO(dt): switch to a typed error.
						if strings.Contains(err.Error(), "existing range size") {
							log.VEventf(ctx, 1, "%s scattered non-empty range rejected: %v", b.name, err)
						} else {
							log.Warningf(ctx, "%s failed to scatter	: %v", b.name, err)
						}
					} else {
						b.batch.stats.Scatters++
						b.batch.stats.ScatterMoved += resp.ReplicasScatteredBytes
						if resp.ReplicasScatteredBytes > 0 {
							log.VEventf(ctx, 1, "%s split scattered %s in non-empty range %s", b.name, sz(resp.ReplicasScatteredBytes), resp.RangeInfos[0].Desc.KeySpan().AsRawSpanWithNoLocals())
						}
					}
				}
			}
		}
	}

	// If the stats have been computed on-the-fly, set the last updated time
	// before ingesting the SST.
	if (b.batch.ms != enginepb.MVCCStats{}) {
		b.batch.ms.LastUpdateNanos = timeutil.Now().UnixNano()
	}

	// Take a copy of the fields that can be captured by the call to addSSTable
	// below, that could occur asynchronously.
	mvccStats := b.batch.ms
	data := b.batch.sstFile.Data()
	batchTS := b.batch.ts
	currentBatchSummary := b.batch.rowCounter.BulkOpSummary
	performanceStats := b.batch.copyStats()

	res, err := b.maybeDelay(ctx)
	if err != nil {
		// Create a goroutine to push the error to `syncFlush`.
		b.mustSyncBeforeFlush = true
		b.asyncAddSSTs.Go(func() error {
			return err
		})
		return
	}

	// If we're flushing due to a range boundary, we we might be flushing this
	// one buffer into many different ranges, and doing so one-by-one, waiting
	// for each round-trip serially, could really add up; a buffer of random
	// data that covers all of a 2000 range table would be flushing 2000 SSTs,
	// each of which might be quite small, like 256kib, but still see, say, 50ms
	// or more round-trip time. Doing those serially would then take minutes. If
	// we can, instead send this SST and move on to the next while it is sent,
	// we could reduce that considerably. One concern with doing so however is
	// that you could potentially end up with an entire buffer's worth of SSTs
	// all inflight at once, effectively doubling the memory footprint, so we
	// need to reserve memory from a monitor for the sst before we move on to
	// the next one.
	pipelineFlush := reason == rangeFlush

	var reserved int64
	if pipelineFlush {
		if err := b.mem.Grow(ctx, int64(cap(data))); err != nil {
			log.VEventf(ctx, 3, "%s unable to reserve enough memory to flush async: %v", b.name, err)
			pipelineFlush = false
		} else {
			reserved = int64(cap(data))
		}
	}

	if !pipelineFlush {
		b.mustSyncBeforeFlush = true
	}

	b.asyncAddSSTs.GoCtx(func(ctx context.Context) error {
		defer res.Release()
		defer b.mem.Shrink(ctx, reserved)

		var results []addSSTResult
		if !debugDropSSTOnFlush.Get(&b.settings.SV) {
			results, err = b.adder.AddSSTable(ctx, batchTS, start, end, data, mvccStats, performanceStats)
			if err != nil {
				return err
			}
		}

		// Now that we have completed ingesting the SSTables we take a lock and
		// process the flush results.
		b.mu.Lock()
		defer b.mu.Unlock()

		for _, addResult := range results {
			if b.writeAtBatchTS {
				b.mu.maxWriteTS.Forward(addResult.timestamp)
			}
			if !pipelineFlush {
				// If this was sent async then, by the time the reply gets back, it
				// might not be the last range anymore. We can just discard the last
				// range reply in this case though because async sends are only used
				// for SSTs sent due to range boundaries, i.e. when we are done with
				// with that range anyway.
				b.lastRange.span = addResult.rangeSpan
				if addResult.rangeSpan.Valid() {
					b.lastRange.remaining = sz(addResult.availableBytes)
					b.lastRange.nextExistingKey = addResult.followingLikelyNonEmptySpanStart
				}
			}
		}

		// Update the statistics associated with the current batch. We do this on
		// our captured copy of the currentBatchSummary instead of the
		// b.batchRowCounter since the caller may have reset the batcher by the time
		// this flush completes. This is possible in the case of an async flush.
		currentBatchSummary.DataSize += int64(size)
		currentBatchSummary.SSTDataSize += int64(len(data))

		// Check if the caller has registered a callback to consume a per batch
		// summary.
		if b.mu.onFlush != nil {
			b.mu.onFlush(currentBatchSummary)
		}

		performanceStats.LogicalDataSize += int64(size)
		performanceStats.SSTDataSize += int64(len(data))
		afterFlush := timeutil.Now()
		performanceStats.BatchWait += afterFlush.Sub(beforeFlush)
		performanceStats.Duration = afterFlush.Sub(b.mu.lastFlush)
		performanceStats.LastFlushTime = hlc.Timestamp{WallTime: b.mu.lastFlush.UnixNano()}
		performanceStats.CurrentFlushTime = hlc.Timestamp{WallTime: afterFlush.UnixNano()}

		// Combine the statistics of this batch into the running aggregate
		// maintained by the SSTBatcher.
		b.mu.totalBulkOpSummary.Add(currentBatchSummary)
		b.mu.totalStats.Combine(performanceStats)

		b.mu.lastFlush = afterFlush
		if b.mu.tracingSpan != nil {
			b.mu.tracingSpan.RecordStructured(performanceStats)
		}
		return nil
	})
}

func (b *SSTBatcher) maybeDelay(ctx context.Context) (limit.Reservation, error) {
	// TODO(148371): delete the ingestion delay settings. These are less useful now that admission control
	// is on by default.
	if delay := ingestDelay.Get(&b.settings.SV); delay != 0 {
		if delay > time.Second || log.V(1) {
			log.Infof(ctx, "%s delaying %s before flushing ingestion buffer...", b.name, delay)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return b.limiter.Begin(ctx)
}

// Close closes the underlying SST builder.
func (b *SSTBatcher) Close(ctx context.Context) {
	b.batch.close()
	if b.cancelFlush != nil {
		b.cancelFlush()
	}
	if err := b.syncFlush(); err != nil {
		log.Warningf(ctx, "closing with flushes in-progress encountered an error: %v", err)
	}
	b.pacer.Close()
	b.mem.Close(ctx)
}

// GetSummary returns this batcher's total added rows/bytes/etc.
func (b *SSTBatcher) GetSummary() kvpb.BulkOpSummary {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.totalBulkOpSummary
}
