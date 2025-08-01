// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package gc contains the logic to run scan a range for garbage and issue
// GC requests to remove that garbage.
//
// The Run function is the primary entry point and is called underneath the
// gcQueue in the storage package. It can also be run for debugging.
package gc

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	// KeyVersionChunkBytes is the threshold size for splitting
	// GCRequests into multiple batches. The goal is that the evaluated
	// Raft command for each GCRequest does not significantly exceed
	// this threshold.
	KeyVersionChunkBytes = base.ChunkRaftCommandThresholdBytes
	// defaultClearRangeMinKeys is min number of versions we will try to
	// remove using clear range. If we have less than this number, using pebble
	// range tombstone is more expensive than using multiple point deletions.
	defaultClearRangeMinKeys = 2000
	// defaultMaxPendingKeysSize is a max amount of memory used to store pending
	// keys while trying to decide if clear range operation is feasible. If we
	// don't have enough keys when this value is reached, points GC operation
	// will be flushed to free memory even if we still can potentially issue
	// clear range in the future.
	defaultMaxPendingKeysSize = 64 << 20
	// hlcTimestampSize is the size of hlc timestamp used for mem usage
	// calculations.
	hlcTimestampSize = 16
)

// LockAgeThreshold is the threshold after which an extant locks
// will be resolved.
var LockAgeThreshold = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.gc.intent_age_threshold",
	"locks older than this threshold will be resolved when encountered by the MVCC GC queue",
	2*time.Hour,
	settings.DurationWithMinimum(2*time.Minute),
	settings.WithName("kv.gc.lock_age_threshold"),
)

// TxnCleanupThreshold is the threshold after which a transaction is
// considered abandoned and fit for removal, as measured by the
// maximum of its last heartbeat and read timestamp. Abort spans for the
// transaction are cleaned up at the same time.
//
// TODO(tschottdorf): need to enforce at all times that this is much
// larger than the heartbeat interval used by the coordinator.
var TxnCleanupThreshold = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.gc.txn_cleanup_threshold",
	"the threshold after which a transaction is considered abandoned and "+
		"fit for removal, as measured by the maximum of its last heartbeat and timestamp",
	time.Hour,
)

// MaxLocksPerCleanupBatch is the maximum number of locks that GC will send
// for resolution as a single batch.
//
// The setting is also used by foreground requests like QueryResolvedTimestamp
// that do not need to resolve intents synchronously when they encounter them,
// but do want to perform best-effort asynchronous intent resolution. The
// setting dictates how many intents these requests will collect at a time.
//
// The default value is set to half of the maximum lock table size at the time
// of writing. This value is subject to tuning in real environment as we have
// more data available.
var MaxLocksPerCleanupBatch = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.gc.intent_cleanup_batch_size",
	"if non zero, gc will split found locks into batches of this size when trying to resolve them",
	5000,
	settings.NonNegativeInt,
	settings.WithName("kv.gc.lock_cleanup_batch_size"),
)

// MaxLockKeyBytesPerCleanupBatch is the maximum lock bytes that GC will try to
// send as a single batch to resolution. This number is approximate and only
// includes size of the lock keys.
//
// The setting is also used by foreground requests like QueryResolvedTimestamp
// that do not need to resolve intents synchronously when they encounter them,
// but do want to perform best-effort asynchronous intent resolution. The
// setting dictates how many intents these requests will collect at a time.
//
// The default value is a conservative limit to prevent pending lock key sizes
// from ballooning.
var MaxLockKeyBytesPerCleanupBatch = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.gc.intent_cleanup_batch_byte_size",
	"if non zero, gc will split found locks into batches of this size when trying to resolve them",
	1e6,
	settings.NonNegativeInt,
	settings.WithName("kv.gc.lock_cleanup_batch_byte_size"),
)

// ClearRangeMinKeys is a minimum number of keys that GC will consider
// for executing a ClearRange request. Since ClearRange requests translate into
// pebble tombstones, they should be used on large sequence of keys only.
var ClearRangeMinKeys = settings.RegisterIntSetting(settings.SystemOnly,
	"kv.gc.clear_range_min_keys",
	"if non zero, gc will issue clear range requests if number of consecutive garbage keys exceeds this threshold",
	defaultClearRangeMinKeys,
	settings.NonNegativeInt,
)

// AdmissionPriority determines the admission priority level to use for MVCC GC
// work.
var AdmissionPriority = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"kv.gc.admission_priority",
	"the admission priority to use for mvcc gc work",
	"bulk_normal_pri",
	map[admissionpb.WorkPriority]string{
		admissionpb.BulkNormalPri: "bulk_normal_pri",
		admissionpb.NormalPri:     "normal_pri",
		admissionpb.UserHighPri:   "user_high_pri",
	},
)

// CalculateThreshold calculates the GC threshold given the policy and the
// current view of time.
func CalculateThreshold(now hlc.Timestamp, gcttl time.Duration) (threshold hlc.Timestamp) {
	ttlNanos := gcttl.Nanoseconds()
	return now.Add(-ttlNanos, 0)
}

// TimestampForThreshold inverts CalculateThreshold. It returns the timestamp
// which should be used for now to arrive at the passed threshold.
func TimestampForThreshold(threshold hlc.Timestamp, gcttl time.Duration) (ts hlc.Timestamp) {
	ttlNanos := gcttl.Nanoseconds()
	return threshold.Add(ttlNanos, 0)
}

// Thresholder is part of the GCer interface.
type Thresholder interface {
	SetGCThreshold(context.Context, Threshold) error
}

// PureGCer is part of the GCer interface.
type PureGCer interface {
	GC(context.Context, []kvpb.GCRequest_GCKey, []kvpb.GCRequest_GCRangeKey,
		*kvpb.GCRequest_GCClearRange,
	) error
}

// A GCer is an abstraction used by the MVCC GC queue to carry out chunked deletions.
type GCer interface {
	Thresholder
	PureGCer
}

// NoopGCer implements GCer by doing nothing.
type NoopGCer struct{}

var _ GCer = NoopGCer{}

// SetGCThreshold implements storage.GCer.
func (NoopGCer) SetGCThreshold(context.Context, Threshold) error { return nil }

// GC implements storage.GCer.
func (NoopGCer) GC(
	context.Context,
	[]kvpb.GCRequest_GCKey,
	[]kvpb.GCRequest_GCRangeKey,
	*kvpb.GCRequest_GCClearRange,
) error {
	return nil
}

// Threshold holds the key and txn span GC thresholds, respectively.
type Threshold struct {
	Key hlc.Timestamp
	Txn hlc.Timestamp
}

// Info contains statistics and insights from a GC run.
type Info struct {
	// Now is the timestamp used for age computations.
	Now hlc.Timestamp
	// GCTTL is the TTL this garbage collection cycle.
	GCTTL time.Duration
	// Stats about the userspace key-values considered, namely the number of
	// keys with GC'able data, the number of "old" locks and the number of
	// associated distinct transactions.
	NumKeysAffected, NumRangeKeysAffected, LocksConsidered, LockTxns int
	// TransactionSpanTotal is the total number of entries in the transaction span.
	TransactionSpanTotal int
	// Summary of transactions which were found GCable (assuming that
	// potentially necessary intent resolutions did not fail).
	TransactionSpanGCAborted, TransactionSpanGCCommitted int
	TransactionSpanGCStaging, TransactionSpanGCPending   int
	TransactionSpanGCPrepared                            int
	// AbortSpanTotal is the total number of transactions present in the AbortSpan.
	AbortSpanTotal int
	// AbortSpanConsidered is the number of AbortSpan entries old enough to be
	// considered for removal. An "entry" corresponds to one transaction;
	// more than one key-value pair may be associated with it.
	AbortSpanConsidered int
	// AbortSpanGCNum is the number of AbortSpan entries fit for removal (due
	// to their transactions having terminated).
	AbortSpanGCNum int
	// PushTxn is the total number of pushes attempted in this cycle. Note that we
	// could try to push single transaction multiple times because of intent
	// batching so this number is equal or greater than actual number of transactions
	// pushed.
	PushTxn int
	// ResolveTotal is the total number of attempted intent resolutions in
	// this cycle.
	ResolveTotal int
	// Threshold is the computed expiration timestamp. Equal to `Now - GCTTL`.
	Threshold hlc.Timestamp
	// AffectedVersionsKeyBytes is the number of (fully encoded) bytes deleted from keys in the storage engine.
	// Note that this does not account for compression that the storage engine uses to store data on disk. Real
	// space savings tends to be smaller due to this compression, and space may be released only at a later point
	// in time.
	AffectedVersionsKeyBytes int64
	// AffectedVersionsValBytes is the number of (fully encoded) bytes deleted from values in the storage engine.
	// See AffectedVersionsKeyBytes for caveats.
	AffectedVersionsValBytes int64
	// AffectedVersionsRangeKeyBytes is the number of (fully encoded) bytes deleted from range keys.
	// For this counter, we don't count start and end key unless all versions are deleted, but we
	// do count timestamp size for each version.
	AffectedVersionsRangeKeyBytes int64
	// AffectedVersionsRangeValBytes is the number of (fully encoded) bytes deleted from values that
	// belong to removed range keys.
	AffectedVersionsRangeValBytes int64
	// ClearRangeSpanOperations number of ClearRange requests performed by GC. This
	// number includes full range clear requests as well as requests covering
	// multiple keys or versions of the same key.
	ClearRangeSpanOperations int
	// ClearRangeSpanFailures number of ClearRange requests GC failed to perform.
	ClearRangeSpanFailures int
}

// RunOptions contains collection of limits that GC run applies when performing operations
type RunOptions struct {
	// LockAgeThreshold is the minimum age a lock must have before this GC run
	// tries to resolve the lock.
	LockAgeThreshold time.Duration
	// MaxLocksPerIntentCleanupBatch is the maximum number of lock resolution requests passed
	// to the intent resolver in a single batch. Helps reducing memory impact of cleanup operations.
	MaxLocksPerIntentCleanupBatch int64
	// MaxLockKeyBytesPerIntentCleanupBatch similar to MaxLocksPerIntentCleanupBatch but counts
	// number of bytes lock keys occupy.
	MaxLockKeyBytesPerIntentCleanupBatch int64
	// MaxTxnsPerIntentCleanupBatch is a maximum number of txns passed to intent resolver to
	// process in one go. This number should be lower than intent resolver default to
	// prevent further splitting in resolver.
	MaxTxnsPerIntentCleanupBatch int64
	// IntentCleanupBatchTimeout is the timeout for processing a batch of intents. 0 to disable.
	IntentCleanupBatchTimeout time.Duration
	// TxnCleanupThreshold is the threshold after which a transaction is
	// considered abandoned and fit for removal, as measured by the maximum of
	// its last heartbeat and read timestamp.
	TxnCleanupThreshold time.Duration
	// ClearRangeMinKeys is minimum number of keys to delete with a ClearRange
	// operation. If there's less than this number of keys to delete, GC would
	// fall back to point deletions.
	// 0 means usage of clear range requests is disabled.
	ClearRangeMinKeys int64
	// MaxKeyVersionChunkBytes is the max size of keys for all versions of objects a single gc
	// batch would contain. This includes not only keys within the request, but also all versions
	// covered below request keys. This is important because of the resulting raft command size
	// generated in response to GC request.
	MaxKeyVersionChunkBytes int64
	// MaxPendingKeysSize maximum amount of bytes of pending GC batches kept in
	// memory while searching for eligible clear range span. If span of minimum
	// configured length is not found before this limit is reached, gc will resort
	// to issuing point delete requests for the oldest batch to free up memory
	// before resuming further iteration.
	MaxPendingKeysSize int64
}

// CleanupIntentsFunc synchronously resolves the supplied intents
// (which may be PENDING, in which case they are first pushed) while
// taking care of proper batching.
type CleanupIntentsFunc func(context.Context, []roachpb.Lock) error

// CleanupTxnIntentsAsyncFunc asynchronously cleans up intents from a
// transaction record, pushing the transaction first if it is
// PENDING. Once all intents are resolved successfully, removes the
// transaction record.
type CleanupTxnIntentsAsyncFunc func(context.Context, *roachpb.Transaction) error

// Run runs garbage collection for the specified range on the
// provided snapshot (which is not mutated). It uses the provided GCer
// to run garbage collection once on all implicated spans,
// cleanupIntentsFn to resolve intents synchronously, and
// cleanupTxnIntentsAsyncFn to asynchronously cleanup intents and
// associated transaction record on success.
func Run(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap storage.Reader,
	now, newThreshold hlc.Timestamp,
	options RunOptions,
	gcTTL time.Duration,
	gcer GCer,
	cleanupIntentsFn CleanupIntentsFunc,
	cleanupTxnIntentsAsyncFn CleanupTxnIntentsAsyncFunc,
) (Info, error) {

	txnExp := now.Add(-options.TxnCleanupThreshold.Nanoseconds(), 0)
	if err := gcer.SetGCThreshold(ctx, Threshold{
		Key: newThreshold,
		Txn: txnExp,
	}); err != nil {
		return Info{}, errors.Wrap(err, "failed to set GC thresholds")
	}

	info := Info{
		GCTTL:     gcTTL,
		Now:       now,
		Threshold: newThreshold,
	}

	// Process all replicated locks first and resolve any that have been around
	// for longer than the LockAgeThreshold.
	err := processReplicatedLocks(ctx, desc, snap, now, options.LockAgeThreshold,
		intentBatcherOptions{
			maxLocksPerIntentCleanupBatch:        options.MaxLocksPerIntentCleanupBatch,
			maxLockKeyBytesPerIntentCleanupBatch: options.MaxLockKeyBytesPerIntentCleanupBatch,
			maxTxnsPerIntentCleanupBatch:         options.MaxTxnsPerIntentCleanupBatch,
			intentCleanupBatchTimeout:            options.IntentCleanupBatchTimeout,
		}, cleanupIntentsFn, &info)
	if err != nil {
		return Info{}, err
	}
	fastPath, err := processReplicatedKeyRange(ctx, desc, snap, newThreshold,
		populateBatcherOptions(options), gcer, &info)
	if err != nil {
		return Info{}, err
	}
	err = processReplicatedRangeTombstones(ctx, desc, snap, fastPath, now, newThreshold, gcer, &info)
	if err != nil {
		return Info{}, err
	}

	// From now on, all keys processed are range-local and inline (zero timestamp).

	// Process local range key entries (txn records, queue last processed times).
	if err := processLocalKeyRange(ctx, snap, desc, txnExp, &info, cleanupTxnIntentsAsyncFn,
		gcer); err != nil {
		if errors.Is(err, ctx.Err()) {
			return Info{}, err
		}
		log.Warningf(ctx, "while gc'ing local key range: %s", err)
	}

	// Clean up the AbortSpan.
	log.Event(ctx, "processing AbortSpan")
	if err := processAbortSpan(ctx, snap, desc.RangeID, txnExp, &info, gcer); err != nil {
		if errors.Is(err, ctx.Err()) {
			return Info{}, err
		}
		log.Warningf(ctx, "while gc'ing abort span: %s", err)
	}

	log.Eventf(ctx, "GC'ed keys; stats %+v", info)

	return info, nil
}

func populateBatcherOptions(options RunOptions) gcKeyBatcherThresholds {
	batcherOptions := gcKeyBatcherThresholds{
		clearRangeMinKeys:         int(options.ClearRangeMinKeys),
		batchGCKeysBytesThreshold: options.MaxKeyVersionChunkBytes,
		maxPendingKeysSize:        int(options.MaxPendingKeysSize),
	}
	if batcherOptions.clearRangeMinKeys > 0 {
		batcherOptions.clearRangeEnabled = true
	}
	if batcherOptions.batchGCKeysBytesThreshold == 0 {
		batcherOptions.batchGCKeysBytesThreshold = KeyVersionChunkBytes
	}
	if batcherOptions.maxPendingKeysSize == 0 {
		batcherOptions.maxPendingKeysSize = defaultMaxPendingKeysSize
	}
	return batcherOptions
}

// processReplicatedKeyRange identifies garbage and sends GC requests to
// remove it.
//
// The logic iterates all versions of all keys in the range from oldest to
// newest. Intents are not handled by this function; they're simply skipped
// over. Returns true if clear range was used to remove user data.
func processReplicatedKeyRange(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap storage.Reader,
	threshold hlc.Timestamp,
	batcherThresholds gcKeyBatcherThresholds,
	gcer PureGCer,
	info *Info,
) (bool, error) {
	// Perform fast path check prior to performing GC. Fast path only collects
	// user key span portion, so we don't need to clean it up once again if
	// we succeeded.
	excludeUserKeySpan := false
	{
		start := desc.StartKey.AsRawKey()
		end := desc.EndKey.AsRawKey()
		if coveredByRangeTombstone, err := storage.CanGCEntireRange(ctx, snap, start, end,
			threshold); err == nil && coveredByRangeTombstone {
			if err = gcer.GC(ctx, nil, nil, &kvpb.GCRequest_GCClearRange{
				StartKey: start,
				EndKey:   end,
			}); err == nil {
				excludeUserKeySpan = true
				info.ClearRangeSpanOperations++
			} else {
				log.Warningf(ctx, "failed to perform GC clear range operation on range %s: %s",
					desc.String(), err)
				info.ClearRangeSpanFailures++
			}
		}
	}

	return excludeUserKeySpan, rditer.IterateMVCCReplicaKeySpans(
		ctx, desc, snap, rditer.IterateOptions{
			CombineRangesAndPoints: true,
			Reverse:                true,
			ExcludeUserKeySpan:     excludeUserKeySpan,
			ReadCategory:           fs.MVCCGCReadCategory,
		}, func(iterator storage.MVCCIterator, span roachpb.Span, keyType storage.IterKeyType) error {
			// Iterate all versions of all keys from oldest to newest. If a version is an
			// intent it will have the highest timestamp of any versions and will be
			// followed by a metadata entry.
			// The loop determines if next object is garbage, non-garbage or intent and
			// notifies batcher with its detail. Batcher is responsible for accumulating
			// pending key data and sending sending keys to GCer as needed.
			// It could also request the main loop to rewind to a previous point to
			// retry (this is needed when attempt to collect a clear range batch fails
			// in the middle of key versions).
			it := makeGCIterator(iterator, threshold)

			b := gcKeyBatcher{
				gcKeyBatcherThresholds: batcherThresholds,
				gcer:                   gcer,
				info:                   info,
				pointsBatches:          make([]pointsBatch, 1),
				// We must clone here as we reuse key slice to avoid realocating on every
				// key.
				clearRangeEndKey: span.EndKey.Clone(),
				prevWasNewest:    true,
			}

			for ; ; it.step() {
				var err error

				s, ok := it.state()
				if !ok {
					if it.err != nil {
						return it.err
					}
					break
				}

				switch {
				case s.curIsNotValue():
					// Skip over non mvcc data.
					err = b.foundNonGCableData(ctx, s.cur, true /* isNewestPoint */)
				case s.curIsIntent():
					// Skip over intents; they cannot be GC-ed. We simply ignore them --
					// processReplicatedLocks will resolve them, if necessary.
					err = b.foundNonGCableData(ctx, s.cur, true /* isNewestPoint */)
					if err != nil {
						return err
					}
					// Force step over the intent metadata as well to move on to the next
					// key.
					it.step()
				default:
					if isGarbage(threshold, s.cur, s.next, s.curIsNewest(), s.firstRangeTombstoneTsAtOrBelowGC) {
						err = b.foundGarbage(ctx, s.cur, s.curLastKeyVersion())
					} else {
						err = b.foundNonGCableData(ctx, s.cur, s.curLastKeyVersion())
					}
				}
				if err != nil {
					return err
				}
			}

			return b.flushLastBatch(ctx)
		})
}

// processReplicatedLocks identifies extant replicated locks which have been
// around longer than the supplied lockAgeThreshold and resolves them.
func processReplicatedLocks(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	reader storage.Reader,
	now hlc.Timestamp,
	lockAgeThreshold time.Duration,
	options intentBatcherOptions,
	cleanupIntentsFn CleanupIntentsFunc,
	info *Info,
) error {
	// Compute lock expiration (lock age at which we attempt to resolve).
	lockExp := now.Add(-lockAgeThreshold.Nanoseconds(), 0)
	intentBatcher := newIntentBatcher(cleanupIntentsFn, options, info)

	process := func(ltStartKey, ltEndKey roachpb.Key) error {
		opts := storage.LockTableIteratorOptions{
			LowerBound:   ltStartKey,
			UpperBound:   ltEndKey,
			MatchMinStr:  lock.Shared, // any strength
			ReadCategory: fs.MVCCGCReadCategory,
		}
		iter, err := storage.NewLockTableIterator(ctx, reader, opts)
		if err != nil {
			return err
		}
		defer iter.Close()

		var ok bool
		for ok, err = iter.SeekEngineKeyGE(storage.EngineKey{Key: ltStartKey}); ok; ok, err = iter.NextEngineKey() {
			if err != nil {
				return err
			}
			var meta enginepb.MVCCMetadata
			err = iter.ValueProto(&meta)
			if err != nil {
				return err
			}
			if meta.Txn == nil {
				return errors.AssertionFailedf("lock without transaction")
			}
			// Keep track of lock to resolve if it is older than the expiration
			// threshold.
			if meta.Timestamp.ToTimestamp().Less(lockExp) {
				info.LocksConsidered++
				key, err := iter.EngineKey()
				if err != nil {
					return err
				}
				ltKey, err := key.ToLockTableKey()
				if err != nil {
					return err
				}
				if err := intentBatcher.addAndMaybeFlushIntents(ctx, ltKey.Key, ltKey.Strength, &meta); err != nil {
					if errors.Is(err, ctx.Err()) {
						return err
					}
					log.Warningf(ctx, "failed to cleanup intents batch: %v", err)
				}
			}
		}
		return nil
	}

	// We want to find/resolve replicated locks over both local and global
	// keys. That's what the call to Select below will give us.
	ltSpans := rditer.Select(desc.RangeID, rditer.SelectOpts{
		ReplicatedBySpan:      desc.RSpan(),
		ReplicatedSpansFilter: rditer.ReplicatedSpansLocksOnly,
	})
	for _, sp := range ltSpans {
		if err := process(sp.Key, sp.EndKey); err != nil {
			return err
		}
	}

	// We need to send out last intent cleanup batch, if present.
	if err := intentBatcher.maybeFlushPendingIntents(ctx); err != nil {
		if errors.Is(err, ctx.Err()) {
			return err
		}
		log.Warningf(ctx, "failed to cleanup intents batch: %v", err)
	}
	return nil
}

// gcBatchCounters contain statistics about garbage that is collected for the
// range of keys.
type gcBatchCounters struct {
	keyBytes, valBytes int64
	// keysAffected includes key when it is first encountered, but would not be
	// included if key versions were split between batches.
	keysAffected int
	// memUsed is amount of bytes batch takes in memory (sum of all keys and
	// timestamps excluding any supporting structures).
	memUsed int
	// versionsAffected is a number of key versions covered by batch. This is
	// used for clear range accounting.
	versionsAffected int
}

func (c gcBatchCounters) updateGcInfo(info *Info) {
	info.AffectedVersionsKeyBytes += c.keyBytes
	info.AffectedVersionsValBytes += c.valBytes
	info.NumKeysAffected += c.keysAffected
}

func (c gcBatchCounters) String() string {
	return fmt.Sprintf("Counters{keyBytes=%d, valBytes=%d, keys=%d, versions=%d, memUsed=%d}",
		c.keyBytes, c.valBytes, c.keysAffected, c.versionsAffected, c.memUsed)
}

// gcKeyBatcherThresholds collection configuration options.
type gcKeyBatcherThresholds struct {
	batchGCKeysBytesThreshold int64
	clearRangeMinKeys         int
	clearRangeEnabled         bool
	maxPendingKeysSize        int
}

type pointsBatch struct {
	gcBatchCounters
	batchGCKeys []kvpb.GCRequest_GCKey
	alloc       bufalloc.ByteAllocator
}

func (b pointsBatch) String() string {
	return fmt.Sprintf("PointsBatch{keyCount=%d, counters=%s}",
		len(b.batchGCKeys), b.gcBatchCounters.String())
}

// gcKeyBatcher is responsible for collecting MVCCKeys and feeding them to GCer
// for removal.
// It is receiving notifications if next key is garbage or not and makes decision
// how to batch the data and weather to use point deletion requests or clear
// range requests.
// Batcher will create GC requests containing either collection of individual
// mvcc keys (batch) or clear range span. It will look on three thresholds when
// building a request:
//   - clearRangeMinKeys - minimum number of sequential keys eligible for clear
//     range
//   - batchGCKeysBytesThreshold - maximum byte size of all deleted key versions
//     per request
//   - maxPendingKeysSize - maximum number of bytes taken by pending requests in
//     memory
//
// Batcher tries to find the longest sequence of keys to delete and if the
// sequence is not found (because of non collectable data inbetween) it will
// resort to sending request with point deletions.
// To achieve this behaviour batcher will collect point batches in parallel to
// tracking the sequence length. Once minimum sequence length is reached, it
// stops collecting point batches since we are guaranteed to send a clear range.
// If however it reaches maxPendingKeysSize limit before clearRangeMinKeys is
// met (that would happen if keys are particularly large) then the oldest batch
// is sent, and clear range end is moved to the lowest key if sent batch.
// Once non-garbage key is found, if sequence length is at or above
// clearRangeMinKeys threshold, GC will first send relevant portion of first
// points batch (up to the first key covered by clear range) and then clear
// range request for the remainder of keys. After that it drops all pending
// batches.
// If however the sequence is too short then batcher will send all previous
// batches up to the current one. Record current key as an end of clear range
// span (remember that we are iterating from back to front), record current
// batch counters in partialPointBatchCounters and restart sequence
// search. partialPointBatchCounters is required to make accurate counter
// updates when clear range starts mid points batch. It covers part of the
// first batch from its beginning to the point where clear range could start,
// and remainder of the batch is covered by counters for clear range span.
// Described behaviour support the following pending batches invariant:
//   - only the oldest batch in the history could be split by non-garbage key in
//     the middle, other batches always cover contiguous sets of keys.
type gcKeyBatcher struct {
	gcKeyBatcherThresholds

	// prevWasNewest indicates that we are at oldest version of the key.
	// its value is a saved isNewest from previous key.
	prevWasNewest bool
	// Points batches are accumulated until we cross clear range threshold.
	// There's always at least one points batch initialized.
	pointsBatches []pointsBatch
	// totalMemUsed sum of memUsed of all points batches cached to avoid
	// recomputing on every check.
	totalMemUsed int

	// Tracking of clear range requests, only used if clearRangeEnabled is true.

	// clearRangeEndKey contains a key following first garbage key found. It would
	// be greater or equal to the last key (lowest, collected last) of the first
	// (collected first, containing highest keys) points batch. It is guaranteed
	// that there are no non-garbage keys between this key and the start of first
	// batch.
	// Updated whenever batcher is provided with latest version of non garbage
	// key or when oldest points batch is flushed.
	clearRangeEndKey roachpb.Key
	// Last garbage key found. Its Key component is updated whenever new key is
	// found, timestamp component updated on each garbage key. Its slice is shared
	// with points batch allocator if batcher didn't reach min key size, or shared
	// with clearRangeStartKeyAlloc which is used to eliminate unnecessary
	// allocations for the start key.
	clearRangeStartKey storage.MVCCKey
	// We use this key slice to avoid allocating for every new key. We can't
	// blindly reuse slice in clearRangeStartKey because we avoid allocations
	// for clearRangeStartKey if they could be shared with points batch.
	// This is saving us any extra key copying if we can't find enough consecutive
	// keys.
	clearRangeStartKeyAlloc roachpb.Key
	// GC Counters for data covered by currently tracked clear range span.
	clearRangeCounters gcBatchCounters
	// Stats counters saved from points batch when clear range tracking started.
	// Those stats combined with clearRangeCounters cover all unprocessed keys
	// since last sent request.
	partialPointBatchCounters gcBatchCounters

	gcer PureGCer
	info *Info
}

func (b *gcKeyBatcher) String() string {
	pbs := make([]string, len(b.pointsBatches))
	for i, pb := range b.pointsBatches {
		pbs[i] = pb.String()
	}
	return fmt.Sprintf("gcKeyBatcher{batches=[%s], clearRangeCnt=%s, partialBatchCnt=%s, totalMem=%d}",
		strings.Join(pbs, ", "),
		b.clearRangeCounters.String(),
		b.partialPointBatchCounters.String(),
		b.totalMemUsed)
}

func (b *gcKeyBatcher) foundNonGCableData(
	ctx context.Context, cur *mvccKeyValue, isNewestPoint bool,
) (err error) {
	b.prevWasNewest = isNewestPoint
	if !b.clearRangeEnabled {
		return nil
	}

	// Check if there are any complete clear range or point batches collected
	// and flush them as we reached end of current consecutive key span.
	err = b.maybeFlushPendingBatches(ctx)
	if err != nil {
		return err
	}

	if isNewestPoint {
		b.clearRangeEndKey = append(b.clearRangeEndKey[:0], cur.key.Key...)
	}
	return nil
}

func (b *gcKeyBatcher) foundGarbage(
	ctx context.Context, cur *mvccKeyValue, isNewestPoint bool,
) (err error) {
	// If we are restarting clear range collection, then last points batch might
	// be reverted to partial at the time of flush. We will save its
	// pre-clear-range state and use for GC stats update.
	if b.clearRangeEnabled && b.prevWasNewest && b.clearRangeCounters.versionsAffected == 0 {
		b.partialPointBatchCounters = b.pointsBatches[len(b.pointsBatches)-1].gcBatchCounters
	}

	var key roachpb.Key
	var keySize = int64(cur.key.EncodedSize())
	// If still collecting points, add to points first.
	if !b.clearRangeEnabled || b.clearRangeCounters.versionsAffected < b.clearRangeMinKeys {
		// First check if current batch is full and add a new points batch.
		i := len(b.pointsBatches) - 1
		if b.pointsBatches[i].gcBatchCounters.keyBytes >= b.batchGCKeysBytesThreshold {
			// If clear range is disabled, flush batches immediately as they are
			// formed.
			if !b.clearRangeEnabled {
				if err = b.flushPointsBatch(ctx, &b.pointsBatches[i]); err != nil {
					return err
				}
			} else {
				b.pointsBatches = append(b.pointsBatches, pointsBatch{})
				i++
			}
		}
		if b.prevWasNewest {
			b.pointsBatches[i].gcBatchCounters.keysAffected++
		}
		// Whenever new key is started or new batch is started with the same key in
		// it, record key value using batches' allocator.
		if b.prevWasNewest || len(b.pointsBatches[i].batchGCKeys) == 0 {
			b.pointsBatches[i].alloc, key = b.pointsBatches[i].alloc.Copy(cur.key.Key, 0)
			b.pointsBatches[i].batchGCKeys = append(b.pointsBatches[i].batchGCKeys,
				kvpb.GCRequest_GCKey{Key: key, Timestamp: cur.key.Timestamp})
			keyMemUsed := len(key) + hlcTimestampSize
			b.pointsBatches[i].memUsed += keyMemUsed
			b.totalMemUsed += keyMemUsed
		} else {
			// For already registered key just bump the timestamp to the newer one.
			b.pointsBatches[i].batchGCKeys[len(b.pointsBatches[i].batchGCKeys)-1].Timestamp = cur.key.Timestamp
		}
		b.pointsBatches[i].gcBatchCounters.keyBytes += keySize
		b.pointsBatches[i].gcBatchCounters.valBytes += int64(cur.mvccValueLen)
		b.pointsBatches[i].gcBatchCounters.versionsAffected++

		// Check if we exceeded in memory bytes limit allowed for points collection.
		if i > 0 && b.totalMemUsed >= b.maxPendingKeysSize {
			lastKey := b.pointsBatches[0].batchGCKeys[len(b.pointsBatches[0].batchGCKeys)-1].Key
			// If oldest batch intersected with currently tracked clear range request
			// then bump clear range end key to the one that follows lowest key of the
			// cleared batch. We know that all keys for current key with lower
			// timestamps are eligible for collection and end range is exclusive.
			// The check is needed because clearRangeEndKey cloud be bumped already
			// to current key by non gc data on the previous key, but only now we
			// exceeded the size threshold and trying to remove oldest batch.
			if b.clearRangeEndKey.Compare(lastKey) > 0 {
				flushedCounters := b.pointsBatches[0].gcBatchCounters
				// If flushed batch was split by start of the range, then we must remove
				// preceding part from clear range total to avoid.
				b.clearRangeCounters.keyBytes += b.partialPointBatchCounters.keyBytes - flushedCounters.keyBytes
				b.clearRangeCounters.valBytes += b.partialPointBatchCounters.valBytes - flushedCounters.valBytes
				b.clearRangeCounters.keysAffected += b.partialPointBatchCounters.keysAffected - flushedCounters.keysAffected
				b.clearRangeCounters.versionsAffected += b.partialPointBatchCounters.versionsAffected - flushedCounters.versionsAffected
				b.clearRangeCounters.memUsed += b.partialPointBatchCounters.memUsed - flushedCounters.memUsed
				b.clearRangeEndKey = lastKey[:len(lastKey):len(lastKey)].Next()
			}
			b.partialPointBatchCounters = gcBatchCounters{}

			// We accumulated more keys in memory than allowed by thresholds, we need
			// to flush oldest batch to protect node from exhausting memory.
			err := b.flushOldestPointBatches(ctx, 1)
			if err != nil {
				return err
			}
		}
	}

	if b.clearRangeEnabled {
		if b.prevWasNewest {
			if key == nil {
				// Reuse key slice for start range to avoid allocating on every key
				// change while tracking clear range.
				b.clearRangeStartKeyAlloc = append(b.clearRangeStartKeyAlloc[:0], cur.key.Key...)
				key = b.clearRangeStartKeyAlloc
			}
			b.clearRangeStartKey = storage.MVCCKey{Key: key}
			b.clearRangeCounters.keysAffected++
		}
		b.clearRangeStartKey.Timestamp = cur.key.Timestamp
		b.clearRangeCounters.keyBytes += keySize
		b.clearRangeCounters.valBytes += int64(cur.mvccValueLen)
		b.clearRangeCounters.versionsAffected++
	}

	b.prevWasNewest = isNewestPoint
	return nil
}

// maybeFlushPendingBatches flushes accumulated GC requests. If clear range is
// used and current data is eligible for deletion then pending points batch and
// clear range request are flushed.
// If there's not enough point keys to justify clear range, then point point
// batches may be flushed:
//   - if there's a single batch and it didn't reach desired size, nothing is
//     flushed
//   - if there's more than single points batch accumulated, then all batches
//     except for last are flushed
func (b *gcKeyBatcher) maybeFlushPendingBatches(ctx context.Context) (err error) {
	if b.clearRangeEnabled && b.clearRangeCounters.versionsAffected >= b.clearRangeMinKeys {
		// Optionally flush parts of the first batch if it is not fully covered by
		// pending clear range span.
		batchLen := len(b.pointsBatches[0].batchGCKeys)
		// Find a key that is equal or less than end of clear range key.
		// Batch is sorted in the reverse order, timestamps are not relevant since
		// we can't have the same key more than once within single points batch.
		lastIdx := sort.Search(batchLen, func(i int) bool {
			return b.pointsBatches[0].batchGCKeys[i].Key.Compare(b.clearRangeEndKey) < 0
		})
		if lastIdx == 0 {
			// If lastIdx == 0 we can have a clear range request which goes back to
			// previous key completely covering this range. If this is true then we
			// don't need to handle this batch and can tighten the clear range span
			// to cover only up to the start of first batch.
			b.clearRangeEndKey = b.pointsBatches[0].batchGCKeys[0].Key.Next()
		} else {
			b.pointsBatches[0].batchGCKeys = b.pointsBatches[0].batchGCKeys[:lastIdx]
			b.pointsBatches[0].gcBatchCounters = b.partialPointBatchCounters
			err := b.flushPointsBatch(ctx, &b.pointsBatches[0])
			if err != nil {
				return err
			}
		}
		b.pointsBatches = make([]pointsBatch, 1)

		// Flush clear range.
		// To reduce locking contention between keys, if we have a range that only
		// covers multiple versions of a single key we could avoid latching range
		// and fall back to poing GC behaviour where gc threshold guards against
		// key interference. We do it by setting end key to StartKey.Next() and thus
		// signalling gc request handler to skip locking.
		endRange := b.clearRangeEndKey
		if b.clearRangeCounters.keysAffected == 1 {
			endRange = b.clearRangeStartKey.Key.Next()
		}
		if err := b.gcer.GC(ctx, nil, nil, &kvpb.GCRequest_GCClearRange{
			StartKey:          b.clearRangeStartKey.Key,
			StartKeyTimestamp: b.clearRangeStartKey.Timestamp,
			EndKey:            endRange,
		}); err != nil {
			if errors.Is(err, ctx.Err()) {
				return err
			}
			// Even though we are batching the GC process, it's
			// safe to continue because we bumped the GC
			// thresholds. We may leave some inconsistent history
			// behind, but nobody can read it.
			log.Warningf(ctx, "failed to GC keys with clear range: %v", err)
			b.info.ClearRangeSpanFailures++
		}
		b.clearRangeCounters.updateGcInfo(b.info)
		b.info.ClearRangeSpanOperations++
		b.totalMemUsed = 0
	} else if flushTo := len(b.pointsBatches) - 1; flushTo > 0 {
		err := b.flushOldestPointBatches(ctx, flushTo)
		if err != nil {
			return err
		}
	}
	b.clearRangeCounters = gcBatchCounters{}
	b.clearRangeEndKey = nil
	return nil
}

// flushOldestPointBatches flushes oldest points batch and returns its counters.
// unsafeLastKey must not be retained as it will prevent batch allocator to be
// released.
func (b *gcKeyBatcher) flushOldestPointBatches(ctx context.Context, count int) (err error) {
	var i int
	for i = 0; i < count; i++ {
		if err = b.flushPointsBatch(ctx, &b.pointsBatches[i]); err != nil {
			return err
		}
	}
	remaining := len(b.pointsBatches) - i
	copy(b.pointsBatches[0:remaining], b.pointsBatches[i:])
	// Zero out remaining batches to free app allocators.
	for i = remaining; i < len(b.pointsBatches); i++ {
		b.pointsBatches[i] = pointsBatch{}
	}
	b.pointsBatches = b.pointsBatches[:remaining]
	return nil
}

// flushPointsBatch flushes points batch and zeroes out its content.
func (b *gcKeyBatcher) flushPointsBatch(ctx context.Context, batch *pointsBatch) (err error) {
	if err := b.gcer.GC(ctx, batch.batchGCKeys, nil, nil); err != nil {
		if errors.Is(err, ctx.Err()) {
			return err
		}
		// Even though we are batching the GC process, it's
		// safe to continue because we bumped the GC
		// thresholds. We may leave some inconsistent history
		// behind, but nobody can read it.
		log.Warningf(ctx, "failed to GC a batch of keys: %v", err)
	}
	batch.gcBatchCounters.updateGcInfo(b.info)
	b.totalMemUsed -= batch.gcBatchCounters.memUsed
	*batch = pointsBatch{}
	return nil
}

func (b *gcKeyBatcher) flushLastBatch(ctx context.Context) (err error) {
	if len(b.pointsBatches[0].batchGCKeys) == 0 {
		return nil
	}
	b.pointsBatches = append(b.pointsBatches, pointsBatch{})
	return b.maybeFlushPendingBatches(ctx)
}

type intentBatcher struct {
	cleanupIntentsFn CleanupIntentsFunc

	options intentBatcherOptions

	// Maps from txn ID to bool and intent slice to accumulate a batch.
	pendingTxns          map[uuid.UUID]bool
	pendingLocks         []roachpb.Lock
	collectedIntentBytes int64

	alloc bufalloc.ByteAllocator

	gcStats *Info
}

type intentBatcherOptions struct {
	maxLocksPerIntentCleanupBatch        int64
	maxLockKeyBytesPerIntentCleanupBatch int64
	maxTxnsPerIntentCleanupBatch         int64
	intentCleanupBatchTimeout            time.Duration
}

// newIntentBatcher initializes an intentBatcher. Batcher will take ownership of
// provided *Info object while doing cleanup and update its counters.
func newIntentBatcher(
	cleanupIntentsFunc CleanupIntentsFunc, options intentBatcherOptions, gcStats *Info,
) intentBatcher {
	if options.maxLocksPerIntentCleanupBatch <= 0 {
		options.maxLocksPerIntentCleanupBatch = math.MaxInt64
	}
	if options.maxLockKeyBytesPerIntentCleanupBatch <= 0 {
		options.maxLockKeyBytesPerIntentCleanupBatch = math.MaxInt64
	}
	if options.maxTxnsPerIntentCleanupBatch <= 0 {
		options.maxTxnsPerIntentCleanupBatch = math.MaxInt64
	}
	return intentBatcher{
		cleanupIntentsFn: cleanupIntentsFunc,
		options:          options,
		pendingTxns:      make(map[uuid.UUID]bool),
		gcStats:          gcStats,
	}
}

// addAndMaybeFlushIntents collects intent for resolving batch, if
// any of batching limits is reached sends batch for resolution.
// Flushing is done retroactively e.g. if newly added intent would exceed
// the limits the batch would be flushed and new intent is saved for
// the subsequent batch.
// Returns error if batch flushing was needed and failed.
func (b *intentBatcher) addAndMaybeFlushIntents(
	ctx context.Context, key roachpb.Key, str lock.Strength, meta *enginepb.MVCCMetadata,
) error {
	var err error = nil
	txnID := meta.Txn.ID
	_, existingTransaction := b.pendingTxns[txnID]
	// Check batching thresholds if we need to flush collected data. Transaction
	// count is treated specially because we want to check it only when we find
	// a new transaction.
	if int64(len(b.pendingLocks)) >= b.options.maxLocksPerIntentCleanupBatch ||
		b.collectedIntentBytes >= b.options.maxLockKeyBytesPerIntentCleanupBatch ||
		!existingTransaction && int64(len(b.pendingTxns)) >= b.options.maxTxnsPerIntentCleanupBatch {
		err = b.maybeFlushPendingIntents(ctx)
	}

	// We need to register passed intent regardless of flushing operation result
	// so that batcher is left in consistent state and don't miss any keys if
	// caller resumes batching.
	b.alloc, key = b.alloc.Copy(key, 0)
	b.pendingLocks = append(b.pendingLocks, roachpb.MakeLock(meta.Txn, key, str))
	b.collectedIntentBytes += int64(len(key))
	b.pendingTxns[txnID] = true

	return err
}

// maybeFlushPendingIntents resolves currently collected intents.
func (b *intentBatcher) maybeFlushPendingIntents(ctx context.Context) error {
	if len(b.pendingLocks) == 0 {
		// If there's nothing to flush we will try to preserve context
		// for the sake of consistency with how flush behaves when context
		// is canceled during cleanup.
		return ctx.Err()
	}

	var err error
	cleanupIntentsFn := func(ctx context.Context) error {
		return b.cleanupIntentsFn(ctx, b.pendingLocks)
	}
	if b.options.intentCleanupBatchTimeout > 0 {
		err = timeutil.RunWithTimeout(
			ctx, "intent GC batch", b.options.intentCleanupBatchTimeout, cleanupIntentsFn)
	} else {
		err = cleanupIntentsFn(ctx)
	}
	if err == nil {
		// LockTxns and PushTxn will be equal here, since
		// pushes to transactions whose record lies in this
		// range (but which are not associated to a remaining
		// intent on it) happen asynchronously and are accounted
		// for separately. Thus higher up in the stack, we
		// expect PushTxn > LockTxns.
		b.gcStats.LockTxns += len(b.pendingTxns)
		// All transactions in pendingTxns may be PENDING and
		// cleanupIntentsFn will push them to finalize them.
		b.gcStats.PushTxn += len(b.pendingTxns)

		b.gcStats.ResolveTotal += len(b.pendingLocks)
	}

	// Get rid of current transactions and intents regardless of
	// status as we need to go on cleaning up without retries.
	for k := range b.pendingTxns {
		delete(b.pendingTxns, k)
	}
	b.pendingLocks = b.pendingLocks[:0]
	b.collectedIntentBytes = 0
	return err
}

// isGarbage makes a determination whether a key ('cur') is garbage.
// If its timestamp is below firstRangeTombstoneTsAtOrBelowGC then all versions
// were deleted by the range key regardless if they are isNewestPoint or not.
//
// If 'next' is non-nil, it should be the chronologically newer version of the
// same key (or the metadata KV if cur is an intent). If isNewest is false, next
// must be non-nil. isNewest implies that this is the highest timestamp committed
// version for this key. If isNewest is true and next is non-nil, it is an
// intent. Conservatively we have to assume that the intent will get aborted,
// so we will be able to GC just the values that we could remove if there
// weren't an intent. Hence this definition of isNewest.
//
// We keep all values (including deletes) above the expiration time, plus
// the first value before or at the expiration time. This allows reads to be
// guaranteed as described above. However if this were the only rule, then if
// the most recent write was a delete, it would never be removed. Thus, when a
// deleted value is the most recent before expiration, it can be deleted.
func isGarbage(
	threshold hlc.Timestamp,
	cur, next *mvccKeyValue,
	isNewestPoint bool,
	firstRangeTombstoneTsAtOrBelowGC hlc.Timestamp,
) bool {
	// If the value is not at or below the threshold then it's not garbage.
	if belowThreshold := cur.key.Timestamp.LessEq(threshold); !belowThreshold {
		return false
	}
	if cur.key.Timestamp.Less(firstRangeTombstoneTsAtOrBelowGC) {
		if util.RaceEnabled {
			if threshold.Less(firstRangeTombstoneTsAtOrBelowGC) {
				panic(fmt.Sprintf("gc attempt to remove key: using range tombstone %s above gc threshold %s",
					firstRangeTombstoneTsAtOrBelowGC.String(), threshold.String()))
			}
		}
		return true
	}
	isDelete := cur.mvccValueIsTombstone
	if isNewestPoint && !isDelete {
		return false
	}
	// INVARIANT: !isNewestPoint || isDelete
	// Therefore !isDelete => !isNewestPoint, which can be restated as
	// !isDelete => next != nil. We verify this invariant here.
	// If this value is not a delete, then we need to make sure that the next
	// value is also at or below the threshold.
	// NB: This doesn't need to check whether next is nil because we know
	// isNewestPoint is false when evaluating rhs of the or below.
	if !isDelete && next == nil {
		panic("huh")
	}
	return isDelete || next.key.Timestamp.LessEq(threshold)
}

// processLocalKeyRange scans the local range key entries, consisting of
// transaction records, queue last processed timestamps, and range descriptors.
//
// - Transaction entries:
//
//   - For expired transactions , schedule the intents for
//     asynchronous resolution. The actual transaction spans are not
//     returned for GC in this pass, but are separately GC'ed after
//     successful resolution of all intents. The exception is if there
//     are no intents on the txn record, in which case it's returned for
//     immediate GC.
//
//   - Queue last processed times: cleanup any entries which don't match
//     this range's start key. This can happen on range merges.
func processLocalKeyRange(
	ctx context.Context,
	snap storage.Reader,
	desc *roachpb.RangeDescriptor,
	cutoff hlc.Timestamp,
	info *Info,
	cleanupTxnIntentsAsyncFn CleanupTxnIntentsAsyncFunc,
	gcer PureGCer,
) error {
	b := makeBatchingInlineGCer(gcer, func(err error) {
		log.Warningf(ctx, "failed to GC from local key range: %s", err)
	})
	defer b.Flush(ctx)

	handleTxnIntents := func(key roachpb.Key, txn *roachpb.Transaction) error {
		// If the transaction needs to be pushed or there are intents to
		// resolve, invoke the cleanup function.
		if !txn.Status.IsFinalized() || len(txn.LockSpans) > 0 {
			return cleanupTxnIntentsAsyncFn(ctx, txn)
		}
		b.FlushingAdd(ctx, key)
		return nil
	}

	handleOneTransaction := func(kv roachpb.KeyValue) error {
		var txn roachpb.Transaction
		if err := kv.Value.GetProto(&txn); err != nil {
			return err
		}
		info.TransactionSpanTotal++
		if cutoff.LessEq(txn.LastActive()) {
			return nil
		}

		// The transaction record should be considered for removal.
		switch txn.Status {
		case roachpb.PENDING:
			info.TransactionSpanGCPending++
		case roachpb.PREPARED:
			info.TransactionSpanGCPrepared++
		case roachpb.STAGING:
			info.TransactionSpanGCStaging++
		case roachpb.ABORTED:
			info.TransactionSpanGCAborted++
		case roachpb.COMMITTED:
			info.TransactionSpanGCCommitted++
		default:
			panic(fmt.Sprintf("invalid transaction state: %s", txn))
		}
		return handleTxnIntents(kv.Key, &txn)
	}

	handleOneQueueLastProcessed := func(kv roachpb.KeyValue, rangeKey roachpb.RKey) error {
		if !rangeKey.Equal(desc.StartKey) {
			// Garbage collect the last processed timestamp if it doesn't match start key.
			b.FlushingAdd(ctx, kv.Key)
		}
		return nil
	}

	handleOne := func(kv roachpb.KeyValue) error {
		rangeKey, suffix, _, err := keys.DecodeRangeKey(kv.Key)
		if err != nil {
			return err
		}
		if suffix.Equal(keys.LocalTransactionSuffix.AsRawKey()) {
			if err := handleOneTransaction(kv); err != nil {
				return err
			}
		} else if suffix.Equal(keys.LocalQueueLastProcessedSuffix.AsRawKey()) {
			if err := handleOneQueueLastProcessed(kv, roachpb.RKey(rangeKey)); err != nil {
				return err
			}
		}
		return nil
	}

	startKey := keys.MakeRangeKeyPrefix(desc.StartKey)
	endKey := keys.MakeRangeKeyPrefix(desc.EndKey)

	_, err := storage.MVCCIterate(ctx, snap, startKey, endKey, hlc.Timestamp{},
		storage.MVCCScanOptions{ReadCategory: fs.MVCCGCReadCategory},
		func(kv roachpb.KeyValue) error {
			return handleOne(kv)
		})
	return err
}

// processAbortSpan iterates through the local AbortSpan entries
// and collects entries which indicate that a client which was running
// this transaction must have realized that it has been aborted (due to
// heartbeating having failed). The parameter minAge is typically a
// multiple of the heartbeat timeout used by the coordinator.
func processAbortSpan(
	ctx context.Context,
	snap storage.Reader,
	rangeID roachpb.RangeID,
	threshold hlc.Timestamp,
	info *Info,
	gcer PureGCer,
) error {
	b := makeBatchingInlineGCer(gcer, func(err error) {
		log.Warningf(ctx, "unable to GC from abort span: %s", err)
	})
	defer b.Flush(ctx)
	abortSpan := abortspan.New(rangeID)
	return abortSpan.Iterate(ctx, snap, func(key roachpb.Key, v roachpb.AbortSpanEntry) error {
		info.AbortSpanTotal++
		if v.Timestamp.Less(threshold) {
			info.AbortSpanGCNum++
			b.FlushingAdd(ctx, key)
		}
		return nil
	})
}

type rangeKeyBatcher struct {
	gcer      GCer
	batchSize int64

	pending     []storage.MVCCRangeKey
	pendingSize int64
}

// addAndMaybeFlushRangeKeys will try to add a range key stack to the existing
// batch and flush it if the batch is full.
// unsafeRangeKeyValues contains all range key values with the same key range
// that has to be GCd.
// To ensure the resulting batch is not too large, we need to account for all
// removed versions. This method will try to include versions from oldest to
// newest and will stop if we either reach batch size or reach the newest
// provided version. Only the last version of this iteration will be flushed.
// If more versions remained after flush, process would be resumed.
func (b *rangeKeyBatcher) addAndMaybeFlushRangeKeys(
	ctx context.Context, rangeKeys storage.MVCCRangeKeyStack,
) error {
	maxKey := rangeKeys.Len() - 1
	for i := maxKey; i >= 0; i-- {
		rangeKeySize := int64(rangeKeys.AsRangeKey(rangeKeys.Versions[i]).EncodedSize())
		hasData := len(b.pending) > 0 || i < maxKey
		if hasData && (b.pendingSize+rangeKeySize) >= b.batchSize {
			// If we need to send a batch, add previous key from history that we
			// already accounted for and flush pending.
			if i < maxKey {
				b.addRangeKey(rangeKeys.AsRangeKey(rangeKeys.Versions[i+1]))
			}
			if err := b.flushPendingFragments(ctx); err != nil {
				return err
			}
		}
		b.pendingSize += rangeKeySize
	}
	b.addRangeKey(rangeKeys.AsRangeKey(rangeKeys.Versions[0]))
	return nil
}

func (b *rangeKeyBatcher) addRangeKey(unsafeRk storage.MVCCRangeKey) {
	if len(b.pending) == 0 {
		b.pending = append(b.pending, unsafeRk.Clone())
		return
	}
	lastFragment := b.pending[len(b.pending)-1]
	// If new fragment is adjacent to previous one and has the same timestamp,
	// merge fragments.
	if lastFragment.EndKey.Equal(unsafeRk.StartKey) &&
		lastFragment.Timestamp.Equal(unsafeRk.Timestamp) {
		lastFragment.EndKey = unsafeRk.EndKey.Clone()
		b.pending[len(b.pending)-1] = lastFragment
	} else {
		b.pending = append(b.pending, unsafeRk.Clone())
	}
}

func (b *rangeKeyBatcher) flushPendingFragments(ctx context.Context) error {
	if pendingCount := len(b.pending); pendingCount > 0 {
		toSend := make([]kvpb.GCRequest_GCRangeKey, pendingCount)
		for i, rk := range b.pending {
			toSend[i] = kvpb.GCRequest_GCRangeKey{
				StartKey:  rk.StartKey,
				EndKey:    rk.EndKey,
				Timestamp: rk.Timestamp,
			}
		}
		b.pending = b.pending[:0]
		b.pendingSize = 0
		return b.gcer.GC(ctx, nil, toSend, nil)
	}
	return nil
}

func processReplicatedRangeTombstones(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap storage.Reader,
	excludeUserKeySpan bool,
	now hlc.Timestamp,
	gcThreshold hlc.Timestamp,
	gcer GCer,
	info *Info,
) error {
	iter := rditer.NewReplicaMVCCDataIterator(ctx, desc, snap, rditer.ReplicaDataIteratorOptions{
		Reverse:            false,
		IterKind:           storage.MVCCKeyIterKind,
		KeyTypes:           storage.IterKeyTypeRangesOnly,
		ExcludeUserKeySpan: excludeUserKeySpan,
		ReadCategory:       fs.MVCCGCReadCategory,
	})
	defer iter.Close()

	b := rangeKeyBatcher{
		gcer:      gcer,
		batchSize: KeyVersionChunkBytes,
	}
	for {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		// Fetch range keys and filter out those above the GC threshold.
		rangeKeys := iter.RangeKeys()
		hasSurvivors := rangeKeys.Trim(hlc.Timestamp{}, gcThreshold)

		if !rangeKeys.IsEmpty() {
			if err := b.addAndMaybeFlushRangeKeys(ctx, rangeKeys); err != nil {
				return err
			}
			info.NumRangeKeysAffected++
			keyBytes := storage.MVCCVersionTimestampSize * int64(rangeKeys.Len())
			if !hasSurvivors {
				keyBytes += int64(len(rangeKeys.Bounds.Key)) + int64(len(rangeKeys.Bounds.EndKey))
			}
			info.AffectedVersionsRangeKeyBytes += keyBytes
			for _, v := range rangeKeys.Versions {
				info.AffectedVersionsRangeValBytes += int64(len(v.Value))
			}
		}
		iter.Next()
	}
	return b.flushPendingFragments(ctx)
}

// batchingInlineGCer is a helper to paginate the GC of inline (i.e. zero
// timestamp keys). After creation, keys are added via FlushingAdd(). A
// final call to Flush() empties out the buffer when all keys were added.
type batchingInlineGCer struct {
	gcer  PureGCer
	onErr func(error)

	size   int
	max    int
	gcKeys []kvpb.GCRequest_GCKey
}

func makeBatchingInlineGCer(gcer PureGCer, onErr func(error)) batchingInlineGCer {
	return batchingInlineGCer{gcer: gcer, onErr: onErr, max: KeyVersionChunkBytes}
}

func (b *batchingInlineGCer) FlushingAdd(ctx context.Context, key roachpb.Key) {
	b.gcKeys = append(b.gcKeys, kvpb.GCRequest_GCKey{Key: key})
	b.size += len(key)
	if b.size < b.max {
		return
	}
	b.Flush(ctx)
}

func (b *batchingInlineGCer) Flush(ctx context.Context) {
	err := b.gcer.GC(ctx, b.gcKeys, nil, nil)
	b.gcKeys = nil
	b.size = 0
	if err != nil {
		b.onErr(err)
	}
}
