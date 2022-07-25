// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	// KeyVersionChunkBytes is the threshold size for splitting
	// GCRequests into multiple batches. The goal is that the evaluated
	// Raft command for each GCRequest does not significantly exceed
	// this threshold.
	KeyVersionChunkBytes = base.ChunkRaftCommandThresholdBytes
	// minRangeDeleteVersions is min number of versions we will try to delete
	// using range deletion. Thinking behind this constant is that if we have
	// less than this count, using pebble range tombstone is more expensive
	// than using multiple point deletions.
	defaultMinRangeDeleteVersions = 10000
	// defaultMaxPendingKeysSize is a max amount of memory used to store pending
	// keys while trying to decide if clear range operation is feasible. If we
	// don't have enough keys when this value is reached, points GC operation
	// will be flushed to free memory even if we still can potentially issue
	// clear range in the future.
	defaultMaxPendingKeysSize = 1 << 20
	// hlcTimestampSize is the size of hlc timestamp used for mem usage
	// calculations.
	hlcTimestampSize = 16
)

// IntentAgeThreshold is the threshold after which an extant intent
// will be resolved.
var IntentAgeThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.gc.intent_age_threshold",
	"intents older than this threshold will be resolved when encountered by the MVCC GC queue",
	2*time.Hour,
	func(d time.Duration) error {
		if d < 2*time.Minute {
			return errors.New("intent age threshold must be >= 2 minutes")
		}
		return nil
	},
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

// MaxIntentsPerCleanupBatch is the maximum number of intents that GC will send
// for intent resolution as a single batch.
//
// The setting is also used by foreground requests like QueryResolvedTimestamp
// that do not need to resolve intents synchronously when they encounter them,
// but do want to perform best-effort asynchronous intent resolution. The
// setting dictates how many intents these requests will collect at a time.
//
// The default value is set to half of the maximum lock table size at the time
// of writing. This value is subject to tuning in real environment as we have
// more data available.
var MaxIntentsPerCleanupBatch = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.gc.intent_cleanup_batch_size",
	"if non zero, gc will split found intents into batches of this size when trying to resolve them",
	5000,
	settings.NonNegativeInt,
)

// MaxIntentKeyBytesPerCleanupBatch is the maximum intent bytes that GC will try
// to send as a single batch to intent resolution. This number is approximate
// and only includes size of the intent keys.
//
// The setting is also used by foreground requests like QueryResolvedTimestamp
// that do not need to resolve intents synchronously when they encounter them,
// but do want to perform best-effort asynchronous intent resolution. The
// setting dictates how many intents these requests will collect at a time.
//
// The default value is a conservative limit to prevent pending intent key sizes
// from ballooning.
var MaxIntentKeyBytesPerCleanupBatch = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.gc.intent_cleanup_batch_byte_size",
	"if non zero, gc will split found intents into batches of this size when trying to resolve them",
	1e6,
	settings.NonNegativeInt,
)

// MinKeyNumberForClearRange system setting restricting stuff
// TODO(oleg): write a description
var MinKeyNumberForClearRange = settings.RegisterIntSetting(settings.SystemOnly,
	"kv.gc.clear_range_min_keys",
	"if non zero, gc will issue clear range requests if number of consecutive garbage keys exceeds this threshold",
	defaultMinRangeDeleteVersions,
	settings.NonNegativeInt,
)

// AdmissionPriority determines the admission priority level to use for MVCC GC
// work.
var AdmissionPriority = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"kv.gc.admission_priority",
	"the admission priority to use for mvcc gc work",
	"bulk_normal_pri",
	map[int64]string{
		int64(admissionpb.BulkNormalPri): "bulk_normal_pri",
		int64(admissionpb.NormalPri):     "normal_pri",
		int64(admissionpb.UserHighPri):   "user_high_pri",
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
	GC(context.Context, []roachpb.GCRequest_GCKey, []roachpb.GCRequest_GCRangeKey,
		*roachpb.GCRequest_GCClearRangeKey, *roachpb.GCRequest_GCClearSubRangeKey,
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
	[]roachpb.GCRequest_GCKey,
	[]roachpb.GCRequest_GCRangeKey,
	*roachpb.GCRequest_GCClearRangeKey,
	*roachpb.GCRequest_GCClearSubRangeKey,
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
	// keys with GC'able data, the number of "old" intents and the number of
	// associated distinct transactions.
	NumKeysAffected, NumRangeKeysAffected, IntentsConsidered, IntentTxns int
	// TransactionSpanTotal is the total number of entries in the transaction span.
	TransactionSpanTotal int
	// Summary of transactions which were found GCable (assuming that
	// potentially necessary intent resolutions did not fail).
	TransactionSpanGCAborted, TransactionSpanGCCommitted int
	TransactionSpanGCStaging, TransactionSpanGCPending   int
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
	// ClearRangeKeyOperations reports 1 if GC succeeded performing collection with
	// ClearRange operation.
	ClearRangeKeyOperations int
	// ClearRangeKeyFailures reports 1 if GC identified a possibility to collect
	// with ClearRange operation, but request failed.
	ClearRangeKeyFailures int
	// FullRangeDeleteOperations is 1 if fast path delete range request was successfully used to
	// remove all replicated data from a range.
	FullRangeDeleteOperations int64
}

// RunOptions contains collection of limits that GC run applies when performing operations
type RunOptions struct {
	// IntentAgeThreshold is the minimum age an intent must have before this GC run
	// tries to resolve the intent.
	IntentAgeThreshold time.Duration
	// MaxIntentsPerIntentCleanupBatch is the maximum number of intent resolution requests passed
	// to the intent resolver in a single batch. Helps reducing memory impact of cleanup operations.
	MaxIntentsPerIntentCleanupBatch int64
	// MaxIntentKeyBytesPerIntentCleanupBatch similar to MaxIntentsPerIntentCleanupBatch but counts
	// number of bytes intent keys occupy.
	MaxIntentKeyBytesPerIntentCleanupBatch int64
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
	// ClearRangeMinKeys is minimum count of keys to delete with a DeleteRangeKeys type request.
	// If there's less than this number of keys to delete, GC would fall back to point deletions.
	// 0 means default value of defaultMinRangeDeleteVersions is used.
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
type CleanupIntentsFunc func(context.Context, []roachpb.Intent) error

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

	fastPath, err := processReplicatedKeyRange(ctx, desc, snap, now, newThreshold, options.IntentAgeThreshold,
		populateBatcherOptions(options),
		gcer,
		intentBatcherOptions{
			maxIntentsPerIntentCleanupBatch:        options.MaxIntentsPerIntentCleanupBatch,
			maxIntentKeyBytesPerIntentCleanupBatch: options.MaxIntentKeyBytesPerIntentCleanupBatch,
			maxTxnsPerIntentCleanupBatch:           options.MaxTxnsPerIntentCleanupBatch,
			intentCleanupBatchTimeout:              options.IntentCleanupBatchTimeout,
		}, cleanupIntentsFn, &info)
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
// newest. Expired intents are written into the txnMap and intentKeyMap.
// Returns true if clear range was used to remove all user data.
func processReplicatedKeyRange(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap storage.Reader,
	now hlc.Timestamp,
	threshold hlc.Timestamp,
	intentAgeThreshold time.Duration,
	batcherThresholds gcKeyBatcherThresholds,
	gcer GCer,
	options intentBatcherOptions,
	cleanupIntentsFn CleanupIntentsFunc,
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
			if err = gcer.GC(ctx, nil, nil, &roachpb.GCRequest_GCClearRangeKey{
				StartKey: start,
				EndKey:   end,
			}, nil); err == nil {
				excludeUserKeySpan = true
				info.ClearRangeKeyOperations++
			} else {
				log.Warningf(ctx, "failed to perform GC clear range operation on range %s: %s",
					desc.String(), err)
				info.ClearRangeKeyFailures++
			}
		}
	}

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now.Add(-intentAgeThreshold.Nanoseconds(), 0)

	return excludeUserKeySpan, rditer.IterateMVCCReplicaKeySpans(desc, snap, rditer.IterateOptions{
		CombineRangesAndPoints: true,
		Reverse:                true,
		ExcludeUserKeySpan:     excludeUserKeySpan,
	}, func(iterator storage.MVCCIterator, span roachpb.Span, keyType storage.IterKeyType) error {
		intentBatcher := newIntentBatcher(cleanupIntentsFn, options, info)

		// handleIntent will deserialize transaction info and if intent is older than
		// threshold enqueue it to batcher, otherwise ignore it.
		handleIntent := func(keyValue *mvccKeyValue) error {
			meta := &enginepb.MVCCMetadata{}
			if err := protoutil.Unmarshal(keyValue.metaValue, meta); err != nil {
				log.Errorf(ctx, "unable to unmarshal MVCC metadata for key %q: %+v", keyValue.key, err)
				return nil
			}
			if meta.Txn != nil {
				// Keep track of intent to resolve if older than the intent
				// expiration threshold.
				if meta.Timestamp.ToTimestamp().Less(intentExp) {
					info.IntentsConsidered++
					if err := intentBatcher.addAndMaybeFlushIntents(ctx, keyValue.key.Key, meta); err != nil {
						if errors.Is(err, ctx.Err()) {
							return err
						}
						log.Warningf(ctx, "failed to cleanup intents batch: %v", err)
					}
				}
			}
			return nil
		}

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
			pointsBatches:          make([]pointsBatch, 1),
			clearRangeEndKey:       desc.EndKey.AsRawKey(),
			prevWasNewest:          true,
		}

		for ; ; it.step() {
			var upd gcBatchCounters
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
				upd, err = b.foundNonGCableData(ctx, s.cur, true /* isNewestPoint */)
			case s.curIsIntent():
				upd, err = b.foundNonGCableData(ctx, s.cur, true /* isNewestPoint */)
				if err != nil {
					return err
				}
				if err = handleIntent(s.next); err != nil {
					return err
				}
				// For intents, we force step over the intent metadata after provisional
				// value is found.
				it.step()
			default:
				if isGarbage(threshold, s.cur, s.next, s.curIsNewest(), s.firstRangeTombstoneTsAtOrBelowGC) {
					upd, err = b.foundGarbage(ctx, s.cur, s.curLastKeyVersion())
				} else {
					upd, err = b.foundNonGCableData(ctx, s.cur, s.curLastKeyVersion())
				}
			}
			if err != nil {
				return err
			}
			upd.updateGcInfo(info)
			// TODO(oleg): remove all printf statements from gc.go
			//fmt.Printf("%s: %s\n", s.cur.key.String(), b.String())
		}

		upd, err := b.flushLastBatch(ctx)
		if err != nil {
			return err
		}
		upd.updateGcInfo(info)

		// We need to send out last intent cleanup batch.
		if err := intentBatcher.maybeFlushPendingIntents(ctx); err != nil {
			if errors.Is(err, ctx.Err()) {
				return err
			}
			log.Warningf(ctx, "failed to cleanup intents batch: %v", err)
		}
		return nil
	})
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

func (c *gcBatchCounters) add(o gcBatchCounters) {
	c.keyBytes += o.keyBytes
	c.valBytes += o.valBytes
	c.keysAffected += o.keysAffected
	c.versionsAffected += o.versionsAffected
	c.memUsed += o.memUsed
}

func (c *gcBatchCounters) sub(o gcBatchCounters) {
	c.keyBytes -= o.keyBytes
	c.valBytes -= o.valBytes
	c.keysAffected -= o.keysAffected
	c.versionsAffected -= o.versionsAffected
	c.memUsed -= o.memUsed
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
	batchGCKeys []roachpb.GCRequest_GCKey
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
// Internally it would start collecting point data into the batch until it
// reaches configured batchGCKeysBytesThreshold (which limits size of the raft
// entry that this request will create later). At this point it will decide
// if it should switch into ClearRange mode. If decision is made to proceed with
// clear range, current state is saved into checkpoint.
// If later non garbage data is found, batcher looks on number of keys covered
// by already checked range and will either:
//   - send clear range batch up to the previous key
//   - restore state from checkpoint and send point key batch then rewind to the
//     version after the last sent key
type gcKeyBatcher struct {
	gcKeyBatcherThresholds

	prevWasNewest bool
	// Points batches are accumulated until we cross clear range threshold.
	// There's always at least one points batch initialized.
	pointsBatches []pointsBatch
	// totalMemUsed sum of memUsed of all points batches cached to avoid
	// recomputing on every check.
	totalMemUsed int

	// Tracking of clear range requests.
	clearRangeEndKey          roachpb.Key
	clearRangeStartKey        storage.MVCCKey
	clearRangeCounters        gcBatchCounters
	partialPointBatchCounters gcBatchCounters

	gcer GCer
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
) (counterUpdate gcBatchCounters, err error) {
	b.prevWasNewest = isNewestPoint
	if !b.clearRangeEnabled {
		return counterUpdate, nil
	}

	// Check if there are any complete clear range or point batches collected
	// and flush them as we reached end of current consecutive key span.
	counterUpdate, err = b.maybeFlushPendingBatches(ctx)
	if err != nil {
		return gcBatchCounters{}, err
	}
	b.clearRangeCounters = gcBatchCounters{}

	if isNewestPoint {
		b.clearRangeEndKey = append(b.clearRangeEndKey[:0], cur.key.Key...)
	}
	return counterUpdate, nil
}

func (b *gcKeyBatcher) foundGarbage(
	ctx context.Context, cur *mvccKeyValue, isNewestPoint bool,
) (counterUpdate gcBatchCounters, err error) {
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
		newBatchStarted := false
		if b.pointsBatches[i].gcBatchCounters.keyBytes >= b.batchGCKeysBytesThreshold {
			// If clear range is disabled, flush batches immediately as they are
			// formed.
			if !b.clearRangeEnabled {
				if counterUpdate, err = b.flushPointsBatch(ctx, &b.pointsBatches[i]); err != nil {
					return gcBatchCounters{}, err
				}
			} else {
				b.pointsBatches = append(b.pointsBatches, pointsBatch{})
				i++
			}
			newBatchStarted = true
		}
		if b.prevWasNewest {
			b.pointsBatches[i].gcBatchCounters.keysAffected++
		}
		// Whenever new key is started or new batch is started with the same key in
		// it, record key value using batches' allocator.
		if b.prevWasNewest || newBatchStarted {
			b.pointsBatches[i].alloc, key = b.pointsBatches[i].alloc.Copy(cur.key.Key, 0)
			b.pointsBatches[i].batchGCKeys = append(b.pointsBatches[i].batchGCKeys,
				roachpb.GCRequest_GCKey{Key: key, Timestamp: cur.key.Timestamp})
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
			//fmt.Printf("flushing overflow points batch\n")
			// We accumulated more keys in memory than allowed by thresholds, we need
			// to flush oldest batch to protect node from exhausting memory.
			lastKey, update, err := b.flushOldestPointBatches(ctx, 1)
			if err != nil {
				return gcBatchCounters{}, err
			}
			counterUpdate.add(update)
			// If oldest batch intersected with currently tracked clear range request
			// then bump clear range end key to the one that follows lowest key of the
			// cleared batch. We know that all keys for current key with lower
			// timestamps are eligible for collection and end range is exclusive.
			// The check is needed because clearRangeEndKey cloud be bumped already
			// to current key by non gc data on the previous key, but only now we
			// exceeded the size threshold and trying to remove oldest batch.
			if b.clearRangeEndKey.Compare(lastKey) > 0 {
				// If flushed batch was split by start of the range, then we must remove
				// preceding part from clear range total to avoid.
				b.clearRangeCounters.sub(update)
				b.clearRangeCounters.add(b.partialPointBatchCounters)
				b.clearRangeEndKey = lastKey[:len(lastKey):len(lastKey)].Next()
			}
			b.partialPointBatchCounters = gcBatchCounters{}
		}
	}

	if b.clearRangeEnabled {
		if b.prevWasNewest {
			if key == nil {
				// We reuse the start key slice to avoid reallocating it on every new
				// non-garbage key.
				key = append(b.clearRangeStartKey.Key[:0], cur.key.Key...)
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
	return counterUpdate, nil
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
func (b *gcKeyBatcher) maybeFlushPendingBatches(
	ctx context.Context,
) (counterUpdate gcBatchCounters, err error) {
	// Find where in first points batch is start key and flush "prefix".
	if b.clearRangeEnabled && b.clearRangeCounters.versionsAffected >= b.clearRangeMinKeys {
		//fmt.Printf("flushing range batch on live data\n")
		// Optionally flush parts of the first batch if it doesn't match
		// end of the range.
		batchLen := len(b.pointsBatches[0].batchGCKeys)
		// Find a key that is equal or less than end of clear range key.
		// Batch is sorted in the reverse order, timestamps are not relevant since
		// we can't have the same key more than once within single points batch.
		lastIdx := sort.Search(batchLen, func(i int) bool {
			return b.pointsBatches[0].batchGCKeys[i].Key.Compare(b.clearRangeEndKey) <= 0
		})
		if lastIdx != batchLen {
			b.pointsBatches[0].batchGCKeys = b.pointsBatches[0].batchGCKeys[:lastIdx+1]
			b.pointsBatches[0].gcBatchCounters = b.partialPointBatchCounters
			upd, err := b.flushPointsBatch(ctx, &b.pointsBatches[0])
			if err != nil {
				return gcBatchCounters{}, err
			}
			counterUpdate.add(upd)
		}
		b.pointsBatches = make([]pointsBatch, 1)

		// Flush clear range.
		if err := b.gcer.GC(ctx, nil, nil, nil, &roachpb.GCRequest_GCClearSubRangeKey{
			StartKey:          b.clearRangeStartKey.Key,
			StartKeyTimestamp: b.clearRangeStartKey.Timestamp,
			EndKey:            b.clearRangeEndKey,
		}); err != nil {
			if errors.Is(err, ctx.Err()) {
				return gcBatchCounters{}, err
			}
			// Even though we are batching the GC process, it's
			// safe to continue because we bumped the GC
			// thresholds. We may leave some inconsistent history
			// behind, but nobody can read it.
			log.Warningf(ctx, "failed to GC keys with clear range: %v", err)
		}
		counterUpdate.add(b.clearRangeCounters)
		b.totalMemUsed = 0
	} else if flushTo := len(b.pointsBatches) - 1; flushTo > 0 {
		//fmt.Printf("flushing points batch on live data\n")
		lastKey, update, err := b.flushOldestPointBatches(ctx, flushTo)
		if err != nil {
			return gcBatchCounters{}, err
		}
		counterUpdate.add(update)
		b.clearRangeEndKey = lastKey[:len(lastKey):len(lastKey)].Next()
	}
	return counterUpdate, nil
}

// flushOldestPointBatches flushes oldest points batch and returns its counters.
// unsafeLastKey must not be retained as it will prevent batch allocator to be
// released.
func (b *gcKeyBatcher) flushOldestPointBatches(
	ctx context.Context, count int,
) (unsafeLastKey roachpb.Key, counterUpdate gcBatchCounters, err error) {
	var i int
	for i = 0; i < count; i++ {
		var upd gcBatchCounters
		unsafeLastKey = b.pointsBatches[i].batchGCKeys[len(b.pointsBatches[i].batchGCKeys)-1].Key
		if upd, err = b.flushPointsBatch(ctx, &b.pointsBatches[i]); err != nil {
			return nil, gcBatchCounters{}, err
		}
		counterUpdate.add(upd)
	}
	remaining := len(b.pointsBatches) - i
	copy(b.pointsBatches[0:remaining], b.pointsBatches[i:])
	// Zero out remaining batches to free app allocators.
	for i = remaining; i < len(b.pointsBatches); i++ {
		b.pointsBatches[i] = pointsBatch{}
	}
	b.pointsBatches = b.pointsBatches[:remaining]
	return unsafeLastKey, counterUpdate, nil
}

// flushPointsBatch flushes points batch and zeroes out its content.
// TODO(oleg): check if we could get rid of zeroing out?
func (b *gcKeyBatcher) flushPointsBatch(
	ctx context.Context, batch *pointsBatch,
) (counters gcBatchCounters, err error) {
	if err := b.gcer.GC(ctx, batch.batchGCKeys, nil, nil, nil); err != nil {
		if errors.Is(err, ctx.Err()) {
			return counters, err
		}
		// Even though we are batching the GC process, it's
		// safe to continue because we bumped the GC
		// thresholds. We may leave some inconsistent history
		// behind, but nobody can read it.
		log.Warningf(ctx, "failed to GC a batch of keys: %v", err)
	}
	counters = batch.gcBatchCounters
	b.totalMemUsed -= counters.memUsed
	*batch = pointsBatch{}
	return counters, nil
}

func (b *gcKeyBatcher) flushLastBatch(
	ctx context.Context,
) (counterUpdate gcBatchCounters, err error) {
	if len(b.pointsBatches[0].batchGCKeys) == 0 {
		return gcBatchCounters{}, nil
	}
	b.pointsBatches = append(b.pointsBatches, pointsBatch{})
	return b.maybeFlushPendingBatches(ctx)
}

type intentBatcher struct {
	cleanupIntentsFn CleanupIntentsFunc

	options intentBatcherOptions

	// Maps from txn ID to bool and intent slice to accumulate a batch.
	pendingTxns          map[uuid.UUID]bool
	pendingIntents       []roachpb.Intent
	collectedIntentBytes int64

	alloc bufalloc.ByteAllocator

	gcStats *Info
}

type intentBatcherOptions struct {
	maxIntentsPerIntentCleanupBatch        int64
	maxIntentKeyBytesPerIntentCleanupBatch int64
	maxTxnsPerIntentCleanupBatch           int64
	intentCleanupBatchTimeout              time.Duration
}

// newIntentBatcher initializes an intentBatcher. Batcher will take ownership of
// provided *Info object while doing cleanup and update its counters.
func newIntentBatcher(
	cleanupIntentsFunc CleanupIntentsFunc, options intentBatcherOptions, gcStats *Info,
) intentBatcher {
	if options.maxIntentsPerIntentCleanupBatch <= 0 {
		options.maxIntentsPerIntentCleanupBatch = math.MaxInt64
	}
	if options.maxIntentKeyBytesPerIntentCleanupBatch <= 0 {
		options.maxIntentKeyBytesPerIntentCleanupBatch = math.MaxInt64
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
	ctx context.Context, key roachpb.Key, meta *enginepb.MVCCMetadata,
) error {
	var err error = nil
	txnID := meta.Txn.ID
	_, existingTransaction := b.pendingTxns[txnID]
	// Check batching thresholds if we need to flush collected data. Transaction
	// count is treated specially because we want to check it only when we find
	// a new transaction.
	if int64(len(b.pendingIntents)) >= b.options.maxIntentsPerIntentCleanupBatch ||
		b.collectedIntentBytes >= b.options.maxIntentKeyBytesPerIntentCleanupBatch ||
		!existingTransaction && int64(len(b.pendingTxns)) >= b.options.maxTxnsPerIntentCleanupBatch {
		err = b.maybeFlushPendingIntents(ctx)
	}

	// We need to register passed intent regardless of flushing operation result
	// so that batcher is left in consistent state and don't miss any keys if
	// caller resumes batching.
	b.alloc, key = b.alloc.Copy(key, 0)
	b.pendingIntents = append(b.pendingIntents, roachpb.MakeIntent(meta.Txn, key))
	b.collectedIntentBytes += int64(len(key))
	b.pendingTxns[txnID] = true

	return err
}

// maybeFlushPendingIntents resolves currently collected intents.
func (b *intentBatcher) maybeFlushPendingIntents(ctx context.Context) error {
	if len(b.pendingIntents) == 0 {
		// If there's nothing to flush we will try to preserve context
		// for the sake of consistency with how flush behaves when context
		// is canceled during cleanup.
		return ctx.Err()
	}

	var err error
	cleanupIntentsFn := func(ctx context.Context) error {
		return b.cleanupIntentsFn(ctx, b.pendingIntents)
	}
	if b.options.intentCleanupBatchTimeout > 0 {
		err = contextutil.RunWithTimeout(
			ctx, "intent GC batch", b.options.intentCleanupBatchTimeout, cleanupIntentsFn)
	} else {
		err = cleanupIntentsFn(ctx)
	}
	if err == nil {
		// IntentTxns and PushTxn will be equal here, since
		// pushes to transactions whose record lies in this
		// range (but which are not associated to a remaining
		// intent on it) happen asynchronously and are accounted
		// for separately. Thus higher up in the stack, we
		// expect PushTxn > IntentTxns.
		b.gcStats.IntentTxns += len(b.pendingTxns)
		// All transactions in pendingTxns may be PENDING and
		// cleanupIntentsFn will push them to finalize them.
		b.gcStats.PushTxn += len(b.pendingTxns)

		b.gcStats.ResolveTotal += len(b.pendingIntents)
	}

	// Get rid of current transactions and intents regardless of
	// status as we need to go on cleaning up without retries.
	for k := range b.pendingTxns {
		delete(b.pendingTxns, k)
	}
	b.pendingIntents = b.pendingIntents[:0]
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

	_, err := storage.MVCCIterate(ctx, snap, startKey, endKey, hlc.Timestamp{}, storage.MVCCScanOptions{},
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
		toSend := make([]roachpb.GCRequest_GCRangeKey, pendingCount)
		for i, rk := range b.pending {
			toSend[i] = roachpb.GCRequest_GCRangeKey{
				StartKey:  rk.StartKey,
				EndKey:    rk.EndKey,
				Timestamp: rk.Timestamp,
			}
		}
		b.pending = b.pending[:0]
		b.pendingSize = 0
		return b.gcer.GC(ctx, nil, toSend, nil, nil)
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
	iter := rditer.NewReplicaMVCCDataIterator(desc, snap, rditer.ReplicaDataIteratorOptions{
		Reverse:            false,
		IterKind:           storage.MVCCKeyIterKind,
		KeyTypes:           storage.IterKeyTypeRangesOnly,
		ExcludeUserKeySpan: excludeUserKeySpan,
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
	gcKeys []roachpb.GCRequest_GCKey
}

func makeBatchingInlineGCer(gcer PureGCer, onErr func(error)) batchingInlineGCer {
	return batchingInlineGCer{gcer: gcer, onErr: onErr, max: KeyVersionChunkBytes}
}

func (b *batchingInlineGCer) FlushingAdd(ctx context.Context, key roachpb.Key) {
	b.gcKeys = append(b.gcKeys, roachpb.GCRequest_GCKey{Key: key})
	b.size += len(key)
	if b.size < b.max {
		return
	}
	b.Flush(ctx)
}

func (b *batchingInlineGCer) Flush(ctx context.Context) {
	err := b.gcer.GC(ctx, b.gcKeys, nil, nil, nil)
	b.gcKeys = nil
	b.size = 0
	if err != nil {
		b.onErr(err)
	}
}
