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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
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
	defaultMinRangeDeleteVersions = 100
	// defaultClearRangeCooldownVerions is min number of versions of consecutive
	// garbage (across multiple keys) needed to start collecting clear range
	// requests.
	defaultClearRangeCooldownVerions = 100
	// defaultMax number of scrapped clear range attempts before abandoning
	// attempts to use clear range for a GC run.
	defaultClearRangeMaxRestartThreshold = 5
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
		[]roachpb.GCRequest_GCClearRangeKey,
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
	[]roachpb.GCRequest_GCClearRangeKey,
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
	// DeleteRangeMinKeys is minimum count of keys to delete with a DeleteRangeKeys type request.
	// If there's less than this number of keys to delete, GC would fall back to point deletions.
	// 0 means default value of defaultMinRangeDeleteVersions is used.
	DeleteRangeMinKeys int
	// MaxKeyVersionChunkBytes is the max size of keys for all versions of objects a single gc
	// batch would contain. This includes not only keys within the request, but also all versions
	// covered below request keys. This is important because of the resulting raft command size
	// generated in response to GC request.
	MaxKeyVersionChunkBytes int64
	// MaxClearRangeRestartCount is maximum number of unsuccessful attempts to build clear range
	// request batcher will do before abandoning the clear range operations. If set to 0 default
	// value is used. To disable set to -1 to avoid default and always fail restart check.
	MaxClearRangeRestartCount int
	// ClearRangeCooldownCount is number of consecutive garbage object versions batcher need before
	// to start considering clear range gc request creation. If set to 0 default value is used.
	ClearRangeCooldownCount int
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

	txnExp := now.Add(-kvserverbase.TxnCleanupThreshold.Nanoseconds(), 0)
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

	err := processReplicatedKeyRange(ctx, desc, snap, now, newThreshold, options.IntentAgeThreshold,
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
	err = processReplicatedRangeTombstones(ctx, desc, snap, now, newThreshold, gcer, &info)
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
		deleteRangeMinKeys:            options.DeleteRangeMinKeys,
		batchGCKeysBytesThreshold:     options.MaxKeyVersionChunkBytes,
		clearRangeCooldownThreshold:   options.ClearRangeCooldownCount,
		clearRangeMaxRestartThreshold: options.MaxClearRangeRestartCount,
	}
	if batcherOptions.deleteRangeMinKeys == 0 {
		batcherOptions.deleteRangeMinKeys = defaultMinRangeDeleteVersions
	}
	if batcherOptions.batchGCKeysBytesThreshold == 0 {
		batcherOptions.batchGCKeysBytesThreshold = KeyVersionChunkBytes
	}
	if batcherOptions.clearRangeCooldownThreshold == 0 {
		batcherOptions.clearRangeCooldownThreshold = defaultClearRangeCooldownVerions
	}
	if batcherOptions.clearRangeMaxRestartThreshold == 0 {
		batcherOptions.clearRangeMaxRestartThreshold = defaultClearRangeMaxRestartThreshold
	}
	return batcherOptions
}

// processReplicatedKeyRange identifies garbage and sends GC requests to
// remove it.
//
// The logic iterates all versions of all keys in the range from oldest to
// newest. Expired intents are written into the txnMap and intentKeyMap.
func processReplicatedKeyRange(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap storage.Reader,
	now hlc.Timestamp,
	threshold hlc.Timestamp,
	intentAgeThreshold time.Duration,
	batcherOptions gcKeyBatcherThresholds,
	gcer GCer,
	options intentBatcherOptions,
	cleanupIntentsFn CleanupIntentsFunc,
	info *Info,
) error {
	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now.Add(-intentAgeThreshold.Nanoseconds(), 0)

	return rditer.IterateMVCCReplicaKeySpans(desc, snap, rditer.IterateOptions{
		CombineRangesAndPoints: true,
		Reverse:                true,
	}, func(iterator storage.MVCCIterator, span roachpb.Span, keyType storage.IterKeyType) error {
		intentBatcher := newIntentBatcher(cleanupIntentsFn, options, info)

		// handleIntent will deserialize transaction info and if intent is older than
		// threshold enqueue it to batcher, otherwise ignore it.
		handleIntent := func(keyValue *storage.MVCCKeyValue) error {
			meta := &enginepb.MVCCMetadata{}
			if err := protoutil.Unmarshal(keyValue.Value, meta); err != nil {
				log.Errorf(ctx, "unable to unmarshal MVCC metadata for key %q: %+v", keyValue.Key, err)
				return nil
			}
			if meta.Txn != nil {
				// Keep track of intent to resolve if older than the intent
				// expiration threshold.
				if meta.Timestamp.ToTimestamp().Less(intentExp) {
					info.IntentsConsidered++
					if err := intentBatcher.addAndMaybeFlushIntents(ctx, keyValue.Key.Key, meta); err != nil {
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
			deleteRangeMinKeys:        batcherOptions.deleteRangeMinKeys,
			batchGCKeysBytesThreshold: batcherOptions.batchGCKeysBytesThreshold,
			gcer:                      gcer,
			state:                     batcherState{prevWasNewest: true},
			rangeClearLastKey:         desc.EndKey.AsRawKey(),
			clearRangeGate: clearRangeGate{
				cooldownThreshold: batcherOptions.clearRangeCooldownThreshold,
				maxRestartCount:   batcherOptions.clearRangeMaxRestartThreshold,
			},
		}

		for ; ; it.step() {
			var rewindKey storage.MVCCKey
			var upd batchCounters
			var err error

			s, ok := it.state()
			if !ok {
				if it.err != nil {
					return it.err
				}
				// We need to flush last batch here just in case we'll have to rewind
				// and start again if number of keys is too small for clear range.
				rewindKey, upd, err = b.flushLastBatch(ctx)
				if err != nil {
					return err
				}
				upd.updateGcInfo(info)
				if len(rewindKey.Key) > 0 {
					// If last batch is a clear range but it too small, it may request a
					// restart to use point batches.
					it.seek(rewindKey)
					continue
				}
				break
			}

			isNewestPoint := s.curIsNewest()
			switch {
			case s.curIsNotValue():
				rewindKey, upd, err = b.foundNonGCAbleData(ctx, s.cur, isNewestPoint, true /* isMeta */)
			case s.curIsIntent():
				rewindKey, upd, err = b.foundNonGCAbleData(ctx, s.cur, isNewestPoint, true /* isMeta */)
				// Note that if we had to rewind then we will return to this key again
				// and we don't need to process it now.
				if len(rewindKey.Key) == 0 {
					if err = handleIntent(s.next); err != nil {
						return err
					}
				}
			default:
				if isGarbage(threshold, s.cur, s.next, isNewestPoint, s.firstRangeTombstoneTsAtOrBelowGC) {
					rewindKey, upd, err = b.foundGarbage(ctx, s.cur, isNewestPoint)
				} else {
					rewindKey, upd, err = b.foundNonGCAbleData(ctx, s.cur, isNewestPoint, false /* isMeta */)
				}
			}
			if err != nil {
				return err
			}
			upd.updateGcInfo(info)
			if len(rewindKey.Key) > 0 {
				it.seek(rewindKey)
			}
		}

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

// batchCounters contain statistics about garbage that is collected for the
// range of keys.
type batchCounters struct {
	keyBytes, valBytes int64
	keysAffected       int
}

func (c batchCounters) updateGcInfo(info *Info) {
	info.AffectedVersionsKeyBytes += c.keyBytes
	info.AffectedVersionsValBytes += c.valBytes
	info.NumKeysAffected += c.keysAffected
}

func (c *batchCounters) merge(other batchCounters) {
	c.keyBytes += other.keyBytes
	c.valBytes += other.valBytes
	c.keysAffected += other.keysAffected
}

// batchInfo contains transient information that allows GC to rollback its
// processing to a previous key. Includes counters already accumulated at the
// checkpoint time and the last processed key.
type batchInfo struct {
	batchCounters
	// Last processed key to use with SeekLT() upon restart.
	lastProcessedKey storage.MVCCKey
}

func (c *batchInfo) isEmpty() bool {
	return len(c.lastProcessedKey.Key) == 0
}

// Content of other is incorporated into current batch. Other must be more
// recent in terms of key progress.
func (c *batchInfo) merge(other batchInfo) {
	c.batchCounters.merge(other.batchCounters)
	c.lastProcessedKey = other.lastProcessedKey
}

func (c *batchInfo) clear() {
	*c = batchInfo{}
}

// gcKeyBatcherThresholds collection configuration options.
type gcKeyBatcherThresholds struct {
	deleteRangeMinKeys            int
	batchGCKeysBytesThreshold     int64
	clearRangeCooldownThreshold   int
	clearRangeMaxRestartThreshold int
}

// batcherState contains info about currently processed batch of keys.
// It accounts for fully collected keys and the last key separately to allow
// performing ClearRange operations that are key bound, not version bound.
type batcherState struct {
	currentBatch  batchInfo
	lastKey       batchInfo
	prevWasNewest bool
}

func (cp *batcherState) isEmpty() bool {
	return cp.currentBatch.isEmpty() && cp.lastKey.isEmpty()
}

func (cp *batcherState) mergeLastKey() {
	cp.currentBatch.merge(cp.lastKey)
	cp.lastKey.clear()
}

func (cp *batcherState) keySize() int64 {
	return cp.currentBatch.keyBytes + cp.lastKey.keyBytes
}

func (cp *batcherState) clear() {
	cp.lastKey.clear()
	cp.currentBatch.clear()
}

// gcKeyBatcher is responsibe for collecting MVCCKeys and feeding them to GCer
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
// - send clear range batch up to the last fully covered key, rewind to the
//   first version after cleared key
// - restore state from checkpoint and send point key batch, rewind to the
//   version after the last sent key
type gcKeyBatcher struct {
	// Batcher configuration options.
	deleteRangeMinKeys        int
	batchGCKeysBytesThreshold int64

	// Fields are used to accumulate point GC batch.
	batchGCKeys []roachpb.GCRequest_GCKey
	alloc       bufalloc.ByteAllocator

	// Info about currently accumulated batch including garbage size counters,
	// current key and state that allows detection when we advance to next key
	// without a need to compare key values.
	state batcherState
	// Whenever we switch to range deletion mode, pointsKeyBatch captures state
	// of batcher on the last eligible object so that we could rewind to that
	// state if we decide not to proceed with range deletion.
	checkpoint batcherState

	// Ranged deletion.
	// Point versions counter to assess feasibility of range del.
	sequentialKeysFound int
	// canDeleteRange is true when batch starts and turns to false if we found a
	// non GC-able object during point batch collection.
	canDeleteRange bool
	// End key for potential clean range request.
	rangeClearLastKey roachpb.Key
	// clearRangeGate tracks found live and garbage keys to enable or disable
	// clear range based on observed patterns.
	clearRangeGate clearRangeGate

	gcer GCer
}

func (b *gcKeyBatcher) foundNonGCAbleData(
	ctx context.Context, cur *storage.MVCCKeyValue, isNewestPoint, isMeta bool,
) (rewindKey storage.MVCCKey, counterUpdate batchCounters, err error) {
	b.clearRangeGate.foundLiveData()

	// Not tracking range deletion yet.
	if b.checkpoint.isEmpty() {
		// Range collection could still be restarted in presence of garbage if:
		//  - batch is empty;
		//  - we are at newest point or we are at non-value or intent.
		if len(b.batchGCKeys) > 0 || !isNewestPoint || !isMeta {
			b.canDeleteRange = false
		}
		// We also need to handle the special case where we restart range on
		// key boundary. In this case we need to bump range clear last key to
		// match current.
		if len(b.batchGCKeys) == 0 && isNewestPoint {
			b.rangeClearLastKey = cur.Key.Key.Clone()
		}
		if isMeta {
			// We don't need to update anything on non-versionable object and we
			// don't need to flush anything.
			return storage.MVCCKey{}, batchCounters{}, nil
		}
		// For non-meta objects we still need to handle isNewest/prevWasNewest
		if b.state.prevWasNewest {
			b.state.mergeLastKey()
			b.state.lastKey.lastProcessedKey = cur.Key.Clone()
		}
		b.state.prevWasNewest = isNewestPoint
		return storage.MVCCKey{}, batchCounters{}, nil
	}

	// We are already tracking range deletion, we should try to compute end range
	// here and also capture start position to resume future range tracking.
	if !cur.Key.Key.Equal(b.state.lastKey.lastProcessedKey.Key) {
		// We are already at the next key, we can use newest as previous.
		b.state.mergeLastKey()
	} else {
		// If they are equal, we hit an intent and can't clean this key with range
		// op.
		b.sequentialKeysFound--
	}
	if !isMeta {
		b.state.prevWasNewest = isNewestPoint
	}
	return b.maybeFlushPendingRange(ctx)
}

func (b *gcKeyBatcher) foundGarbage(
	ctx context.Context, cur *storage.MVCCKeyValue, isNewestPoint bool,
) (rewindKey storage.MVCCKey, counterUpdate batchCounters, err error) {
	b.clearRangeGate.foundGarbageData()

	// Aggregate last key batch into current.
	if b.state.prevWasNewest {
		// Check if we just started a batch and we can try to collect a range.
		if len(b.batchGCKeys) == 0 && b.clearRangeGate.shouldRestartClearRange() {
			b.canDeleteRange = true
		}

		b.state.mergeLastKey()
		b.sequentialKeysFound++

		// Start filling up new batch with current key garbage data.
		b.state.lastKey.keysAffected++
		if b.checkpoint.isEmpty() {
			b.alloc, b.state.lastKey.lastProcessedKey.Key = b.alloc.Copy(cur.Key.Key, 0)
		} else {
			b.state.lastKey.lastProcessedKey = cur.Key.Clone()
		}
	}

	// Accumulate pending counters for sizes.
	b.state.lastKey.keyBytes += int64(cur.Key.EncodedSize())
	b.state.lastKey.valBytes += int64(len(cur.Value))

	// Accumulate point keys into batch if needed.
	if b.checkpoint.isEmpty() {
		b.state.lastKey.lastProcessedKey.Timestamp = cur.Key.Timestamp
		// We put a key into batch when it is found for the first time in a batch
		// e.g. if prev key was newest in its history or if batch is empty because
		// we just flushed it.
		// In other cases we just update its timestamp to track highest.
		if b.state.prevWasNewest || len(b.batchGCKeys) == 0 {
			b.batchGCKeys = append(b.batchGCKeys, roachpb.GCRequest_GCKey{
				Key:       b.state.lastKey.lastProcessedKey.Key,
				Timestamp: cur.Key.Timestamp,
			})
		} else {
			b.batchGCKeys[len(b.batchGCKeys)-1].Timestamp = cur.Key.Timestamp
		}

		shouldFinishPointsBatch := b.state.keySize() >= b.batchGCKeysBytesThreshold
		// We can try to restart clear range collection, but for that we need to
		// realign start of points batch with the first version of the key.
		if !b.canDeleteRange && b.clearRangeGate.shouldRestartClearRange() && b.state.prevWasNewest {
			shouldFinishPointsBatch = true
		}
		if shouldFinishPointsBatch {
			b.state.prevWasNewest = isNewestPoint
			if !b.canDeleteRange {
				// If limit was reached, delegate to GC'r to remove collected batch.
				counterUpdate, err = b.flushPointsBatch(ctx)
				return storage.MVCCKey{}, counterUpdate, err
			}
			// We now proceed with range clear operation. Save current state for
			// potential rewind and only accumulate counters from now on.
			b.checkpoint = b.state
		}
	}

	b.state.prevWasNewest = isNewestPoint
	return storage.MVCCKey{}, counterUpdate, nil
}

// Send out points batch and clean up context for current batch.
// If we are rewinding after that, caller should be concerned with that.
// Rewind position is lost and needs to be captured separately by caller if
// needed.
func (b *gcKeyBatcher) flushPointsBatch(
	ctx context.Context,
) (counterUpdate batchCounters, err error) {
	if err := b.gcer.GC(ctx, b.batchGCKeys, nil, nil); err != nil {
		if errors.Is(err, ctx.Err()) {
			return counterUpdate, err
		}
		// Even though we are batching the GC process, it's
		// safe to continue because we bumped the GC
		// thresholds. We may leave some inconsistent history
		// behind, but nobody can read it.
		log.Warningf(ctx, "failed to GC a batch of keys: %v", err)
	}
	b.batchGCKeys = nil
	b.alloc = bufalloc.ByteAllocator{}
	b.sequentialKeysFound = 0

	// After flushing the batch we need to aggregate counters for included keys
	// which include previous keys as well as keys in the current batch.
	// We need to preserve last processed key as current key might have more
	// versions. We will do it using newly created alloc as it should belong to
	// subsequent batch.
	var savedKey roachpb.Key
	b.alloc, savedKey = b.alloc.Copy(b.state.lastKey.lastProcessedKey.Key, 0)
	b.state.mergeLastKey()
	counterUpdate = b.state.currentBatch.batchCounters
	b.state.clear()
	b.state.lastKey.lastProcessedKey.Key = savedKey
	// Finally update lower boundary for potential subsequent clear range operation.
	b.rangeClearLastKey = savedKey
	return counterUpdate, nil
}

// Make decision if range batch should be flushed and flush either range or
// points.
func (b *gcKeyBatcher) maybeFlushPendingRange(
	ctx context.Context,
) (rewindKey storage.MVCCKey, counterUpdate batchCounters, err error) {
	// If current pending range batch is too small to send, flush last collected
	// point batch, revert the state and request an iterator rewind.
	// Note that we always ignore current key as we don't know it all versions
	// were processed and we check that we have at least one full previous key.
	if b.sequentialKeysFound < b.deleteRangeMinKeys || b.state.currentBatch.keysAffected == 0 {
		b.clearRangeGate.abandonedClearRange()
		// If we have to rewind to last collected points batch, then we restore
		// to checkpoint, flush points batch and request a rewind.
		b.state = b.checkpoint
		b.checkpoint.clear()
		rewindKey = b.state.lastKey.lastProcessedKey
		cnt, err := b.flushPointsBatch(ctx)
		b.canDeleteRange = false
		return rewindKey, cnt, err
	}
	return b.flushRangeBatch(ctx)
}

// Flush range batch sends range batch and optionally reverts to last key
// with non complete deletion.
func (b *gcKeyBatcher) flushRangeBatch(
	ctx context.Context,
) (rewindKey storage.MVCCKey, counterUpdate batchCounters, err error) {
	if err := b.gcer.GC(ctx, nil, nil, []roachpb.GCRequest_GCClearRangeKey{{
		StartKey: b.state.currentBatch.lastProcessedKey.Key,
		EndKey:   b.rangeClearLastKey,
	}}); err != nil {
		if errors.Is(err, ctx.Err()) {
			return storage.MVCCKey{}, counterUpdate, err
		}
		// Even though we are batching the GC process, it's
		// safe to continue because we bumped the GC
		// thresholds. We may leave some inconsistent history
		// behind, but nobody can read it.
		log.Warningf(ctx, "failed to GC keys with clear range: %v", err)
	}

	// Clean up stale points key batch and its resources.
	b.checkpoint.clear()
	b.batchGCKeys = nil
	b.alloc = bufalloc.ByteAllocator{}
	b.sequentialKeysFound = 0

	b.alloc, b.rangeClearLastKey = b.alloc.Copy(b.state.currentBatch.lastProcessedKey.Key, 0)
	counterUpdate = b.state.currentBatch.batchCounters
	rewindKey = b.state.currentBatch.lastProcessedKey
	b.state.clear()

	// Since we discard last key here and rewind to previous we should set
	// prevWasNewest to true to allow for key capture for the next object.
	b.state.prevWasNewest = true

	return rewindKey, counterUpdate, nil
}

// When we reached the end of iteration we need to flush last batch.
func (b *gcKeyBatcher) flushLastBatch(
	ctx context.Context,
) (rewindKey storage.MVCCKey, counterUpdate batchCounters, err error) {
	if b.checkpoint.isEmpty() {
		if len(b.batchGCKeys) > 0 {
			counterUpdate, err = b.flushPointsBatch(ctx)
		}
		return rewindKey, counterUpdate, err
	}
	// Aggregate last key as it never had a chance to merge after isNewest.
	if !b.state.lastKey.isEmpty() {
		b.state.mergeLastKey()
	}
	return b.maybeFlushPendingRange(ctx)
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

// clearRangeGate tracks enabling and disabling of clear range request
// collection within gcKeyBatcher.
// It counts number of consecutive garbage values to see if it is worth starting
// a clear range request.
// When batcher scraps a clear range and rewinds clearRangeGate will ensure we
// don't speculatively start creating more clear range requests before the live
// data that caused previous request to stop is skipped.
// It also counts how many scrapped clear range were made and will stop
// attempts all together when limit is reached.
type clearRangeGate struct {
	// How many consecutive garbage versions gc needs to see before trying
	// to start collecting clear range batch.
	// If this value set to a very high value, clear range is effectively
	// disabled.
	cooldownThreshold int
	// Current number of consecutive garbage versions.
	cooldownCount int
	// Only do cooldown if we know that there's no live data ahead.
	cooldownDisabled bool

	// Number of rewind operations to tolerate before giving up on clear range
	// batch building.
	maxRestartCount int
	// Total number of rewind operations allowed before before clear range is
	// disabled for this GC run.
	rewindCount int
}

func (e *clearRangeGate) foundGarbageData() {
	if !e.cooldownDisabled {
		e.cooldownCount++
	}
}

func (e *clearRangeGate) foundLiveData() {
	e.cooldownCount = 0
	e.cooldownDisabled = false
}

// abandonedClearRange is a notification from batcher that we tried to collect
// a clear range request, but number of keys included was too low to justify
// the operation and it was scrapped.
func (e *clearRangeGate) abandonedClearRange() {
	// Clear range is abandoned when we found live data and rewinded to last
	// known points batch. This is a wasteful condition so we try to check if
	// we have to rewind to much and disable this behaviour all together once
	// certain threshold is crossed.
	e.rewindCount++
	e.cooldownDisabled = true
}

// shouldRestartClearRange tells batcher if it is worth starting new batch and
// attempt to perform a clear range.
func (e *clearRangeGate) shouldRestartClearRange() bool {
	return e.cooldownCount >= e.cooldownThreshold && e.rewindCount < e.maxRestartCount
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
	cur, next *storage.MVCCKeyValue,
	isNewestPoint bool,
	firstRangeTombstoneTsAtOrBelowGC hlc.Timestamp,
) bool {
	// If the value is not at or below the threshold then it's not garbage.
	if belowThreshold := cur.Key.Timestamp.LessEq(threshold); !belowThreshold {
		return false
	}
	if cur.Key.Timestamp.Less(firstRangeTombstoneTsAtOrBelowGC) {
		if util.RaceEnabled {
			if threshold.Less(firstRangeTombstoneTsAtOrBelowGC) {
				panic(fmt.Sprintf("gc attempt to remove key: using range tombstone %s above gc threshold %s",
					firstRangeTombstoneTsAtOrBelowGC.String(), threshold.String()))
			}
		}
		return true
	}
	isDelete := len(cur.Value) == 0
	if isNewestPoint && !isDelete {
		return false
	}
	// If this value is not a delete, then we need to make sure that the next
	// value is also at or below the threshold.
	// NB: This doesn't need to check whether next is nil because we know
	// isNewestPoint is false when evaluating rhs of the or below.
	if !isDelete && next == nil {
		panic("huh")
	}
	return isDelete || next.Key.Timestamp.LessEq(threshold)
}

// processLocalKeyRange scans the local range key entries, consisting of
// transaction records, queue last processed timestamps, and range descriptors.
//
// - Transaction entries:
//   - For expired transactions , schedule the intents for
//     asynchronous resolution. The actual transaction spans are not
//     returned for GC in this pass, but are separately GC'ed after
//     successful resolution of all intents. The exception is if there
//     are no intents on the txn record, in which case it's returned for
//     immediate GC.
//
// - Queue last processed times: cleanup any entries which don't match
//   this range's start key. This can happen on range merges.
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
		return b.gcer.GC(ctx, nil, toSend, nil)
	}
	return nil
}

func processReplicatedRangeTombstones(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap storage.Reader,
	now hlc.Timestamp,
	gcThreshold hlc.Timestamp,
	gcer GCer,
	info *Info,
) error {
	iter := rditer.NewReplicaMVCCDataIterator(desc, snap, rditer.ReplicaDataIteratorOptions{
		Reverse:  false,
		IterKind: storage.MVCCKeyIterKind,
		KeyTypes: storage.IterKeyTypeRangesOnly,
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
	err := b.gcer.GC(ctx, b.gcKeys, nil, nil)
	b.gcKeys = nil
	b.size = 0
	if err != nil {
		b.onErr(err)
	}
}
