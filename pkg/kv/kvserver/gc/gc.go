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
		*roachpb.GCRequest_GCClearRangeKey,
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
func (NoopGCer) GC(context.Context, []roachpb.GCRequest_GCKey, []roachpb.GCRequest_GCRangeKey,
	*roachpb.GCRequest_GCClearRangeKey,
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

	err := processReplicatedKeyRange(ctx, desc, snap, now, newThreshold, options.IntentAgeThreshold, gcer,
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
	if err := processLocalKeyRange(ctx, snap, desc, txnExp, &info, cleanupTxnIntentsAsyncFn, gcer); err != nil {
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
	gcer GCer,
	options intentBatcherOptions,
	cleanupIntentsFn CleanupIntentsFunc,
	info *Info,
) error {
	var alloc bufalloc.ByteAllocator
	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now.Add(-intentAgeThreshold.Nanoseconds(), 0)

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
	// followed by a metadata entry. The loop will determine whether a given key
	// has garbage and, if so, will determine the timestamp of the latest version
	// which is garbage to be added to the current batch. If the current version
	// pushes the size of keys to be removed above the limit, the current key will
	// be added with that version and the batch will be sent. When the newest
	// version for a key has been reached, if haveGarbageForThisKey, we'll add the
	// current key to the batch with the gcTimestampForThisKey.
	var (
		batchGCKeys           []roachpb.GCRequest_GCKey
		batchGCKeysBytes      int64
		haveGarbageForThisKey bool
		gcTimestampForThisKey hlc.Timestamp
		sentBatchForThisKey   bool
	)
	it := makeGCIterator(desc, snap, threshold)
	defer it.close()
	for ; ; it.step() {
		s, ok := it.state()
		if !ok {
			if it.err != nil {
				return it.err
			}
			break
		}
		if s.curIsNotValue() { // Step over metadata or other system keys
			continue
		}
		if s.curIsIntent() {
			if err := handleIntent(s.next); err != nil {
				return err
			}
			continue
		}
		// No more values in buffer or next value has different key.
		isNewestPoint := s.curIsNewest()
		if isGarbage(threshold, s.cur, s.next, isNewestPoint, s.firstRangeTombstoneTsAtOrBelowGC) {
			keyBytes := int64(s.cur.Key.EncodedSize())
			batchGCKeysBytes += keyBytes
			haveGarbageForThisKey = true
			gcTimestampForThisKey = s.cur.Key.Timestamp
			info.AffectedVersionsKeyBytes += keyBytes
			info.AffectedVersionsValBytes += int64(len(s.cur.Value))
		}
		// We bump how many keys were processed when we reach newest key and looking
		// if key has garbage or if garbage for this key was included in previous
		// batch.
		if affected := isNewestPoint && (sentBatchForThisKey || haveGarbageForThisKey); affected {
			info.NumKeysAffected++
			// If we reached newest timestamp for the key then we should reset sent
			// batch to ensure subsequent keys are not included in affected keys if
			// they don't have garbage.
			sentBatchForThisKey = false
		}
		shouldSendBatch := batchGCKeysBytes >= KeyVersionChunkBytes
		if shouldSendBatch || isNewestPoint && haveGarbageForThisKey {
			alloc, s.cur.Key.Key = alloc.Copy(s.cur.Key.Key, 0)
			batchGCKeys = append(batchGCKeys, roachpb.GCRequest_GCKey{
				Key:       s.cur.Key.Key,
				Timestamp: gcTimestampForThisKey,
			})
			haveGarbageForThisKey = false
			gcTimestampForThisKey = hlc.Timestamp{}

			// Mark that we sent a batch for this key so we know that we had garbage
			// even if it turns out that there's no more garbage for this key.
			// We want to count a key as affected once even if we paginate the
			// deletion of its versions.
			sentBatchForThisKey = shouldSendBatch && !isNewestPoint
		}
		// If limit was reached, delegate to GC'r to remove collected batch.
		if shouldSendBatch {
			if err := gcer.GC(ctx, batchGCKeys, nil, nil); err != nil {
				if errors.Is(err, ctx.Err()) {
					return err
				}
				// Even though we are batching the GC process, it's
				// safe to continue because we bumped the GC
				// thresholds. We may leave some inconsistent history
				// behind, but nobody can read it.
				log.Warningf(ctx, "failed to GC a batch of keys: %v", err)
			}
			batchGCKeys = nil
			batchGCKeysBytes = 0
			alloc = bufalloc.ByteAllocator{}
		}
	}
	// We need to send out last intent cleanup batch.
	if err := intentBatcher.maybeFlushPendingIntents(ctx); err != nil {
		if errors.Is(err, ctx.Err()) {
			return err
		}
		log.Warningf(ctx, "failed to cleanup intents batch: %v", err)
	}
	if len(batchGCKeys) > 0 {
		if err := gcer.GC(ctx, batchGCKeys, nil, nil); err != nil {
			return err
		}
	}
	return nil
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
