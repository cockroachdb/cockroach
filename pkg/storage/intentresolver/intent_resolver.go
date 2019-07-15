// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package intentresolver

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/internal/client/requestbatcher"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// CleanupFunc is used to report the result of later writes in the face of
// WriteIntentError. It should never be called with both a newWIErr and a
// newIntentTxn.
type CleanupFunc func(
	newWIErr *roachpb.WriteIntentError,
	newIntentTxn *enginepb.TxnMeta,
)

const (
	// defaultTaskLimit is the maximum number of asynchronous tasks
	// that may be started by intentResolver. When this limit is reached
	// asynchronous tasks will start to block to apply backpressure.  This is a
	// last line of defense against issues like #4925.
	// TODO(bdarnell): how to determine best value?
	defaultTaskLimit = 1000

	// asyncIntentResolutionTimeout is the timeout when processing a group of
	// intents asynchronously. The timeout prevents async intent resolution from
	// getting stuck. Since processing intents is best effort, we'd rather give
	// up than wait too long (this helps avoid deadlocks during test shutdown).
	asyncIntentResolutionTimeout = 30 * time.Second

	// intentResolverBatchSize is the maximum number of intents that will be
	// resolved in a single batch. Batches that span many ranges (which is
	// possible for the commit of a transaction that spans many ranges) will be
	// split into many batches by the DistSender.
	// TODO(ajwerner): justify this value
	intentResolverBatchSize = 100

	// cleanupIntentsTxnsPerBatch is the number of transactions whose
	// corresponding intents will be resolved at a time. Intents are batched
	// by transaction to avoid timeouts while resolving intents and ensure that
	// progress is made.
	cleanupIntentsTxnsPerBatch = 100

	// defaultGCBatchIdle is the default duration which the gc request batcher
	// will wait between requests for a range before sending it.
	defaultGCBatchIdle = -1 // disabled

	// defaultGCBatchWait is the default duration which the gc request batcher
	// will wait between requests for a range before sending it.
	defaultGCBatchWait = time.Second

	// intentResolutionBatchWait is used to configure the RequestBatcher which
	// batches intent resolution requests across transactions. Intent resolution
	// needs to occur in a relatively short period of time after the completion
	// of a transaction in order to minimize the contention footprint of the write
	// for other contending reads or writes. The chosen value was selected based
	// on some light experimentation to ensure that performance does not degrade
	// in the face of highly contended workloads.
	defaultIntentResolutionBatchWait = 10 * time.Millisecond

	// intentResolutionBatchIdle is similar to the above setting but is used when
	// when no additional traffic hits the batch.
	defaultIntentResolutionBatchIdle = 5 * time.Millisecond
)

// Config contains the dependencies to construct an IntentResolver.
type Config struct {
	Clock                *hlc.Clock
	DB                   *client.DB
	Stopper              *stop.Stopper
	AmbientCtx           log.AmbientContext
	TestingKnobs         storagebase.IntentResolverTestingKnobs
	RangeDescriptorCache kvbase.RangeDescriptorCache

	TaskLimit                    int
	MaxGCBatchWait               time.Duration
	MaxGCBatchIdle               time.Duration
	MaxIntentResolutionBatchWait time.Duration
	MaxIntentResolutionBatchIdle time.Duration
}

// IntentResolver manages the process of pushing transactions and
// resolving intents.
type IntentResolver struct {
	Metrics Metrics

	clock        *hlc.Clock
	db           *client.DB
	stopper      *stop.Stopper
	testingKnobs storagebase.IntentResolverTestingKnobs
	ambientCtx   log.AmbientContext
	sem          chan struct{}    // Semaphore to limit async goroutines.
	contentionQ  *contentionQueue // manages contention on individual keys

	rdc kvbase.RangeDescriptorCache

	gcBatcher *requestbatcher.RequestBatcher
	irBatcher *requestbatcher.RequestBatcher

	mu struct {
		syncutil.Mutex
		// Map from txn ID being pushed to a refcount of requests waiting on the push.
		inFlightPushes map[uuid.UUID]int
		// Set of txn IDs whose list of intent spans are being resolved. Note that
		// this pertains only to EndTransaction-style intent cleanups, whether called
		// directly after EndTransaction evaluation or during GC of txn spans.
		inFlightTxnCleanups map[uuid.UUID]struct{}
	}
	every log.EveryN
}

func setConfigDefaults(c *Config) {
	if c.TaskLimit == 0 {
		c.TaskLimit = defaultTaskLimit
	}
	if c.TaskLimit == -1 || c.TestingKnobs.ForceSyncIntentResolution {
		c.TaskLimit = 0
	}
	if c.MaxGCBatchIdle == 0 {
		c.MaxGCBatchIdle = defaultGCBatchIdle
	}
	if c.MaxGCBatchWait == 0 {
		c.MaxGCBatchWait = defaultGCBatchWait
	}
	if c.MaxIntentResolutionBatchIdle == 0 {
		c.MaxIntentResolutionBatchIdle = defaultIntentResolutionBatchIdle
	}
	if c.MaxIntentResolutionBatchWait == 0 {
		c.MaxIntentResolutionBatchWait = defaultIntentResolutionBatchWait
	}
	if c.RangeDescriptorCache == nil {
		c.RangeDescriptorCache = nopRangeDescriptorCache{}
	}
}

type nopRangeDescriptorCache struct{}

var zeroRangeDescriptor = &roachpb.RangeDescriptor{}

func (nrdc nopRangeDescriptorCache) LookupRangeDescriptor(
	ctx context.Context, key roachpb.RKey,
) (*roachpb.RangeDescriptor, error) {
	return zeroRangeDescriptor, nil
}

// New creates an new IntentResolver.
func New(c Config) *IntentResolver {
	setConfigDefaults(&c)
	ir := &IntentResolver{
		clock:        c.Clock,
		db:           c.DB,
		stopper:      c.Stopper,
		sem:          make(chan struct{}, c.TaskLimit),
		contentionQ:  newContentionQueue(c.Clock, c.DB),
		every:        log.Every(time.Minute),
		Metrics:      makeMetrics(),
		rdc:          c.RangeDescriptorCache,
		testingKnobs: c.TestingKnobs,
	}
	ir.mu.inFlightPushes = map[uuid.UUID]int{}
	ir.mu.inFlightTxnCleanups = map[uuid.UUID]struct{}{}
	ir.gcBatcher = requestbatcher.New(requestbatcher.Config{
		Name:            "intent_resolver_gc_batcher",
		MaxMsgsPerBatch: 1024,
		MaxWait:         c.MaxGCBatchWait,
		MaxIdle:         c.MaxGCBatchIdle,
		Stopper:         c.Stopper,
		Sender:          c.DB.NonTransactionalSender(),
	})
	batchSize := intentResolverBatchSize
	if c.TestingKnobs.MaxIntentResolutionBatchSize > 0 {
		batchSize = c.TestingKnobs.MaxIntentResolutionBatchSize
	}
	ir.irBatcher = requestbatcher.New(requestbatcher.Config{
		Name:            "intent_resolver_ir_batcher",
		MaxMsgsPerBatch: batchSize,
		MaxWait:         c.MaxIntentResolutionBatchWait,
		MaxIdle:         c.MaxIntentResolutionBatchIdle,
		Stopper:         c.Stopper,
		Sender:          c.DB.NonTransactionalSender(),
	})
	return ir
}

// NumContended exists to ease writing tests at the storage level above which
// want to verify that contention is occurring within the IntentResolver.
func (ir *IntentResolver) NumContended(key roachpb.Key) int {
	return ir.contentionQ.numContended(key)
}

// ProcessWriteIntentError tries to push the conflicting
// transaction(s) responsible for the given WriteIntentError, and to
// resolve those intents if possible. Returns a cleanup function and
// potentially a new error to be used in place of the original. The
// cleanup function should be invoked by the caller after the request
// which experienced the conflict has completed with a parameter
// specifying a transaction in the event that the request left its own
// intent.
func (ir *IntentResolver) ProcessWriteIntentError(
	ctx context.Context,
	wiPErr *roachpb.Error,
	args roachpb.Request,
	h roachpb.Header,
	pushType roachpb.PushTxnType,
) (CleanupFunc, *roachpb.Error) {
	wiErr, ok := wiPErr.GetDetail().(*roachpb.WriteIntentError)
	if !ok {
		return nil, roachpb.NewErrorf("not a WriteIntentError: %v", wiPErr)
	}

	if log.V(6) {
		log.Infof(ctx, "resolving write intent %s", wiErr)
	}

	// Possibly queue this processing if the write intent error is for a
	// single intent affecting a unitary key.
	var cleanup func(*roachpb.WriteIntentError, *enginepb.TxnMeta)
	if len(wiErr.Intents) == 1 && len(wiErr.Intents[0].Span.EndKey) == 0 {
		var done bool
		var pErr *roachpb.Error
		// Note that the write intent error may be mutated here in the event
		// that this pusher is queued to wait for a different transaction
		// instead.
		cleanup, wiErr, done, pErr = ir.contentionQ.add(ctx, wiErr, h)
		if done || pErr != nil {
			return cleanup, pErr
		}
	}

	resolveIntents, pErr := ir.maybePushIntents(
		ctx, wiErr.Intents, h, pushType, false, /* skipIfInFlight */
	)
	if pErr != nil {
		return cleanup, pErr
	}

	// We always poison due to limitations of the API: not poisoning equals
	// clearing the AbortSpan, and if our pushee transaction first got pushed
	// for timestamp (by us), then (by someone else) aborted and poisoned, and
	// then we run the below code, we're clearing the AbortSpan illegaly.
	// Furthermore, even if our pushType is not PUSH_ABORT, we may have ended
	// up with the responsibility to abort the intents (for example if we find
	// the transaction aborted).
	//
	// To do better here, we need per-intent information on whether we need to
	// poison.
	if err := ir.ResolveIntents(ctx, resolveIntents,
		ResolveOptions{Wait: false, Poison: true}); err != nil {
		return cleanup, roachpb.NewError(err)
	}

	return cleanup, nil
}

func getPusherTxn(h roachpb.Header) roachpb.Transaction {
	// If the txn is nil, we communicate a priority by sending an empty
	// txn with only the priority set. This is official usage of PushTxn.
	txn := h.Txn
	if txn == nil {
		txn = &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Priority: roachpb.MakePriority(h.UserPriority),
			},
		}
	}
	return *txn
}

// maybePushIntents tries to push the conflicting transaction(s)
// responsible for the given intents: either move its
// timestamp forward on a read/write conflict, abort it on a
// write/write conflict, or do nothing if the transaction is no longer
// pending.
//
// Returns a slice of intents which can now be resolved, and an error.
// The returned intents should be resolved via IntentResolver.ResolveIntents.
//
// If skipIfInFlight is true, then no PushTxns will be sent and no
// intents will be returned for any transaction for which there is
// another push in progress. This should only be used by callers who
// are not relying on the side effect of a push (i.e. only
// pushType==PUSH_TOUCH), and who also don't need to synchronize with
// the resolution of those intents (e.g. asynchronous resolutions of
// intents skipped on inconsistent reads).
//
// Callers are involved with
// a) conflict resolution for commands being executed at the Store with the
//    client waiting,
// b) resolving intents encountered during inconsistent operations, and
// c) resolving intents upon EndTransaction which are not local to the given
//    range. This is the only path in which the transaction is going to be
//    in non-pending state and doesn't require a push.
func (ir *IntentResolver) maybePushIntents(
	ctx context.Context,
	intents []roachpb.Intent,
	h roachpb.Header,
	pushType roachpb.PushTxnType,
	skipIfInFlight bool,
) ([]roachpb.Intent, *roachpb.Error) {
	// Attempt to push the transaction(s) which created the conflicting intent(s).
	pushTxns := make(map[uuid.UUID]enginepb.TxnMeta)
	for _, intent := range intents {
		if intent.Status != roachpb.PENDING {
			// The current intent does not need conflict resolution
			// because the transaction is already finalized.
			// This shouldn't happen as all intents created are in
			// the PENDING status.
			return nil, roachpb.NewErrorf("unexpected %s intent: %+v", intent.Status, intent)
		}
		pushTxns[intent.Txn.ID] = intent.Txn
	}

	pushedTxns, pErr := ir.MaybePushTransactions(ctx, pushTxns, h, pushType, skipIfInFlight)
	if pErr != nil {
		return nil, pErr
	}
	return updateIntentTxnStatus(ctx, pushedTxns, intents, skipIfInFlight, nil), nil
}

// updateIntentTxnStatus takes a slice of intents and a set of pushed
// transactions (like returned from MaybePushTransactions) and updates
// each intent with its corresponding TxnMeta and Status.
// resultSlice is an optional value to allow the caller to preallocate
// the returned intent slice.
func updateIntentTxnStatus(
	ctx context.Context,
	pushedTxns map[uuid.UUID]roachpb.Transaction,
	intents []roachpb.Intent,
	skipIfInFlight bool,
	results []roachpb.Intent,
) []roachpb.Intent {
	for _, intent := range intents {
		pushee, ok := pushedTxns[intent.Txn.ID]
		if !ok {
			// The intent was not pushed.
			if !skipIfInFlight {
				log.Fatalf(ctx, "no PushTxn response for intent %+v", intent)
			}
			// It must have been skipped.
			continue
		}
		intent.Txn = pushee.TxnMeta
		intent.Status = pushee.Status
		results = append(results, intent)
	}
	return results
}

// MaybePushTransactions is like maybePushIntents except it takes a set of
// transactions to push instead of a set of intents. This set of provided
// transactions may be modified by the method. It returns a set of transaction
// protos corresponding to the pushed transactions.
func (ir *IntentResolver) MaybePushTransactions(
	ctx context.Context,
	pushTxns map[uuid.UUID]enginepb.TxnMeta,
	h roachpb.Header,
	pushType roachpb.PushTxnType,
	skipIfInFlight bool,
) (map[uuid.UUID]roachpb.Transaction, *roachpb.Error) {
	// Decide which transactions to push and which to ignore because
	// of other in-flight requests. For those transactions that we
	// will be pushing, increment their ref count in the in-flight
	// pushes map.
	ir.mu.Lock()
	for txnID := range pushTxns {
		_, pushTxnInFlight := ir.mu.inFlightPushes[txnID]
		if pushTxnInFlight && skipIfInFlight {
			// Another goroutine is working on this transaction so we can
			// skip it.
			if log.V(1) {
				log.Infof(ctx, "skipping PushTxn for %s; attempt already in flight", txnID)
			}
			delete(pushTxns, txnID)
		} else {
			ir.mu.inFlightPushes[txnID]++
		}
	}
	cleanupInFlightPushes := func() {
		ir.mu.Lock()
		for txnID := range pushTxns {
			ir.mu.inFlightPushes[txnID]--
			if ir.mu.inFlightPushes[txnID] == 0 {
				delete(ir.mu.inFlightPushes, txnID)
			}
		}
		ir.mu.Unlock()
	}
	ir.mu.Unlock()
	if len(pushTxns) == 0 {
		return nil, nil
	}

	pusherTxn := getPusherTxn(h)
	log.Eventf(ctx, "pushing %d transaction(s)", len(pushTxns))

	// Attempt to push the transaction(s).
	b := &client.Batch{}
	b.Header.Timestamp = ir.clock.Now()
	for _, pushTxn := range pushTxns {
		b.AddRawRequest(&roachpb.PushTxnRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: pushTxn.Key,
			},
			PusherTxn:       pusherTxn,
			PusheeTxn:       pushTxn,
			PushTo:          h.Timestamp.Next(),
			InclusivePushTo: true,
			PushType:        pushType,
		})
	}
	err := ir.db.Run(ctx, b)
	cleanupInFlightPushes()
	if err != nil {
		return nil, b.MustPErr()
	}

	br := b.RawResponse()
	pushedTxns := map[uuid.UUID]roachpb.Transaction{}
	for _, resp := range br.Responses {
		txn := resp.GetInner().(*roachpb.PushTxnResponse).PusheeTxn
		if _, ok := pushedTxns[txn.ID]; ok {
			log.Fatalf(ctx, "have two PushTxn responses for %s", txn.ID)
		}
		pushedTxns[txn.ID] = txn
		log.Eventf(ctx, "%s is now %s", txn.ID, txn.Status)
	}
	return pushedTxns, nil
}

// runAsyncTask semi-synchronously runs a generic task function. If
// there is spare capacity in the limited async task semaphore, it's
// run asynchronously; otherwise, it's run synchronously if
// allowSyncProcessing is true; if false, an error is returned.
func (ir *IntentResolver) runAsyncTask(
	ctx context.Context, allowSyncProcessing bool, taskFn func(context.Context),
) error {
	if ir.testingKnobs.DisableAsyncIntentResolution {
		return errors.New("intents not processed as async resolution is disabled")
	}
	err := ir.stopper.RunLimitedAsyncTask(
		// If we've successfully launched a background task, dissociate
		// this work from our caller's context and timeout.
		ir.ambientCtx.AnnotateCtx(context.Background()),
		"storage.IntentResolver: processing intents",
		ir.sem,
		false, /* wait */
		taskFn,
	)
	if err != nil {
		if err == stop.ErrThrottled {
			ir.Metrics.IntentResolverAsyncThrottled.Inc(1)
			if allowSyncProcessing {
				// A limited task was not available. Rather than waiting for
				// one, we reuse the current goroutine.
				taskFn(ctx)
				return nil
			}
		}
		return errors.Wrapf(err, "during async intent resolution")
	}
	return nil
}

// CleanupIntentsAsync asynchronously processes intents which were
// encountered during another command but did not interfere with the
// execution of that command. This occurs during inconsistent
// reads.
func (ir *IntentResolver) CleanupIntentsAsync(
	ctx context.Context, intents []result.IntentsWithArg, allowSyncProcessing bool,
) error {
	now := ir.clock.Now()
	for _, item := range intents {
		if err := ir.runAsyncTask(ctx, allowSyncProcessing, func(ctx context.Context) {
			err := contextutil.RunWithTimeout(ctx, "async intent resolution",
				asyncIntentResolutionTimeout, func(ctx context.Context) error {
					_, err := ir.CleanupIntents(ctx, item.Intents, now, roachpb.PUSH_TOUCH)
					return err
				})
			if err != nil && ir.every.ShouldLog() {
				log.Warning(ctx, err)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

// CleanupIntents processes a collection of intents by pushing each
// implicated transaction using the specified pushType. Intents
// belonging to non-pending transactions after the push are resolved.
// On success, returns the number of resolved intents. On error, a
// subset of the intents may have been resolved, but zero will be
// returned.
func (ir *IntentResolver) CleanupIntents(
	ctx context.Context, intents []roachpb.Intent, now hlc.Timestamp, pushType roachpb.PushTxnType,
) (int, error) {
	h := roachpb.Header{Timestamp: now}

	// All transactions in MaybePushTxns (called by maybePushIntents) will be sent
	// in a single batch. In order to ensure that progress is made, we want to
	// ensure that this batch does not become too big as to time out due to a
	// deadline set above this call. If the attempt to push intents times out
	// before any intents have been resolved, no progress is made. Since batches
	// are atomic, a batch that times out has no effect. Hence, we chunk the work
	// to ensure progress even when a timeout is eventually hit.
	sort.Sort(intentsByTxn(intents))
	resolved := 0
	const skipIfInFlight = true
	pushTxns := make(map[uuid.UUID]enginepb.TxnMeta)
	for unpushed := intents; len(unpushed) > 0; {
		for k := range pushTxns { // clear the pushTxns map
			delete(pushTxns, k)
		}
		var prevTxnID uuid.UUID
		var i int
		for i = 0; i < len(unpushed); i++ {
			if curTxn := unpushed[i].Txn; curTxn.ID != prevTxnID {
				if len(pushTxns) == cleanupIntentsTxnsPerBatch {
					break
				}
				prevTxnID = curTxn.ID
				pushTxns[curTxn.ID] = curTxn
			}
		}

		pushedTxns, pErr := ir.MaybePushTransactions(ctx, pushTxns, h, pushType, skipIfInFlight)
		if pErr != nil {
			return 0, errors.Wrapf(pErr.GoError(), "failed to push during intent resolution")
		}
		resolveIntents := updateIntentTxnStatus(ctx, pushedTxns, unpushed[:i],
			skipIfInFlight, unpushed[:0])
		// resolveIntents with poison=true because we're resolving
		// intents outside of the context of an EndTransaction.
		//
		// Naively, it doesn't seem like we need to poison the abort
		// cache since we're pushing with PUSH_TOUCH - meaning that
		// the primary way our Push leads to aborting intents is that
		// of the transaction having timed out (and thus presumably no
		// client being around any more, though at the time of writing
		// we don't guarantee that). But there are other paths in which
		// the Push comes back successful while the coordinating client
		// may still be active. Examples of this are when:
		//
		// - the transaction was aborted by someone else, but the
		//   coordinating client may still be running.
		// - the transaction entry wasn't written yet, which at the
		//   time of writing has our push abort it, leading to the
		//   same situation as above.
		//
		// Thus, we must poison.
		if err := ir.ResolveIntents(
			ctx, resolveIntents, ResolveOptions{Wait: true, Poison: true},
		); err != nil {
			return 0, errors.Wrapf(err, "failed to resolve intents")
		}
		resolved += len(resolveIntents)
		unpushed = unpushed[i:]
	}
	return resolved, nil
}

// CleanupTxnIntentsAsync asynchronously cleans up intents owned by
// a transaction on completion.
func (ir *IntentResolver) CleanupTxnIntentsAsync(
	ctx context.Context,
	rangeID roachpb.RangeID,
	endTxns []result.EndTxnIntents,
	allowSyncProcessing bool,
) error {
	now := ir.clock.Now()
	for _, et := range endTxns {
		if err := ir.runAsyncTask(ctx, allowSyncProcessing, func(ctx context.Context) {
			locked, release := ir.lockInFlightTxnCleanup(ctx, et.Txn.ID)
			if !locked {
				return
			}
			defer release()
			intents := roachpb.AsIntents(et.Txn.IntentSpans, &et.Txn)
			if err := ir.cleanupFinishedTxnIntents(ctx, rangeID, &et.Txn, intents, now, et.Poison, nil); err != nil {
				if ir.every.ShouldLog() {
					log.Warningf(ctx, "failed to cleanup transaction intents: %v", err)
				}
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

// lockInFlightTxnCleanup ensures that only a single attempt is being made
// to cleanup the intents belonging to the specified transaction. Returns
// whether this attempt to lock succeeded and if so, a function to release
// the lock, to be invoked subsequently by the caller.
func (ir *IntentResolver) lockInFlightTxnCleanup(
	ctx context.Context, txnID uuid.UUID,
) (locked bool, release func()) {
	ir.mu.Lock()
	defer ir.mu.Unlock()
	_, inFlight := ir.mu.inFlightTxnCleanups[txnID]
	if inFlight {
		log.Eventf(ctx, "skipping txn resolved; already in flight")
		return false, nil
	}
	ir.mu.inFlightTxnCleanups[txnID] = struct{}{}
	return true, func() {
		ir.mu.Lock()
		delete(ir.mu.inFlightTxnCleanups, txnID)
		ir.mu.Unlock()
	}
}

// CleanupTxnIntentsOnGCAsync cleans up extant intents owned by a single
// transaction, asynchronously (but returning an error if the IntentResolver's
// semaphore is maxed out). If the transaction is not finalized, but expired, it
// is pushed first to abort it. onComplete is called if non-nil upon completion
// of async task with the intention that it be used as a hook to update metrics.
// It will not be called if an error is returned.
func (ir *IntentResolver) CleanupTxnIntentsOnGCAsync(
	ctx context.Context,
	rangeID roachpb.RangeID,
	txn *roachpb.Transaction,
	intents []roachpb.Intent,
	now hlc.Timestamp,
	onComplete func(pushed, succeeded bool),
) error {
	return ir.stopper.RunLimitedAsyncTask(
		// If we've successfully launched a background task,
		// dissociate this work from our caller's context and
		// timeout.
		ir.ambientCtx.AnnotateCtx(context.Background()),
		"processing txn intents",
		ir.sem,
		// We really do not want to hang up the GC queue on this kind of
		// processing, so it's better to just skip txns which we can't
		// pass to the async processor (wait=false). Their intents will
		// get cleaned up on demand, and we'll eventually get back to
		// them. Not much harm in having old txn records lying around in
		// the meantime.
		false, /* wait */
		func(ctx context.Context) {
			var pushed, succeeded bool
			defer func() {
				if onComplete != nil {
					onComplete(pushed, succeeded)
				}
			}()
			locked, release := ir.lockInFlightTxnCleanup(ctx, txn.ID)
			if !locked {
				return
			}
			defer release()
			// If the transaction is not yet finalized, but expired, push it
			// before resolving the intents.
			if !txn.Status.IsFinalized() {
				if !txnwait.IsExpired(now, txn) {
					log.VErrEventf(ctx, 3, "cannot push a %s transaction which is not expired: %s", txn.Status, txn)
					return
				}
				b := &client.Batch{}
				b.Header.Timestamp = now
				b.AddRawRequest(&roachpb.PushTxnRequest{
					RequestHeader: roachpb.RequestHeader{Key: txn.Key},
					PusherTxn: roachpb.Transaction{
						TxnMeta: enginepb.TxnMeta{Priority: enginepb.MaxTxnPriority},
					},
					PusheeTxn:       txn.TxnMeta,
					PushType:        roachpb.PUSH_ABORT,
					InclusivePushTo: true,
				})
				pushed = true
				if err := ir.db.Run(ctx, b); err != nil {
					log.VErrEventf(ctx, 2, "failed to push %s, expired txn (%s): %s", txn.Status, txn, err)
					return
				}
				// Get the pushed txn and update the intents slice.
				txn = &b.RawResponse().Responses[0].GetInner().(*roachpb.PushTxnResponse).PusheeTxn
				for i := range intents {
					intents[i].Txn = txn.TxnMeta
					intents[i].Status = txn.Status
				}
			}
			var onCleanupComplete func(error)
			if onComplete != nil {
				onCompleteCopy := onComplete // copy onComplete for use in onCleanupComplete
				onCleanupComplete = func(err error) {
					onCompleteCopy(pushed, err == nil)
				}
			}
			// Set onComplete to nil to disable the deferred call as the call has now
			// been delegated to the callback passed to cleanupFinishedTxnIntents.
			onComplete = nil
			err := ir.cleanupFinishedTxnIntents(ctx, rangeID, txn, intents, now, false /* poison */, onCleanupComplete)
			if err != nil {
				if ir.every.ShouldLog() {
					log.Warningf(ctx, "failed to cleanup transaction intents: %+v", err)
				}
			}
		},
	)
}

func (ir *IntentResolver) gcTxnRecord(
	ctx context.Context, rangeID roachpb.RangeID, txn *roachpb.Transaction,
) error {
	// We successfully resolved the intents, so we're able to GC from
	// the txn span directly.
	txnKey := keys.TransactionKey(txn.Key, txn.ID)
	// This is pretty tricky. Transaction keys are range-local and
	// so they are encoded specially. The key range addressed by
	// (txnKey, txnKey.Next()) might be empty (since Next() does
	// not imply monotonicity on the address side). Instead, we
	// send this request to a range determined using the resolved
	// transaction anchor, i.e. if the txn is anchored on
	// /Local/RangeDescriptor/"a"/uuid, the key range below would
	// be ["a", "a\x00"). However, the first range is special again
	// because the above procedure results in KeyMin, but we need
	// at least KeyLocalMax.
	//
	// #7880 will address this by making GCRequest less special and
	// thus obviating the need to cook up an artificial range here.
	var gcArgs roachpb.GCRequest
	{
		key := keys.MustAddr(txn.Key)
		if localMax := keys.MustAddr(keys.LocalMax); key.Less(localMax) {
			key = localMax
		}
		endKey := key.Next()

		gcArgs.RequestHeader = roachpb.RequestHeader{
			Key:    key.AsRawKey(),
			EndKey: endKey.AsRawKey(),
		}
	}
	gcArgs.Keys = append(gcArgs.Keys, roachpb.GCRequest_GCKey{
		Key: txnKey,
	})
	// Although the IntentResolver has a RangeDescriptorCache it could consult to
	// to determine the range to which this request corresponds, GCRequests are
	// always issued on behalf of the range on which this record resides which is
	// a strong signal that it is the range which will contain the transaction
	// record now.
	_, err := ir.gcBatcher.Send(ctx, rangeID, &gcArgs)
	if err != nil {
		return errors.Wrapf(err, "could not GC completed transaction anchored at %s",
			roachpb.Key(txn.Key))
	}
	return nil
}

// cleanupFinishedTxnIntents cleans up extant intents owned by a single
// transaction and when all intents have been successfully resolved, the
// transaction record is GC'ed asynchronously. onComplete will be called when
// all processing has completed which is likely to be after this call returns
// in the case of success.
func (ir *IntentResolver) cleanupFinishedTxnIntents(
	ctx context.Context,
	rangeID roachpb.RangeID,
	txn *roachpb.Transaction,
	intents []roachpb.Intent,
	now hlc.Timestamp,
	poison bool,
	onComplete func(error),
) (err error) {
	defer func() {
		// When err is non-nil we are guaranteed that the async task is not started
		// so there is no race on calling onComplete.
		if err != nil && onComplete != nil {
			onComplete(err)
		}
	}()
	// Resolve intents.
	min, _ := txn.InclusiveTimeBounds()
	opts := ResolveOptions{Wait: true, Poison: poison, MinTimestamp: min}
	if err := ir.ResolveIntents(ctx, intents, opts); err != nil {
		return errors.Wrapf(err, "failed to resolve intents")
	}
	// Run transaction record GC outside of ir.sem.
	return ir.stopper.RunAsyncTask(
		ctx,
		"storage.IntentResolver: cleanup txn records",
		func(ctx context.Context) {
			err := ir.gcTxnRecord(ctx, rangeID, txn)
			if onComplete != nil {
				onComplete(err)
			}
			if err != nil {
				if ir.every.ShouldLog() {
					log.Warningf(ctx, "failed to gc transaction record: %v", err)
				}
			}
		})
}

// ResolveOptions is used during intent resolution. It specifies whether the
// caller wants the call to block, and whether the ranges containing the intents
// are to be poisoned.
type ResolveOptions struct {
	// Resolve intents synchronously. When set to `false`, requests a
	// semi-synchronous operation, returning when all local commands have
	// been *proposed* but not yet committed or executed. This ensures that
	// if a waiting client retries immediately after calling this function,
	// it will not hit the same intents again.
	//
	// TODO(bdarnell): Note that this functionality has been removed and
	// will be ignored, pending resolution of #8360.
	Wait   bool
	Poison bool
	// The original transaction timestamp from the earliest txn epoch; if
	// supplied, resolution of intent ranges can be optimized in some cases.
	MinTimestamp hlc.Timestamp
}

// lookupRangeID maps a key to a RangeID for best effort batching of intent
// resolution requests.
func (ir *IntentResolver) lookupRangeID(ctx context.Context, key roachpb.Key) roachpb.RangeID {
	rKey, err := keys.Addr(key)
	if err != nil {
		if ir.every.ShouldLog() {
			log.Warningf(ctx, "failed to resolve addr for key %q: %+v", key, err)
		}
		return 0
	}
	rDesc, err := ir.rdc.LookupRangeDescriptor(ctx, rKey)
	if err != nil {
		if ir.every.ShouldLog() {
			log.Warningf(ctx, "failed to look up range descriptor for key %q: %+v", key, err)
		}
		return 0
	}
	return rDesc.RangeID
}

// ResolveIntents synchronously resolves intents accordings to opts.
func (ir *IntentResolver) ResolveIntents(
	ctx context.Context, intents []roachpb.Intent, opts ResolveOptions,
) error {
	if len(intents) == 0 {
		return nil
	}
	// Avoid doing any work on behalf of expired contexts. See
	// https://github.com/cockroachdb/cockroach/issues/15997.
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "aborted resolving intents")
	}
	log.Eventf(ctx, "resolving intents [wait=%t]", opts.Wait)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type resolveReq struct {
		rangeID roachpb.RangeID
		req     roachpb.Request
	}
	var resolveReqs []resolveReq
	var resolveRangeReqs []roachpb.Request
	for i := range intents {
		intent := intents[i] // avoids a race in `i, intent := range ...`
		if len(intent.EndKey) == 0 {
			resolveReqs = append(resolveReqs,
				resolveReq{
					rangeID: ir.lookupRangeID(ctx, intent.Key),
					req: &roachpb.ResolveIntentRequest{
						RequestHeader: roachpb.RequestHeaderFromSpan(intent.Span),
						IntentTxn:     intent.Txn,
						Status:        intent.Status,
						Poison:        opts.Poison,
					},
				})
		} else {
			resolveRangeReqs = append(resolveRangeReqs, &roachpb.ResolveIntentRangeRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(intent.Span),
				IntentTxn:     intent.Txn,
				Status:        intent.Status,
				Poison:        opts.Poison,
				MinTimestamp:  opts.MinTimestamp,
			})
		}
	}

	respChan := make(chan requestbatcher.Response, len(resolveReqs))
	for _, req := range resolveReqs {
		if err := ir.irBatcher.SendWithChan(ctx, respChan, req.rangeID, req.req); err != nil {
			return err
		}
	}
	for seen := 0; seen < len(resolveReqs); seen++ {
		select {
		case resp := <-respChan:
			if resp.Err != nil {
				return resp.Err
			}
			_ = resp.Resp // ignore the response
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Resolve spans differently. We don't know how many intents will be
	// swept up with each request, so we limit the spanning resolve
	// requests to a maximum number of keys and resume as necessary.
	for _, req := range resolveRangeReqs {
		for {
			b := &client.Batch{}
			b.Header.MaxSpanRequestKeys = intentResolverBatchSize
			b.AddRawRequest(req)
			if err := ir.db.Run(ctx, b); err != nil {
				return err
			}
			// Check response to see if it must be resumed.
			resp := b.RawResponse().Responses[0].GetInner().(*roachpb.ResolveIntentRangeResponse)
			if resp.ResumeSpan == nil {
				break
			}
			reqCopy := *(req.(*roachpb.ResolveIntentRangeRequest))
			reqCopy.SetSpan(*resp.ResumeSpan)
			req = &reqCopy
		}
	}

	return nil
}

// intentsByTxn implements sort.Interface to sort intents based on txnID.
type intentsByTxn []roachpb.Intent

var _ sort.Interface = intentsByTxn(nil)

func (s intentsByTxn) Len() int      { return len(s) }
func (s intentsByTxn) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s intentsByTxn) Less(i, j int) bool {
	return bytes.Compare(s[i].Txn.ID[:], s[j].Txn.ID[:]) < 0
}
