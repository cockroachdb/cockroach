// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package storage

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	// defaultIntentResolverTaskLimit is the maximum number of asynchronous tasks
	// that may be started by intentResolver. When this limit is reached
	// asynchronous tasks will start to block to apply backpressure.  This is a
	// last line of defense against issues like #4925.
	// TODO(bdarnell): how to determine best value?
	defaultIntentResolverTaskLimit = 100

	// intentResolverTimeout is the timeout when processing a group of intents.
	// The timeout prevents intent resolution from getting stuck. Since
	// processing intents is best effort, we'd rather give up than wait too long
	// (this helps avoid deadlocks during test shutdown).
	intentResolverTimeout = 30 * time.Second

	// intentResolverBatchSize is the maximum number of intents that will
	// be resolved in a single batch. Batches that span many ranges (which
	// is possible for the commit of a transaction that spans many ranges)
	// will be split into many batches with NoopRequests by the
	// DistSender, leading to high CPU overhead and quadratic memory
	// usage.
	intentResolverBatchSize = 100
)

// intentResolver manages the process of pushing transactions and
// resolving intents.
type intentResolver struct {
	store *Store

	sem chan struct{} // Semaphore to limit async goroutines.

	mu struct {
		syncutil.Mutex
		// Map from txn ID being pushed to a refcount of intents waiting on the push.
		inFlightPushes map[uuid.UUID]int
		// Set of txn IDs whose list of intent spans are being resolved. Note that
		// this pertains only to EndTransaction-style intent cleanups, whether called
		// directly after EndTransaction evaluation or during GC of txn spans.
		inFlightTxnCleanups map[uuid.UUID]struct{}
	}
}

func newIntentResolver(store *Store, taskLimit int) *intentResolver {
	ir := &intentResolver{
		store: store,
		sem:   make(chan struct{}, taskLimit),
	}
	ir.mu.inFlightPushes = map[uuid.UUID]int{}
	ir.mu.inFlightTxnCleanups = map[uuid.UUID]struct{}{}
	return ir
}

// processWriteIntentError tries to push the conflicting
// transaction(s) responsible for the given WriteIntentError, and to
// resolve those intents if possible. Returns a new error to be used
// in place of the original.
func (ir *intentResolver) processWriteIntentError(
	ctx context.Context,
	wiPErr *roachpb.Error,
	args roachpb.Request,
	h roachpb.Header,
	pushType roachpb.PushTxnType,
) *roachpb.Error {
	wiErr, ok := wiPErr.GetDetail().(*roachpb.WriteIntentError)
	if !ok {
		return roachpb.NewErrorf("not a WriteIntentError: %v", wiPErr)
	}

	if log.V(6) {
		log.Infof(ctx, "resolving write intent %s", wiErr)
	}

	resolveIntents, pErr := ir.maybePushTransactions(
		ctx, wiErr.Intents, h, pushType, false, /* skipIfInFlight */
	)
	if pErr != nil {
		return pErr
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
	if err := ir.resolveIntents(ctx, resolveIntents,
		ResolveOptions{Wait: false, Poison: true}); err != nil {
		return roachpb.NewError(err)
	}

	return nil
}

// maybePushTransactions tries to push the conflicting transaction(s)
// responsible for the given intents: either move its
// timestamp forward on a read/write conflict, abort it on a
// write/write conflict, or do nothing if the transaction is no longer
// pending.
//
// Returns a slice of intents which can now be resolved, and an error.
// The returned intents should be resolved via intentResolver.resolveIntents.
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
func (ir *intentResolver) maybePushTransactions(
	ctx context.Context,
	intents []roachpb.Intent,
	h roachpb.Header,
	pushType roachpb.PushTxnType,
	skipIfInFlight bool,
) ([]roachpb.Intent, *roachpb.Error) {
	now := ir.store.Clock().Now()

	partialPusherTxn := h.Txn
	// If there's no pusher, we communicate a priority by sending an empty
	// txn with only the priority set. This is official usage of PushTxn.
	if partialPusherTxn == nil {
		partialPusherTxn = &roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Priority: roachpb.MakePriority(h.UserPriority),
			},
		}
	}

	// Split intents into those we need to push and those which are good to
	// resolve.
	ir.mu.Lock()
	// TODO(tschottdorf): can optimize this and use same underlying slice.
	var pushIntents []roachpb.Intent
	cleanupPushIntentsLocked := func() {
		for _, intent := range pushIntents {
			ir.mu.inFlightPushes[intent.Txn.ID]--
			if ir.mu.inFlightPushes[intent.Txn.ID] == 0 {
				delete(ir.mu.inFlightPushes, intent.Txn.ID)
			}
		}
	}
	pushTxns := map[uuid.UUID]enginepb.TxnMeta{}
	for _, intent := range intents {
		if intent.Status != roachpb.PENDING {
			// The current intent does not need conflict resolution
			// because the transaction is already finalized.
			// This shouldn't happen as all intents created are in
			// the PENDING status.
			cleanupPushIntentsLocked()
			ir.mu.Unlock()
			return nil, roachpb.NewErrorf("unexpected %s intent: %+v", intent.Status, intent)
		}
		_, alreadyPushing := pushTxns[intent.Txn.ID]
		_, pushTxnInFlight := ir.mu.inFlightPushes[intent.Txn.ID]
		if !alreadyPushing && pushTxnInFlight && skipIfInFlight {
			// Another goroutine is working on this transaction so we can
			// skip it.
			if log.V(1) {
				log.Infof(ctx, "skipping PushTxn for %s; attempt already in flight", intent.Txn.ID)
			}
			continue
		} else {
			pushTxns[intent.Txn.ID] = intent.Txn
			pushIntents = append(pushIntents, intent)
			ir.mu.inFlightPushes[intent.Txn.ID]++
		}
	}
	ir.mu.Unlock()
	if len(pushIntents) == 0 {
		return nil, nil
	}

	log.Eventf(ctx, "pushing %d transaction(s)", len(pushTxns))

	// Attempt to push the transaction(s) which created the conflicting intent(s).
	var pushReqs []roachpb.Request
	for _, pushTxn := range pushTxns {
		pushReqs = append(pushReqs, &roachpb.PushTxnRequest{
			Span: roachpb.Span{
				Key: pushTxn.Key,
			},
			PusherTxn: *partialPusherTxn,
			PusheeTxn: pushTxn,
			PushTo:    h.Timestamp,
			// The timestamp is used by PushTxn for figuring out whether the
			// transaction is abandoned. If we used the argument's timestamp
			// here, we would run into busy loops because that timestamp
			// usually stays fixed among retries, so it will never realize
			// that a transaction has timed out. See #877.
			Now:      now,
			PushType: pushType,
		})
	}
	b := &client.Batch{}
	b.AddRawRequest(pushReqs...)
	var pErr *roachpb.Error
	if err := ir.store.db.Run(ctx, b); err != nil {
		pErr = b.MustPErr()
	}
	ir.mu.Lock()
	cleanupPushIntentsLocked()
	ir.mu.Unlock()
	if pErr != nil {
		return nil, pErr
	}

	br := b.RawResponse()
	pushedTxns := map[uuid.UUID]roachpb.Transaction{}
	for _, resp := range br.Responses {
		txn := resp.GetInner().(*roachpb.PushTxnResponse).PusheeTxn
		if _, ok := pushedTxns[txn.ID]; ok {
			log.Fatalf(ctx, "have two PushTxn responses for %s\nreqs: %+v", txn.ID, pushReqs)
		}
		pushedTxns[txn.ID] = txn
		log.Eventf(ctx, "%s is now %s", txn.ID, txn.Status)
	}

	var resolveIntents []roachpb.Intent
	for _, intent := range pushIntents {
		pushee, ok := pushedTxns[intent.Txn.ID]
		if !ok {
			log.Fatalf(ctx, "no PushTxn response for intent %+v\nreqs: %+v", intent, pushReqs)
		}
		intent.Txn = pushee.TxnMeta
		intent.Status = pushee.Status
		resolveIntents = append(resolveIntents, intent)
	}
	return resolveIntents, nil
}

// runAsyncTask semi-synchronously runs a generic task function. If
// there is spare capacity in the limited async task semaphore, it's
// run asynchonrously; otherwise, it's run synchronously if
// allowSyncProcessing is true; if false, an error is returned.
func (ir *intentResolver) runAsyncTask(
	ctx context.Context, r *Replica, allowSyncProcessing bool, taskFn func(context.Context),
) error {
	if r.store.TestingKnobs().DisableAsyncIntentResolution {
		return errors.New("intents not processed as async resolution is disabled")
	}
	err := r.store.Stopper().RunLimitedAsyncTask(
		// If we've successfully launched a background task, dissociate
		// this work from our caller's context and timeout.
		ir.store.AnnotateCtx(context.Background()),
		"storage.intentResolver: processing intents",
		ir.sem,
		false, /* wait */
		taskFn,
	)
	if err == stop.ErrThrottled && allowSyncProcessing {
		// A limited task was not available. Rather than waiting for one, we
		// reuse the current goroutine.
		taskFn(ctx)
	} else if err != nil {
		return errors.Wrapf(err, "during async intent resolution")
	}
	return nil
}

// processIntentsAsync asynchronously processes intents which were
// encountered during another command but did not interfere with the
// execution of that command. This occurs during inconsistent
// reads.
func (ir *intentResolver) processIntentsAsync(
	ctx context.Context, r *Replica, intents []result.IntentsWithArg, allowSyncProcessing bool,
) error {
	now := r.store.Clock().Now()
	for _, item := range intents {
		if err := ir.runAsyncTask(ctx, r, allowSyncProcessing, func(ctx context.Context) {
			if _, err := ir.cleanupIntents(ctx, item.Intents, now, roachpb.PUSH_TOUCH); err != nil {
				log.Warning(ctx, err)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

// cleanupIntents processes a collection of intents by pushing each
// implicated transaction using the specified pushType. Intents
// belonging to non-pending transactions after the push are resolved.
// On success, returns the number of resolved intents. On error, a
// subset of the intents may have been resolved, but zero will be
// returned.
func (ir *intentResolver) cleanupIntents(
	ctx context.Context, intents []roachpb.Intent, now hlc.Timestamp, pushType roachpb.PushTxnType,
) (int, error) {
	h := roachpb.Header{Timestamp: now}
	resolveIntents, pushErr := ir.maybePushTransactions(
		ctx, intents, h, pushType, true, /* skipIfInFlight */
	)
	if pushErr != nil {
		return 0, errors.Wrapf(pushErr.GoError(), "failed to push during intent resolution")
	}

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
	if err := ir.resolveIntents(
		ctx, resolveIntents, ResolveOptions{Wait: true, Poison: true},
	); err != nil {
		return 0, errors.Wrapf(err, "failed to resolve intents")
	}
	return len(resolveIntents), nil
}

// cleanupTxnIntentsAsync asynchronously cleans up intents owned by
// a transaction on completion.
func (ir *intentResolver) cleanupTxnIntentsAsync(
	ctx context.Context, r *Replica, endTxns []result.EndTxnIntents, allowSyncProcessing bool,
) error {
	now := r.store.Clock().Now()
	for _, et := range endTxns {
		if err := ir.runAsyncTask(ctx, r, allowSyncProcessing, func(ctx context.Context) {
			locked, release := ir.lockInFlightTxnCleanup(ctx, et.Txn.ID)
			if !locked {
				return
			}
			defer release()
			intents := roachpb.AsIntents(et.Txn.Intents, &et.Txn)
			if err := ir.cleanupFinishedTxnIntents(ctx, &et.Txn, intents, now, et.Poison); err != nil {
				log.Warningf(ctx, "failed to cleanup transaction intents: %s", err)
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
func (ir *intentResolver) lockInFlightTxnCleanup(
	ctx context.Context, txnID uuid.UUID,
) (bool, func()) {
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

// cleanupTxnIntentsOnGCAsync cleans up extant intents owned by a
// single transaction, asynchronously (but returning an error if the
// intentResolver's semaphore is maxed out). If the transaction is
// PENDING, but expired, it is pushed first to abort it. This method
// updates the metrics for intents resolved on GC on success.
func (ir *intentResolver) cleanupTxnIntentsOnGCAsync(
	ctx context.Context, txn *roachpb.Transaction, intents []roachpb.Intent, now hlc.Timestamp,
) error {
	return ir.store.Stopper().RunLimitedAsyncTask(
		// If we've successfully launched a background task,
		// dissociate this work from our caller's context and
		// timeout.
		ir.store.AnnotateCtx(context.Background()),
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
			locked, release := ir.lockInFlightTxnCleanup(ctx, txn.ID)
			if !locked {
				return
			}
			defer release()

			// If the transaction is still pending, but expired, push it
			// before resolving the intents.
			if txn.Status == roachpb.PENDING {
				if !txnwait.IsExpired(now, txn) {
					log.VErrEventf(ctx, 3, "cannot push a PENDING transaction which is not expired: %s", txn)
					return
				}
				b := &client.Batch{}
				b.AddRawRequest(&roachpb.PushTxnRequest{
					Span: roachpb.Span{Key: txn.Key},
					PusherTxn: roachpb.Transaction{
						TxnMeta: enginepb.TxnMeta{Priority: roachpb.MaxTxnPriority},
					},
					PusheeTxn: txn.TxnMeta,
					Now:       now,
					PushType:  roachpb.PUSH_ABORT,
				})
				ir.store.metrics.GCPushTxn.Inc(1)
				if err := ir.store.DB().Run(ctx, b); err != nil {
					log.VErrEventf(ctx, 2, "failed to push PENDING, expired txn (%s): %s", txn, err)
					return
				}
				// Get the pushed txn and update the intents slice.
				txn = &b.RawResponse().Responses[0].GetInner().(*roachpb.PushTxnResponse).PusheeTxn
				for _, intent := range intents {
					intent.Txn = txn.TxnMeta
					intent.Status = txn.Status
				}
			}

			if err := ir.cleanupFinishedTxnIntents(ctx, txn, intents, now, false /* poison */); err != nil {
				log.Warningf(ctx, "failed to cleanup transaction intents: %s", err)
			} else {
				ir.store.metrics.GCResolveSuccess.Inc(int64(len(intents)))
			}
		},
	)
}

// cleanupFinishedTxnIntents cleans up extant intents owned by a
// single transaction and when all intents have been successfully
// resolved, the transaction record is GC'ed.
func (ir *intentResolver) cleanupFinishedTxnIntents(
	ctx context.Context,
	txn *roachpb.Transaction,
	intents []roachpb.Intent,
	now hlc.Timestamp,
	poison bool,
) error {
	// Resolve intents.
	min, _ := txn.InclusiveTimeBounds()
	opts := ResolveOptions{Wait: true, Poison: poison, MinTimestamp: min}
	if err := ir.resolveIntents(ctx, intents, opts); err != nil {
		return errors.Wrapf(err, "failed to resolve intents")
	}

	// We successfully resolved the intents, so we're able to GC from
	// the txn span directly.
	b := &client.Batch{}
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

		gcArgs.Span = roachpb.Span{
			Key:    key.AsRawKey(),
			EndKey: endKey.AsRawKey(),
		}
	}

	gcArgs.Keys = append(gcArgs.Keys, roachpb.GCRequest_GCKey{
		Key: txnKey,
	})
	b.AddRawRequest(&gcArgs)
	if err := ir.store.db.Run(ctx, b); err != nil {
		return errors.Wrapf(err, "could not GC completed transaction anchored at %s",
			roachpb.Key(txn.Key))
	}

	return nil
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

func (ir *intentResolver) resolveIntents(
	ctx context.Context, intents []roachpb.Intent, opts ResolveOptions,
) error {
	if len(intents) == 0 {
		return nil
	}
	// Avoid doing any work on behalf of expired contexts. See
	// https://github.com/cockroachdb/cockroach/issues/15997.
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Eventf(ctx, "resolving intents [wait=%t]", opts.Wait)

	var resolveReqs []roachpb.Request
	var resolveRangeReqs []roachpb.Request
	for i := range intents {
		intent := intents[i] // avoids a race in `i, intent := range ...`
		if len(intent.EndKey) == 0 {
			resolveReqs = append(resolveReqs, &roachpb.ResolveIntentRequest{
				Span:      intent.Span,
				IntentTxn: intent.Txn,
				Status:    intent.Status,
				Poison:    opts.Poison,
			})
		} else {
			resolveRangeReqs = append(resolveRangeReqs, &roachpb.ResolveIntentRangeRequest{
				Span:         intent.Span,
				IntentTxn:    intent.Txn,
				Status:       intent.Status,
				Poison:       opts.Poison,
				MinTimestamp: opts.MinTimestamp,
			})
		}
	}

	// Sort the intents to maximize batching by range.
	sort.Slice(resolveReqs, func(i, j int) bool {
		return resolveReqs[i].Header().Key.Compare(resolveReqs[j].Header().Key) < 0
	})

	// Resolve all of the intents in batches of size intentResolverBatchSize.
	// The maximum timeout is intentResolverTimeout, and this is applied to
	// each batch to ensure forward progress is made. A large set of intents
	// might require more time than a single timeout allows.
	for len(resolveReqs) > 0 {
		b := &client.Batch{}
		if len(resolveReqs) > intentResolverBatchSize {
			b.AddRawRequest(resolveReqs[:intentResolverBatchSize]...)
			resolveReqs = resolveReqs[intentResolverBatchSize:]
		} else {
			b.AddRawRequest(resolveReqs...)
			resolveReqs = nil
		}
		// Everything here is best effort; so give the context a timeout
		// to avoid waiting too long. This may be a larger timeout than
		// the context already has, in which case we'll respect the
		// existing timeout. A single txn can have more intents than we
		// can handle in the normal timeout, which would prevent us from
		// ever cleaning up all of its intents in time to then delete the
		// txn record, causing an infinite loop on that txn record, where
		// the same initial set of intents is endlessly re-resolved.
		ctxWithTimeout, cancel := context.WithTimeout(ctx, intentResolverTimeout)
		err := ir.store.DB().Run(ctxWithTimeout, b)
		cancel()
		if err != nil {
			// Bail out on the first error.
			return err
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
			ctxWithTimeout, cancel := context.WithTimeout(ctx, intentResolverTimeout)
			err := ir.store.DB().Run(ctxWithTimeout, b)
			cancel()
			if err != nil {
				// Bail out on the first error.
				return err
			}
			// Check response to see if it must be resumed.
			resp := b.RawResponse().Responses[0].GetInner().(*roachpb.ResolveIntentRangeResponse)
			if resp.ResumeSpan == nil {
				break
			}
			reqCopy := *(req.(*roachpb.ResolveIntentRangeRequest))
			reqCopy.SetHeader(*resp.ResumeSpan)
			req = &reqCopy
		}
	}

	return nil
}
