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
//
// Author: Ben Darnell

package storage

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"golang.org/x/net/context"
)

// intentResolverTaskLimit is the maximum number of asynchronous tasks
// that may be started by intentResolver. When this limit is reached
// asynchronous tasks will start to block to apply backpressure.
// This is a last line of defense against issues like #4925.
// TODO(bdarnell): how to determine best value?
const intentResolverTaskLimit = 100

// intentResolverBatchSize is the maximum number of intents that will
// be resolved in a single batch. Batches that span many ranges (which
// is possible for the commit of a transaction that spans many ranges)
// will be split into many batches with NoopRequests by the
// DistSender, leading to high CPU overhead and quadratic memory
// usage.
const intentResolverBatchSize = 100

// intentResolver manages the process of pushing transactions and
// resolving intents.
type intentResolver struct {
	store *Store

	sem chan struct{} // Semaphore to limit async goroutines.

	mu struct {
		syncutil.Mutex
		// Maps transaction ids to a refcount.
		inFlight map[uuid.UUID]int
	}
}

func newIntentResolver(store *Store) *intentResolver {
	ir := &intentResolver{
		store: store,
		sem:   make(chan struct{}, intentResolverTaskLimit),
	}
	ir.mu.inFlight = map[uuid.UUID]int{}
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

	resolveIntents, pErr := ir.maybePushTransactions(ctx, wiErr.Intents, h, pushType, false)
	if pErr != nil {
		return pErr
	}

	if err := ir.resolveIntents(ctx, resolveIntents,
		false /* !wait */, pushType == roachpb.PUSH_ABORT /* poison */); err != nil {
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
	var pushIntents, nonPendingIntents []roachpb.Intent
	pushTxns := map[uuid.UUID]enginepb.TxnMeta{}
	for _, intent := range intents {
		if intent.Status != roachpb.PENDING {
			// The current intent does not need conflict resolution
			// because the transaction is already finalized.
			// This shouldn't happen as all intents created are in
			// the PENDING status.
			nonPendingIntents = append(nonPendingIntents, intent)
		} else if _, ok := ir.mu.inFlight[*intent.Txn.ID]; ok && skipIfInFlight {
			// Another goroutine is working on this transaction so we can
			// skip it.
			if log.V(1) {
				log.Infof(ctx, "skipping PushTxn for %s; attempt already in flight", intent.Txn.ID)
			}
			continue
		} else {
			pushTxns[*intent.Txn.ID] = intent.Txn
			pushIntents = append(pushIntents, intent)
			ir.mu.inFlight[*intent.Txn.ID]++
		}
	}
	ir.mu.Unlock()
	if len(nonPendingIntents) > 0 {
		return nil, roachpb.NewErrorf("unexpected aborted/resolved intents: %+v", nonPendingIntents)
	} else if len(pushIntents) == 0 {
		return []roachpb.Intent(nil), nil
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
			Now:           now,
			PushType:      pushType,
			NewPriorities: true,
		})
	}
	b := &client.Batch{}
	b.AddRawRequest(pushReqs...)
	var pErr *roachpb.Error
	if err := ir.store.db.Run(ctx, b); err != nil {
		pErr = b.MustPErr()
	}
	ir.mu.Lock()
	for _, intent := range pushIntents {
		ir.mu.inFlight[*intent.Txn.ID]--
		if ir.mu.inFlight[*intent.Txn.ID] == 0 {
			delete(ir.mu.inFlight, *intent.Txn.ID)
		}
	}
	ir.mu.Unlock()
	if pErr != nil {
		return nil, pErr
	}

	br := b.RawResponse()
	pushedTxns := map[uuid.UUID]roachpb.Transaction{}
	for _, resp := range br.Responses {
		txn := resp.GetInner().(*roachpb.PushTxnResponse).PusheeTxn
		if _, ok := pushedTxns[*txn.ID]; ok {
			panic(fmt.Sprintf("have two PushTxn responses for %s", txn.ID))
		}
		pushedTxns[*txn.ID] = txn
	}

	var resolveIntents []roachpb.Intent
	for _, intent := range pushIntents {
		pushee, ok := pushedTxns[*intent.Txn.ID]
		if !ok {
			panic(fmt.Sprintf("no PushTxn response for intent %+v", intent))
		}
		intent.Txn = pushee.TxnMeta
		intent.Status = pushee.Status
		resolveIntents = append(resolveIntents, intent)
	}
	return resolveIntents, nil
}

// processIntentsAsync asynchronously processes intents which were
// encountered during another command but did not interfere with the
// execution of that command. This occurs in two cases: inconsistent
// reads and EndTransaction (which queues its own external intents for
// processing via this method). The two cases are handled somewhat
// differently and would be better served by different entry points,
// but combining them simplifies the plumbing necessary in Replica.
func (ir *intentResolver) processIntentsAsync(r *Replica, intents []intentsWithArg) {
	if r.store.TestingKnobs().DisableAsyncIntentResolution {
		return
	}
	now := r.store.Clock().Now()
	ctx := context.TODO()
	stopper := r.store.Stopper()

	for _, item := range intents {
		err := stopper.RunLimitedAsyncTask(
			ctx, ir.sem, false /* wait */, func(ctx context.Context) {
				ir.processIntents(ctx, r, item, now)
			})
		if err != nil {
			if err == stop.ErrThrottled {
				// A limited task was not available. Rather than waiting for one, we
				// reuse the current goroutine.
				ir.processIntents(ctx, r, item, now)
			} else {
				log.Warningf(ctx, "failed to resolve intents: %s", err)
				return
			}
		}
	}
}

func (ir *intentResolver) processIntents(
	ctx context.Context, r *Replica, item intentsWithArg, now hlc.Timestamp,
) {
	// Everything here is best effort; give up rather than waiting
	// too long (helps avoid deadlocks during test shutdown,
	// although this is imperfect due to the use of an
	// uninterruptible WaitGroup.Wait in beginCmds).
	ctxWithTimeout, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()

	if item.args.Method() != roachpb.EndTransaction {
		h := roachpb.Header{Timestamp: now}
		resolveIntents, pushErr := ir.maybePushTransactions(ctxWithTimeout,
			item.intents, h, roachpb.PUSH_TOUCH, true /* skipInFlight */)

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
		if err := ir.resolveIntents(ctxWithTimeout, resolveIntents,
			true /* wait */, true /* poison */); err != nil {
			log.Warningf(ctx, "%s: failed to resolve intents: %s", r, err)
			return
		}
		if pushErr != nil {
			log.Warningf(ctx, "%s: failed to push during intent resolution: %s", r, pushErr)
			return
		}
	} else { // EndTransaction
		// For EndTransaction, we know the transaction is finalized so
		// we can skip the push and go straight to the resolve.
		//
		// This mechanism assumes that when an EndTransaction fails,
		// the client makes no assumptions about the result. For
		// example, an attempt to explicitly rollback the transaction
		// may succeed (triggering this code path), but the result may
		// not make it back to the client.
		if err := ir.resolveIntents(ctxWithTimeout, item.intents,
			true /* wait */, false /* !poison */); err != nil {
			log.Warningf(ctx, "%s: failed to resolve intents: %s", r, err)
			return
		}

		// We successfully resolved the intents, so we're able to GC from
		// the txn span directly.
		b := &client.Batch{}
		txn := item.intents[0].Txn
		txnKey := keys.TransactionKey(txn.Key, *txn.ID)

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
			log.Warningf(ctx, "could not GC completed transaction anchored at %s: %s",
				roachpb.Key(txn.Key), err)
			return
		}
	}
}

// resolveIntents resolves the given intents. `wait` is currently a
// no-op; all intents are resolved synchronously.
//
// TODO(bdarnell): Restore the wait=false optimization when/if #8360
// is fixed. `wait=false` requests a semi-synchronous operation,
// returning when all local commands have been *proposed* but not yet
// committed or executed. This ensures that if a waiting client
// retries immediately after calling this function, it will not hit
// the same intents again (in the absence of #8360, we provide this
// guarantee by resolving the intents synchronously regardless of the
// `wait` argument).
func (ir *intentResolver) resolveIntents(
	ctx context.Context, intents []roachpb.Intent, wait bool, poison bool,
) error {
	// Force synchronous operation; see above TODO.
	wait = true
	if len(intents) == 0 {
		return nil
	}
	// We're doing async stuff below; those need new traces.
	ctx, cleanup := tracing.EnsureContext(ctx, ir.store.Tracer(), "resolve intents")
	defer cleanup()
	log.Eventf(ctx, "resolving intents [wait=%t]", wait)

	var reqs []roachpb.Request
	for i := range intents {
		intent := intents[i] // avoids a race in `i, intent := range ...`
		var resolveArgs roachpb.Request
		{
			if len(intent.EndKey) == 0 {
				resolveArgs = &roachpb.ResolveIntentRequest{
					Span:      intent.Span,
					IntentTxn: intent.Txn,
					Status:    intent.Status,
					Poison:    poison,
				}
			} else {
				resolveArgs = &roachpb.ResolveIntentRangeRequest{
					Span:      intent.Span,
					IntentTxn: intent.Txn,
					Status:    intent.Status,
					Poison:    poison,
				}
			}
		}

		reqs = append(reqs, resolveArgs)
	}

	// Sort the intents to maximize batching by range.
	sort.Slice(reqs, func(i, j int) bool {
		return reqs[i].Header().Key.Compare(reqs[j].Header().Key) < 0
	})

	// Resolve all of the intents.
	var wg sync.WaitGroup
	var errCh chan error
	if wait {
		// If the caller is waiting, use this channel to collect the first
		// non-nil error (if any) from the async tasks.
		errCh = make(chan error, 1)
	}
	for len(reqs) > 0 {
		b := &client.Batch{}
		if len(reqs) > intentResolverBatchSize {
			b.AddRawRequest(reqs[:intentResolverBatchSize]...)
			reqs = reqs[intentResolverBatchSize:]
		} else {
			b.AddRawRequest(reqs...)
			reqs = nil
		}
		wg.Add(1)
		action := func() error {
			// TODO(tschottdorf): no tracing here yet.
			return ir.store.DB().Run(ctx, b)
		}
		if ir.store.Stopper().RunLimitedAsyncTask(
			ctx, ir.sem, true, /* wait */
			func(ctx context.Context) {
				defer wg.Done()

				if err := action(); err != nil {
					// If we have a waiting caller, pass the first non-nil
					// error out on the channel.
					select {
					case errCh <- err:
						return
					default:
					}
					// No caller waiting or channel full, so just log the error.
					log.Warningf(ctx, "unable to resolve external intents: %s", err)
				}
			}) != nil {
			wg.Done()
			// Try async to not keep the caller waiting, but when draining
			// just go ahead and do it synchronously. See #1684.
			// TODO(tschottdorf): This is ripe for removal.
			if err := action(); err != nil {
				return err
			}
		}
	}

	if wait {
		// Wait for all resolutions to complete. We don't want to return
		// as soon as the first one fails because of issue #8360 (see
		// comment at the top of this method)
		wg.Wait()
		select {
		case err := <-errCh:
			return err
		default:
		}
	}

	return nil
}
