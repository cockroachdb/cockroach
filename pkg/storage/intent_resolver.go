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
	"container/list"
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

	// intentResolverBatchSize is the maximum number of intents that will be
	// resolved in a single batch. Batches that span many ranges (which is
	// possible for the commit of a transaction that spans many ranges) will be
	// split into many batches by the DistSender, leading to high CPU overhead
	// and quadratic memory usage.
	intentResolverBatchSize = 100
)

type pusher struct {
	txn      *roachpb.Transaction
	waitCh   chan *enginepb.TxnMeta
	detectCh chan struct{}
}

func newPusher(txn *roachpb.Transaction) *pusher {
	p := &pusher{
		txn:    txn,
		waitCh: make(chan *enginepb.TxnMeta, 1),
	}
	if p.activeTxn() {
		p.detectCh = make(chan struct{}, 1)
		// Signal the channel in order to begin dependency cycle detection.
		p.detectCh <- struct{}{}
	}
	return p
}

func (p *pusher) activeTxn() bool {
	return p.txn != nil && p.txn.Key != nil
}

type contendedKey struct {
	ll *list.List
	// lastTxnMeta is the most recent txn to own the intent. Active
	// transactions in the contention queue for this intent must push
	// this txn record in order to detect dependency cycles.
	lastTxnMeta *enginepb.TxnMeta
}

func newContendedKey() *contendedKey {
	return &contendedKey{
		ll: list.New(),
	}
}

// setLastTxnMeta sets the most recent txn meta and sends to all
// pushers w/ active txns still in the queue to push, to guarantee
// detection of dependency cycles.
func (ck *contendedKey) setLastTxnMeta(txnMeta *enginepb.TxnMeta) {
	ck.lastTxnMeta = txnMeta
	if txnMeta == nil {
		return
	}
	for e := ck.ll.Front(); e != nil; e = e.Next() {
		p := e.Value.(*pusher)
		if p.detectCh != nil {
			select {
			case p.detectCh <- struct{}{}:
			default:
			}
		}
	}
}

// contentionQueue handles contention on keys with conflicting intents
// by forming queues of "pushers" which are requests that experienced
// a WriteIntentError. There is a queue for each key with one or more
// pushers. Queues are complicated by the difference between pushers
// with writing transactions (i.e. they may have a transaction record
// which can be pushed) and non-writing transactions (e.g.,
// non-transactional requests, and read-only transactions).
//
// Queues are linked lists, with each element containing a transaction
// (can be nil), and a wait channel. The wait channel is closed when
// the request is dequeued and run to completion, whether to success
// or failure. Pushers wait on the most recent pusher in the queue to
// complete. However, pushers with an active transaction (i.e., txns
// with a non-nil key) must send a PushTxn RPC. This is necessary in
// order to properly detect dependency cycles.
type contentionQueue struct {
	// keys is a map from key to a linked list of pusher instances,
	// ordered as a FIFO queue.
	store *Store
	mu    struct {
		syncutil.Mutex
		keys map[string]*contendedKey
	}
}

func newContentionQueue(store *Store) *contentionQueue {
	cq := &contentionQueue{
		store: store,
	}
	cq.mu.keys = map[string]*contendedKey{}
	return cq
}

func txnID(txn *roachpb.Transaction) string {
	if txn == nil {
		return "nil txn"
	}
	return txn.ID.Short()
}

// add adds the intent specified in the supplied wiErr to the
// contention queue. This may block the current goroutine if the
// pusher has no transaction or the transaction is not yet writing
// (i.e. read-only or hasn't successfully executed BeginTxn).
//
// Note that the supplied wiErr write intent error must have only a
// single intent (len(wiErr.Intents) == 1).
//
// Returns a cleanup function to be invoked by the caller after the
// original request completes, a possibly updated WriteIntentError and
// a bool indicating whether the intent resolver should regard the
// original push / resolve as no longer applicable and skip those
// steps to retry the original request that generated the
// WriteIntentError. The cleanup function takes two arguments, a
// newWIErr, non-nil in case the re-executed request experienced
// another write intent error and could not complete; and
// newIntentTxn, nil if the re-executed request left no intent, and
// non-nil if it did.
func (cq *contentionQueue) add(
	ctx context.Context, wiErr *roachpb.WriteIntentError, h roachpb.Header,
) (
	func(newWIErr *roachpb.WriteIntentError, newIntentTxn *enginepb.TxnMeta),
	*roachpb.WriteIntentError,
	bool,
) {
	if len(wiErr.Intents) != 1 {
		log.Fatalf(ctx, "write intent error must contain only a single intent: %s", wiErr)
	}
	intent := wiErr.Intents[0]
	key := string(intent.Span.Key)
	curPusher := newPusher(h.Txn)
	log.VEventf(ctx, 3, "adding %s to contention queue on intent %s @%s", txnID(h.Txn), intent.Key, intent.Txn.ID.Short())

	// Consider prior pushers in reverse arrival order to build queue
	// by waiting on the most recent overlapping pusher.
	var waitCh chan *enginepb.TxnMeta
	var curElement *list.Element

	cq.mu.Lock()
	contended, ok := cq.mu.keys[key]
	if !ok {
		contended = newContendedKey()
		contended.setLastTxnMeta(&intent.Txn)
		cq.mu.keys[key] = contended
	} else if contended.lastTxnMeta == nil || contended.lastTxnMeta.ID != intent.Txn.ID {
		contended.setLastTxnMeta(&intent.Txn)
	}

	// Get the prior pusher to wait on.
	if e := contended.ll.Back(); e != nil {
		p := e.Value.(*pusher)
		waitCh = p.waitCh
		log.VEventf(ctx, 3, "%s waiting on %s", txnID(curPusher.txn), txnID(p.txn))
	}

	// Append the current pusher to the queue.
	curElement = contended.ll.PushBack(curPusher)
	cq.mu.Unlock()

	// Delay before pushing in order to detect dependency cycles.
	const dependencyCyclePushDelay = 100 * time.Millisecond

	// Wait on prior pusher, if applicable.
	var done bool
	if waitCh != nil {
		var detectCh chan struct{}
		var detectReady <-chan time.Time
		// If the current pusher has an active txn, we need to push the
		// transaction which owns the intent to detect dependency cycles.
		// To avoid unnecessary push traffic, insert a delay by
		// instantiating the detectReady, which sets detectCh when it
		// fires. When detectCh receives, it sends a push to the most
		// recent txn to own the intent.
		if curPusher.detectCh != nil {
			detectReady = time.After(dependencyCyclePushDelay)
		}

	Loop:
		for {
			select {
			case txnMeta, ok := <-waitCh:
				if !ok {
					log.Fatalf(ctx, "the wait channel of a prior pusher was used twice (pusher=%s)", txnMeta)
				}
				// If the prior pusher wrote an intent, push it instead by
				// creating a copy of the WriteIntentError with updated txn.
				if txnMeta != nil {
					log.VEventf(ctx, 3, "%s exiting contention queue to push %s", txnID(curPusher.txn), txnMeta.ID.Short())
					wiErrCopy := *wiErr
					wiErrCopy.Intents = []roachpb.Intent{
						{
							Span:   intent.Span,
							Txn:    *txnMeta,
							Status: roachpb.PENDING,
						},
					}
					wiErr = &wiErrCopy
				} else {
					// No intent was left by the prior pusher; don't push, go
					// immediately to retrying the conflicted request.
					log.VEventf(ctx, 3, "%s exiting contention queue to proceed", txnID(curPusher.txn))
					done = true
				}
				break Loop

			case <-ctx.Done():
				// The pusher's context is done. Return without pushing.
				done = true
				break Loop

			case <-detectReady:
				// When the detect timer fires, set detectCh and loop.
				log.VEventf(ctx, 3, "%s cycle detection is ready", txnID(curPusher.txn))
				detectCh = curPusher.detectCh

			case <-detectCh:
				cq.mu.Lock()
				frontOfQueue := curElement == contended.ll.Front()
				pusheeTxn := contended.lastTxnMeta
				cq.mu.Unlock()
				// If we're at the start of the queue, or there's no pushee
				// transaction (the previous pusher didn't leave an intent),
				// loop and wait for the wait channel to signal.
				if frontOfQueue {
					log.VEventf(ctx, 3, "%s at front of queue; breaking from loop", txnID(curPusher.txn))
					break Loop
				} else if pusheeTxn == nil {
					log.VEventf(ctx, 3, "%s cycle detection skipped because there is no txn to push", txnID(curPusher.txn))
					detectCh = nil
					detectReady = time.After(dependencyCyclePushDelay)
					continue
				}
				pushReq := &roachpb.PushTxnRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: pusheeTxn.Key,
					},
					PusherTxn: getPusherTxn(h),
					PusheeTxn: *pusheeTxn,
					PushTo:    h.Timestamp,
					Now:       cq.store.Clock().Now(),
					PushType:  roachpb.PUSH_ABORT,
				}
				b := &client.Batch{}
				b.AddRawRequest(pushReq)
				log.VEventf(ctx, 3, "%s pushing %s to detect dependency cycles", txnID(curPusher.txn), pusheeTxn.ID.Short())
				if err := cq.store.db.Run(ctx, b); err != nil {
					log.VErrEventf(ctx, 2, "while waiting in push contention queue to push %s: %s", pusheeTxn.ID.Short(), b.MustPErr())
					done = true // done=true to avoid uselessly trying to push and resolve
					break Loop
				}
				// Note that this pusher may have aborted the pushee, but it
				// should still wait on the previous pusher's wait channel.
				detectCh = nil
				detectReady = time.After(dependencyCyclePushDelay)
			}
		}
	}

	return func(newWIErr *roachpb.WriteIntentError, newIntentTxn *enginepb.TxnMeta) {
		if newWIErr == nil {
			log.VEventf(ctx, 3, "%s finished, leaving intent? %t (owned by %s)", txnID(curPusher.txn), newIntentTxn != nil, newIntentTxn)
		} else {
			log.VEventf(ctx, 3, "%s encountered another write intent error %s", txnID(curPusher.txn), newWIErr)
		}
		cq.mu.Lock()
		// If the current element isn't the front, it's being removed
		// because its context was canceled. Swap the wait channel with
		// the previous element.
		if curElement != contended.ll.Front() {
			prevPusher := curElement.Prev().Value.(*pusher)
			if waitCh != nil && prevPusher.waitCh != waitCh {
				log.Fatalf(ctx, "expected previous pusher's wait channel to be the one current pusher waited on")
			}
			prevPusher.waitCh, curPusher.waitCh = curPusher.waitCh, prevPusher.waitCh
		}
		// If the pusher re-executed its request and encountered another
		// write intent error, check if it's for the same intent; if so,
		// we can set the newIntentTxn to match the new intent.
		if newWIErr != nil && len(newWIErr.Intents) == 1 &&
			len(newWIErr.Intents[0].EndKey) == 0 && newWIErr.Intents[0].Key.Equal(intent.Key) {
			newIntentTxn = &newWIErr.Intents[0].Txn
		}
		contended.ll.Remove(curElement)
		if contended.ll.Len() == 0 {
			delete(cq.mu.keys, key)
		} else if newIntentTxn != nil {
			contended.setLastTxnMeta(newIntentTxn)
		} else if newWIErr != nil {
			// Note that we don't update last txn meta unless we know for
			// sure the txn which has written the most recent intent to the
			// contended key (i.e. newWIErr != nil).
			contended.setLastTxnMeta(nil)
		}
		curPusher.waitCh <- newIntentTxn
		close(curPusher.waitCh)
		cq.mu.Unlock()
	}, wiErr, done
}

// intentResolver manages the process of pushing transactions and
// resolving intents.
type intentResolver struct {
	store *Store

	sem         chan struct{}    // Semaphore to limit async goroutines.
	contentionQ *contentionQueue // manages contention on individual keys

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

func newIntentResolver(store *Store, taskLimit int) *intentResolver {
	ir := &intentResolver{
		store:       store,
		sem:         make(chan struct{}, taskLimit),
		contentionQ: newContentionQueue(store),
		every:       log.Every(time.Minute),
	}
	ir.mu.inFlightPushes = map[uuid.UUID]int{}
	ir.mu.inFlightTxnCleanups = map[uuid.UUID]struct{}{}
	return ir
}

// processWriteIntentError tries to push the conflicting
// transaction(s) responsible for the given WriteIntentError, and to
// resolve those intents if possible. Returns a cleanup function and
// potentially a new error to be used in place of the original. The
// cleanup function should be invoked by the caller after the request
// which experienced the conflict has completed with a parameter
// specifying a transaction in the event that the request left its own
// intent.
func (ir *intentResolver) processWriteIntentError(
	ctx context.Context,
	wiPErr *roachpb.Error,
	args roachpb.Request,
	h roachpb.Header,
	pushType roachpb.PushTxnType,
) (func(newWIErr *roachpb.WriteIntentError, newIntentTxn *enginepb.TxnMeta), *roachpb.Error) {
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
		// Note that the write intent error may be mutated here in the event
		// that this pusher is queued to wait for a different transaction
		// instead.
		if cleanup, wiErr, done = ir.contentionQ.add(ctx, wiErr, h); done {
			return cleanup, nil
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
	if err := ir.resolveIntents(ctx, resolveIntents,
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
func (ir *intentResolver) maybePushIntents(
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

	pushedTxns, pErr := ir.maybePushTransactions(ctx, pushTxns, h, pushType, skipIfInFlight)
	if pErr != nil {
		return nil, pErr
	}

	var resolveIntents []roachpb.Intent
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
		resolveIntents = append(resolveIntents, intent)
	}
	return resolveIntents, nil
}

// maybePushTransactions is like maybePushIntents except it takes a set of
// transactions to push instead of a set of intents. This set of provided
// transactions may be modified by the method. It returns a set of transaction
// protos corresponding to the pushed transactions.
func (ir *intentResolver) maybePushTransactions(
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

	log.Eventf(ctx, "pushing %d transaction(s)", len(pushTxns))

	// Attempt to push the transaction(s).
	now := ir.store.Clock().Now()
	pusherTxn := getPusherTxn(h)
	var pushReqs []roachpb.Request
	for _, pushTxn := range pushTxns {
		pushReqs = append(pushReqs, &roachpb.PushTxnRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: pushTxn.Key,
			},
			PusherTxn: pusherTxn,
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
	cleanupInFlightPushes()
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
	return pushedTxns, nil
}

// runAsyncTask semi-synchronously runs a generic task function. If
// there is spare capacity in the limited async task semaphore, it's
// run asynchronously; otherwise, it's run synchronously if
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
	if err != nil {
		if err == stop.ErrThrottled {
			ir.store.metrics.IntentResolverAsyncThrottled.Inc(1)
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

// cleanupIntentsAsync asynchronously processes intents which were
// encountered during another command but did not interfere with the
// execution of that command. This occurs during inconsistent
// reads.
func (ir *intentResolver) cleanupIntentsAsync(
	ctx context.Context, r *Replica, intents []result.IntentsWithArg, allowSyncProcessing bool,
) error {
	now := r.store.Clock().Now()
	for _, item := range intents {
		if err := ir.runAsyncTask(ctx, r, allowSyncProcessing, func(ctx context.Context) {
			if _, err := ir.cleanupIntents(ctx, item.Intents, now, roachpb.PUSH_TOUCH); err != nil {
				if ir.every.ShouldLog() {
					log.Warning(ctx, err)
				}
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
	resolveIntents, pushErr := ir.maybePushIntents(
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
				if ir.every.ShouldLog() {
					log.Warningf(ctx, "failed to cleanup transaction intents: %s", err)
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
					RequestHeader: roachpb.RequestHeader{Key: txn.Key},
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
				if ir.every.ShouldLog() {
					log.Warningf(ctx, "failed to cleanup transaction intents: %s", err)
				}
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

		gcArgs.RequestHeader = roachpb.RequestHeader{
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
				RequestHeader: roachpb.RequestHeaderFromSpan(intent.Span),
				IntentTxn:     intent.Txn,
				Status:        intent.Status,
				Poison:        opts.Poison,
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
			reqCopy.SetSpan(*resp.ResumeSpan)
			req = &reqCopy
		}
	}

	return nil
}
