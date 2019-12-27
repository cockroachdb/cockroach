// Copyright 2018 The Cockroach Authors.
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
	"container/list"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	return p.txn != nil && p.txn.IsWriting()
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
	clock *hlc.Clock
	db    *client.DB

	// keys is a map from key to a linked list of pusher instances,
	// ordered as a FIFO queue.
	mu struct {
		syncutil.Mutex
		keys map[string]*contendedKey
	}
}

func (cq *contentionQueue) numContended(key roachpb.Key) int {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	ck, ok := cq.mu.keys[string(key)]
	if !ok {
		return 0
	}
	return ck.ll.Len()
}

func newContentionQueue(clock *hlc.Clock, db *client.DB) *contentionQueue {
	cq := &contentionQueue{
		clock: clock,
		db:    db,
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
// pusher has no transaction or the transaction is not yet writing.
//
// Note that the supplied wiErr write intent error must have only a
// single intent (len(wiErr.Intents) == 1).
//
// Returns a cleanup function to be invoked by the caller after the
// original request completes, a possibly updated WriteIntentError,
// a bool indicating whether the intent resolver should regard the
// original push / resolve as no longer applicable and skip those
// steps to retry the original request that generated the
// WriteIntentError, and an error if one was encountered while
// waiting in the queue. The cleanup function takes two arguments,
// a newWIErr, non-nil in case the re-executed request experienced
// another write intent error and could not complete; and
// newIntentTxn, nil if the re-executed request left no intent, and
// non-nil if it did. At most one of these two arguments should be
// provided.
func (cq *contentionQueue) add(
	ctx context.Context, wiErr *roachpb.WriteIntentError, h roachpb.Header,
) (CleanupFunc, *roachpb.WriteIntentError, bool, *roachpb.Error) {
	if len(wiErr.Intents) != 1 {
		log.Fatalf(ctx, "write intent error must contain only a single intent: %s", wiErr)
	}
	if hasExtremePriority(h) {
		// Never queue maximum or minimum priority transactions.
		return nil, wiErr, false, nil
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
	} else if contended.lastTxnMeta.ID != intent.Txn.ID {
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
	var pErr *roachpb.Error
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
				pErr = roachpb.NewError(ctx.Err())
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
				// If we're at the start of the queue loop and wait
				// for the wait channel to signal.
				if frontOfQueue {
					log.VEventf(ctx, 3, "%s at front of queue; breaking from loop", txnID(curPusher.txn))
					break Loop
				}

				b := &client.Batch{}
				b.Header.Timestamp = cq.clock.Now()
				b.AddRawRequest(&roachpb.PushTxnRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: pusheeTxn.Key,
					},
					PusherTxn: getPusherTxn(h),
					PusheeTxn: *pusheeTxn,
					PushTo:    h.Timestamp.Next(),
					PushType:  roachpb.PUSH_ABORT,
				})
				log.VEventf(ctx, 3, "%s pushing %s to detect dependency cycles", txnID(curPusher.txn), pusheeTxn.ID.Short())
				if err := cq.db.Run(ctx, b); err != nil {
					pErr = b.MustPErr()
					log.VErrEventf(ctx, 2, "while waiting in push contention queue to push %s: %s", pusheeTxn.ID.Short(), pErr)
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
		if newWIErr != nil && newIntentTxn != nil {
			// The need for this implies that the function should be split
			// into a more rigidly defined handle with multiple accessors.
			// TODO(nvanbenschoten): clean this up and test better when we're
			// not intending the change to be backported.
			panic("newWIErr and newIntentTxn both provided")
		}
		if newWIErr == nil {
			log.VEventf(ctx, 3, "%s finished, leaving intent? %t (owned by %s)", txnID(curPusher.txn), newIntentTxn != nil, newIntentTxn)
		} else {
			log.VEventf(ctx, 3, "%s encountered another write intent error %s", txnID(curPusher.txn), newWIErr)
		}
		cq.mu.Lock()
		defer cq.mu.Unlock()
		// Remove the current element from its list of pushers.
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
		contended.ll.Remove(curElement)

		if contended.ll.Len() == 0 {
			// If the contendedKey's list is now empty, remove it. We don't need
			// to send on or close our waitCh because no one is or ever will wait
			// on it.
			delete(cq.mu.keys, key)
		} else {
			// If the pusher re-executed its request and encountered another
			// write intent error, check if it's for the same intent; if so,
			// we can set the newIntentTxn to match the new intent. If not,
			// make sure that we don't pollute the old contendedKey with any
			// new information.
			if newWIErr != nil {
				sameKey := len(newWIErr.Intents) == 1 && newWIErr.Intents[0].Span.Equal(intent.Span)
				if sameKey {
					newIntentTxn = &newWIErr.Intents[0].Txn
				} else {
					// If the pusher re-executed and found a different intent, make
					// sure that we don't tell the old contendedKey anything about
					// the new intent's transaction. This new intent could be from
					// an earlier request in the batch than the one that previously
					// hit the error, so we don't know anything about the state of
					// the old intent.
					newIntentTxn = nil
				}
			}
			if newIntentTxn != nil {
				// Shallow copy the TxnMeta. After this request returns (i.e.
				// now), we might mutate it (DistSender and such), but the
				// receiver of the channel will read it.
				newIntentTxnCopy := *newIntentTxn
				newIntentTxn = &newIntentTxnCopy
				contended.setLastTxnMeta(newIntentTxn)
			}
			curPusher.waitCh <- newIntentTxn
			close(curPusher.waitCh)
		}
	}, wiErr, done, pErr
}

func hasExtremePriority(h roachpb.Header) bool {
	if h.Txn != nil {
		p := h.Txn.Priority
		return p == enginepb.MaxTxnPriority || p == enginepb.MinTxnPriority
	}
	return false
}
