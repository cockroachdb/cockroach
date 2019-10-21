// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// Send fetches a range based on the header's replica, assembles method, args &
// reply into a Raft Cmd struct and executes the command using the fetched
// range.
//
// An incoming request may be transactional or not. If it is not transactional,
// the timestamp at which it executes may be higher than that optionally
// specified through the incoming BatchRequest, and it is not guaranteed that
// all operations are written at the same timestamp. If it is transactional, a
// timestamp must not be set - it is deduced automatically from the
// transaction. In particular, the read (original) timestamp will be used for
// all reads and the write (provisional commit) timestamp will be used for
// all writes. See the comments on txn.TxnMeta.Timestamp and txn.OrigTimestamp
// for more details.
//
// Should a transactional operation be forced to a higher timestamp (for
// instance due to the timestamp cache or finding a committed value in the path
// of one of its writes), the response will have a transaction set which should
// be used to update the client transaction object.
func (s *Store) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// Attach any log tags from the store to the context (which normally
	// comes from gRPC).
	ctx = s.AnnotateCtx(ctx)
	for _, union := range ba.Requests {
		arg := union.GetInner()
		header := arg.Header()
		if err := verifyKeys(header.Key, header.EndKey, roachpb.IsRange(arg)); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	// Limit the number of concurrent AddSSTable requests, since they're expensive
	// and block all other writes to the same span.
	if ba.IsSingleAddSSTableRequest() {
		before := timeutil.Now()
		if err := s.limiters.ConcurrentAddSSTableRequests.Begin(ctx); err != nil {
			return nil, roachpb.NewError(err)
		}
		defer s.limiters.ConcurrentAddSSTableRequests.Finish()

		beforeEngineDelay := timeutil.Now()
		s.engine.PreIngestDelay(ctx)
		after := timeutil.Now()

		waited, waitedEngine := after.Sub(before), after.Sub(beforeEngineDelay)
		s.metrics.AddSSTableProposalTotalDelay.Inc(waited.Nanoseconds())
		s.metrics.AddSSTableProposalEngineDelay.Inc(waitedEngine.Nanoseconds())
		if waited > time.Second {
			log.Infof(ctx, "SST ingestion was delayed by %v (%v for storage engine back-pressure)",
				waited, waitedEngine)
		}
	}

	if err := ba.SetActiveTimestamp(s.Clock().Now); err != nil {
		return nil, roachpb.NewError(err)
	}

	if s.cfg.TestingKnobs.ClockBeforeSend != nil {
		s.cfg.TestingKnobs.ClockBeforeSend(s.cfg.Clock, ba)
	}

	// Update our clock with the incoming request timestamp. This advances the
	// local node's clock to a high water mark from all nodes with which it has
	// interacted. We hold on to the resulting timestamp - we know that any
	// write with a higher timestamp we run into later must have started after
	// this point in (absolute) time.
	var now hlc.Timestamp
	if s.cfg.TestingKnobs.DisableMaxOffsetCheck {
		now = s.cfg.Clock.Update(ba.Timestamp)
	} else {
		// If the command appears to come from a node with a bad clock,
		// reject it now before we reach that point.
		var err error
		if now, err = s.cfg.Clock.UpdateAndCheckMaxOffset(ba.Timestamp); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			// On panic, don't run the defer. It's probably just going to panic
			// again due to undefined state.
			panic(r)
		}
		if ba.Txn != nil {
			// We're in a Txn, so we can reduce uncertainty restarts by attaching
			// the above timestamp to the returned response or error. The caller
			// can use it to shorten its uncertainty interval when it comes back to
			// this node.
			if pErr != nil {
				pErr.OriginNode = ba.Replica.NodeID
				if txn := pErr.GetTxn(); txn == nil {
					pErr.SetTxn(ba.Txn)
				}
			} else {
				if br.Txn == nil {
					br.Txn = ba.Txn
				}
				// Update our clock with the outgoing response txn timestamp
				// (if timestamp has been forwarded).
				if ba.Timestamp.Less(br.Txn.Timestamp) {
					s.cfg.Clock.Update(br.Txn.Timestamp)
				}
			}
		} else {
			if pErr == nil {
				// Update our clock with the outgoing response timestamp.
				// (if timestamp has been forwarded).
				if ba.Timestamp.Less(br.Timestamp) {
					s.cfg.Clock.Update(br.Timestamp)
				}
			}
		}

		if pErr != nil {
			pErr.Now = now
		} else {
			br.Now = now
		}
	}()

	if ba.Txn != nil {
		// We make our transaction aware that no other operation that causally
		// precedes it could have started after `now`. This is important: If we
		// wind up pushing a value, it will be in our immediate future, and not
		// updating the top end of our uncertainty timestamp would lead to a
		// restart (at least in the absence of a prior observed timestamp from
		// this node, in which case the following is a no-op).
		if _, ok := ba.Txn.GetObservedTimestamp(ba.Replica.NodeID); !ok {
			txnClone := ba.Txn.Clone()
			txnClone.UpdateObservedTimestamp(ba.Replica.NodeID, now)
			ba.Txn = txnClone
		}
	}

	if log.V(1) {
		log.Eventf(ctx, "executing %s", ba)
	} else if log.HasSpanOrEvent(ctx) {
		log.Eventf(ctx, "executing %d requests", len(ba.Requests))
	}

	var cleanupAfterWriteIntentError func(newWIErr *roachpb.WriteIntentError, newIntentTxn *enginepb.TxnMeta)
	defer func() {
		if cleanupAfterWriteIntentError != nil {
			// This request wrote an intent only if there was no error, the request
			// is transactional, the transaction is not yet finalized, and the request
			// wasn't read-only.
			if pErr == nil && ba.Txn != nil && !br.Txn.Status.IsFinalized() && !ba.IsReadOnly() {
				cleanupAfterWriteIntentError(nil, &br.Txn.TxnMeta)
			} else {
				cleanupAfterWriteIntentError(nil, nil)
			}
		}
	}()

	// Add the command to the range for execution; exit retry loop on success.
	for {
		// Exit loop if context has been canceled or timed out.
		if err := ctx.Err(); err != nil {
			return nil, roachpb.NewError(errors.Wrap(err, "aborted during Store.Send"))
		}

		// Get range and add command to the range for execution.
		repl, err := s.GetReplica(ba.RangeID)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if !repl.IsInitialized() {
			repl.mu.RLock()
			replicaID := repl.mu.replicaID
			repl.mu.RUnlock()

			// If we have an uninitialized copy of the range, then we are
			// probably a valid member of the range, we're just in the
			// process of getting our snapshot. If we returned
			// RangeNotFoundError, the client would invalidate its cache,
			// but we can be smarter: the replica that caused our
			// uninitialized replica to be created is most likely the
			// leader.
			return nil, roachpb.NewError(&roachpb.NotLeaseHolderError{
				RangeID:     ba.RangeID,
				LeaseHolder: repl.creatingReplica,
				// The replica doesn't have a range descriptor yet, so we have to build
				// a ReplicaDescriptor manually.
				Replica: roachpb.ReplicaDescriptor{
					NodeID:    repl.store.nodeDesc.NodeID,
					StoreID:   repl.store.StoreID(),
					ReplicaID: replicaID,
				},
			})
		}

		// If necessary, the request may need to wait in the txn wait queue,
		// pending updates to the target transaction for either PushTxn or
		// QueryTxn requests.
		if br, pErr = s.maybeWaitForPushee(ctx, &ba, repl); br != nil || pErr != nil {
			return br, pErr
		}
		br, pErr = repl.Send(ctx, ba)
		if pErr == nil {
			return br, nil
		}

		// Handle push txn failures and write intent conflicts locally and
		// retry. Other errors are returned to caller.
		switch t := pErr.GetDetail().(type) {
		case *roachpb.TransactionPushError:
			// On a transaction push error, retry immediately if doing so will
			// enqueue into the txnWaitQueue in order to await further updates to
			// the unpushed txn's status. We check ShouldPushImmediately to avoid
			// retrying non-queueable PushTxnRequests (see #18191).
			dontRetry := s.cfg.TestingKnobs.DontRetryPushTxnFailures
			if !dontRetry && ba.IsSinglePushTxnRequest() {
				pushReq := ba.Requests[0].GetInner().(*roachpb.PushTxnRequest)
				dontRetry = txnwait.ShouldPushImmediately(pushReq)
			}
			if dontRetry {
				// If we're not retrying on push txn failures return a txn retry error
				// after the first failure to guarantee a retry.
				if ba.Txn != nil {
					err := roachpb.NewTransactionRetryError(
						roachpb.RETRY_REASON_UNKNOWN, "DontRetryPushTxnFailures testing knob")
					return nil, roachpb.NewErrorWithTxn(err, ba.Txn)
				}
				return nil, pErr
			}

			// Enqueue unsuccessfully pushed transaction on the txnWaitQueue and
			// retry the command.
			repl.txnWaitQueue.Enqueue(&t.PusheeTxn)
			pErr = nil

		case *roachpb.IndeterminateCommitError:
			if s.cfg.TestingKnobs.DontRecoverIndeterminateCommits {
				return nil, pErr
			}
			// On an indeterminate commit error, attempt to recover and finalize
			// the stuck transaction. Retry immediately if successful.
			if _, err := s.recoveryMgr.ResolveIndeterminateCommit(ctx, t); err != nil {
				// Do not propagate ambiguous results; assume success and retry original op.
				if _, ok := err.(*roachpb.AmbiguousResultError); !ok {
					// Preserve the error index.
					index := pErr.Index
					pErr = roachpb.NewError(err)
					pErr.Index = index
					return nil, pErr
				}
			}
			// We've recovered the transaction that blocked the push; retry command.
			pErr = nil

		case *roachpb.WriteIntentError:
			// Process and resolve write intent error. We do this here because
			// this is the code path with the requesting client waiting.
			if pErr.Index != nil {
				var pushType roachpb.PushTxnType
				if ba.IsWrite() {
					pushType = roachpb.PUSH_ABORT
				} else {
					pushType = roachpb.PUSH_TIMESTAMP
				}

				index := pErr.Index
				args := ba.Requests[index.Index].GetInner()
				// Make a copy of the header for the upcoming push; we will update
				// the timestamp.
				h := ba.Header
				if h.Txn != nil {
					// We must push at least to h.Timestamp, but in fact we want to
					// go all the way up to a timestamp which was taken off the HLC
					// after our operation started. This allows us to not have to
					// restart for uncertainty as we come back and read.
					obsTS, ok := h.Txn.GetObservedTimestamp(ba.Replica.NodeID)
					if !ok {
						// This was set earlier in this method, so it's
						// completely unexpected to not be found now.
						log.Fatalf(ctx, "missing observed timestamp: %+v", h.Txn)
					}
					h.Timestamp.Forward(obsTS)
					// We are going to hand the header (and thus the transaction proto)
					// to the RPC framework, after which it must not be changed (since
					// that could race). Since the subsequent execution of the original
					// request might mutate the transaction, make a copy here.
					//
					// See #9130.
					h.Txn = h.Txn.Clone()
				}
				// Handle the case where we get more than one write intent error;
				// we need to cleanup the previous attempt to handle it to allow
				// any other pusher queued up behind this RPC to proceed.
				if cleanupAfterWriteIntentError != nil {
					cleanupAfterWriteIntentError(t, nil)
				}
				if cleanupAfterWriteIntentError, pErr =
					s.intentResolver.ProcessWriteIntentError(ctx, pErr, args, h, pushType); pErr != nil {
					// Do not propagate ambiguous results; assume success and retry original op.
					if _, ok := pErr.GetDetail().(*roachpb.AmbiguousResultError); !ok {
						// Preserve the error index.
						pErr.Index = index
						return nil, pErr
					}
					pErr = nil
				}
				// We've resolved the write intent; retry command.
			}

		case *roachpb.MergeInProgressError:
			// A merge was in progress. We need to retry the command after the merge
			// completes, as signaled by the closing of the replica's mergeComplete
			// channel. Note that the merge may have already completed, in which case
			// its mergeComplete channel will be nil.
			mergeCompleteCh := repl.getMergeCompleteCh()
			if mergeCompleteCh != nil {
				select {
				case <-mergeCompleteCh:
					// Merge complete. Retry the command.
				case <-ctx.Done():
					return nil, roachpb.NewError(errors.Wrap(ctx.Err(), "aborted during merge"))
				case <-s.stopper.ShouldQuiesce():
					return nil, roachpb.NewError(&roachpb.NodeUnavailableError{})
				}
			}
			pErr = nil
		}

		if pErr != nil {
			return nil, pErr
		}
	}
}

// maybeWaitForPushee potentially diverts the incoming request to
// the txnwait.Queue, where it will wait for updates to the target
// transaction.
func (s *Store) maybeWaitForPushee(
	ctx context.Context, ba *roachpb.BatchRequest, repl *Replica,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// If this is a push txn request, check the push queue first, which
	// may cause this request to wait and either return a successful push
	// txn response or else allow this request to proceed.
	if ba.IsSinglePushTxnRequest() {
		pushReq := ba.Requests[0].GetInner().(*roachpb.PushTxnRequest)
		pushResp, pErr := repl.txnWaitQueue.MaybeWaitForPush(repl.AnnotateCtx(ctx), repl, pushReq)
		// Copy the request in anticipation of setting the force arg and
		// updating the Now timestamp (see below).
		pushReqCopy := *pushReq
		if pErr == txnwait.ErrDeadlock {
			// We've experienced a deadlock; set Force=true on push request,
			// and set the push type to ABORT.
			pushReqCopy.Force = true
			pushReqCopy.PushType = roachpb.PUSH_ABORT
		} else if pErr != nil {
			return nil, pErr
		} else if pushResp != nil {
			br := &roachpb.BatchResponse{}
			br.Add(pushResp)
			return br, nil
		}
		// Move the push timestamp forward to the current time, as this
		// request may have been waiting to push the txn. If we don't
		// move the timestamp forward to the current time, we may fail
		// to push a txn which has expired.
		now := s.Clock().Now()
		ba.Timestamp.Forward(now)
		ba.Requests = nil
		ba.Add(&pushReqCopy)
	} else if ba.IsSingleQueryTxnRequest() {
		// For query txn requests, wait in the txn wait queue either for
		// transaction update or for dependent transactions to change.
		queryReq := ba.Requests[0].GetInner().(*roachpb.QueryTxnRequest)
		pErr := repl.txnWaitQueue.MaybeWaitForQuery(repl.AnnotateCtx(ctx), repl, queryReq)
		if pErr != nil {
			return nil, pErr
		}
	}

	return nil, nil
}
