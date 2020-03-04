// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
// transaction. In particular, the read timestamp will be used for
// all reads and the write (provisional commit) timestamp will be used for
// all writes. See the comments on txn.TxnMeta.Timestamp and txn.ReadTimestamp
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

	if ba.Txn != nil && ba.Txn.ReadTimestamp.Less(ba.Txn.DeprecatedOrigTimestamp) {
		// For compatibility with 19.2 nodes which might not have set ReadTimestamp,
		// fallback to DeprecatedOrigTimestamp. Note that even if ReadTimestamp is
		// set, it might still be less than DeprecatedOrigTimestamp if the txn was
		// restarted.
		ba.Txn = ba.Txn.Clone()
		ba.Txn.ReadTimestamp = ba.Txn.DeprecatedOrigTimestamp
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
				if ba.Timestamp.Less(br.Txn.WriteTimestamp) {
					s.cfg.Clock.Update(br.Txn.WriteTimestamp)
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

	br, pErr = repl.Send(ctx, ba)
	if pErr == nil {
		return br, nil
	}

	// Augment error if necessary and return.
	switch t := pErr.GetDetail().(type) {
	case *roachpb.RangeKeyMismatchError:
		// On a RangeKeyMismatchError where the batch didn't even overlap
		// the start of the mismatched Range, try to suggest a more suitable
		// Range from this Store.
		rSpan, err := keys.Range(ba.Requests)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if !t.MismatchedRange.ContainsKey(rSpan.Key) {
			if r2 := s.LookupReplica(rSpan.Key); r2 != nil {
				// Only return the correct range descriptor as a hint
				// if we know the current lease holder for that range, which
				// indicates that our knowledge is not stale.
				if l, _ := r2.GetLease(); r2.IsLeaseValid(l, s.Clock().Now()) {
					t.SuggestedRange = r2.Desc()
				}
			}
		}
	case *roachpb.RaftGroupDeletedError:
		// This error needs to be converted appropriately so that clients
		// will retry.
		err := roachpb.NewRangeNotFoundError(repl.RangeID, repl.store.StoreID())
		pErr = roachpb.NewError(err)
	}
	return nil, pErr
}
