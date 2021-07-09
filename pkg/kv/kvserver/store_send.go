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
	"github.com/cockroachdb/cockroach/pkg/util/limit"
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

	if res, err := s.maybeThrottleBatch(ctx, ba); err != nil {
		return nil, roachpb.NewError(err)
	} else if res != nil {
		defer res.Release()
	}

	if err := ba.SetActiveTimestamp(s.Clock().Now); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Update our clock with the incoming request timestamp. This advances the
	// local node's clock to a high water mark from all nodes with which it has
	// interacted.
	if baClockTS, ok := ba.Timestamp.TryToClockTimestamp(); ok {
		if s.cfg.TestingKnobs.DisableMaxOffsetCheck {
			s.cfg.Clock.Update(baClockTS)
		} else {
			// If the command appears to come from a node with a bad clock,
			// reject it instead of updating the local clock and proceeding.
			if err := s.cfg.Clock.UpdateAndCheckMaxOffset(ctx, baClockTS); err != nil {
				return nil, roachpb.NewError(err)
			}
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
				pErr.OriginNode = s.NodeID()
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
					if clockTS, ok := br.Txn.WriteTimestamp.TryToClockTimestamp(); ok {
						s.cfg.Clock.Update(clockTS)
					}
				}
			}
		} else {
			if pErr == nil {
				// Update our clock with the outgoing response timestamp.
				// (if timestamp has been forwarded).
				if ba.Timestamp.Less(br.Timestamp) {
					if clockTS, ok := br.Timestamp.TryToClockTimestamp(); ok {
						s.cfg.Clock.Update(clockTS)
					}
				}
			}
		}

		// We get the latest timestamp - we know that any
		// write with a higher timestamp we run into later must
		// have started after this point in (absolute) time.
		now := s.cfg.Clock.NowAsClockTimestamp()
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
		if _, ok := ba.Txn.GetObservedTimestamp(s.NodeID()); !ok {
			txnClone := ba.Txn.Clone()
			txnClone.UpdateObservedTimestamp(s.NodeID(), s.Clock().NowAsClockTimestamp())
			ba.Txn = txnClone
		}
	}

	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Eventf(ctx, "executing %s", ba)
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
		// TODO(andrei): It seems silly that, if the client specified a RangeID that
		// doesn't match the keys it wanted to access, but this node can serve those
		// keys anyway, we still return a RangeKeyMismatchError to the client
		// instead of serving the request. Particularly since we have the mechanism
		// to communicate correct range information to the client when returning a
		// successful response (i.e. br.RangeInfos).

		// On a RangeKeyMismatchError where the batch didn't even overlap
		// the start of the mismatched Range, try to suggest a more suitable
		// Range from this Store.
		rSpan, err := keys.Range(ba.Requests)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		// The kvclient thought that a particular range id covers rSpans. It was
		// wrong; the respective range doesn't cover all of rSpan, or perhaps it
		// doesn't even overlap it. Clearly the client has a stale range cache.
		// We'll return info on the range that the request ended up being routed to
		// and, to the extent that we have the info, the ranges containing the keys
		// that the client requested, and all the ranges in between.
		ri := t.Ranges()[0]
		skipRID := ri.Desc.RangeID // We already have info on one range, so don't add it again below.
		startKey := ri.Desc.StartKey
		if rSpan.Key.Less(startKey) {
			startKey = rSpan.Key
		}
		endKey := ri.Desc.EndKey
		if endKey.Less(rSpan.EndKey) {
			endKey = rSpan.EndKey
		}
		var ris []roachpb.RangeInfo
		if err := s.visitReplicasByKey(ctx, startKey, endKey, AscendingKeyOrder, func(ctx context.Context, repl *Replica) error {
			// Note that we return the lease even if it's expired. The kvclient can
			// use it as it sees fit.
			ri := repl.GetRangeInfo(ctx)
			if ri.Desc.RangeID == skipRID {
				return nil
			}
			ris = append(ris, ri)
			return nil
		}); err != nil {
			// Errors here should not be possible, but if there is one, it is ignored
			// as attaching RangeInfo is optional.
			log.Warningf(ctx, "unexpected error visiting replicas: %s", err)
			ris = nil // just to be safe
		}
		for _, ri := range ris {
			t.AppendRangeInfo(ctx, ri.Desc, ri.Lease)
		}
		// We have to write `t` back to `pErr` so that it picks up the changes.
		pErr = roachpb.NewError(t)
	case *roachpb.RaftGroupDeletedError:
		// This error needs to be converted appropriately so that clients
		// will retry.
		err := roachpb.NewRangeNotFoundError(repl.RangeID, repl.store.StoreID())
		pErr = roachpb.NewError(err)
	}
	return nil, pErr
}

// maybeThrottleBatch inspects the provided batch and determines whether
// throttling should be applied to avoid overloading the Store. If so, the
// method blocks and returns a reservation that must be released after the
// request has completed.
//
// Of note is that request throttling is all performed above evaluation and
// before a request acquires latches on a range. Otherwise, the request could
// inadvertently block others while being throttled.
func (s *Store) maybeThrottleBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (limit.Reservation, error) {
	if !ba.IsSingleRequest() {
		return nil, nil
	}

	switch t := ba.Requests[0].GetInner().(type) {
	case *roachpb.AddSSTableRequest:
		// Limit the number of concurrent AddSSTable requests, since they're
		// expensive and block all other writes to the same span. However, don't
		// limit AddSSTable requests that are going to ingest as a WriteBatch.
		if t.IngestAsWrites {
			return nil, nil
		}

		before := timeutil.Now()
		res, err := s.limiters.ConcurrentAddSSTableRequests.Begin(ctx)
		if err != nil {
			return nil, err
		}

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
		return res, nil

	case *roachpb.ExportRequest:
		// Limit the number of concurrent Export requests, as these often scan and
		// entire Range at a time and place significant read load on a Store.
		before := timeutil.Now()
		res, err := s.limiters.ConcurrentExportRequests.Begin(ctx)
		if err != nil {
			return nil, err
		}

		waited := timeutil.Since(before)
		s.metrics.ExportRequestProposalTotalDelay.Inc(waited.Nanoseconds())
		if waited > time.Second {
			log.Infof(ctx, "Export request was delayed by %v", waited)
		}
		return res, nil

	default:
		return nil, nil
	}
}
