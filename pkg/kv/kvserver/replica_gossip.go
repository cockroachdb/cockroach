// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const configGossipTTL = 0 // does not expire

func (r *Replica) gossipFirstRange(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gossipFirstRangeLocked(ctx)
}

func (r *Replica) gossipFirstRangeLocked(ctx context.Context) {
	// Gossip is not provided for the bootstrap store and for some tests.
	if r.store.Gossip() == nil {
		return
	}
	log.Event(ctx, "gossiping sentinel and first range")
	if log.V(1) {
		log.Infof(ctx, "gossiping sentinel from store %d, r%d", r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddInfo(
		gossip.KeySentinel, r.store.ClusterID().GetBytes(),
		r.store.cfg.SentinelGossipTTL()); err != nil {
		log.Errorf(ctx, "failed to gossip sentinel: %+v", err)
	}
	if log.V(1) {
		log.Infof(ctx, "gossiping first range from store %d, r%d: %s",
			r.store.StoreID(), r.RangeID, r.shMu.state.Desc.Replicas())
	}
	if err := r.store.Gossip().AddInfoProto(
		gossip.KeyFirstRangeDescriptor, r.shMu.state.Desc, configGossipTTL); err != nil {
		log.Errorf(ctx, "failed to gossip first range metadata: %+v", err)
	}
}

// shouldGossip returns true if this replica should be gossiping. Gossip is
// inherently inconsistent and asynchronous, we're using the lease as a way to
// ensure that only one node gossips at a time.
func (r *Replica) shouldGossip(ctx context.Context) bool {
	return r.OwnsValidLease(ctx, r.store.Clock().NowAsClockTimestamp())
}

// MaybeGossipNodeLivenessRaftMuLocked gossips information for all node liveness
// records stored on this range. To scan and gossip, this replica must hold the
// lease to a range which contains some or all of the node liveness records.
// After scanning the records, it checks against what's already in gossip and
// only gossips records which are out of date.
//
// MaybeGossipNodeLivenessRaftMuLocked must only be called from Raft commands
// while holding the raftMu (which provide the necessary serialization to avoid
// data races).
func (r *Replica) MaybeGossipNodeLivenessRaftMuLocked(
	ctx context.Context, span roachpb.Span,
) error {
	r.raftMu.AssertHeld()
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return nil
	}
	if !r.ContainsKeyRange(span.Key, span.EndKey) || !r.shouldGossip(ctx) {
		return nil
	}

	ba := kvpb.BatchRequest{}
	// Read at the maximum timestamp to ensure that we see the most recent
	// liveness record, regardless of what timestamp it is written at.
	ba.Timestamp = hlc.MaxTimestamp
	ba.Add(&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeaderFromSpan(span)})
	// Call evaluateBatch instead of Send to avoid reacquiring latches.
	rec := NewReplicaEvalContext(
		ctx, r, todoSpanSet, false, /* requiresClosedTSOlderThanStorageSnap */
		kvpb.AdmissionHeader{},
	)
	defer rec.Release()
	rw := r.store.TODOEngine().NewReadOnly(storage.StandardDurability)
	defer rw.Close()

	br, result, pErr :=
		evaluateBatch(
			ctx, kvserverbase.CmdIDKey(""), rw, rec, nil /* ms */, &ba,
			nil /* g */, nil /* st */, uncertainty.Interval{}, readOnlyDefault, false, /* omitInRangefeeds */
		)
	if pErr != nil {
		return errors.Wrapf(pErr.GoError(), "couldn't scan node liveness records in span %s", span)
	}
	if len(result.Local.EncounteredIntents) > 0 {
		return errors.Errorf("unexpected intents on node liveness span %s: %+v", span, result.Local.EncounteredIntents)
	}
	kvs := br.Responses[0].GetInner().(*kvpb.ScanResponse).Rows

	log.VEventf(ctx, 2, "gossiping %d node liveness record(s) from span %s", len(kvs), span)
	if len(kvs) == 1 {
		// Parse the value to both confirm it's well-formatted and to extract the
		// node ID from it. We could instead derive the node ID from the key and
		// skip unmarshaling the value at all, which would be (marginally) faster,
		// but that would be a greater change from how this code has historically
		// worked and it's not likely to be a huge difference.
		kv := kvs[0]
		var kvLiveness livenesspb.Liveness
		if err := kv.Value.GetProto(&kvLiveness); err != nil {
			return errors.Wrapf(err, "failed to unmarshal liveness value %s", kv.Key)
		}
		key := gossip.MakeNodeLivenessKey(kvLiveness.NodeID)
		valBytes, err := kv.Value.GetBytes()
		if err != nil {
			return errors.Wrapf(err, "failed to GetBytes to gossip node liveness for %s", kv.Key)
		}
		if err := r.store.Gossip().AddInfoIfNotRedundant(key, valBytes); err != nil {
			return errors.Wrapf(err, "failed to gossip node liveness for %s", kv.Key)
		}
	} else {
		// Same code as above, but for more than one liveness record. Allocate a
		// slice that can be passed to a bulk gossip operation rather than taking
		// and releasing the gossip mutex separately for each.
		gossipInfos := make([]gossip.InfoToAdd, 0, len(kvs))
		for _, kv := range kvs {
			var kvLiveness livenesspb.Liveness
			if err := kv.Value.GetProto(&kvLiveness); err != nil {
				return errors.Wrapf(err, "failed to unmarshal liveness value %s", kv.Key)
			}
			key := gossip.MakeNodeLivenessKey(kvLiveness.NodeID)
			valBytes, err := kv.Value.GetBytes()
			if err != nil {
				return errors.Wrapf(err, "failed to GetBytes to gossip node liveness for %s", kv.Key)
			}
			gossipInfos = append(gossipInfos, gossip.InfoToAdd{Key: key, Val: valBytes})
		}
		if err := r.store.Gossip().BulkAddInfoIfNotRedundant(gossipInfos); err != nil {
			return errors.Wrapf(err, "failed to gossip node liveness for %d keys", len(kvs))
		}
	}

	return nil
}

// getLeaseForGossip tries to obtain a range lease. Only one of the replicas
// should gossip; the bool returned indicates whether it's us.
func (r *Replica) getLeaseForGossip(ctx context.Context) (bool, *kvpb.Error) {
	// If no Gossip available (some tests) or range too fresh, noop.
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return false, kvpb.NewErrorf("no gossip or range not initialized")
	}
	var hasLease bool
	var pErr *kvpb.Error
	if err := r.store.Stopper().RunTask(
		ctx, "storage.Replica: acquiring lease to gossip",
		func(ctx context.Context) {
			// Check for or obtain the lease, if none active.
			_, pErr = r.redirectOnOrAcquireLease(ctx)
			hasLease = pErr == nil
			if pErr != nil {
				switch e := pErr.GetDetail().(type) {
				case *kvpb.NotLeaseHolderError:
					// NotLeaseHolderError means there is an active lease, but only if
					// the lease is non-empty; otherwise, it's likely a timeout.
					if !e.Lease.Empty() {
						pErr = nil
					}
				default:
					// Any other error is worth being logged visibly.
					log.Warningf(ctx, "could not acquire lease for range gossip: %s", pErr)
				}
			}
		}); err != nil {
		pErr = kvpb.NewError(err)
	}
	return hasLease, pErr
}

// maybeGossipFirstRange adds the sentinel and first range metadata to gossip
// if this is the first range and a range lease can be obtained. The Store
// calls this periodically on first range replicas.
func (r *Replica) maybeGossipFirstRange(ctx context.Context) *kvpb.Error {
	if !r.IsFirstRange() {
		return nil
	}

	// When multiple nodes are initialized with overlapping Gossip addresses, they all
	// will attempt to gossip their cluster ID. This is a fairly obvious misconfiguration,
	// so we error out below.
	if gossipClusterID, err := r.store.Gossip().GetClusterID(); err == nil {
		if gossipClusterID != r.store.ClusterID() {
			log.Fatalf(
				ctx, "store %d belongs to cluster %s, but attempted to join cluster %s via gossip",
				r.store.StoreID(), r.store.ClusterID(), gossipClusterID)
		}
	}

	// Gossip the cluster ID from all replicas of the first range; there
	// is no expiration on the cluster ID.
	if log.V(1) {
		log.Infof(ctx, "gossiping cluster ID %q from store %d, r%d", r.store.ClusterID(),
			r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddClusterID(r.store.ClusterID()); err != nil {
		log.Errorf(ctx, "failed to gossip cluster ID: %+v", err)
	}

	hasLease, pErr := r.getLeaseForGossip(ctx)
	if pErr != nil {
		return pErr
	} else if !hasLease {
		return nil
	}
	r.gossipFirstRange(ctx)
	return nil
}
