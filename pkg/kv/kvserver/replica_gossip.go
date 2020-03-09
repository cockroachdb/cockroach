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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagebase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

const configGossipTTL = 0 // does not expire

func (r *Replica) gossipFirstRange(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
			r.store.StoreID(), r.RangeID, r.mu.state.Desc.Replicas())
	}
	if err := r.store.Gossip().AddInfoProto(
		gossip.KeyFirstRangeDescriptor, r.mu.state.Desc, configGossipTTL); err != nil {
		log.Errorf(ctx, "failed to gossip first range metadata: %+v", err)
	}
}

// shouldGossip returns true if this replica should be gossiping. Gossip is
// inherently inconsistent and asynchronous, we're using the lease as a way to
// ensure that only one node gossips at a time.
func (r *Replica) shouldGossip() bool {
	return r.OwnsValidLease(r.store.Clock().Now())
}

// MaybeGossipSystemConfig scans the entire SystemConfig span and gossips it.
// Further calls come from the trigger on EndTxn or range lease acquisition.
//
// Note that MaybeGossipSystemConfig gossips information only when the
// lease is actually held. The method does not request a range lease
// here since RequestLease and applyRaftCommand call the method and we
// need to avoid deadlocking in redirectOnOrAcquireLease.
//
// MaybeGossipSystemConfig must only be called from Raft commands
// (which provide the necessary serialization to avoid data races).
//
// TODO(nvanbenschoten,bdarnell): even though this is best effort, we
// should log louder when we continually fail to gossip system config.
func (r *Replica) MaybeGossipSystemConfig(ctx context.Context) error {
	if r.store.Gossip() == nil {
		log.VEventf(ctx, 2, "not gossiping system config because gossip isn't initialized")
		return nil
	}
	if !r.IsInitialized() {
		log.VEventf(ctx, 2, "not gossiping system config because the replica isn't initialized")
		return nil
	}
	if !r.ContainsKey(keys.SystemConfigSpan.Key) {
		log.VEventf(ctx, 3,
			"not gossiping system config because the replica doesn't contain the system config's start key")
		return nil
	}
	if !r.shouldGossip() {
		log.VEventf(ctx, 2, "not gossiping system config because the replica doesn't hold the lease")
		return nil
	}

	// TODO(marc): check for bad split in the middle of the SystemConfig span.
	loadedCfg, err := r.loadSystemConfig(ctx)
	if err != nil {
		if err == errSystemConfigIntent {
			log.VEventf(ctx, 2, "not gossiping system config because intents were found on SystemConfigSpan")
			return nil
		}
		return errors.Wrap(err, "could not load SystemConfig span")
	}

	if gossipedCfg := r.store.Gossip().GetSystemConfig(); gossipedCfg != nil && gossipedCfg.Equal(loadedCfg) &&
		r.store.Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
		log.VEventf(ctx, 2, "not gossiping unchanged system config")
		return nil
	}

	log.VEventf(ctx, 2, "gossiping system config")
	if err := r.store.Gossip().AddInfoProto(gossip.KeySystemConfig, loadedCfg, 0); err != nil {
		return errors.Wrap(err, "failed to gossip system config")
	}
	return nil
}

// MaybeGossipNodeLiveness gossips information for all node liveness
// records stored on this range. To scan and gossip, this replica
// must hold the lease to a range which contains some or all of the
// node liveness records. After scanning the records, it checks
// against what's already in gossip and only gossips records which
// are out of date.
func (r *Replica) MaybeGossipNodeLiveness(ctx context.Context, span roachpb.Span) error {
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return nil
	}

	if !r.ContainsKeyRange(span.Key, span.EndKey) || !r.shouldGossip() {
		return nil
	}

	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeaderFromSpan(span)})
	// Call evaluateBatch instead of Send to avoid reacquiring latches.
	rec := NewReplicaEvalContext(r, todoSpanSet)
	rw := r.Engine().NewReadOnly()
	defer rw.Close()

	br, result, pErr :=
		evaluateBatch(ctx, storagebase.CmdIDKey(""), rw, rec, nil, &ba, true /* readOnly */)
	if pErr != nil {
		return errors.Wrapf(pErr.GoError(), "couldn't scan node liveness records in span %s", span)
	}
	if len(result.Local.EncounteredIntents) > 0 {
		return errors.Errorf("unexpected intents on node liveness span %s: %+v", span, result.Local.EncounteredIntents)
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	log.VEventf(ctx, 2, "gossiping %d node liveness record(s) from span %s", len(kvs), span)
	for _, kv := range kvs {
		var kvLiveness, gossipLiveness storagepb.Liveness
		if err := kv.Value.GetProto(&kvLiveness); err != nil {
			return errors.Wrapf(err, "failed to unmarshal liveness value %s", kv.Key)
		}
		key := gossip.MakeNodeLivenessKey(kvLiveness.NodeID)
		// Look up liveness from gossip; skip gossiping anew if unchanged.
		if err := r.store.Gossip().GetInfoProto(key, &gossipLiveness); err == nil {
			if gossipLiveness == kvLiveness && r.store.Gossip().InfoOriginatedHere(key) {
				continue
			}
		}
		if err := r.store.Gossip().AddInfoProto(key, &kvLiveness, 0); err != nil {
			return errors.Wrapf(err, "failed to gossip node liveness (%+v)", kvLiveness)
		}
	}
	return nil
}

var errSystemConfigIntent = errors.New("must retry later due to intent on SystemConfigSpan")

// loadSystemConfig scans the system config span and returns the system
// config.
func (r *Replica) loadSystemConfig(ctx context.Context) (*config.SystemConfigEntries, error) {
	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = roachpb.INCONSISTENT
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.ScanRequest{RequestHeader: roachpb.RequestHeaderFromSpan(keys.SystemConfigSpan)})
	// Call evaluateBatch instead of Send to avoid reacquiring latches.
	rec := NewReplicaEvalContext(r, todoSpanSet)
	rw := r.Engine().NewReadOnly()
	defer rw.Close()

	br, result, pErr := evaluateBatch(
		ctx, storagebase.CmdIDKey(""), rw, rec, nil, &ba, true, /* readOnly */
	)
	if pErr != nil {
		return nil, pErr.GoError()
	}
	if intents := result.Local.DetachEncounteredIntents(); len(intents) > 0 {
		// There were intents, so what we read may not be consistent. Attempt
		// to nudge the intents in case they're expired; next time around we'll
		// hopefully have more luck.
		// This is called from handleReadWriteLocalEvalResult (with raftMu
		// locked), so disallow synchronous processing (which blocks that mutex
		// for too long and is a potential deadlock).
		if err := r.store.intentResolver.CleanupIntentsAsync(ctx, intents, false /* allowSync */); err != nil {
			log.Warning(ctx, err)
		}
		return nil, errSystemConfigIntent
	}
	kvs := br.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
	sysCfg := &config.SystemConfigEntries{}
	sysCfg.Values = kvs
	return sysCfg, nil
}

// getLeaseForGossip tries to obtain a range lease. Only one of the replicas
// should gossip; the bool returned indicates whether it's us.
func (r *Replica) getLeaseForGossip(ctx context.Context) (bool, *roachpb.Error) {
	// If no Gossip available (some tests) or range too fresh, noop.
	if r.store.Gossip() == nil || !r.IsInitialized() {
		return false, roachpb.NewErrorf("no gossip or range not initialized")
	}
	var hasLease bool
	var pErr *roachpb.Error
	if err := r.store.Stopper().RunTask(
		ctx, "storage.Replica: acquiring lease to gossip",
		func(ctx context.Context) {
			// Check for or obtain the lease, if none active.
			_, _, pErr = r.redirectOnOrAcquireLease(ctx)
			hasLease = pErr == nil
			if pErr != nil {
				switch e := pErr.GetDetail().(type) {
				case *roachpb.NotLeaseHolderError:
					// NotLeaseHolderError means there is an active lease, but only if
					// the lease holder is set; otherwise, it's likely a timeout.
					if e.LeaseHolder != nil {
						pErr = nil
					}
				default:
					// Any other error is worth being logged visibly.
					log.Warningf(ctx, "could not acquire lease for range gossip: %s", e)
				}
			}
		}); err != nil {
		pErr = roachpb.NewError(err)
	}
	return hasLease, pErr
}

// maybeGossipFirstRange adds the sentinel and first range metadata to gossip
// if this is the first range and a range lease can be obtained. The Store
// calls this periodically on first range replicas.
func (r *Replica) maybeGossipFirstRange(ctx context.Context) *roachpb.Error {
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
		log.Infof(ctx, "gossiping cluster id %q from store %d, r%d", r.store.ClusterID(),
			r.store.StoreID(), r.RangeID)
	}
	if err := r.store.Gossip().AddClusterID(r.store.ClusterID()); err != nil {
		log.Errorf(ctx, "failed to gossip cluster ID: %+v", err)
	}

	if r.store.cfg.TestingKnobs.DisablePeriodicGossips {
		return nil
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
