// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// ReplicaMark indicates existence or absence of a replica for a RangeID in the
// Store, and defines replica IDs that can exist on the Store in the future.
//
// INVARIANTS:
//   - A replica exists iff 0 < ReplicaID < math.MaxInt32.
//   - If a replica exists, ReplicaID >= NextReplicaID.
//   - Replicas with ID < ReplicaID will never exist on the Store.
//   - Replicas with ID < NextReplicaID will never exist on the Store.
//
// NB: if NextReplicaID == math.MaxInt32, the RangeID has no replica, and will
// never have one again. This usually means that the range is known to have been
// merged with its LHS neighbour.
//
// This type has been introduced as a stronger version of RangeTombstone and
// RaftReplicaID, which previously were considered as separate entities.
// TODO(pav-kv): there is no reason to use one without the other. We should
// eliminate the separate types, and store ReplicaMark under one key.
type ReplicaMark struct {
	kvserverpb.RaftReplicaID
	kvserverpb.RangeTombstone
}

// check returns an error iff the ReplicaMark invariant does not hold.
func (r ReplicaMark) check() error {
	if id := r.ReplicaID; id == MergedTombstoneReplicaID {
		return errors.AssertionFailedf("ReplicaID %d is invalid", r.ReplicaID)
	} else if id != 0 && id < r.NextReplicaID {
		return errors.AssertionFailedf("ReplicaID %d survived RangeTombstone %+v", id, r.RangeTombstone)
	}
	return nil
}

// Exists returns true iff a replica with the given mark exists on the Store.
func (r ReplicaMark) Exists() bool {
	return r.ReplicaID > 0
}

// Is returns true iff a replica exists and has the given ID.
func (r ReplicaMark) Is(id roachpb.ReplicaID) bool {
	return r.Exists() && r.ReplicaID == id
}

// Destroyed returns true iff the ReplicaMark indicates that a replica with the
// given ID can never (re-)appear on the Store.
func (r ReplicaMark) Destroyed(id roachpb.ReplicaID) bool {
	return id == 0 || id < r.ReplicaID || id < r.NextReplicaID
}

// Destroy returns a valid ReplicaMark that destroys the current replica, and
// promises that there will be no future replicas with ID < next. Requires that
// the current replica exists, and next > ReplicaID.
//
// The mark should then be persisted to storage. In practice this means that the
// RangeTombstone is written under RangeTombstoneKey, and the RaftReplicaID is
// cleared from RaftReplicaIDKey.
func (r ReplicaMark) Destroy(next roachpb.ReplicaID) (ReplicaMark, error) {
	if !r.Exists() {
		return r, errors.AssertionFailedf("replica does no exist")
	} else if next <= r.ReplicaID {
		return r, errors.AssertionFailedf("replica can not be destroyed")
	}
	return ReplicaMark{
		RangeTombstone: kvserverpb.RangeTombstone{NextReplicaID: next},
	}, nil
}
