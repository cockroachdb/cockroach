// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/google/btree"
)

// ReplicaPlaceholder represents a "lock" of a part of the keyspace on a given
// *Store for the receipt and application of a raft snapshot. Placeholders
// are kept synchronously in two places in (*Store).mu, namely the
// replicaPlaceholders and replicaByKey maps.
//
// To see why placeholders are necessary, consider the case in which two
// snapshots arrive at a Store, one for r1 and bounds [a,c) and the other for r2
// and [b,c), and assume that the keyspace [a,c) is not associated to any
// Replica on the receiving Store. This situation can occur because even though
// "logically" the keyspace always shards cleanly into replicas, incoming
// snapshots don't always originate from a mutually consistent version of this
// sharding. For example, a range Q might split, creating a range R, but some
// Store might be receiving a snapshot of Q before the split as well as a
// replica of R (which postdates the split). Similar examples are possible with
// merges as well as with arbitrarily complicated combinations of multiple
// merges and splits.
//
// Without placeholders, the following interleaving of two concurrent Raft
// scheduler goroutines g1 and g2 is possible for the above example:
//
// - g1: new raft.Ready for r1 wants to apply snapshot
// - g1: check for conflicts with existing replicas: none found; [a,c) is empty
// - g2: new raft.Ready for r2 wants to apply snapshot
// - g2: check for conflicts with existing replicas: none found; [b,c) is empty
// - g2: apply snapshot: writes replica for r2 to [b,c)
// - g2: done
// - g1: apply snapshot: writes replica for r1 to [a,c)
// - boom: we now have two replicas on this store that overlap
//
// Placeholders avoid this problem because they provide a serialization point
// between g1 and g2: When g1 checks for conflicts, it also checks for an
// existing placeholder (inserting its own atomically when none found), so that
// g2 would later fail the overlap check on g1's placeholder.
//
// The rules for placeholders are as follows:
//
// - placeholders are only installed for uninitialized replicas (under raftMu).
//   In particular, a snapshot that gets sent to an initialized replica installs
//   no placeholder (the initialized replica plays the role of the placeholder).
// - they do not overlap any initialized replica's key bounds. (This invariant
//   is maintained via Store.mu.replicasByKey).
// - a placeholder can only be removed by the operation that installed it, and
//   that operation *must* eventually remove it. In practice, they are inserted
//   before receiving the snapshot data, so they are fairly long-lived. They
//   are removed when the receipt of the snapshot fails, the snapshot is discarded,
//   or the snapshot was fully applied (in which case the placeholder is exchanged
//   for a RangeDescriptor).
// - placeholders must not be copied (i.e. always pass by reference).
//
// In particular, when removing a placeholder we don't have to worry about
// whether we're removing our own or someone else's. This is because they
// can't overlap; and if we inserted one it's still there (since nobody else
// can delete it).
//
// Note also that both initialized and uninitialized replicas can get
// replicaGC'ed (which also removes any placeholders). It is thus possible for a
// snapshot to begin streaming to either an uninitialized (via a placeholder) or
// initialized replica, and for that replica to be removed by replicaGC by the
// time the snapshot is ready to apply. This will simply result the snapshot
// being discarded (and has to - applying the snapshot could lead to the races
// that placeholders were introduced for in the first place) due to the
// replicaID being rejected by the range tombstone (written during replicaGC) in
// snapshot application.
//
// See (*Store).receiveSnapshot for where placeholders are installed. This will
// eventually call (*Store).processRaftSnapshotRequest which triggers the actual
// application.
type ReplicaPlaceholder struct {
	rangeDesc roachpb.RangeDescriptor
	tainted   int32 // atomic
}

var _ rangeKeyItem = (*ReplicaPlaceholder)(nil)

// Desc returns the range Placeholder's descriptor.
func (r *ReplicaPlaceholder) Desc() *roachpb.RangeDescriptor {
	return &r.rangeDesc
}

func (r *ReplicaPlaceholder) key() roachpb.RKey {
	return r.Desc().StartKey
}

// Less implements the btree.Item interface.
func (r *ReplicaPlaceholder) Less(i btree.Item) bool {
	return r.Desc().StartKey.Less(i.(rangeKeyItem).key())
}

func (r *ReplicaPlaceholder) String() string {
	tainted := ""
	if atomic.LoadInt32(&r.tainted) != 0 {
		tainted = ",tainted"
	}
	return fmt.Sprintf("range=%d [%s-%s) (placeholder%s)",
		r.Desc().RangeID, r.rangeDesc.StartKey, r.rangeDesc.EndKey, tainted)
}
