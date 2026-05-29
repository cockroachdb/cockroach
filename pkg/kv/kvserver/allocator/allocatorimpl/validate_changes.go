// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package allocatorimpl

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// ValidateReplicationChanges runs a series of validation checks against the
// given range descriptor and the proposed set of replication changes on the
// range. The replicate queue applies this check before dispatching changes to
// raft; it lives here (rather than in pkg/kv/kvserver) so the allocator
// simulator (asim) can apply the same rejection rules without introducing an
// import cycle with pkg/kv/kvserver.
//
// It ensures the following:
//  1. There are no duplicate changes: we shouldn't be adding or removing a
//     replica twice.
//  2. We're not adding a replica on a store that already has one, unless
//     it's for a promotion or demotion.
//  3. If there are two changes for a single store, the first one must be an
//     addition and the second one must be a removal.
//  4. We're not adding a replica on a node that already has one, unless the
//     range only has one replica.
//  5. We're not removing a replica that doesn't exist.
//  6. Additions to stores that already contain a replica are strictly the
//     ones that correspond to a voter demotion and/or a non-voter promotion.
func ValidateReplicationChanges(desc *roachpb.RangeDescriptor, chgs kvpb.ReplicationChanges) error {
	chgsByStoreID := getChangesByStoreID(chgs)
	chgsByNodeID := getChangesByNodeID(chgs)

	if err := validateAdditionsPerStore(desc, chgsByStoreID); err != nil {
		return err
	}
	if err := validateRemovals(desc, chgsByStoreID); err != nil {
		return err
	}
	if err := validatePromotionsAndDemotions(desc, chgsByStoreID); err != nil {
		return err
	}
	if err := validateOneReplicaPerNode(desc, chgsByNodeID); err != nil {
		return err
	}

	return nil
}

// changesByStoreID represents a map from StoreID to a slice of replication
// changes on that store.
type changesByStoreID map[roachpb.StoreID][]kvpb.ReplicationChange

// changesByNodeID represents a map from NodeID to a slice of replication
// changes on that node.
type changesByNodeID map[roachpb.NodeID][]kvpb.ReplicationChange

func getChangesByStoreID(chgs kvpb.ReplicationChanges) changesByStoreID {
	chgsByStoreID := make(map[roachpb.StoreID][]kvpb.ReplicationChange, len(chgs))
	for _, chg := range chgs {
		if _, ok := chgsByStoreID[chg.Target.StoreID]; !ok {
			chgsByStoreID[chg.Target.StoreID] = make([]kvpb.ReplicationChange, 0, 2)
		}
		chgsByStoreID[chg.Target.StoreID] = append(chgsByStoreID[chg.Target.StoreID], chg)
	}
	return chgsByStoreID
}

func getChangesByNodeID(chgs kvpb.ReplicationChanges) changesByNodeID {
	chgsByNodeID := make(map[roachpb.NodeID][]kvpb.ReplicationChange, len(chgs))
	for _, chg := range chgs {
		if _, ok := chgsByNodeID[chg.Target.NodeID]; !ok {
			chgsByNodeID[chg.Target.NodeID] = make([]kvpb.ReplicationChange, 0, 2)
		}
		chgsByNodeID[chg.Target.NodeID] = append(chgsByNodeID[chg.Target.NodeID], chg)
	}
	return chgsByNodeID
}

// validateAdditionsPerStore ensures that we're not trying to add the same type
// of replica to a store that already has one or that we're not trying to add
// any type of replica to a store that has a LEARNER.
func validateAdditionsPerStore(
	desc *roachpb.RangeDescriptor, chgsByStoreID changesByStoreID,
) error {
	for storeID, chgs := range chgsByStoreID {
		for _, chg := range chgs {
			if chg.ChangeType.IsRemoval() {
				continue
			}
			// If the replica already exists, check that we're not trying to add the
			// same type of replica again.
			//
			// NB: Trying to add a different type of replica, for instance, a
			// NON_VOTER to a store that already has a VOTER is fine when we're trying
			// to swap a VOTER with a NON_VOTER. Ensuring that this is indeed the case
			// is outside the scope of this particular helper method. See
			// validatePromotionsAndDemotions for how that is checked.
			replDesc, found := desc.GetReplicaDescriptor(storeID)
			if !found {
				// The store we're trying to add to doesn't already have a replica, all
				// good.
				continue
			}
			switch t := replDesc.Type; t {
			case roachpb.LEARNER:
				// Looks like we found a learner with the same store and node id. One of
				// the following is true:
				// 1. some previous leaseholder was trying to add it with the
				// learner+snapshot+voter cycle and got interrupted.
				// 2. we hit a race between the replicate queue and AdminChangeReplicas.
				// 3. We're trying to swap a voting replica with a non-voting replica
				// before the voting replica has been upreplicated and switched from
				// LEARNER to VOTER_FULL.
				return errors.AssertionFailedf(
					"trying to add(%+v) to a store that already has a %s", chg, t)
			case roachpb.VOTER_FULL:
				if chg.ChangeType == roachpb.ADD_VOTER {
					return errors.AssertionFailedf(
						"trying to add a voter to a store that already has a %s", t)
				}
			case roachpb.NON_VOTER:
				if chg.ChangeType == roachpb.ADD_NON_VOTER {
					return errors.AssertionFailedf(
						"trying to add a non-voter to a store that already has a %s", t)
				}
			default:
				return errors.AssertionFailedf("store(%d) being added to already contains a"+
					" replica of an unexpected type: %s", storeID, t)
			}
		}
	}
	return nil
}

// validateRemovals ensures that replicas being removed actually exist and that
// the type of replica being removed matches the type of the removal.
func validateRemovals(desc *roachpb.RangeDescriptor, chgsByStoreID changesByStoreID) error {
	for storeID, chgs := range chgsByStoreID {
		for _, chg := range chgs {
			if chg.ChangeType.IsAddition() {
				continue
			}
			replDesc, found := desc.GetReplicaDescriptor(storeID)
			if !found {
				return errors.AssertionFailedf("trying to remove a replica that doesn't exist: %+v", chg)
			}
			// Ensure that the type of replica being removed is the same as the type
			// of replica present in the range descriptor.
			switch t := replDesc.Type; t {
			case roachpb.VOTER_FULL, roachpb.LEARNER:
				if chg.ChangeType != roachpb.REMOVE_VOTER {
					return errors.AssertionFailedf("type of replica being removed (%s) does not match"+
						" expectation for change: %+v", t, chg)
				}
			case roachpb.NON_VOTER:
				if chg.ChangeType != roachpb.REMOVE_NON_VOTER {
					return errors.AssertionFailedf("type of replica being removed (%s) does not match"+
						" expectation for change: %+v", t, chg)
				}
			default:
				return errors.AssertionFailedf("unexpected replica type for removal %+v: %s", chg, t)
			}
		}
	}
	return nil
}

// validatePromotionsAndDemotions ensures the following:
// 1. All additions of voters to stores that already have a non-voter are
// accompanied by a removal of that non-voter (which is interpreted as a
// promotion of a non-voter to a voter).
// 2. All additions of non-voters to stores that already have a voter are
// accompanied by a removal of that voter (which is interpreted as a demotion of
// a voter to a non-voter)
func validatePromotionsAndDemotions(
	desc *roachpb.RangeDescriptor, chgsByStoreID changesByStoreID,
) error {
	for storeID, chgs := range chgsByStoreID {
		replDesc, found := desc.GetReplicaDescriptor(storeID)
		switch len(chgs) {
		case 0:
			continue
		case 1:
			if chgs[0].ChangeType.IsAddition() {
				// If there's only one addition on this store, without an accompanying
				// removal, then the change cannot correspond to a promotion/demotion.
				// Thus, the store must not already have a replica.
				if found {
					return errors.AssertionFailedf("trying to add(%+v) to a store(%s) that already"+
						" has a replica(%s)", chgs[0], storeID, replDesc.Type)
				}
			}
		case 2:
			c1, c2 := chgs[0], chgs[1]
			if !found {
				return errors.AssertionFailedf("found 2 changes(%+v) for a store(%d)"+
					" that has no replicas", chgs, storeID)
			}
			if c1.ChangeType.IsAddition() && c2.ChangeType.IsRemoval() {
				// There's only two legal possibilities here:
				// 1. Promotion: ADD_VOTER, REMOVE_NON_VOTER
				// 2. Demotion: ADD_NON_VOTER, REMOVE_VOTER
				//
				// We reject everything else.
				isPromotion := c1.ChangeType == roachpb.ADD_VOTER && c2.ChangeType == roachpb.REMOVE_NON_VOTER
				isDemotion := c1.ChangeType == roachpb.ADD_NON_VOTER && c2.ChangeType == roachpb.REMOVE_VOTER
				if !(isPromotion || isDemotion) {
					return errors.AssertionFailedf("trying to add-remove the same replica(%s):"+
						" %+v", replDesc.Type, chgs)
				}
			} else {
				// NB: validateOneReplicaPerNode has a stronger version of this check,
				// but we check it here anyway for the sake of a more precise error
				// message.
				return errors.AssertionFailedf("the only permissible order of operations within a"+
					" store is add-remove; got %+v", chgs)
			}
		default:
			return errors.AssertionFailedf("more than 2 changes referring to the same store: %+v", chgs)
		}
	}
	return nil
}

// validateOneReplicaPerNode ensures that there are no more than 2 changes for
// any given node and if a node already has a replica, then adding a second
// replica is prohibited unless the existing replica is being removed with it.
func validateOneReplicaPerNode(desc *roachpb.RangeDescriptor, chgsByNodeID changesByNodeID) error {
	replsByNodeID := make(map[roachpb.NodeID]int)
	for _, repl := range desc.Replicas().Descriptors() {
		replsByNodeID[repl.NodeID]++
	}

	for nodeID, chgs := range chgsByNodeID {
		if len(chgs) > 2 {
			return errors.AssertionFailedf("more than 2 changes for the same node(%d): %+v",
				nodeID, chgs)
		}
		switch replsByNodeID[nodeID] {
		case 0:
			// If there are no existing replicas on the node, a rebalance is not
			// possible and there must not be more than 1 change for it.
			//
			// NB: We don't care _what_ kind of change it is. If it's a removal, it
			// will be invalidated by `validateRemovals`.
			if len(chgs) > 1 {
				return errors.AssertionFailedf("unexpected set of changes(%+v) for node %d, which has"+
					" no existing replicas for the range", chgs, nodeID)
			}
		case 1:
			// If the node has exactly one replica, then the only changes allowed on
			// the node are:
			// 1. An addition and a removal (constituting a rebalance within the node)
			// 2. Removal
			switch n := len(chgs); n {
			case 1:
				// Must be a removal unless the range only has a single replica. Ranges
				// with only one replica cannot be atomically rebalanced, and must go
				// through addition and then removal separately. See #40333.
				if !chgs[0].ChangeType.IsRemoval() && len(desc.Replicas().Descriptors()) > 1 {
					return errors.AssertionFailedf("node %d already has a replica; only valid actions"+
						" are a removal or a rebalance(add/remove); got %+v", nodeID, chgs)
				}
			case 2:
				// Must be an addition then removal
				c1, c2 := chgs[0], chgs[1]
				if !(c1.ChangeType.IsAddition() && c2.ChangeType.IsRemoval()) {
					return errors.AssertionFailedf("node %d already has a replica; only valid actions"+
						" are a removal or a rebalance(add/remove); got %+v", nodeID, chgs)
				}
			default:
				return errors.AssertionFailedf("unexpected number of changes for node %d: %+v", nodeID, chgs)
			}
		case 2:
			// If there are 2 replicas on any given node, a removal is the only legal
			// thing to do.
			if !(len(chgs) == 1 && chgs[0].ChangeType.IsRemoval()) {
				return errors.AssertionFailedf("node %d has 2 replicas, expected exactly one of them"+
					" to be removed; got %+v", nodeID, chgs)
			}
		default:
			return errors.AssertionFailedf("node %d unexpectedly has more than 2 replicas: %s",
				nodeID, desc.Replicas().Descriptors())
		}
	}
	return nil
}
