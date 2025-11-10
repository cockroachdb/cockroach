// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// convertLeaseTransferToMMA converts a lease transfer operation to mma replica
// changes. It will be passed to mma.RegisterExternalChanges.
func convertLeaseTransferToMMA(
	desc *roachpb.RangeDescriptor,
	usage allocator.RangeUsageInfo,
	transferFrom, transferTo roachpb.ReplicationTarget,
) mmaprototype.PendingRangeChange {
	// TODO(wenyihu6): we are passing existing replicas to
	// mmaprototype.MakeLeaseTransferChanges just to get the add and remove
	// replica state. See if things could be cleaned up.
	existingReplicas := make([]mmaprototype.StoreIDAndReplicaState, len(desc.InternalReplicas))
	for i, replica := range desc.Replicas().Descriptors() {
		existingReplicas[i] = mmaprototype.StoreIDAndReplicaState{
			StoreID: replica.StoreID,
			ReplicaState: mmaprototype.ReplicaState{
				ReplicaIDAndType: mmaprototype.ReplicaIDAndType{
					ReplicaID: replica.ReplicaID,
					ReplicaType: mmaprototype.ReplicaType{
						ReplicaType: replica.Type,
						// transferFrom is the leaseholder replica.
						IsLeaseholder: replica.StoreID == transferFrom.StoreID,
					},
				},
			},
		}
	}
	replicaChanges := mmaprototype.MakeLeaseTransferChanges(desc.RangeID,
		existingReplicas,
		mmaRangeLoad(usage),
		transferTo,
		transferFrom,
	)
	return mmaprototype.MakePendingRangeChange(desc.RangeID, replicaChanges[:])
}

// convertReplicaChangeToMMA converts a replica change to a mma range change.
// It will be passed to mma.RegisterExternalChange.
func convertReplicaChangeToMMA(
	desc *roachpb.RangeDescriptor,
	usage allocator.RangeUsageInfo,
	changes kvpb.ReplicationChanges,
	leaseholderStoreID roachpb.StoreID,
) mmaprototype.PendingRangeChange {
	rLoad := mmaRangeLoad(usage)
	replicaChanges := make([]mmaprototype.ReplicaChange, 0, len(changes))
	replicaSet := desc.Replicas()

	// TODO(wenyihu6): check what type of replication changes will there be
	// here (can there be two voter removals or additions at the same time?)
	// If yes, lhBeingRemoved may be wrong.
	if (len(changes.VoterRemovals()) > 1 || len(changes.VoterAdditions()) > 1) && buildutil.CrdbTestBuild {
		panic("voter removals should not be more than one at a time")
	}
	var lhBeingRemoved bool
	// A VOTER => NON_VOTER change or vice versa is represented as a removal and
	// addition in changes, and we need to convert it to a single
	// mmaprototype.ReplicaChange. So we gather the changes into a map keyed by
	// the ReplicationTarget.
	type change struct {
		typ mmaprototype.ReplicaChangeType
		// prev is populated when typ is RemoveReplica or ChangeReplica.
		prev mmaprototype.ReplicaState
		// next is populated when typ is AddReplica or ChangeReplica.
		next mmaprototype.ReplicaIDAndType
	}
	changeMap := map[roachpb.ReplicationTarget]change{}
	// Put remove replica changes first so that we can see if the leaseholder
	// is being removed.
	for _, chg := range changes {
		if chg.ChangeType == roachpb.REMOVE_VOTER || chg.ChangeType == roachpb.REMOVE_NON_VOTER {
			filteredSet := replicaSet.Filter(func(r roachpb.ReplicaDescriptor) bool {
				return r.StoreID == chg.Target.StoreID
			})
			replDescriptors := filteredSet.Descriptors()
			if len(replDescriptors) != 1 {
				panic(fmt.Sprintf(
					"no replica found for removal target=%v post-filter=%v pre-filter=%v",
					chg.Target.StoreID, replDescriptors, desc))
			}
			replDesc := replDescriptors[0]
			isLeaseholder := replDesc.StoreID == leaseholderStoreID
			if isLeaseholder && !lhBeingRemoved {
				lhBeingRemoved = true
			}
			removeChange := change{
				typ: mmaprototype.RemoveReplica,
				prev: mmaprototype.ReplicaState{
					ReplicaIDAndType: mmaprototype.ReplicaIDAndType{
						ReplicaID: replDesc.ReplicaID,
						ReplicaType: mmaprototype.ReplicaType{
							ReplicaType:   replDesc.Type,
							IsLeaseholder: isLeaseholder,
						},
					},
				},
			}
			_, exists := changeMap[chg.Target]
			if exists {
				panic(errors.AssertionFailedf("unexpected existing change for remove %v in %v",
					chg.Target, changes))
			}
			changeMap[chg.Target] = removeChange
		}
	}
	for _, chg := range changes {
		switch {
		case chg.ChangeType == roachpb.ADD_VOTER || chg.ChangeType == roachpb.ADD_NON_VOTER:
			rType := roachpb.VOTER_FULL
			if chg.ChangeType == roachpb.ADD_NON_VOTER {
				rType = roachpb.NON_VOTER
			}
			ch, exists := changeMap[chg.Target]
			if exists {
				if ch.typ != mmaprototype.RemoveReplica {
					panic(errors.AssertionFailedf("unexpected existing change type for %v in %v",
						chg.Target, changes))
				}
				ch.typ = mmaprototype.ChangeReplica
			} else {
				ch.typ = mmaprototype.AddReplica
			}
			ch.next = mmaprototype.ReplicaIDAndType{
				ReplicaType: mmaprototype.ReplicaType{
					ReplicaType: rType,
					// TODO(sumeer): can there be multiple ADD_VOTERs?
					IsLeaseholder: lhBeingRemoved && chg.ChangeType == roachpb.ADD_VOTER,
				},
			}
			changeMap[chg.Target] = ch
		case chg.ChangeType == roachpb.REMOVE_VOTER || chg.ChangeType == roachpb.REMOVE_NON_VOTER:
			// Handled above.
			continue
		default:
			panic("unimplemented change type")
		}
	}
	for target, ch := range changeMap {
		var replicaChange mmaprototype.ReplicaChange
		switch ch.typ {
		case mmaprototype.AddReplica:
			replicaChange = mmaprototype.MakeAddReplicaChange(desc.RangeID, rLoad, ch.next, target)
		case mmaprototype.RemoveReplica:
			replicaChange = mmaprototype.MakeRemoveReplicaChange(desc.RangeID, rLoad, ch.prev, target)
		case mmaprototype.ChangeReplica:
			replicaChange = mmaprototype.MakeReplicaTypeChange(desc.RangeID, rLoad, ch.prev, ch.next, target)
		default:
			panic("unimplemented change type")
		}
		replicaChanges = append(replicaChanges, replicaChange)
	}
	return mmaprototype.MakePendingRangeChange(desc.RangeID, replicaChanges)
}
