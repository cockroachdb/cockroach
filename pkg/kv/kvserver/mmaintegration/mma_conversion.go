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
)

// convertLeaseTransferToMMA converts a lease transfer operation to mma replica
// changes. It will be passed to mma.RegisterExternalChanges.
func convertLeaseTransferToMMA(
	desc *roachpb.RangeDescriptor,
	usage allocator.RangeUsageInfo,
	transferFrom, transferTo roachpb.ReplicationTarget,
) []mmaprototype.ReplicaChange {
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
	return replicaChanges[:]
}

// convertReplicaChangeToMMA converts a replica change to mma replica changes.
// It will be passed to mma.RegisterExternalChanges.
func convertReplicaChangeToMMA(
	desc *roachpb.RangeDescriptor,
	usage allocator.RangeUsageInfo,
	changes kvpb.ReplicationChanges,
	leaseholderStoreID roachpb.StoreID,
) []mmaprototype.ReplicaChange {
	rLoad := mmaRangeLoad(usage)
	replicaChanges := make([]mmaprototype.ReplicaChange, 0, len(changes))
	replicaSet := desc.Replicas()

	var lhBeingRemoved bool
	// TODO(wenyihu6): check what type of replication changes will there be
	// here (can there be two voter removals or additions at the same time?)
	// If yes, lhBeingRemoved may be wrong.
	if (len(changes.VoterRemovals()) > 1 || len(changes.VoterAdditions()) > 1) && buildutil.CrdbTestBuild {
		panic("voter removals should not be more than one at a time")
	}

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
			lhBeingRemoved = replDesc.StoreID == leaseholderStoreID
			replicaChanges = append(replicaChanges, mmaprototype.MakeRemoveReplicaChange(
				desc.RangeID, rLoad, mmaprototype.ReplicaState{
					ReplicaIDAndType: mmaprototype.ReplicaIDAndType{
						ReplicaID: replDesc.ReplicaID,
						ReplicaType: mmaprototype.ReplicaType{
							ReplicaType:   replDesc.Type,
							IsLeaseholder: lhBeingRemoved,
						},
					},
				},
				chg.Target))
		}
	}
	for _, chg := range changes {
		switch {
		case chg.ChangeType == roachpb.ADD_VOTER || chg.ChangeType == roachpb.ADD_NON_VOTER:
			rType := roachpb.VOTER_FULL
			if chg.ChangeType == roachpb.ADD_NON_VOTER {
				rType = roachpb.NON_VOTER
			}
			replicaChanges = append(replicaChanges, mmaprototype.MakeAddReplicaChange(
				desc.RangeID, rLoad, mmaprototype.ReplicaState{
					ReplicaIDAndType: mmaprototype.ReplicaIDAndType{
						ReplicaType: mmaprototype.ReplicaType{
							ReplicaType: rType,
							// TODO(sumeer): can there be multiple ADD_VOTERs?
							IsLeaseholder: lhBeingRemoved && chg.ChangeType == roachpb.ADD_VOTER,
						},
					},
				}, chg.Target))
		case chg.ChangeType == roachpb.REMOVE_VOTER || chg.ChangeType == roachpb.REMOVE_NON_VOTER:
			// Handled above.
			continue
		default:
			panic("unimplemented change type")
		}
	}
	return replicaChanges
}
