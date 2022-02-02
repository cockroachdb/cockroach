// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// PrepareStoreReport contains information about all prepared changes for the
// stores. Its purpose is to inform user about actions to be performed.
type PrepareStoreReport struct {
	// MissingStores contains a set of stores that node on which this update is
	// performed is expected to have according to update plan, but are not
	// provided in config.
	// While having a missing store is suspicious, it is not necessary a failure
	// and user would be asked to confirm if it is a desired situation.
	MissingStores []roachpb.StoreID
	// Replicas contain update info about all replicas that are planned for update.
	UpdatedReplicas []PrepareReplicaReport
	// Replicas identified as ones that doesn't need updating (currently because
	// update was already done).
	SkippedReplicas []PrepareReplicaReport
}

// PrepareReplicaReport contains information about prepared change for a replica.
// Its purpose is to inform user about actions to be performed. And create a record
// that would be applied to structured log, rangelog and other downstream systems
// for audit purposes.
type PrepareReplicaReport struct {
	// Replica identification data.
	Replica    roachpb.ReplicaDescriptor
	Descriptor roachpb.RangeDescriptor
	OldReplica roachpb.ReplicaDescriptor

	// AlreadyUpdated is true if state of replica in store already matches desired
	// target state. This would happen if the plan is applied more than once which
	// is safe because it is idempotent, but we want to notify user of the
	// situation.
	AlreadyUpdated bool

	// RemovedReplicas is a set of replicas that were removed from range descriptor.
	RemovedReplicas roachpb.ReplicaSet
}

// RangeID of underlying range.
func (r PrepareReplicaReport) RangeID() roachpb.RangeID {
	return r.Descriptor.RangeID
}

// StartKey of underlying range.
func (r PrepareReplicaReport) StartKey() roachpb.RKey {
	return r.Descriptor.StartKey
}

// PrepareUpdateReplicas prepares all changes to be committed to provided stores
// as a first step of apply stage. This function would write changes to stores
// using provided batches and return a summary of changes that were done together
// with any discrepancies found. The caller could then confirm actions and either
// commit or discard the changes.
// Changes also include update records in the store local keys that are consumed
// on the first start of node. See keys.StoreUnsafeReplicaRecoveryKey for
// details.
func PrepareUpdateReplicas(
	ctx context.Context,
	plan loqrecoverypb.ReplicaUpdatePlan,
	uuidGen uuid.Generator,
	updateTime time.Time,
	nodeID roachpb.NodeID,
	batches map[roachpb.StoreID]storage.Batch,
) (PrepareStoreReport, error) {
	var report PrepareStoreReport

	// Map contains a set of store names that were found in plan for this node,
	// but were not configured in this command invocation.
	missing := make(map[roachpb.StoreID]struct{})
	for _, update := range plan.Updates {
		if nodeID != update.NodeID() {
			continue
		}
		if readWriter, ok := batches[update.StoreID()]; !ok {
			missing[update.StoreID()] = struct{}{}
			continue
		} else {
			replicaReport, err := applyReplicaUpdate(ctx, readWriter, update)
			if err != nil {
				return PrepareStoreReport{}, errors.Wrapf(
					err,
					"failed to prepare update replica for range r%v on store s%d", update.RangeID,
					update.StoreID())
			}
			if !replicaReport.AlreadyUpdated {
				report.UpdatedReplicas = append(report.UpdatedReplicas, replicaReport)
				uuid, err := uuidGen.NewV1()
				if err != nil {
					return PrepareStoreReport{}, errors.Wrap(err,
						"failed to generate uuid to write replica recovery evidence record")
				}
				if err := writeReplicaRecoveryStoreRecord(
					uuid, updateTime.UnixNano(), update, replicaReport, readWriter); err != nil {
					return PrepareStoreReport{}, errors.Wrap(err,
						"failed writing replica recovery evidence record")
				}
			} else {
				report.SkippedReplicas = append(report.SkippedReplicas, replicaReport)
			}
		}
	}

	if len(missing) > 0 {
		report.MissingStores = storeSliceFromSet(missing)
	}
	return report, nil
}

func applyReplicaUpdate(
	ctx context.Context, readWriter storage.ReadWriter, update loqrecoverypb.ReplicaUpdate,
) (PrepareReplicaReport, error) {
	clock := hlc.NewClock(hlc.UnixNano, 0)
	report := PrepareReplicaReport{
		Replica: update.NewReplica,
	}

	// Write the rewritten descriptor to the range-local descriptor
	// key. We do not update the meta copies of the descriptor.
	// Instead, we leave them in a temporarily inconsistent state and
	// they will be overwritten when the cluster recovers and
	// up-replicates this range from its single copy to multiple
	// copies. We rely on the fact that all range descriptor updates
	// start with a CPut on the range-local copy followed by a blind
	// Put to the meta copy.
	//
	// For example, if we have replicas on s1-s4 but s3 and s4 are
	// dead, we will rewrite the replica on s2 to have s2 as its only
	// member only. When the cluster is restarted (and the dead nodes
	// remain dead), the rewritten replica will be the only one able
	// to make progress. It will elect itself leader and upreplicate.
	//
	// The old replica on s1 is untouched by this process. It will
	// eventually either be overwritten by a new replica when s2
	// upreplicates, or it will be destroyed by the replica GC queue
	// after upreplication has happened and s1 is no longer a member.
	// (Note that in the latter case, consistency between s1 and s2 no
	// longer matters; the consistency checker will only run on nodes
	// that the new leader believes are members of the range).
	//
	// Note that this tool does not guarantee fully consistent
	// results; the most recent writes to the raft log may have been
	// lost. In the most unfortunate cases, this means that we would
	// be "winding back" a split or a merge, which is almost certainly
	// to result in irrecoverable corruption (for example, not only
	// will individual values stored in the meta ranges diverge, but
	// there will be keys not represented by any ranges or vice
	// versa).
	key := keys.RangeDescriptorKey(update.StartKey.AsRKey())
	value, _, err := storage.MVCCGet(
		ctx, readWriter, key, clock.Now(), storage.MVCCGetOptions{Inconsistent: true})
	if value == nil {
		return PrepareReplicaReport{}, errors.Errorf(
			"failed to find a range descriptor for range %v", key)
	}
	if err != nil {
		return PrepareReplicaReport{}, err
	}
	var localDesc roachpb.RangeDescriptor
	if err := value.GetProto(&localDesc); err != nil {
		return PrepareReplicaReport{}, err
	}
	// Sanity check that this is indeed the right range.
	if localDesc.RangeID != update.RangeID {
		return PrepareReplicaReport{}, errors.Errorf(
			"unexpected range ID at key: expected r%d but found r%d", update.RangeID, localDesc.RangeID)
	}
	// Check if replica is in a fixed state already if we already applied the change.
	if len(localDesc.InternalReplicas) == 1 &&
		localDesc.InternalReplicas[0].ReplicaID == update.NewReplica.ReplicaID &&
		localDesc.NextReplicaID == update.NextReplicaID {
		report.AlreadyUpdated = true
		return report, nil
	}

	sl := stateloader.Make(localDesc.RangeID)
	ms, err := sl.LoadMVCCStats(ctx, readWriter)
	if err != nil {
		return PrepareReplicaReport{}, errors.Wrap(err, "loading MVCCStats")
	}

	newDesc := localDesc
	replicas := []roachpb.ReplicaDescriptor{
		{
			NodeID:    update.NewReplica.NodeID,
			StoreID:   update.NewReplica.StoreID,
			ReplicaID: update.NewReplica.ReplicaID,
			Type:      update.NewReplica.Type,
		},
	}
	newDesc.SetReplicas(roachpb.MakeReplicaSet(replicas))
	newDesc.NextReplicaID = update.NextReplicaID

	if err := storage.MVCCPutProto(
		ctx, readWriter, &ms, key, clock.Now(),
		nil /* txn */, &newDesc); err != nil {
		return PrepareReplicaReport{}, err
	}
	report.Descriptor = newDesc
	report.RemovedReplicas = localDesc.Replicas()
	report.OldReplica, _ = report.RemovedReplicas.RemoveReplica(
		update.NewReplica.NodeID, update.NewReplica.StoreID)

	// Refresh stats
	if err := sl.SetMVCCStats(ctx, readWriter, &ms); err != nil {
		return PrepareReplicaReport{}, errors.Wrap(err, "updating MVCCStats")
	}

	return report, nil
}

// ApplyUpdateReport contains info about recovery changes applied to stores.
type ApplyUpdateReport struct {
	// IDs of successfully updated stores.
	UpdatedStores []roachpb.StoreID
}

// CommitReplicaChanges saves content storage batches into stores. This is the
// second step of applying recovery plan.
func CommitReplicaChanges(batches map[roachpb.StoreID]storage.Batch) (ApplyUpdateReport, error) {
	var report ApplyUpdateReport
	failed := false
	var updateErrors []string
	// Commit changes to all stores. Stores could have pending changes if plan
	// contains replicas belonging to them, or have no changes if no replicas
	// belong to it or if changes has been applied earlier, and we try to reapply
	// the same plan twice.
	for id, batch := range batches {
		if batch.Empty() {
			continue
		}
		if err := batch.Commit(true); err != nil {
			// If we fail here, we can only try to run the whole process from scratch
			// as this store is somehow broken.
			updateErrors = append(updateErrors, fmt.Sprintf("failed to update store s%d: %v", id, err))
			failed = true
		} else {
			report.UpdatedStores = append(report.UpdatedStores, id)
		}
	}
	if failed {
		return report, errors.Errorf(
			"failed to commit update to one or more stores: %s", strings.Join(updateErrors, "; "))
	}
	return report, nil
}
