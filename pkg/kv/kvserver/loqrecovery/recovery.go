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
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type storeIDSet map[roachpb.StoreID]struct{}

// Logger interface provides interactive commands a way collect important information
// regarding recovery process. This information is presented to user to confirm actions
// before any destructive changes could be planned or done.
type Logger interface {
	// Infof writes a formatted string into "log"
	Infof(string, ...interface{})
	// Warnf writes a formatted string into "log"
	Warnf(string, ...interface{})
}

// DefaultLogger implements recovery Logger interface that captures all lines and can
// provide its content as a string to present to user for subsequent confirmation.
type DefaultLogger struct {
	messages []string
	warnings []string
}

// Infof implements Logger interface.
func (l *DefaultLogger) Infof(format string, args ...interface{}) {
	l.messages = append(l.messages, fmt.Sprintf(format, args...))
}

// Warnf implements Logger interface.
func (l *DefaultLogger) Warnf(format string, args ...interface{}) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

// HasWarnings returns true if logger captured any warnings.
func (l *DefaultLogger) HasWarnings() bool {
	return len(l.warnings) > 0
}

// String implements Stringer interface
func (l *DefaultLogger) String() string {
	return fmt.Sprintf("Messages:\n  %s\n  %s\n",
		strings.Join(l.messages, "\n   "),
		strings.Join(l.warnings, "\n   "))
}

// CollectReplicaInfo captures states of all replicas in all stores for the sake of quorum recovery.
func CollectReplicaInfo(
	ctx context.Context, stores []storage.Engine,
) ([]loqrecoverypb.LocalRangeInfo, error) {
	var descriptors []loqrecoverypb.LocalRangeInfo
	for _, db := range stores {
		storeIdent, err := kvserver.ReadStoreIdent(ctx, db)
		if err != nil {
			return nil, err
		}
		err = kvserver.IterateRangeDescriptorsFromDisk(ctx, db, func(desc roachpb.RangeDescriptor) error {
			rsl := stateloader.Make(desc.RangeID)
			rstate, err := rsl.Load(ctx, db, &desc)
			if err != nil {
				return err
			}
			hstate, err := rsl.LoadHardState(ctx, db)
			if err != nil {
				return err
			}
			replicaData := loqrecoverypb.LocalRangeInfo{
				StoreID:            storeIdent.StoreID,
				NodeID:             storeIdent.NodeID,
				Desc:               desc,
				RaftAppliedIndex:   rstate.RaftAppliedIndex,
				RaftCommittedIndex: hstate.Commit,
				// TODO(oleg): #73282 Track presence of uncommitted descriptors
				HasUncommittedDescriptors: false,
			}
			descriptors = append(descriptors, replicaData)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return descriptors, nil
}

// PlanReplicas analyzes captured replica information to determine which replicas could serve
// as dedicated survivors in ranges where quorum was lost.
// Devised plan doesn't guarantee data consistency after the recovery, only the fact that ranges
// could progress and subsequently perform up-replication.
func PlanReplicas(
	descriptors []loqrecoverypb.LocalRangeInfo, deadStores []roachpb.StoreID, log Logger,
) ([]loqrecoverypb.ReplicaUpdate, error) {
	availableStoreIDs, missingStoreIDs, err := validateReplicaSets(descriptors, deadStores)
	if err != nil {
		return nil, err
	}
	log.Infof("found available stores: %s\n", joinStoreIDs(availableStoreIDs))
	if len(deadStores) == 0 {
		// If dead stores were not explicitly requested, warn about deduced dead stores
		// to confirm before saving plan.
		log.Warnf("found dead stores: %s\n", joinStoreIDs(missingStoreIDs))
	}

	var plan []loqrecoverypb.ReplicaUpdate
	// Find ranges that lost quorum and create updates for them.
	for _, rangeDesc := range descriptors {
		numDeadPeers := 0
		desc := rangeDesc.Desc
		allReplicas := desc.Replicas().Descriptors()
		maxLiveVoter := roachpb.StoreID(-1)
		for _, rep := range allReplicas {
			if _, ok := availableStoreIDs[rep.StoreID]; !ok {
				numDeadPeers++
				continue
			}
			// The designated survivor will be the voter with the highest storeID.
			// Note that an outgoing voter cannot be designated, as the only
			// replication change it could make is to turn itself into a learner, at
			// which point the range is completely messed up.
			//
			// Note: a better heuristic might be to choose the leaseholder store, not
			// the largest store, as this avoids the problem of requests still hanging
			// after running the tool in a rolling-restart fashion (when the lease-
			// holder is under a valid epoch and was ont chosen as designated
			// survivor). However, this choice is less deterministic, as leaseholders
			// are more likely to change than replication configs. The hanging would
			// independently be fixed by the below issue, so staying with largest store
			// is likely the right choice. See:
			//
			// https://github.com/cockroachdb/cockroach/issues/33007
			if rep.IsVoterNewConfig() && rep.StoreID > maxLiveVoter {
				maxLiveVoter = rep.StoreID
			}
		}

		// If there's no dead peer in this group (so can't hope to fix
		// anything by rewriting the descriptor) or the current store is not the
		// one we want to turn into the sole voter, don't do anything.
		if numDeadPeers == 0 {
			continue
		}
		if rangeDesc.StoreID != maxLiveVoter {
			log.Infof("not designated survivor, skipping: %s\n", &desc)
			continue
		}

		// The replica thinks it can make progress anyway, so we leave it alone.
		if desc.Replicas().CanMakeProgress(func(rep roachpb.ReplicaDescriptor) bool {
			_, ok := availableStoreIDs[rep.StoreID]
			return ok
		}) {
			log.Infof("replica has not lost quorum, skipping: %s\n", &desc)
			continue
		}

		// We're the designated survivor and the range does not to be recovered.
		//
		// Rewrite the range as having a single replica. The winning replica is
		// picked arbitrarily: the one with the highest store ID. This is not always
		// the best option: it may lose writes that were committed on another
		// surviving replica that had applied more of the raft log. However, in
		// practice when we have multiple surviving replicas but still need this
		// tool (because the replication factor was 4 or higher), we see that the
		// logs are nearly always in sync and the choice doesn't matter. Correctly
		// picking the replica with the longer log would complicate the use of this
		// tool.
		// Rewrite the replicas list. Bump the replica ID so that in case there are
		// other surviving nodes that were members of the old incarnation of the
		// range, they no longer recognize this revived replica (because they are
		// not in sync with it).
		update := loqrecoverypb.ReplicaUpdate{
			NodeID:   rangeDesc.NodeID,
			StoreID:  rangeDesc.StoreID,
			RangeID:  rangeDesc.Desc.RangeID,
			StartKey: desc.StartKey,
			NewReplica: &roachpb.ReplicaDescriptor{
				NodeID:    rangeDesc.NodeID,
				StoreID:   rangeDesc.StoreID,
				ReplicaID: desc.NextReplicaID,
			},
			NextReplicaID: desc.NextReplicaID + 1,
		}
		log.Infof("replica has lost quorum, recovering: %s -> %s\n", &desc, &update)
		plan = append(plan, update)
	}
	return plan, nil
}

// validateReplicaSets evaluates provided set of replicas and an optional deadStoreIDs
// request and produces consistency info containing:
// availableStores  - all storeIDs for which info was collected, i.e. (barring operator
//                    error) the conclusive list of all remaining stores in the cluster.
// missingStores    - all dead stores (stores that are referenced by replicas, but not
//                    present in any of descriptors)
// If inconsistency is found e.g. no info was provided for a store but it is not present
// in explicit deadStoreIDs list, error is returned.
func validateReplicaSets(
	descriptors []loqrecoverypb.LocalRangeInfo, deadStores []roachpb.StoreID,
) (availableStoreIDs, missingStoreIDs storeIDSet, _ error) {
	// Populate availableStoreIDs with all StoreIDs from which we collected info and,
	// populate missingStoreIDs with all StoreIDs referenced in replica descriptors for
	// which no information was collected.
	availableStoreIDs = make(storeIDSet)
	missingStoreIDs = make(storeIDSet)
	for _, rangeDesc := range descriptors {
		availableStoreIDs[rangeDesc.StoreID] = struct{}{}
		for _, replicaDesc := range rangeDesc.Desc.InternalReplicas {
			missingStoreIDs[replicaDesc.StoreID] = struct{}{}
		}
	}
	// The difference between all referenced StoreIDs (missingStoreIDs) and the present
	// StoreIDs (presentStoreIDs) should exactly equal the user-provided list of dead
	// stores (deadStores), and the former must be a superset of the latter (since each
	// descriptor found on a store references that store). Verify all of these conditions
	// and error out if one of them does not hold.
	for id := range availableStoreIDs {
		delete(missingStoreIDs, id)
	}
	// Stores that doesn't have info, but are not in explicit list.
	missingButNotDeadStoreIDs := make(storeIDSet)
	for id := range missingStoreIDs {
		missingButNotDeadStoreIDs[id] = struct{}{}
	}
	// Suspicious are available, but requested dead.
	suspiciousStoreIDs := make(storeIDSet)
	for id := range deadStores {
		delete(missingButNotDeadStoreIDs, roachpb.StoreID(id))
		if _, ok := availableStoreIDs[roachpb.StoreID(id)]; ok {
			suspiciousStoreIDs[roachpb.StoreID(id)] = struct{}{}
		}
	}
	if len(suspiciousStoreIDs) > 0 {
		return nil, nil, errors.Errorf(
			"stores %s are listed as dead, but replica info is provided for them",
			joinStoreIDs(suspiciousStoreIDs))
	}
	if len(deadStores) > 0 && len(missingButNotDeadStoreIDs) > 0 {
		// We can't proceed with this if dead nodes were explicitly provided.
		return nil, nil, errors.Errorf(
			"information about stores %s were not provided, nor they are listed as dead",
			joinStoreIDs(missingButNotDeadStoreIDs))
	}
	return availableStoreIDs, missingStoreIDs, nil
}

// MarshalAndWriteResults serializes plan and write into file or stdout.
func MarshalAndWriteResults(results interface{}, filename string) error {
	jsonpb := protoutil.JSONPb{Indent: "  "}
	var out []byte
	var err error
	if out, err = jsonpb.Marshal(results); err != nil {
		return errors.Wrap(err, "failed to marshal results")
	}

	if filename != "" {
		// If file exists confirm overwriting it.
		if _, err = os.Stat(filename); err == nil {
			return errors.Newf("file '%s' already exists", filename)
		}
		err = os.WriteFile(filename, out, fs.ModePerm)
		if err != nil {
			return errors.Wrap(err, "failed to write results file")
		}
	} else {
		_, err = os.Stdout.Write(out)
		return errors.Wrap(err, "failed to write results")
	}
	return nil
}

// PrepareUpdateReplicas creates batch updates from plan to provided stores.
// All found discrepancies are collected into list of warnings.
// This is the first step in applying recovery plan to stores. After this step
// all stores are still intact but changes are prepared successfully.
func PrepareUpdateReplicas(
	ctx context.Context,
	plan []loqrecoverypb.ReplicaUpdate,
	nodeID roachpb.NodeID,
	stores map[roachpb.StoreID]storage.Engine,
	log Logger,
) (map[roachpb.StoreID]storage.Batch, error) {
	// TODO(oleg): #73281 Track attempts to apply changes and also fill into audit log
	batches := make(map[roachpb.StoreID]storage.Batch)
	// Make a pre-check for all found stores, so we could confirm action.
	// Map contains a set of store names that were found in plan for this node, but were not
	// configured in this command invocation.
	missing := make(map[roachpb.StoreID]struct{})
	for _, update := range plan {
		if nodeID != update.NodeID {
			continue
		}
		if store, ok := stores[update.StoreID]; !ok {
			missing[update.StoreID] = struct{}{}
		} else {
			_, ok := batches[update.StoreID]
			if !ok {
				batches[update.StoreID] = store.NewBatch()
			}
		}
	}

	// We need to warn about discrepancies.
	if len(missing) > 0 {
		log.Warnf("stores %s expected on the node but no paths were provided", joinStoreIDs(missing))
	}

	for _, update := range plan {
		store, ok := stores[update.StoreID]
		if !ok {
			continue
		}
		batch := batches[update.StoreID]
		if err := prepareReplicaUpdate(ctx, store, batch, update, log); err != nil {
			return nil, errors.Wrapf(err,
				"failed to prepare update replica for range r%v on store s%d", update.RangeID, update.StoreID)
		}
	}
	return batches, nil
}

func prepareReplicaUpdate(
	ctx context.Context,
	store storage.Engine,
	batch storage.Batch,
	update loqrecoverypb.ReplicaUpdate,
	log Logger,
) error {
	clock := hlc.NewClock(hlc.UnixNano, 0)

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
	key := keys.RangeDescriptorKey(update.StartKey)
	value, intent, err := storage.MVCCGet(ctx, store, key, clock.Now(), storage.MVCCGetOptions{Inconsistent: true})
	if value == nil {
		return errors.Errorf("failed to find a range descriptor for range %v", key)
	}
	if err != nil {
		return err
	}
	var desc roachpb.RangeDescriptor
	if err := value.GetProto(&desc); err != nil {
		return err
	}
	// Sanity check that this is indeed the right range.
	if desc.RangeID != update.RangeID {
		return errors.Errorf("unexpected range ID at key: expected r%d but found r%d", update.RangeID, desc.RangeID)
	}
	// Check if replica is in a fixed state already if we already applied the change.
	if len(desc.InternalReplicas) == 1 &&
		desc.InternalReplicas[0].ReplicaID == update.NewReplica.ReplicaID &&
		desc.NextReplicaID == update.NextReplicaID {
		log.Warnf("replica is already updated: %s\n", desc)
		return nil
	}

	sl := stateloader.Make(desc.RangeID)
	ms, err := sl.LoadMVCCStats(ctx, batch)
	if err != nil {
		return errors.Wrap(err, "loading MVCCStats")
	}

	if intent != nil {
		// We rely on the property that transactions involving the range
		// descriptor always start on the range-local descriptor's key. When there
		// is an intent, this means that it is likely that the transaction did not
		// commit, so we abort the intent.
		//
		// However, this is not guaranteed. For one, applying a command is not
		// synced to disk, so in theory whichever store becomes the designated
		// survivor may temporarily have "forgotten" that the transaction
		// committed in its applied state (it would still have the committed log
		// entry, as this is durable state, so it would come back once the node
		// was running, but we don't see that materialized state in
		// unsafe-remove-dead-replicas). This is unlikely to be a problem in
		// practice, since we assume that the store was shut down gracefully and
		// besides, the write likely had plenty of time to make it to durable
		// storage. More troubling is the fact that the designated survivor may
		// simply not yet have learned that the transaction committed; it may not
		// have been in the quorum and could've been slow to catch up on the log.
		// It may not even have the intent; in theory the remaining replica could
		// have missed any number of transactions on the range descriptor (even if
		// they are in the log, they may not yet be applied, and the replica may
		// not yet have learned that they are committed). This is particularly
		// troubling when we miss a split, as the right-hand side of the split
		// will exist in the meta ranges and could even be able to make progress.
		// For yet another thing to worry about, note that the determinism (across
		// different nodes) assumed in this tool can easily break down in similar
		// ways (not all stores are going to have the same view of what the
		// descriptors are), and so multiple replicas of a range may declare
		// themselves the designated survivor. Long story short, use of this tool
		// with or without the presence of an intent can - in theory - really
		// tear the cluster apart.
		//
		// A solution to this would require a global view, where in a first step
		// we collect from each store in the cluster the replicas present and
		// compute from that a "recovery plan", i.e. set of replicas that will
		// form the recovered keyspace. We may then find that no such recovery
		// plan is trivially achievable, due to any of the above problems. But
		// in the common case, we do expect one to exist.
		log.Infof("aborting intent: %s (txn %s)\n", key, intent.Txn.ID)

		// A crude form of the intent resolution process: abort the
		// transaction by deleting its record.
		txnKey := keys.TransactionKey(intent.Txn.Key, intent.Txn.ID)
		if err := storage.MVCCDelete(ctx, batch, &ms, txnKey, hlc.Timestamp{}, nil); err != nil {
			return err
		}
		update := roachpb.LockUpdate{
			Span:   roachpb.Span{Key: intent.Key},
			Txn:    intent.Txn,
			Status: roachpb.ABORTED,
		}
		if _, err := storage.MVCCResolveWriteIntent(ctx, batch, &ms, update); err != nil {
			return err
		}
	}
	// Apply update to descriptor, but we could verify that it wasn't already updated.
	newDesc := desc
	replicas := []roachpb.ReplicaDescriptor{{
		NodeID:    update.NewReplica.NodeID,
		StoreID:   update.NewReplica.StoreID,
		ReplicaID: update.NewReplica.ReplicaID,
		Type:      update.NewReplica.Type,
	}}
	newDesc.SetReplicas(roachpb.MakeReplicaSet(replicas))
	newDesc.NextReplicaID = update.NextReplicaID

	// Write back
	if err := storage.MVCCPutProto(ctx, batch, &ms, key, clock.Now(),
		nil /* txn */, &newDesc); err != nil {
		return err
	}

	// Refresh stats
	if err := sl.SetMVCCStats(ctx, batch, &ms); err != nil {
		return errors.Wrap(err, "updating MVCCStats")
	}

	log.Infof("updating replica for r%d\n", desc.RangeID)
	return nil
}

// ApplyRecoveryChanges saves content storage batches into stores. This is the second step
// of applying recovery plan.
func ApplyRecoveryChanges(batches map[roachpb.StoreID]storage.Batch, log Logger) error {
	failed := false
	// Since we reached here we updated all stores
	for id, batch := range batches {
		// We can have empty batches if we try to apply the same change multiple times.
		// This is fine if we have to retry after previous errors.
		if !batch.Empty() {
			if err := batch.Commit(true); err != nil {
				// If we fail here, we'll have to rerun and provide only a subset of stores
				// to avoid hitting already changed replicas.
				log.Warnf("failed to update store s%d: %v\n", id, err)
				failed = true
			} else {
				log.Infof("updated store s%d\n", id)
			}
		}
	}
	if failed {
		return errors.Errorf("Failed to commit update to one or more stores")
	}
	return nil
}

// Make a string of stores 'set' in ascending order.
func joinStoreIDs(missing map[roachpb.StoreID]struct{}) string {
	storeIDs := make([]int, 0, len(missing))
	for k := range missing {
		storeIDs = append(storeIDs, int(k))
	}
	sort.Ints(storeIDs)
	storeNames := make([]string, 0, len(missing))
	for id := range storeIDs {
		storeNames = append(storeNames, fmt.Sprintf("s%d", id))
	}
	return strings.Join(storeNames, ", ")
}
