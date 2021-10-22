// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/quorum"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

type confirmAction func(message string) (bool, error)

// debugRecoverCmd is the root of all recover quorum commands
var debugRecoverCmd = &cobra.Command{
	Use:   "recover [command]",
	Short: "quorum recovery commands",
	Long: `Set of commands to collect process and apply quorum recovery.

Recovery commands allow for lossy recovery of cluster to a possibly
inconsistent state if quorum was lost on one or more ranges due to
node failures.

To perform recovery using debug recover subcommands you should:

0. Stop the cluster

1. Run cockroach debug recover collect-info on every node to collect
replication state from all surviving nodes. Outputs of these invocations
should be collected and made locally available for the next step.

2. Run cockroach debug recover make-plan providing all files generated
on previous step. Planner will decide which replicas should survive and
upreplicate. Mind that it is not always possible to pick replica with the
most up to date and consistent data.

3. Run cockroach debug recover execute-plan on every node using plan
generated on the previous step. Each node will pick relevant portion of
the plan and update local replicas accordingly to restore quorum.

4. Start the cluster.

If it was possible to produce and apply the plan, then cluster should
become operational again. It is not guaranteed that there's no data loss
and that all database consistency was not compromised.
`,
	RunE: UsageAndErr,
}

func init() {
	debugRecoverCmd.AddCommand(
		recoverDeadReplicaCollectCmd,
		recoverDeadReplicaPlanCmd,
		recoverDeadReplicaExecuteCmd)
}

var recoverDeadReplicaCollectCmd = &cobra.Command{
	Use:   "collect-info [destination-file]",
	Short: "collect replica information for range recovery in event of quorum loss",
	Long: `
Collect replica information by reading descriptors from underlying stores.

Replica information is written to a file for subsequent analysis by make-plan command.

See debug recover help for more details on how to use this command.
`,
	Args: cobra.MaximumNArgs(1),
	RunE: runDebugDeadReplicaCollect,
}

var debugDeadReplicaCollectOpts struct {
	Stores base.StoreSpecList
}

func runDebugDeadReplicaCollect(_ *cobra.Command, args []string) error {
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	var stores []storage.Engine
	for _, store := range debugDeadReplicaCollectOpts.Stores.Specs {
		db, err := OpenExistingStore(store.Path, stopper, true /* readOnly */)
		if err != nil {
			return err
		}
		stores = append(stores, db)
		defer db.Close()
	}

	descriptors, err := collectReplicaInfo(ctx, stores)
	if err != nil {
		return err
	}

	dest := ""
	promptOut := os.Stderr
	if len(args) > 0 {
		dest = args[0]
		promptOut = os.Stdout
	}
	return marshalAndWriteResults(descriptors, dest, getConfirmAction(promptOut))
}

func collectReplicaInfo(
	ctx context.Context, stores []storage.Engine,
) ([]quorum.LocalRangeInfo, error) {
	var descriptors []quorum.LocalRangeInfo
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
			replicaData := quorum.LocalRangeInfo{
				StoreID:                   storeIdent.StoreID,
				NodeID:                    storeIdent.NodeID,
				Desc:                      desc,
				RaftAppliedIndex:          rstate.RaftAppliedIndex,
				RaftCommittedIndex:        hstate.Commit,
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

var recoverDeadReplicaPlanCmd = &cobra.Command{
	Use:   "make-plan [replica-files]",
	Short: "Generate replica removal plan range recovery in event of quorum loss",
	Long: `
Make plan aggregate info obtained from all nodes using debug recover collect-info
command and devises a plan that would to recover from loss of quorum.

This command will identify ranges where raft consensus could not progress and pick
survivor replicas to act as a source of truth.

Once plan is produced, it needs to be applied on all nodes to update replica states.

See debug recover help for more details on how to use this command.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: runDebugPlanReplicaRemoval,
}

var debugDeadReplicaPlanOpts struct {
	outputFileName string
	deadStores     []int
}

// TODO(oleg): have a dead stores option that provides dead node list
// we would confirm if it matches info in replica files during plan creation.
func runDebugPlanReplicaRemoval(_ *cobra.Command, args []string) error {
	// Deserialize replica data collected from nodes.
	var descriptors []quorum.LocalRangeInfo
	for _, filename := range args {
		f, err := os.Open(filename)
		if err != nil {
			return errors.Wrapf(err, "failed to open replica info file %s", filename)
		}
		data, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return errors.Wrapf(err, "failed to read replica info file %s", filename)
		}

		var nodeDescriptors []quorum.LocalRangeInfo
		if err = json.Unmarshal(data, &nodeDescriptors); err != nil {
			return errors.Wrapf(err, "failed to unmarshal replica info from file %s", filename)
		}
		descriptors = append(descriptors, nodeDescriptors...)
	}

	// If we need to print plan to stdout, use stderr for confirmations as needed.
	promptOut := os.Stdout
	if debugDeadReplicaPlanOpts.outputFileName == "" {
		promptOut = os.Stderr
	}
	confirm := getConfirmAction(promptOut)

	// Run planner
	plan, err := planReplicas(descriptors /*, confirm*/)
	if err != nil {
		return err
	}

	return marshalAndWriteResults(plan, debugDeadReplicaPlanOpts.outputFileName, confirm)
}

func planReplicas(descriptors []quorum.LocalRangeInfo) ([]quorum.ReplicaUpdate, error) {
	plan := []quorum.ReplicaUpdate{}

	// TODO(oleg): compare with provided stores list if available
	// Find on live stores from all descriptors
	liveStoreIDs := make(map[roachpb.StoreID]struct{})
	for _, rangeDesc := range descriptors {
		liveStoreIDs[rangeDesc.StoreID] = struct{}{}
	}
	fmt.Fprintf(os.Stderr, "Found live stores: %v\n", joinStoreIDs(liveStoreIDs))

	// Find ranges that lost quorum and create updates for them
	for _, rangeDesc := range descriptors {
		numDeadPeers := 0
		desc := rangeDesc.Desc
		allReplicas := desc.Replicas().Descriptors()
		maxLiveVoter := roachpb.StoreID(-1)
		for _, rep := range allReplicas {
			if _, ok := liveStoreIDs[rep.StoreID]; !ok {
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
			fmt.Fprintf(os.Stderr, "Not designated survivor, skipping: %s\n", &desc)
			continue
		}

		// The replica thinks it can make progress anyway, so we leave it alone.
		if desc.Replicas().CanMakeProgress(func(rep roachpb.ReplicaDescriptor) bool {
			_, ok := liveStoreIDs[rep.StoreID]
			return ok
		}) {
			fmt.Fprintf(os.Stderr, "Replica has not lost quorum, skipping: %s\n", &desc)
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
		update := quorum.ReplicaUpdate{
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
		fmt.Fprintf(os.Stderr, "Replica has lost quorum, recovering: %s -> %s\n", &desc, &update)
		plan = append(plan, update)
	}
	return plan, nil
}

// marshalAndWriteResults serializes plan and write into file or stdout.
func marshalAndWriteResults(results interface{}, filename string, confirm confirmAction) error {
	var out []byte
	var err error
	if out, err = json.Marshal(results); err != nil {
		return errors.Wrap(err, "failed to marshal results")
	}

	if filename != "" {
		// If file exists confirm overwriting it.
		if _, err = os.Stat(filename); err == nil {
			var proceed bool
			proceed, err = confirm(fmt.Sprintf("File '%s' exists, overwrite", filename))
			if err != nil {
				return err
			}
			if !proceed {
				return errors.Newf("Refusing to overwrite '%s'", filename)
			}
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

var recoverDeadReplicaExecuteCmd = &cobra.Command{
	Use:   "apply-plan plan-file",
	Short: "remove dead replicas according to devised removal plan",
	Long: `
Apply changes to replicas in the provided stores using a plan.

In ranges with a lost quorum, designated replicas would serve as a source of
truth and allow raft to progress so that data becomes accessible.

See debug recover help for more details on how to use this command.
`,
	Args: cobra.ExactArgs(1),
	RunE: runDebugDeadReplicaRemove,
}

var debugDeadReplicaRemoveOpts struct {
	Stores base.StoreSpecList
}

func runDebugDeadReplicaRemove(_ *cobra.Command, args []string) error {
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	planFile := args[0]
	f, err := os.Open(planFile)
	if err != nil {
		return errors.Wrapf(err, "failed to open removal plan file %s", planFile)
	}

	data, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		return errors.Wrapf(err, "failed to read removal plan file %s", planFile)
	}

	var nodeUpdates []quorum.ReplicaUpdate
	if err = json.Unmarshal(data, &nodeUpdates); err != nil {
		return errors.Wrapf(err, "failed to unmarshal removal plan from file %s", planFile)
	}

	// Open all stores
	localNodeID := roachpb.NodeID(0)
	stores := make(map[roachpb.StoreID]storage.Engine)
	for _, storeSpec := range debugDeadReplicaRemoveOpts.Stores.Specs {
		db, err := OpenExistingStore(storeSpec.Path, stopper, false /* readOnly */)
		if err != nil {
			return err
		}
		defer db.Close()
		storeIdent, err := kvserver.ReadStoreIdent(ctx, db)
		if err != nil {
			return err
		}
		if localNodeID != storeIdent.NodeID {
			if localNodeID != roachpb.NodeID(0) {
				// TODO(oleg): does it make any sense to check?
				return errors.Errorf("Found stores from multiple node IDs n%d, n%d. Can only run in context of single node.", localNodeID, storeIdent.NodeID)
			}
			localNodeID = storeIdent.NodeID
		}
		stores[storeIdent.StoreID] = db
	}

	// Perform deletion
	err = removeReplicas(ctx, nodeUpdates, localNodeID, stores, getConfirmAction(os.Stdout))
	return err
}

func removeReplicas(
	ctx context.Context,
	plan []quorum.ReplicaUpdate,
	nodeID roachpb.NodeID,
	stores map[roachpb.StoreID]storage.Engine,
	confirm confirmAction,
) error {
	// Maybe track attempts to apply changes to filter cases where we run the same plan again?
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

	if len(missing) > 0 {
		if ok, err := confirm(fmt.Sprintf(
			"Stores %s expected on the node but no paths were provided. Proceed with remaining stores",
			joinStoreIDs(missing))); !ok || err != nil {
			if !ok {
				fmt.Printf("Aborting at user request\n")
			}
			return err
		}
	}

	for _, update := range plan {
		if nodeID != update.NodeID {
			continue
		}
		store, ok := stores[update.StoreID]
		if !ok {
			continue
		}
		// We always have a batch if store exists since we populate them during initial
		// store check.
		batch := batches[update.StoreID]
		if err := prepareRemoveReplicaFromStore(ctx, store, batch, update); err != nil {
			return errors.Wrapf(err,
				"failed to update replica for key %v on store s%d", update.StartKey, update.StoreID)
		}
	}
	if len(batches) == 0 {
		return nil
	}
	// Confirm destructive action.
	if proceed, err := confirm("Proceed with above changes"); !proceed || err != nil {
		if !proceed {
			fmt.Println("Aborting at user request.")
		}
		return err
	}

	// Since we reached here we updated all stores
	for id, batch := range batches {
		// We can have empty batches if we try to apply the same change multiple times.
		// This is fine if we have to retry after previous errors.
		if !batch.Empty() {
			if err := batch.Commit(true); err != nil {
				// If we fail here, we'll have to rerun and provide only a subset of stores
				// to avoid hitting already changed replicas.
				fmt.Printf("Failed to update store s%d\n", id)
				return err
			}
			fmt.Printf("Updated store s%d\n", id)
		}
	}
	return nil
}

func prepareRemoveReplicaFromStore(
	ctx context.Context, store storage.Engine, batch storage.Batch, update quorum.ReplicaUpdate,
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
	// Sanity check that this is indeed the right range
	if desc.RangeID != update.RangeID {
		return errors.Errorf("unexpected range ID at key: expected r%d but found r%d", update.RangeID, desc.RangeID)
	}
	// Check if replica is in a fixed state already if we already applied the change.
	if len(desc.InternalReplicas) == 1 &&
		desc.InternalReplicas[0].ReplicaID == update.NewReplica.ReplicaID &&
		desc.NextReplicaID == update.NextReplicaID {
		fmt.Printf("Replica is already updated: %s\n", desc)
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
		fmt.Printf("Aborting intent: %s (txn %s)\n", key, intent.Txn.ID)

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

	fmt.Printf("Updating replica for r%d\n", desc.RangeID)
	return nil
}

func getConfirmAction(out io.Writer) confirmAction {
	switch interactivePromptOpts.action {
	case "p":
		reader := bufio.NewReader(os.Stdin)
		return func(message string) (bool, error) {
			fmt.Fprintf(out, "%s [y/N] ", message)
			line, err := reader.ReadString('\n')
			if err != nil {
				return false, err
			}
			fmt.Fprintf(out, "\n")
			if len(line) > 1 && (line[0] == 'y' || line[0] == 'Y') {
				return true, nil
			}
			return false, nil
		}
	case "y":
		return func(_ string) (bool, error) {
			return true, nil
		}
	default:
		// Default to reject in case some input validation bug slips.
		return func(_ string) (bool, error) {
			return false, nil
		}
	}
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
