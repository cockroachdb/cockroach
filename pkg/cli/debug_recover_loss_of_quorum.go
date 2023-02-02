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
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/strutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/hintdetail"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// confirmActionFlag defines a pflag to parse a confirm option.
type confirmActionFlag int

const (
	prompt confirmActionFlag = iota
	allNo
	allYes
)

// Type implements the pflag.Value interface.
func (l *confirmActionFlag) Type() string { return "confirmAction" }

// String implements the pflag.Value interface.
func (l *confirmActionFlag) String() string {
	switch *l {
	case allYes:
		return "y"
	case allNo:
		return "n"
	case prompt:
		return "p"
	}
	log.Fatalf(context.Background(), "unknown confirm action flag value %d", *l)
	return ""
}

// Set implements the pflag.Value interface.
func (l *confirmActionFlag) Set(value string) error {
	switch strings.ToLower(value) {
	case "y", "yes":
		*l = allYes
	case "n", "no":
		*l = allNo
	case "p", "ask":
		*l = prompt
	default:
		return errors.Errorf("unrecognized value for confirmation flag: %s", value)
	}
	return nil
}

// debugRecoverCmd is the root of all recover quorum commands
var debugRecoverCmd = &cobra.Command{
	Use:   "recover [command]",
	Short: "commands to recover unavailable ranges in case of quorum loss",
	Long: `Set of commands to recover unavailable ranges.

If cluster permanently loses several nodes containing multiple replicas of
the range it is possible for that range to lose quorum. Those ranges can
not converge on their final state and can't proceed further. This is a 
potential data and consistency loss situation. In most cases cluster should 
be restored from the latest backup.

As a workaround, when consistency could be sacrificed temporarily to restore
availability quickly it is possible to instruct remaining replicas to act as
if they have the most up to date view of data and continue operation. There's
no way to guarantee which replica contains most up-to-date information if at
all so heuristics based on raft state are used to pick one.

As a result of such recovery, some of the data could be lost, indexes could
become corrupted and database constraints could be violated. So manual
recovery steps to verify the data and ensure database consistency should be
taken ASAP. Those actions should be done at application level.

'debug recover' set of commands is used as a last resort to perform range
recovery operation. To perform recovery one should perform this sequence
of actions:

0. Decommission failed nodes preemptively to eliminate the possibility of
them coming back online and conflicting with the recovered state. Note that
if system ranges suffer loss of quorum, it may be impossible to decommission
nodes. In that case, recovery can proceed, but those nodes must be prevented
from communicating with the cluster and must be decommissioned once the cluster
is back online after recovery.

1. Stop the cluster

2. Run 'cockroach debug recover collect-info' on every node to collect
replication state from all surviving nodes. Outputs of these invocations
should be collected and made locally available for the next step.

3. Run 'cockroach debug recover make-plan' providing all files generated
on step 1. Planner will decide which replicas should survive and
up-replicate.

4. Run 'cockroach debug recover execute-plan' on every node using plan
generated on the previous step. Each node will pick relevant portion of
the plan and update local replicas accordingly to restore quorum.

5. Start the cluster.

If it was possible to produce and apply the plan, then cluster should
become operational again. It is not guaranteed that there's no data loss
and that all database consistency was not compromised.

Example run:

If we have a cluster of 5 nodes 1-5 where we lost nodes 3 and 4. Each node
has two stores and they are numbered as 1,2 on node 1; 3,4 on node 2 etc.
Recovery commands to recover unavailable ranges would be:

Decommission dead nodes and stop the cluster.

[cockroach@node1 ~]$ cockroach debug recover collect-info --store=/mnt/cockroach-data-1 --store=/mnt/cockroach-data-2 >info-node1.json
[cockroach@node2 ~]$ cockroach debug recover collect-info --store=/mnt/cockroach-data-1 --store=/mnt/cockroach-data-2 >info-node2.json
[cockroach@node5 ~]$ cockroach debug recover collect-info --store=/mnt/cockroach-data-1 --store=/mnt/cockroach-data-2 >info-node5.json

[cockroach@base ~]$ scp cockroach@node1:info-node1.json .
[cockroach@base ~]$ scp cockroach@node2:info-node1.json .
[cockroach@base ~]$ scp cockroach@node5:info-node1.json .

[cockroach@base ~]$ cockroach debug recover make-plan --dead-store-ids=5,6,7,8 info-node1.json info-node2.json info-node5.json >recover-plan.json

[cockroach@base ~]$ scp recover-plan.json cockroach@node1:
[cockroach@base ~]$ scp recover-plan.json cockroach@node2:
[cockroach@base ~]$ scp recover-plan.json cockroach@node5:

[cockroach@node1 ~]$ cockroach debug recover apply-plan --store=/mnt/cockroach-data-1 --store=/mnt/cockroach-data-2 recover-plan.json
Info:
  updating replica for r1
Proceed with above changes [y/N]y
Info:
  updated store s2
[cockroach@node2 ~]$ cockroach debug recover apply-plan --store=/mnt/cockroach-data-1 --store=/mnt/cockroach-data-2 recover-plan.json
[cockroach@node5 ~]$ cockroach debug recover apply-plan --store=/mnt/cockroach-data-1 --store=/mnt/cockroach-data-2 recover-plan.json

Now the cluster could be started again.
`,
	RunE: UsageAndErr,
}

var recoverCommands = []*cobra.Command{
	debugRecoverCollectInfoCmd,
	debugRecoverPlanCmd,
	debugRecoverExecuteCmd,
	//debugRecoverVerify,
}

func init() {
	debugRecoverCmd.AddCommand(
		debugRecoverCollectInfoCmd,
		debugRecoverPlanCmd,
		debugRecoverExecuteCmd)
}

var debugRecoverCollectInfoCmd = &cobra.Command{
	Use:   "collect-info [destination-file]",
	Short: "collect replica information from a cluster",
	Long: `
Collect information about replicas in the cluster.

The command can collect data from an online or an offline cluster.

In the first case, the address of a single healthy cluster node must be provided
using the --host flag. This designated node will handle collection of data from
all surviving nodes.

In the second case data is read directly from local stores on each node.
CockroachDB must not be running on any node. The location of each store must be
provided using the --store flag. The command must be executed for all surviving
stores.

Multiple store locations can be provided to the command to collect all info
from all stores on a node at once. It is also possible to call it per store, in
that case all resulting files should be fed to the plan subcommand.

Collected information is written to a destination file if file name is provided,
or to stdout.

See debug recover command help for more details on how to use this command.
`,
	Args: cobra.MaximumNArgs(1),
	RunE: runDebugDeadReplicaCollect,
}

var debugRecoverCollectInfoOpts struct {
	Stores base.StoreSpecList
}

func runDebugDeadReplicaCollect(cmd *cobra.Command, args []string) error {
	// We must have cancellable context here to obtain grpc client connection.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var replicaInfo loqrecoverypb.ClusterReplicaInfo
	var stats loqrecovery.CollectionStats

	if len(debugRecoverCollectInfoOpts.Stores.Specs) == 0 {
		c, finish, err := getAdminClient(ctx, serverCfg)
		if err != nil {
			return errors.Wrapf(err, "failed to get admin connection to cluster")
		}
		defer finish()
		replicaInfo, stats, err = loqrecovery.CollectRemoteReplicaInfo(ctx, c)
		if err != nil {
			return errors.WithHint(errors.Wrap(err,
				"failed to retrieve replica info from cluster"),
				"Check cluster health and retry the operation.")
		}
	} else {
		var stores []storage.Engine
		for _, storeSpec := range debugRecoverCollectInfoOpts.Stores.Specs {
			db, err := OpenEngine(storeSpec.Path, stopper, storage.MustExist, storage.ReadOnly)
			if err != nil {
				return errors.WithHint(errors.Wrapf(err,
					"failed to open store at path %q", storeSpec.Path),
					"Ensure that store path is correct and that it is not used by another process.")
			}
			stores = append(stores, db)
		}
		var err error
		replicaInfo, stats, err = loqrecovery.CollectStoresReplicaInfo(ctx, stores)
		if err != nil {
			return errors.Wrapf(err, "failed to collect replica info from local stores")
		}
	}

	var writer io.Writer = os.Stdout
	if len(args) > 0 {
		filename := args[0]
		if _, err := os.Stat(filename); err == nil {
			return errors.Newf("file %q already exists", filename)
		}

		outFile, err := os.Create(filename)
		if err != nil {
			return errors.Wrapf(err, "failed to create file %q", filename)
		}
		defer outFile.Close()
		writer = outFile
	}
	jsonpb := protoutil.JSONPb{Indent: "  "}
	out, err := jsonpb.Marshal(&replicaInfo)
	if err != nil {
		return errors.Wrap(err, "failed to marshal collected replica info")
	}
	if _, err := writer.Write(out); err != nil {
		return errors.Wrap(err, "failed to write collected replica info")
	}
	_, _ = fmt.Fprintf(stderr, `Collected recovery info from:
nodes             %d
stores            %d
Collected info:
replicas          %d
range descriptors %d
`, stats.Nodes, stats.Stores, replicaInfo.ReplicaCount(), stats.Descriptors)
	return nil
}

var debugRecoverPlanCmd = &cobra.Command{
	Use:   "make-plan [replica-files]",
	Short: "generate a plan to recover ranges that lost quorum",
	Long: `
Devise a plan to restore ranges that lost a quorum.

The command analyzes information about replicas from all surviving nodes of a
cluster, finds ranges that lost quorum and makes decisions about which replicas
should act as survivors to restore quorum.

Information about replicas could be collected directly by connecting to the
cluster or from files generated by the collect-info command. In former case,
cluster connection parameters must be specified. If latter case, file names
should be provided as arguments. 

After the data is analyzed, a recovery plan is written into a file or stdout.

This command only creates a plan and doesn't change any data.'

See debug recover command help for more details on how to use this command.
`,
	Args: cobra.MinimumNArgs(0),
	RunE: runDebugPlanReplicaRemoval,
}

var debugRecoverPlanOpts struct {
	outputFileName string
	deadStoreIDs   []int
	deadNodeIDs    []int
	confirmAction  confirmActionFlag
	force          bool
}

var planSpecificFlags = map[string]struct{}{
	"plan":           {},
	"dead-store-ids": {},
	"dead-node-ids":  {},
	"force":          {},
	"confirm":        {},
}

func runDebugPlanReplicaRemoval(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	var replicas loqrecoverypb.ClusterReplicaInfo
	var err error

	if debugRecoverPlanOpts.deadStoreIDs != nil && debugRecoverPlanOpts.deadNodeIDs != nil {
		return errors.New("debug recover make-plan command accepts either --dead-node-ids or --dead-store-ids")
	}

	var stats loqrecovery.CollectionStats
	if len(args) == 0 {
		// If no replica info is provided, try to connect to a cluster default or
		// explicitly provided to retrieve replica info.
		c, finish, err := getAdminClient(ctx, serverCfg)
		if err != nil {
			return errors.Wrapf(err, "failed to get admin connection to cluster")
		}
		defer finish()
		replicas, stats, err = loqrecovery.CollectRemoteReplicaInfo(ctx, c)
		if err != nil {
			return errors.Wrapf(err, "failed to retrieve replica info from cluster")
		}
	} else {
		replicas, err = readReplicaInfoData(args)
		if err != nil {
			return err
		}
	}

	var deadStoreIDs []roachpb.StoreID
	for _, id := range debugRecoverPlanOpts.deadStoreIDs {
		deadStoreIDs = append(deadStoreIDs, roachpb.StoreID(id))
	}

	var deadNodeIDs []roachpb.NodeID
	for _, id := range debugRecoverPlanOpts.deadNodeIDs {
		deadNodeIDs = append(deadNodeIDs, roachpb.NodeID(id))
	}

	plan, report, err := loqrecovery.PlanReplicas(
		ctx,
		replicas,
		deadStoreIDs,
		deadNodeIDs,
		uuid.DefaultGenerator)
	if err != nil {
		return err
	}

	if stats.Nodes > 0 {
		_, _ = fmt.Fprintf(stderr, `Nodes scanned:           %d
`, stats.Nodes)
	}
	_, _ = fmt.Fprintf(stderr, `Total replicas analyzed: %d
Ranges without quorum:   %d
Discarded live replicas: %d

`, report.TotalReplicas, len(report.PlannedUpdates), report.DiscardedNonSurvivors)
	_, _ = fmt.Fprintf(stderr, "Proposed changes:\n")
	for _, r := range report.PlannedUpdates {
		_, _ = fmt.Fprintf(stderr, "  range r%d:%s updating replica %s to %s. "+
			"Discarding available replicas: [%s], discarding dead replicas: [%s].\n",
			r.RangeID, r.StartKey, r.OldReplica, r.NewReplica,
			r.DiscardedAvailableReplicas, r.DiscardedDeadReplicas)
	}

	argStoresMsg := ""
	if len(deadStoreIDs) > 0 {
		argStoresMsg = ", (matches --dead-store-ids)"
	}
	if len(deadNodeIDs) > 0 {
		argStoresMsg = ", (matches --dead-node-ids)"
	}
	_, _ = fmt.Fprintf(stderr, "\nDiscovered dead nodes, will be marked as decommissioned:\n%s\n%s\n\n",
		formatNodeStores(report.MissingNodes, "  "), argStoresMsg)

	planningErr := report.Error()
	if planningErr != nil {
		// Need to warn user before they make a decision that ignoring
		// inconsistencies is a really bad idea.
		_, _ = fmt.Fprintf(stderr,
			"Found replica inconsistencies:\n\n%s\n\nOnly proceed as a last resort!\n",
			hintdetail.FlattenDetails(planningErr))
	}

	if debugRecoverPlanOpts.confirmAction == allNo {
		return errors.New("abort")
	}

	switch debugRecoverPlanOpts.confirmAction {
	case prompt:
		opts := "y/N"
		if planningErr != nil {
			opts = "f/N"
		}
		done := false
		for !done {
			_, _ = fmt.Fprintf(stderr, "Proceed with plan creation [%s] ", opts)
			reader := bufio.NewReader(os.Stdin)
			line, err := reader.ReadString('\n')
			if err != nil {
				return errors.Wrap(err, "failed to read user input")
			}
			line = strings.ToLower(strings.TrimSpace(line))
			if len(line) == 0 {
				line = "n"
			}
			switch line {
			case "y":
				// We ignore y if we have errors. In that case you can only force or
				// abandon attempt.
				if planningErr != nil {
					continue
				}
				done = true
			case "f":
				done = true
			case "n":
				return errors.New("abort")
			}
		}
	case allYes:
		if planningErr != nil && !debugRecoverPlanOpts.force {
			return errors.Errorf(
				"can not create plan because of errors and no --force flag is given")
		}
	default:
		return errors.New("unexpected CLI error, try using different --confirm option value")
	}

	if len(plan.Updates) == 0 {
		_, _ = fmt.Fprintln(stderr, "Found no ranges in need of recovery, nothing to do.")
		return nil
	}

	planFile := "<plan file>"
	var writer io.Writer = os.Stdout
	if len(debugRecoverPlanOpts.outputFileName) > 0 {
		if _, err = os.Stat(debugRecoverPlanOpts.outputFileName); err == nil {
			return errors.Newf("file %q already exists", debugRecoverPlanOpts.outputFileName)
		}
		outFile, err := os.Create(debugRecoverPlanOpts.outputFileName)
		if err != nil {
			return errors.Wrapf(err, "failed to create file %q", debugRecoverPlanOpts.outputFileName)
		}
		defer outFile.Close()
		writer = outFile
		planFile = path.Base(debugRecoverPlanOpts.outputFileName)
	}

	jsonpb := protoutil.JSONPb{Indent: "  "}
	var out []byte
	if out, err = jsonpb.Marshal(&plan); err != nil {
		return errors.Wrap(err, "failed to marshal recovery plan")
	}
	if _, err = writer.Write(out); err != nil {
		return errors.Wrap(err, "failed to write recovery plan")
	}

	// No args means we collected connection info from cluster and need to
	// preserve flags for subsequent invocation.
	remoteArgs := getCLIClusterFlags(len(args) == 0, cmd, func(flag string) bool {
		_, filter := planSpecificFlags[flag]
		return filter
	})

	_, _ = fmt.Fprintf(stderr, `Plan created.
To stage recovery application in half-online mode invoke:

'cockroach debug recover apply-plan %s %s'

Alternatively distribute plan to below nodes and invoke 'debug recover apply-plan --store=<store-dir> %s' on:
`, remoteArgs, planFile, planFile)
	for _, node := range report.UpdatedNodes {
		_, _ = fmt.Fprintf(stderr, "- node n%d, store(s) %s\n", node.NodeID, strutil.JoinIDs("s", node.StoreIDs))
	}

	return nil
}

func readReplicaInfoData(fileNames []string) (loqrecoverypb.ClusterReplicaInfo, error) {
	var replicas loqrecoverypb.ClusterReplicaInfo
	for _, filename := range fileNames {
		data, err := os.ReadFile(filename)
		if err != nil {
			return loqrecoverypb.ClusterReplicaInfo{}, errors.Wrapf(err, "failed to read replica info file %q", filename)
		}

		var nodeReplicas loqrecoverypb.ClusterReplicaInfo
		jsonpb := protoutil.JSONPb{}
		if err = jsonpb.Unmarshal(data, &nodeReplicas); err != nil {
			return loqrecoverypb.ClusterReplicaInfo{}, errors.WithHint(errors.Wrapf(err,
				"failed to unmarshal replica info from file %q", filename),
				"Ensure that replica info file is generated with the same binary version and file is not corrupted.")
		}
		if err = replicas.Merge(nodeReplicas); err != nil {
			return loqrecoverypb.ClusterReplicaInfo{}, errors.Wrapf(err,
				"failed to merge replica info from file %q", filename)
		}
	}
	return replicas, nil
}

var debugRecoverExecuteCmd = &cobra.Command{
	Use:   "apply-plan plan-file",
	Short: "update replicas in given stores according to recovery plan",
	Long: `
Apply changes to replicas in the provided stores using a plan.

This command will read a plan and update replicas that belong to the
given stores. Stores must be provided using --store flags. 

See debug recover command help for more details on how to use this command.
`,
	Args: cobra.ExactArgs(1),
	RunE: runDebugExecuteRecoverPlan,
}

var debugRecoverExecuteOpts struct {
	Stores        base.StoreSpecList
	confirmAction confirmActionFlag
}

// runDebugExecuteRecoverPlan is using the following pattern when performing command
// First call prepare update on stores to ensure changes could be done. This operation
// will create update batches as needed and create a report about proposed changes.
// After that user is asked to confirm the action either explicitly or by consulting
// --confirm flag.
// If action is confirmed, then all changes are committed to the storage.
func runDebugExecuteRecoverPlan(cmd *cobra.Command, args []string) error {
	// We need cancellable context here to obtain grpc client connection.
	// getAdminClient will refuse otherwise.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	planFile := args[0]
	data, err := os.ReadFile(planFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read plan file %q", planFile)
	}

	var nodeUpdates loqrecoverypb.ReplicaUpdatePlan
	jsonpb := protoutil.JSONPb{Indent: "  "}
	if err = jsonpb.Unmarshal(data, &nodeUpdates); err != nil {
		return errors.Wrapf(err, "failed to unmarshal plan from file %q", planFile)
	}

	if len(debugRecoverExecuteOpts.Stores.Specs) == 0 {
		return stageRecoveryOntoCluster(ctx, cmd, planFile, nodeUpdates)
	}
	return applyRecoveryToLocalStore(ctx, nodeUpdates)
}

func stageRecoveryOntoCluster(
	ctx context.Context, cmd *cobra.Command, planFile string, plan loqrecoverypb.ReplicaUpdatePlan,
) error {
	c, finish, err := getAdminClient(ctx, serverCfg)
	if err != nil {
		return errors.Wrapf(err, "failed to get admin connection to cluster")
	}
	defer finish()

	// Check existing plan on nodes
	type planConflict struct {
		nodeID roachpb.NodeID
		planID string
	}
	vr, err := c.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{})
	if err != nil {
		return errors.Wrap(err, "failed to retrieve loss of quorum recovery status from cluster")
	}
	var conflicts []planConflict
	for _, ns := range vr.Statuses {
		if ns.PendingPlanID != nil && !ns.PendingPlanID.Equal(plan.PlanID) {
			conflicts = append(conflicts, planConflict{nodeID: ns.NodeID, planID: ns.PendingPlanID.String()})
		}
	}

	// Proposed report
	_, _ = fmt.Fprintf(stderr, "Proposed changes in plan %s:\n", plan.PlanID)
	for _, u := range plan.Updates {
		_, _ = fmt.Fprintf(stderr, "  range r%d:%s updating replica %s to %s on node n%d and discarding all others.\n",
			u.RangeID, roachpb.Key(u.StartKey), u.OldReplicaID, u.NextReplicaID, u.NodeID())
	}
	_, _ = fmt.Fprintf(stderr, "\nNodes %s will be marked as decommissioned.\n", strutil.JoinIDs("n", plan.DecommissionedNodeIDs))

	if len(conflicts) > 0 {
		_, _ = fmt.Fprintf(stderr, "\nConflicting staged plans will be replaced:\n")
		for _, cp := range conflicts {
			_, _ = fmt.Fprintf(stderr, "  plan %s is staged on node n%d.\n", cp.planID, cp.nodeID)
		}
	}
	_, _ = fmt.Fprintln(stderr)

	// Confirm actions
	switch debugRecoverExecuteOpts.confirmAction {
	case prompt:
		_, _ = fmt.Fprintf(stderr, "\nProceed with staging plan [y/N] ")
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		if err != nil {
			return errors.Wrap(err, "failed to read user input")
		}
		_, _ = fmt.Fprintf(stderr, "\n")
		if len(line) < 1 || (line[0] != 'y' && line[0] != 'Y') {
			_, _ = fmt.Fprint(stderr, "Aborted at user request\n")
			return nil
		}
	case allYes:
		// All actions enabled by default.
	default:
		return errors.New("Aborted by --confirm option")
	}

	maybeWrapStagingError := func(msg string, res *serverpb.RecoveryStagePlanResponse, err error) error {
		if err != nil {
			return errors.Wrapf(err, "%s", msg)
		}
		if len(res.Errors) > 0 {
			return errors.Newf("%s:\n%s", msg, strings.Join(res.Errors, "\n"))
		}
		return nil
	}

	if len(conflicts) > 0 {
		// We don't want to combine removing old plan and adding new one since it
		// could produce cluster with multiple plans at the same time which could
		// make situation worse.
		res, err := c.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{AllNodes: true, ForcePlan: true})
		if err := maybeWrapStagingError("failed removing existing loss of quorum replica recovery plan from cluster", res, err); err != nil {
			return err
		}
	}
	sr, err := c.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{Plan: &plan, AllNodes: true})
	if err := maybeWrapStagingError("failed to stage loss of quorum recovery plan on cluster",
		sr, err); err != nil {
		return err
	}

	remoteArgs := getCLIClusterFlags(true, cmd, func(flag string) bool {
		_, filter := planSpecificFlags[flag]
		return filter
	})

	nodeSet := make(map[roachpb.NodeID]interface{})
	for _, r := range plan.Updates {
		nodeSet[r.NodeID()] = struct{}{}
	}

	_, _ = fmt.Fprintf(stderr, `Plan staged. To complete recovery restart nodes %s.

To verify recovery status invoke:

'cockroach debug recover verify %s %s'
`, strutil.JoinIDs("n", sortedKeys(nodeSet)), remoteArgs, planFile)
	return nil
}

func sortedKeys[T ~int | ~int32 | ~int64](set map[T]any) []T {
	var sorted []T
	for k := range set {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	return sorted
}

func applyRecoveryToLocalStore(
	ctx context.Context, nodeUpdates loqrecoverypb.ReplicaUpdatePlan,
) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var localNodeID roachpb.NodeID
	batches := make(map[roachpb.StoreID]storage.Batch)
	for _, storeSpec := range debugRecoverExecuteOpts.Stores.Specs {
		store, err := OpenEngine(storeSpec.Path, stopper, storage.MustExist)
		if err != nil {
			return errors.Wrapf(err, "failed to open store at path %q. ensure that store path is "+
				"correct and that it is not used by another process", storeSpec.Path)
		}
		batch := store.NewBatch()
		defer store.Close()
		defer batch.Close()

		storeIdent, err := kvstorage.ReadStoreIdent(ctx, store)
		if err != nil {
			return err
		}
		if localNodeID != storeIdent.NodeID {
			if localNodeID != roachpb.NodeID(0) {
				return errors.Errorf("found stores from multiple node IDs n%d, n%d. "+
					"can only run in context of single node.", localNodeID, storeIdent.NodeID)
			}
			localNodeID = storeIdent.NodeID
		}
		batches[storeIdent.StoreID] = batch
	}

	updateTime := timeutil.Now()
	prepReport, err := loqrecovery.PrepareUpdateReplicas(
		ctx, nodeUpdates, uuid.DefaultGenerator, updateTime, localNodeID, batches)
	if err != nil {
		return err
	}

	for _, r := range prepReport.SkippedReplicas {
		_, _ = fmt.Fprintf(stderr, "Replica %s for range r%d is already updated.\n",
			r.Replica, r.RangeID())
	}

	if len(prepReport.UpdatedReplicas) == 0 {
		if len(prepReport.MissingStores) > 0 {
			return errors.Newf("stores %s expected on the node but no paths were provided",
				strutil.JoinIDs("s", prepReport.MissingStores))
		}
		_, _ = fmt.Fprintf(stderr, "No updates planned on this node.\n")
		return nil
	}

	for _, r := range prepReport.UpdatedReplicas {
		message := fmt.Sprintf(
			"Replica %s for range %d:%s will be updated to %s with peer replica(s) removed: %s",
			r.OldReplica, r.RangeID(), r.StartKey(), r.Replica, r.RemovedReplicas)
		if r.AbortedTransaction {
			message += fmt.Sprintf(", and range update transaction %s aborted.",
				r.AbortedTransactionID.Short())
		}
		_, _ = fmt.Fprintf(stderr, "%s\n", message)
	}

	switch debugRecoverExecuteOpts.confirmAction {
	case prompt:
		_, _ = fmt.Fprintf(stderr, "\nProceed with above changes [y/N] ")
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		if err != nil {
			return errors.Wrap(err, "failed to read user input")
		}
		_, _ = fmt.Fprintf(stderr, "\n")
		if len(line) < 1 || (line[0] != 'y' && line[0] != 'Y') {
			_, _ = fmt.Fprint(stderr, "Aborted at user request\n")
			return nil
		}
	case allYes:
		// All actions enabled by default.
	default:
		return errors.New("Aborted by --confirm option")
	}

	// Apply batches to the stores.
	applyReport, err := loqrecovery.CommitReplicaChanges(batches)
	_, _ = fmt.Fprintf(stderr, "Updated store(s): %s\n", strutil.JoinIDs("s", applyReport.UpdatedStores))
	return err
}

func formatNodeStores(locations []loqrecovery.NodeStores, indent string) string {
	hasMultiStore := false
	for _, v := range locations {
		hasMultiStore = hasMultiStore || len(v.StoreIDs) > 1
	}
	if !hasMultiStore {
		// we only have a single store per node, no need to list stores.
		nodeNames := make([]string, 0, len(locations))
		for _, node := range locations {
			nodeNames = append(nodeNames, fmt.Sprintf("n%d", node.NodeID))
		}
		return indent + strings.Join(nodeNames, ", ")
	}
	nodeDetails := make([]string, 0, len(locations))
	for _, node := range locations {
		nodeDetails = append(nodeDetails,
			indent+fmt.Sprintf("n%d: store(s): %s", node.NodeID, strutil.JoinIDs("s", node.StoreIDs)))
	}
	return strings.Join(nodeDetails, "\n")
}

// getCLIClusterFlags recreates command line flags from current command
// discarding any flags that filter returns true for.
func getCLIClusterFlags(fromCfg bool, cmd *cobra.Command, filter func(flag string) bool) string {
	if !fromCfg {
		return " --host <node-hostname>[:<port>] [--certs-dir <certificates-dir>|--insecure]"
	}
	var buf strings.Builder
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Changed && !filter(f.Name) {
			_, _ = fmt.Fprintf(&buf, " --%s=%v", f.Name, f.Value.String())
		}
	})
	return buf.String()
}

// setDebugRecoverContextDefaults resets values of command line flags to
// their default values to ensure tests don't interfere with each other.
func setDebugRecoverContextDefaults() {
	debugRecoverCollectInfoOpts.Stores.Specs = nil
	debugRecoverPlanOpts.outputFileName = ""
	debugRecoverPlanOpts.confirmAction = prompt
	debugRecoverPlanOpts.deadStoreIDs = nil
	debugRecoverPlanOpts.deadStoreIDs = nil
	debugRecoverExecuteOpts.Stores.Specs = nil
	debugRecoverExecuteOpts.confirmAction = prompt
}
