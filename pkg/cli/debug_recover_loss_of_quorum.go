// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

type outputFormatHelper struct {
	maxPrintedKeyLength uint
}

func (o outputFormatHelper) formatKey(key roachpb.Key) string {
	return o.truncateStr(key.String())
}

func (o outputFormatHelper) formatSpan(span roachpb.Span) string {
	return o.truncateStr(span.String())
}

func (o outputFormatHelper) truncateStr(str string) string {
	if o.maxPrintedKeyLength < 1 || uint(len(str)) <= o.maxPrintedKeyLength {
		return str
	}
	if o.maxPrintedKeyLength > 3 {
		return str[:o.maxPrintedKeyLength-3] + "..."
	}
	return str[:o.maxPrintedKeyLength]
}

var formatHelper = outputFormatHelper{
	maxPrintedKeyLength: 80,
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
recovery operation.

Loss of quorum recovery could be performed in two modes half-online and
offline. Half-online approach is a preferred one but offline approach is
preserved for compatibility with any tooling that may pre-exist. The main
advantage of half-online approach is that replica info could be collected
and recovery plan could be staged without stopping the cluster. Only nodes that
contain replicas of affected ranges needs to be subsequently restarted to
complete the recovery.

To perform recovery using half-online approach one should perform this sequence
of actions:

1. Run 'cockroach debug recover make-plan' in a half-online mode to collect
replica information from surviving nodes of a cluster and decide which
replicas should survive and up-replicate.

2. Run 'cockroach debug recover apply-plan' in half online mode to distribute
plan to surviving cluster nodes for application. At this point plan is staged
and can't be reverted.

3. Follow instructions from apply plan to perform a rolling restart of nodes
that need to update their storage. Restart should be done using appropriate
automation that used to run the cluster.

4. Optionally use 'cockroach debug recover verify' to check recovery progress
and resulting range health.

If it was possible to produce distribute and apply the plan, then cluster should
become operational again. It is not guaranteed that there's no data loss
and that all database consistency was not compromised.

If for whatever reasons half-online approach is not feasible or fails when
collecting info or distributing recovery plans, one could perform this sequence
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

4. Run 'cockroach debug recover apply-plan' on every node using plan
generated on the previous step. Each node will pick relevant portion of
the plan and update local replicas accordingly to restore quorum.

5. Start the cluster.

If it was possible to produce and apply the plan, then cluster should
become operational again. It is not guaranteed that there's no data loss
and that all database consistency was not compromised.

Example run #1 (half-online mode):

If we have a cluster of 5 nodes 1-5 where we lost nodes 3 and 4. Each node
has two stores and they are numbered as 1,2 on node 1; 3,4 on node 2 etc.
Recovery commands to recover unavailable ranges would be (most command output
is omitted for brevity):

[cockroach@admin ~]$ cockroach debug recover make-plan --host cockroach-1.cockroachlabs.com --certs-dir=root_certs -o recovery-plan.json

[cockroach@admin ~]$ cockroach debug recover apply-plan --host cockroach-1.cockroachlabs.com --certs-dir=root_certs recovery-plan.json

Proceed with staging plan [y/N] y

Plan staged. To complete recovery restart nodes n2, n3.

[cockroach@admin ~]$ # restart-nodes 2 3 as instructed by apply-plan.

[cockroach@admin ~]$ cockroach debug recover verify --host cockroach-1.cockroachlabs.com --certs-dir=root_certs recovery-plan.json

Loss of quorum recovery is complete.

Example run #2 (offline mode):

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
	debugRecoverVerifyCmd,
}

func init() {
	debugRecoverCmd.AddCommand(
		debugRecoverCollectInfoCmd,
		debugRecoverPlanCmd,
		debugRecoverExecuteCmd,
		debugRecoverVerifyCmd)
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
	Stores         base.StoreSpecList
	maxConcurrency int
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
		replicaInfo, stats, err = loqrecovery.CollectRemoteReplicaInfo(ctx, c,
			debugRecoverCollectInfoOpts.maxConcurrency, stderr /* logOutput */)
		if err != nil {
			return errors.WithHint(errors.Wrap(err,
				"failed to retrieve replica info from cluster"),
				"Check cluster health and retry the operation.")
		}
	} else {
		var stores []storage.Engine
		for _, storeSpec := range debugRecoverCollectInfoOpts.Stores.Specs {
			db, err := OpenEngine(storeSpec.Path, stopper, fs.ReadOnly, storage.MustExist)
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
	out, err := loqrecovery.MarshalReplicaInfo(replicaInfo)
	if err != nil {
		return err
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
	maxConcurrency int
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
		replicas, stats, err = loqrecovery.CollectRemoteReplicaInfo(ctx, c,
			debugRecoverPlanOpts.maxConcurrency, stderr /* logOutput */)
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
			r.RangeID, formatHelper.formatKey(r.StartKey.AsRawKey()), r.OldReplica, r.NewReplica,
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

	var out []byte
	if out, err = loqrecovery.MarshalPlan(plan); err != nil {
		return err
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

cockroach debug recover apply-plan %s %s

Alternatively distribute plan to below nodes and invoke 'debug recover apply-plan --store=<store-dir> %s' on:
`, remoteArgs, planFile, planFile)
	for _, node := range report.UpdatedNodes {
		_, _ = fmt.Fprintf(stderr, "- node n%d, store(s) %s\n", node.NodeID,
			strutil.JoinIDs("s", node.StoreIDs))
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

		nodeReplicas, err := loqrecovery.UnmarshalReplicaInfo(data)
		if err != nil {
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
	Stores                base.StoreSpecList
	confirmAction         confirmActionFlag
	ignoreInternalVersion bool
	maxConcurrency        int
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

	nodeUpdates, err := loqrecovery.UnmarshalPlan(data)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal plan from file %q", planFile)
	}

	if len(debugRecoverExecuteOpts.Stores.Specs) == 0 {
		return stageRecoveryOntoCluster(ctx, cmd, planFile, nodeUpdates,
			debugRecoverExecuteOpts.ignoreInternalVersion, debugRecoverExecuteOpts.maxConcurrency)
	}
	return applyRecoveryToLocalStore(ctx, nodeUpdates, debugRecoverExecuteOpts.ignoreInternalVersion)
}

func stageRecoveryOntoCluster(
	ctx context.Context,
	cmd *cobra.Command,
	planFile string,
	plan loqrecoverypb.ReplicaUpdatePlan,
	ignoreInternalVersion bool,
	maxConcurrency int,
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
	vr, err := c.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{
		MaxConcurrency: int32(maxConcurrency),
	})
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
			u.RangeID, formatHelper.formatKey(roachpb.Key(u.StartKey)), u.OldReplicaID, u.NextReplicaID, u.NodeID())
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
		res, err := c.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
			AllNodes:       true,
			ForcePlan:      true,
			MaxConcurrency: int32(maxConcurrency),
		})
		if err := maybeWrapStagingError("failed removing existing loss of quorum replica recovery plan from cluster", res, err); err != nil {
			return err
		}
	}
	sr, err := c.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:                      &plan,
		AllNodes:                  true,
		ForceLocalInternalVersion: ignoreInternalVersion,
		MaxConcurrency:            int32(maxConcurrency),
	})
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
	for _, ln := range plan.StaleLeaseholderNodeIDs {
		nodeSet[ln] = struct{}{}
	}

	_, _ = fmt.Fprintf(stderr, `Plan staged. To complete recovery restart nodes %s.

To verify recovery status invoke:

cockroach debug recover verify %s %s
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
	ctx context.Context, nodeUpdates loqrecoverypb.ReplicaUpdatePlan, ignoreInternalVersion bool,
) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var localNodeID roachpb.NodeID
	batches := make(map[roachpb.StoreID]storage.Batch)
	stores := make([]storage.Engine, len(debugRecoverExecuteOpts.Stores.Specs))
	for i, storeSpec := range debugRecoverExecuteOpts.Stores.Specs {
		store, err := OpenEngine(storeSpec.Path, stopper, fs.ReadWrite, storage.MustExist)
		if err != nil {
			return errors.Wrapf(err, "failed to open store at path %q. ensure that store path is "+
				"correct and that it is not used by another process", storeSpec.Path)
		}
		stores[i] = store
		batch := store.NewBatch()
		//nolint:deferloop TODO(#137605)
		defer store.Close()
		//nolint:deferloop TODO(#137605)
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

	if err := loqrecovery.CheckEnginesVersion(ctx, stores, nodeUpdates, ignoreInternalVersion); err != nil {
		return err
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

var debugRecoverVerifyCmd = &cobra.Command{
	Use:   "verify [plan-file]",
	Short: "verify loss of quorum recovery application status",
	Long: `
Check cluster loss of quorum recovery state.

Verify command will check if all nodes applied recovery plan and that all
necessary nodes are decommissioned.

If invoked without a plan file, command will print status of all nodes in the
cluster.

The address of a single healthy cluster node must be provided using the --host
flag. This designated node will retrieve and check status of all nodes in the
cluster.

See debug recover command help for more details on how to use this command.
`,
	Args: cobra.MaximumNArgs(1),
	RunE: runDebugVerify,
}

var debugRecoverVerifyOpts struct {
	maxConcurrency int
}

func runDebugVerify(cmd *cobra.Command, args []string) error {
	// We must have cancellable context here to obtain grpc client connection.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var updatePlan loqrecoverypb.ReplicaUpdatePlan
	if len(args) > 0 {
		planFile := args[0]
		data, err := os.ReadFile(planFile)
		if err != nil {
			return errors.Wrapf(err, "failed to read plan file %q", planFile)
		}
		updatePlan, err = loqrecovery.UnmarshalPlan(data)
		if err != nil {
			return errors.Wrapf(err, "failed to unmarshal plan from file %q", planFile)
		}
	}

	// Plan statuses.
	if len(updatePlan.Updates) > 0 {
		_, _ = fmt.Printf("Checking application of recovery plan %s\n", updatePlan.PlanID)
	}

	c, finish, err := getAdminClient(ctx, serverCfg)
	if err != nil {
		return errors.Wrapf(err, "failed to get admin connection to cluster")
	}
	defer finish()
	req := serverpb.RecoveryVerifyRequest{
		DecommissionedNodeIDs: updatePlan.DecommissionedNodeIDs,
		MaxReportedRanges:     20,
		MaxConcurrency:        int32(debugRecoverVerifyOpts.maxConcurrency),
	}
	// Maybe switch to non-nullable?
	if !updatePlan.PlanID.Equal(uuid.UUID{}) {
		req.PendingPlanID = &updatePlan.PlanID
	}
	res, err := c.RecoveryVerify(ctx, &req)
	if err != nil {
		return errors.Wrapf(err, "failed to retrieve replica info from cluster")
	}

	if len(res.UnavailableRanges.Ranges) > 0 {
		_, _ = fmt.Fprintf(stderr, "Unavailable ranges:\n")
		for _, d := range res.UnavailableRanges.Ranges {
			_, _ = fmt.Fprintf(stderr, " r%d : %s, key span %s\n",
				d.RangeID, d.Health.Name(), formatHelper.formatSpan(d.Span))
		}
	}
	if res.UnavailableRanges.Error != "" {
		_, _ = fmt.Fprintf(stderr, "Failed to complete range health check: %s\n",
			res.UnavailableRanges.Error)
	}

	diff := diffPlanWithNodeStatus(updatePlan, res.Statuses)
	if len(diff.report) > 0 {
		if len(updatePlan.Updates) > 0 {
			_, _ = fmt.Fprintf(stderr, "Recovery plan application progress:\n")
		} else {
			_, _ = fmt.Fprintf(stderr, "Recovery plans:\n")
		}
	}
	for _, line := range diff.report {
		_, _ = fmt.Fprintf(stderr, "%s\n", line)
	}

	// Node statuses.
	allDecommissioned := true
	var b strings.Builder
	for id, status := range res.DecommissionedNodeStatuses {
		if !status.Decommissioned() {
			b.WriteString(fmt.Sprintf(" n%d: %s\n", id, status))
			allDecommissioned = false
		}
	}
	if len(res.DecommissionedNodeStatuses) > 0 {
		if allDecommissioned {
			_, _ = fmt.Fprintf(stderr, "All dead nodes are decommissioned.\n")
		} else {
			_, _ = fmt.Fprintf(stderr, "Nodes not yet decommissioned:\n%s", b.String())
		}
	}

	if len(updatePlan.Updates) > 0 {
		if !allDecommissioned || diff.pending > 0 {
			return errors.New("loss of quorum recovery is not finished yet")
		}
		if diff.errors > 0 || !res.UnavailableRanges.Empty() {
			return errors.New("loss of quorum recovery did not fully succeed")
		}
		_, _ = fmt.Fprintf(stderr, "Loss of quorum recovery is complete.\n")
	} else {
		if diff.errors > 0 || !res.UnavailableRanges.Empty() {
			return errors.New("cluster has unhealthy ranges")
		}
	}
	return nil
}

type clusterDiff struct {
	report  []string
	pending int
	errors  int
}

func (d *clusterDiff) append(line string) {
	d.report = append(d.report, line)
}

func (d *clusterDiff) appendPending(line string) {
	d.report = append(d.report, line)
	d.pending++
}

func (d *clusterDiff) appendError(line string) {
	d.report = append(d.report, line)
	d.errors++
}

func diffPlanWithNodeStatus(
	updatePlan loqrecoverypb.ReplicaUpdatePlan, nodes []loqrecoverypb.NodeRecoveryStatus,
) clusterDiff {
	var result clusterDiff

	nodesWithPlan := make(map[roachpb.NodeID]bool)
	for _, r := range updatePlan.Updates {
		nodesWithPlan[r.NodeID()] = true
	}
	for _, id := range updatePlan.StaleLeaseholderNodeIDs {
		nodesWithPlan[id] = true
	}

	// Sort statuses by node id for ease of readability.
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	// Plan statuses.
	if len(nodesWithPlan) > 0 {
		// Invoked with plan, need to verify application of concrete plan to the
		// cluster.
		for _, status := range nodes {
			if nodesWithPlan[status.NodeID] {
				// Nodes that we expect plan to be pending or applied.
				switch {
				case status.AppliedPlanID != nil && status.AppliedPlanID.Equal(updatePlan.PlanID) && status.Error != "":
					result.appendError(fmt.Sprintf(" plan application failed on node n%d: %s", status.NodeID, status.Error))
				case status.AppliedPlanID != nil && status.AppliedPlanID.Equal(updatePlan.PlanID):
					result.append(fmt.Sprintf(" plan applied successfully on node n%d", status.NodeID))
				case status.PendingPlanID != nil && status.PendingPlanID.Equal(updatePlan.PlanID):
					result.appendPending(fmt.Sprintf(" plan application pending on node n%d", status.NodeID))
				case status.PendingPlanID != nil:
					result.appendError(fmt.Sprintf(" unexpected staged plan %s on node n%d", *status.PendingPlanID, status.NodeID))
				case status.PendingPlanID == nil:
					result.appendError(fmt.Sprintf(" failed to find staged plan on node n%d", status.NodeID))
				}
				delete(nodesWithPlan, status.NodeID)
			} else {
				switch {
				case status.PendingPlanID != nil && status.PendingPlanID.Equal(updatePlan.PlanID):
					result.appendError(fmt.Sprintf(" plan staged on n%d but no replicas is planned for update on the node", status.NodeID))
				case status.PendingPlanID != nil:
					result.appendError(fmt.Sprintf(" unexpected staged plan %s on node n%d", *status.PendingPlanID, status.NodeID))
				}
			}
		}
		// Check if any nodes that must have a plan staged or applied are missing
		// from received node statuses.
		var missing []roachpb.NodeID
		for k := range nodesWithPlan {
			missing = append(missing, k)
		}
		slices.Sort(missing)
		for _, id := range missing {
			result.appendError(fmt.Sprintf(" failed to find node n%d where plan must be staged", id))
		}
	} else {
		// Invoked without a plan, just dump collected information without making
		// any conclusions.
		for _, status := range nodes {
			if status.PendingPlanID != nil {
				result.append(fmt.Sprintf(" node n%d staged plan: %s", status.NodeID,
					*status.PendingPlanID))
			}
			switch {
			case status.Error != "" && status.AppliedPlanID != nil:
				result.append(fmt.Sprintf(" node n%d failed to apply plan %s: %s", status.NodeID,
					*status.AppliedPlanID, status.Error))
			case status.Error != "":
				result.append(fmt.Sprintf(" node n%d failed to apply plan: %s", status.NodeID,
					status.Error))
			case status.AppliedPlanID != nil:
				result.append(fmt.Sprintf(" node n%d applied plan: %s at %s", status.NodeID,
					*status.AppliedPlanID, status.ApplyTimestamp))
			}
		}
	}
	return result
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

// debugRecoverDefaultMaxConcurrency is the default concurrency that will be
// used when fanning out RPCs to nodes in the cluster while servicing a debug
// recover command.
var debugRecoverDefaultMaxConcurrency = 2 * runtime.GOMAXPROCS(0)

// setDebugRecoverContextDefaults resets values of command line flags to
// their default values to ensure tests don't interfere with each other.
func setDebugRecoverContextDefaults() {
	debugRecoverCollectInfoOpts.Stores.Specs = nil
	debugRecoverCollectInfoOpts.maxConcurrency = debugRecoverDefaultMaxConcurrency
	debugRecoverPlanOpts.outputFileName = ""
	debugRecoverPlanOpts.confirmAction = prompt
	debugRecoverPlanOpts.deadStoreIDs = nil
	debugRecoverPlanOpts.deadNodeIDs = nil
	debugRecoverPlanOpts.maxConcurrency = debugRecoverDefaultMaxConcurrency
	debugRecoverExecuteOpts.Stores.Specs = nil
	debugRecoverExecuteOpts.confirmAction = prompt
	debugRecoverExecuteOpts.maxConcurrency = debugRecoverDefaultMaxConcurrency
	debugRecoverVerifyOpts.maxConcurrency = debugRecoverDefaultMaxConcurrency
}
