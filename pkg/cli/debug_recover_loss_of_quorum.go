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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
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
	panic(fmt.Sprintf("unknown confirm action flag value %d", *l))
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

// dialog provides a crude facility for commands to interact with user or
// to force certain outcome based on command line flag.
type dialog interface {
	// confirm displays a message and asks for yes/no confirmation.
	confirm(message string) (bool, error)
	// printf prints message to either stderr or stdout.
	printf(format string, values ...interface{})
}

// interactiveDialog asks user to confirm actions.
type interactiveDialog struct {
	out    io.Writer
	reader *bufio.Reader
}

func (i interactiveDialog) confirm(message string) (bool, error) {
	fmt.Fprintf(i.out, "%s [y/N] ", message)
	line, err := i.reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	fmt.Fprintf(i.out, "\n")
	if len(line) > 1 && (line[0] == 'y' || line[0] == 'Y') {
		return true, nil
	}
	return false, nil
}

func (i interactiveDialog) printf(format string, values ...interface{}) {
	if i.out != nil {
		fmt.Fprintf(i.out, format, values...)
	}
}

// defaultedDialog will return a fixed answer which usually comes from
// the --confirm command line flag.
type defaultedDialog struct {
	out    io.Writer
	result bool
}

func (d defaultedDialog) confirm(message string) (bool, error) {
	return d.result, nil
}

func (d defaultedDialog) printf(format string, values ...interface{}) {
	if d.out != nil {
		fmt.Fprintf(d.out, format, values...)
	}
}

// debugRecoverCmd is the root of all recover quorum commands
var debugRecoverCmd = &cobra.Command{
	Use:   "recover [command]",
	Short: "commands to recover unavailable ranges in case of quorum loss",
	Long: `Set of commands to recover unavailable ranges.

If cluster permanently loses several nodes containing multiple replicas of
the range it is possible for that range to loose quorum. Those ranges can
not converge on their final state and can't proceed further. This is a 
potential data and consistency loss situation. In most cases cluster should 
be restored from the latest backup.

As a workaround, when consistency could be sacrificed temporarily to restore
availability quickly it is possible to instruct remaining replicas to act as
if they have most up to date view of data and continue operation. There's no
way to guarantee which replica contains most up-to-date information if at all
so heuristics based on raft state are used to pick one.

As a result of such recovery, some of the data could be lost, indexes could
become corrupted and database constraints could be violated. So manual
recovery steps to verify the data and ensure database consistency should be
taken ASAP. Those actions should be done at application level.

'debug recover' set of commands is used as a last resort to perform range
recovery operation. To perform recovery one should perform this sequence
of actions:

0. Stop the cluster

1. Run 'cockroach debug recover collect-info' on every node to collect
replication state from all surviving nodes. Outputs of these invocations
should be collected and made locally available for the next step.

2. Run 'cockroach debug recover make-plan' providing all files generated
on step 1. Planner will decide which replicas should survive and
up-replicate.

3. Run 'cockroach debug recover execute-plan' on every node using plan
generated on the previous step. Each node will pick relevant portion of
the plan and update local replicas accordingly to restore quorum.

4. Start the cluster.

If it was possible to produce and apply the plan, then cluster should
become operational again. It is not guaranteed that there's no data loss
and that all database consistency was not compromised.

Example run:

If we have cluster of 5 nodes 1-5 where we lost nodes 3 and 4. Each node
has two stores and they are numbered as 1,2 on node 1; 3,4 on node 2 etc.
Recovery commands to recover unavailable ranges would be:

Stop the cluster.

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

Now cluster could be started again.
`,
	RunE: UsageAndErr,
}

func init() {
	debugRecoverCmd.AddCommand(
		debugRecoverCollectInfoCmd,
		debugRecoverPlanCmd,
		debugRecoverExecuteCmd)
}

var debugRecoverCollectInfoCmd = &cobra.Command{
	Use:   "collect-info [destination-file]",
	Short: "collect replica information from the given stores",
	Long: `
Collect information about replicas by reading data from underlying stores. Store
locations must be provided using --store flags.

Collected information is written to a destination file if file name is provided,
or to stdout.

Multiple store locations could be provided to the command to collect all info from
node at once. It is also possible to call it per store, in that case all resulting
files should be fed to plan subcommand.

See debug recover help for more details on how to use this command.
`,
	Args: cobra.MaximumNArgs(1),
	RunE: runDebugDeadReplicaCollect,
}

var debugRecoverCollectInfoOpts struct {
	Stores base.StoreSpecList
}

func runDebugDeadReplicaCollect(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(cmd.Context())

	var stores []storage.Engine
	for _, store := range debugRecoverCollectInfoOpts.Stores.Specs {
		db, err := OpenExistingStore(store.Path, stopper, true /* readOnly */)
		if err != nil {
			return err
		}
		stores = append(stores, db)
	}

	replicaInfo, err := loqrecovery.CollectReplicaInfo(cmd.Context(), stores)
	if err != nil {
		return err
	}

	var writer io.Writer = os.Stdout
	if len(args) > 0 {
		filename := args[0]
		if _, err = os.Stat(filename); err == nil {
			return errors.Newf("file '%s' already exists", filename)
		}

		outFile, err := os.Create(filename)
		if err != nil {
			return errors.Wrapf(err, "failed to create file %s", filename)
		}
		defer outFile.Close()
		writer = outFile
	}
	if err = loqrecovery.MarshalAndWriteResults(replicaInfo, writer); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Collected info about %d replicas.", len(replicaInfo.Replicas))
	return nil
}

var debugRecoverPlanCmd = &cobra.Command{
	Use:   "make-plan [replica-files]",
	Short: "generate a plan to recover ranges that lost quorum",
	Long: `
Devise a plan to restore ranges that lost a quorum.

This command will read files with information about replicas collected from all
surviving nodes of a cluster and make a decision which replicas should be survivors
for the ranges where quorum was lost.
Decision is then written into a file or stdout.

This command only creates a plan and doesn't change any data.'

See debug recover help for more details on how to use this command.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: runDebugPlanReplicaRemoval,
}

var debugRecoverPlanOpts struct {
	outputFileName string
	deadStoreIDs   []int
	confirmAction  confirmActionFlag
}

func runDebugPlanReplicaRemoval(_ *cobra.Command, args []string) error {
	// Deserialize replica data collected from nodes.
	var replicas []loqrecoverypb.Replicas
	for _, filename := range args {
		data, err := ioutil.ReadFile(filename)
		if err != nil {
			return errors.Wrapf(err, "failed to read replica info file %s", filename)
		}

		var nodeReplicas loqrecoverypb.Replicas
		jsonpb := protoutil.JSONPb{Indent: "  "}
		if err = jsonpb.Unmarshal(data, &nodeReplicas); err != nil {
			return errors.Wrapf(err, "failed to unmarshal replica info from file %s", filename)
		}
		replicas = append(replicas, nodeReplicas)
	}

	console := getConfirmAction(debugRecoverPlanOpts.confirmAction, os.Stderr)
	var deadStoreIDs []roachpb.StoreID
	for _, id := range debugRecoverPlanOpts.deadStoreIDs {
		deadStoreIDs = append(deadStoreIDs, roachpb.StoreID(id))
	}

	// Run planner.
	var messages loqrecovery.DefaultLogger
	plan, err := loqrecovery.PlanReplicas(replicas, deadStoreIDs, &messages)
	if err != nil {
		return err
	}

	console.printf("%s", messages.String())

	if len(plan.Updates) == 0 && !messages.HasWarnings() {
		console.printf("No recoverable ranges found.\n")
		return nil
	}

	if messages.HasWarnings() {
		if proceed, err := console.confirm("Proceed with plan creation"); !proceed || err != nil {
			if !proceed {
				console.printf("Aborting at user request.\n")
			}
			return err
		}
	}

	var writer io.Writer = os.Stdout
	if len(debugRecoverPlanOpts.outputFileName) > 0 {
		if _, err = os.Stat(debugRecoverPlanOpts.outputFileName); err == nil {
			return errors.Newf("file '%s' already exists", debugRecoverPlanOpts.outputFileName)
		}
		outFile, err := os.Create(debugRecoverPlanOpts.outputFileName)
		if err != nil {
			return errors.Wrapf(err, "failed to create file %s", debugRecoverPlanOpts.outputFileName)
		}
		defer outFile.Close()
		writer = outFile
	}
	if err = loqrecovery.MarshalAndWriteResults(plan, writer); err != nil {
		return err
	}
	console.printf("Plan created\n")
	return nil
}

var debugRecoverExecuteCmd = &cobra.Command{
	Use:   "apply-plan plan-file",
	Short: "update replicas in given stores according to recovery plan",
	Long: `
Apply changes to replicas in the provided stores using a plan.

This command will read a plan and update replicas that belong to the
given stores. Stores must be provided using --store flags. 

See debug recover help for more details on how to use this command.
`,
	Args: cobra.ExactArgs(1),
	RunE: runDebugExecuteRecoverPlan,
}

var debugRecoverExecuteOpts struct {
	Stores        base.StoreSpecList
	confirmAction confirmActionFlag
}

func runDebugExecuteRecoverPlan(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(cmd.Context())

	console := getConfirmAction(debugRecoverExecuteOpts.confirmAction, os.Stderr)

	planFile := args[0]
	data, err := ioutil.ReadFile(planFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read plan file %s", planFile)
	}

	var nodeUpdates loqrecoverypb.Updates
	jsonpb := protoutil.JSONPb{Indent: "  "}
	if err = jsonpb.Unmarshal(data, &nodeUpdates); err != nil {
		return errors.Wrapf(err, "failed to unmarshal plan from file %s", planFile)
	}

	// Open all stores.
	var localNodeID roachpb.NodeID
	stores := make(map[roachpb.StoreID]storage.Engine)
	for _, storeSpec := range debugRecoverExecuteOpts.Stores.Specs {
		db, err := OpenExistingStore(storeSpec.Path, stopper, false /* readOnly */)
		if err != nil {
			return err
		}
		storeIdent, err := kvserver.ReadStoreIdent(cmd.Context(), db)
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

	// Create batches of changes for each store according to the plan.
	var messages loqrecovery.DefaultLogger
	updates, err := loqrecovery.PrepareUpdateReplicas(cmd.Context(), nodeUpdates, localNodeID, stores, &messages)
	if err != nil {
		return err
	}

	if len(updates) == 0 && !messages.HasWarnings() {
		console.printf("No updated planned on this node\n")
		return nil
	}

	if proceed, err := console.confirm(fmt.Sprintf("%s\nProceed with above changes", messages.String())); !proceed || err != nil {
		if !proceed {
			return errors.New("aborting at user request")
		}
		return err
	}

	// Apply batches to the stores.
	messages = loqrecovery.DefaultLogger{}
	err = loqrecovery.ApplyRecoveryChanges(updates, &messages)
	console.printf(messages.String())
	return err
}

func getConfirmAction(opt confirmActionFlag, out io.Writer) dialog {
	switch opt {
	case prompt:
		return interactiveDialog{reader: bufio.NewReader(os.Stdin), out: out}
	case allYes:
		return defaultedDialog{result: true, out: out}
	default:
		// Default to reject in case some input validation bug slips.
		return defaultedDialog{result: false, out: out}
	}
}
