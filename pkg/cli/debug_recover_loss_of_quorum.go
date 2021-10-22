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
	allYes confirmActionFlag = iota
	allNo
	prompt
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
	return fmt.Sprintf("unknown confirm action flag value %d", *l)
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
		debugRecoverCollectInfoCmd,
		debugRecoverPlanCmd,
		debugRecoverExecuteCmd)
}

var debugRecoverCollectInfoCmd = &cobra.Command{
	Use:   "collect-info [destination-file]",
	Short: "collect replica information for range recovery in event of quorum loss",
	Long: `
Collect replica information by reading descriptors from underlying stores.

Collected information is written to a destination file if file name is provided,
or to stdout.
If stdout is used, confirm action should be set to 'y' or 'n' to avoid interactive
prompt interfering with writing the results.

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

	descriptors, err := loqrecovery.CollectReplicaInfo(cmd.Context(), stores)
	if err != nil {
		return err
	}

	dest := ""
	if len(args) > 0 {
		dest = args[0]
	}
	return loqrecovery.MarshalAndWriteResults(descriptors, dest)
}

var debugRecoverPlanCmd = &cobra.Command{
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

var debugRecoverPlanOpts struct {
	outputFileName string
	deadStoreIDs   []int
	confirmAction  confirmActionFlag
}

func runDebugPlanReplicaRemoval(_ *cobra.Command, args []string) error {
	// Deserialize replica data collected from nodes.
	var descriptors []loqrecoverypb.LocalRangeInfo
	for _, filename := range args {
		data, err := ioutil.ReadFile(filename)
		if err != nil {
			return errors.Wrapf(err, "failed to read replica info file %s", filename)
		}

		var nodeDescriptors []loqrecoverypb.LocalRangeInfo
		jsonpb := protoutil.JSONPb{Indent: "  "}
		if err = jsonpb.Unmarshal(data, &nodeDescriptors); err != nil {
			return errors.Wrapf(err, "failed to unmarshal replica info from file %s", filename)
		}
		descriptors = append(descriptors, nodeDescriptors...)
	}

	console := getConfirmAction(debugRecoverPlanOpts.confirmAction, os.Stderr)
	var deadStoreIDs []roachpb.StoreID
	for _, id := range debugRecoverPlanOpts.deadStoreIDs {
		deadStoreIDs = append(deadStoreIDs, roachpb.StoreID(id))
	}

	// Run planner.
	var messages loqrecovery.DefaultLogger
	plan, err := loqrecovery.PlanReplicas(descriptors, deadStoreIDs, &messages)
	if err != nil {
		return err
	}

	console.printf("%s", messages.String())
	if messages.HasWarnings() {
		if proceed, err := console.confirm("proceed with plan creation"); !proceed || err != nil {
			if !proceed {
				console.printf("Aborting at user request.")
			}
			return err
		}
	}

	return loqrecovery.MarshalAndWriteResults(plan, debugRecoverPlanOpts.outputFileName)
}

var debugRecoverExecuteCmd = &cobra.Command{
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

var debugRecoverExecuteOpts struct {
	Stores        base.StoreSpecList
	confirmAction confirmActionFlag
}

func runDebugDeadReplicaRemove(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(cmd.Context())

	console := getConfirmAction(debugRecoverExecuteOpts.confirmAction, os.Stderr)

	planFile := args[0]
	data, err := ioutil.ReadFile(planFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read plan file %s", planFile)
	}

	var nodeUpdates []loqrecoverypb.ReplicaUpdate
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

	if proceed, err := console.confirm(fmt.Sprintf("%s\nProceed with above changes", messages.String())); !proceed || err != nil {
		if !proceed {
			console.printf("Aborting at user request.")
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
