// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cli

import (
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	localTimeFormat = "2006-01-02 15:04:05"
)

var lsNodesColumnHeaders = []string{
	"id",
}

var lsNodesCmd = &cobra.Command{
	Use:   "ls",
	Short: "lists the IDs of all nodes in the cluster",
	Long: `
Display the node IDs for all active (that is, running and not decommissioned) members of the cluster.
To retrieve the IDs for inactive members, see 'node status --decommission'.
	`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runLsNodes),
}

func runLsNodes(cmd *cobra.Command, args []string) error {
	const showDecommissioned = false
	nodeStatuses, _, err := runStatusNodeInner(showDecommissioned, nil)
	if err != nil {
		return err
	}

	var rows [][]string
	for _, nodeStatus := range nodeStatuses {
		rows = append(rows, []string{
			strconv.FormatInt(int64(nodeStatus.Desc.NodeID), 10),
		})
	}

	return printQueryOutput(os.Stdout, lsNodesColumnHeaders, newRowSliceIter(rows, "r"))
}

var baseNodeColumnHeaders = []string{
	"id",
	"address",
	"build",
	"updated_at",
	"started_at",
	"is_live",
}

var statusNodesColumnHeadersForRanges = []string{
	"replicas_leaders",
	"replicas_leaseholders",
	"ranges",
	"ranges_unavailable",
	"ranges_underreplicated",
}

var statusNodesColumnHeadersForStats = []string{
	"live_bytes",
	"key_bytes",
	"value_bytes",
	"intent_bytes",
	"system_bytes",
}

var statusNodesColumnHeadersForDecommission = []string{
	"gossiped_replicas",
	"is_decommissioning",
	"is_draining",
}

var statusNodeCmd = &cobra.Command{
	Use:   "status [<node id>]",
	Short: "shows the status of a node or all nodes",
	Long: `
	If a node ID is specified, this will show the status for the corresponding node. If no node ID
	is specified, this will display the status for all nodes in the cluster.
	`,
	Args: cobra.MaximumNArgs(1),
	RunE: MaybeDecorateGRPCError(runStatusNode),
}

func runStatusNode(cmd *cobra.Command, args []string) error {
	nodeStatuses, decommissionStatusResp, err := runStatusNodeInner(
		nodeCtx.statusShowDecommission || nodeCtx.statusShowAll, args,
	)
	if err != nil {
		return err
	}

	sliceIter := newRowSliceIter(nodeStatusesToRows(nodeStatuses, decommissionStatusResp), getStatusNodeAlignment())
	return printQueryOutput(os.Stdout, getStatusNodeHeaders(), sliceIter)
}

func runStatusNodeInner(
	showDecommissioned bool, args []string,
) ([]status.NodeStatus, *serverpb.DecommissionStatusResponse, error) {
	ctx, cancel := cmdTimeoutContext(context.Background())
	defer cancel()

	var nodeStatuses []status.NodeStatus

	conn, _, finish, err := getClientGRPCConn(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer finish()

	c := serverpb.NewStatusClient(conn)

	var decommissionStatusRequest *serverpb.DecommissionStatusRequest

	switch len(args) {
	case 0:
		// Show status for all nodes.
		nodes, err := c.Nodes(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return nil, nil, err
		}
		nodeStatuses = nodes.Nodes
		decommissionStatusRequest = &serverpb.DecommissionStatusRequest{
			NodeIDs: []roachpb.NodeID{},
		}

	case 1:
		nodeID := args[0]
		nodeStatus, err := c.Node(ctx, &serverpb.NodeRequest{NodeId: nodeID})
		if err != nil {
			return nil, nil, err
		}
		nodeIDs, err := parseNodeIDs(args)
		if err != nil {
			return nil, nil, err
		}
		decommissionStatusRequest = &serverpb.DecommissionStatusRequest{
			NodeIDs: nodeIDs,
		}
		if nodeStatus.Desc.NodeID == 0 {
			// I'm not sure why the status call doesn't return an error when the given NodeID doesn't
			// exist. This should be revisited.
			//
			// TODO(cdo): Look into why status call returns erroneous data when given node ID of 0.
			return nil, nil, fmt.Errorf("Error: node %s doesn't exist", nodeID)
		}
		nodeStatuses = []status.NodeStatus{*nodeStatus}

	default:
		return nil, nil, errors.Errorf("expected no arguments or a single node ID")
	}

	cAdmin, finish, err := getAdminClient(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer finish()

	decommissionStatusResp, err := cAdmin.DecommissionStatus(ctx, decommissionStatusRequest)
	if err != nil {
		return nil, nil, err
	}

	if !showDecommissioned {
		for _, status := range decommissionStatusResp.Status {
			if !status.Decommissioning || status.IsLive {
				// Show this entry.
				continue
			}
			for i := 0; i < len(nodeStatuses); i++ {
				if nodeStatuses[i].Desc.NodeID == status.NodeID {
					// Hide this entry (by swapping it out with the last one).
					last := len(nodeStatuses) - 1
					nodeStatuses[i] = nodeStatuses[last]
					nodeStatuses = nodeStatuses[:last]
				}
			}
		}
		// Sort the surviving entries (again) by NodeID.
		sort.Slice(nodeStatuses, func(i, j int) bool {
			return nodeStatuses[i].Desc.NodeID < nodeStatuses[j].Desc.NodeID
		})
	}
	return nodeStatuses, decommissionStatusResp, nil
}

func getStatusNodeHeaders() []string {
	headers := baseNodeColumnHeaders

	if nodeCtx.statusShowAll || nodeCtx.statusShowRanges {
		headers = append(headers, statusNodesColumnHeadersForRanges...)
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowStats {
		headers = append(headers, statusNodesColumnHeadersForStats...)
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowDecommission {
		headers = append(headers, statusNodesColumnHeadersForDecommission...)
	}
	return headers
}

func getStatusNodeAlignment() string {
	align := "rllll"
	if nodeCtx.statusShowAll || nodeCtx.statusShowRanges {
		align += "rrrrrr"
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowStats {
		align += "rrrrrr"
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowDecommission {
		align += decommissionResponseAlignment()
	}
	return align
}

// nodeStatusesToRows converts NodeStatuses to SQL-like result rows, so that we can pretty-print
// them. We also pass a decommission status object if status was called with the --decommission flag.
func nodeStatusesToRows(
	statuses []status.NodeStatus, decomStatus *serverpb.DecommissionStatusResponse,
) [][]string {
	// Create results that are like the results for SQL results, so that we can pretty-print them.
	var rows [][]string
	for i, nodeStatus := range statuses {
		hostPort := nodeStatus.Desc.Address.AddressField
		updatedAt := timeutil.Unix(0, nodeStatus.UpdatedAt)
		updatedAtStr := updatedAt.Format(localTimeFormat)
		startedAt := timeutil.Unix(0, nodeStatus.StartedAt)
		startedAtStr := startedAt.Format(localTimeFormat)
		build := nodeStatus.BuildInfo.Tag

		metricVals := map[string]float64{}
		for _, storeStatus := range nodeStatus.StoreStatuses {
			for key, val := range storeStatus.Metrics {
				metricVals[key] += val
			}
		}

		row := []string{strconv.FormatInt(int64(nodeStatus.Desc.NodeID), 10),
			hostPort,
			build,
			updatedAtStr,
			startedAtStr,
			strconv.FormatBool(decomStatus.Status[i].IsLive)}

		if nodeCtx.statusShowAll || nodeCtx.statusShowRanges {
			row = append(row,
				strconv.FormatInt(int64(metricVals["replicas.leaders"]), 10),
				strconv.FormatInt(int64(metricVals["replicas.leaseholders"]), 10),
				strconv.FormatInt(int64(metricVals["replicas"]), 10),
				strconv.FormatInt(int64(metricVals["ranges.unavailable"]), 10),
				strconv.FormatInt(int64(metricVals["ranges.underreplicated"]), 10),
			)
		}
		if nodeCtx.statusShowAll || nodeCtx.statusShowStats {
			row = append(row,
				strconv.FormatInt(int64(metricVals["livebytes"]), 10),
				strconv.FormatInt(int64(metricVals["keybytes"]), 10),
				strconv.FormatInt(int64(metricVals["valbytes"]), 10),
				strconv.FormatInt(int64(metricVals["intentbytes"]), 10),
				strconv.FormatInt(int64(metricVals["sysbytes"]), 10),
			)
		}
		if nodeCtx.statusShowAll || nodeCtx.statusShowDecommission {
			row = append(row, decommissionResponseValueToRows(decomStatus.Status)[i][2:]...)
		}
		rows = append(rows, row)
	}
	return rows
}

var decommissionNodesColumnHeaders = []string{
	"id",
	"is_live",
	"gossiped_replicas",
	"is_decommissioning",
	"is_draining",
}

var decommissionNodeCmd = &cobra.Command{
	Use:   "decommission <node id 1> [<node id 2> ...]",
	Short: "decommissions the node(s)",
	Long: `
Marks the nodes with the supplied IDs as decommissioning.
This will cause leases and replicas to be removed from these nodes.`,
	Args: cobra.MinimumNArgs(1),
	RunE: MaybeDecorateGRPCError(runDecommissionNode),
}

func parseNodeIDs(strNodeIDs []string) ([]roachpb.NodeID, error) {
	nodeIDs := make([]roachpb.NodeID, 0, len(strNodeIDs))
	for _, str := range strNodeIDs {
		i, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			return nil, errors.Errorf("unable to parse %s: %s", str, err)
		}
		nodeIDs = append(nodeIDs, roachpb.NodeID(i))
	}
	return nodeIDs, nil
}

func runDecommissionNode(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, finish, err := getAdminClient(ctx)
	if err != nil {
		return err
	}
	defer finish()

	return runDecommissionNodeImpl(ctx, c, nodeCtx.nodeDecommissionWait, args)
}

func runDecommissionNodeImpl(
	ctx context.Context, c serverpb.AdminClient, wait nodeDecommissionWaitType, args []string,
) error {
	nodeIDs, err := parseNodeIDs(args)
	if err != nil {
		return err
	}
	minReplicaCount := int64(math.MaxInt64)
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     20 * time.Second,
	}

	prevResponse := serverpb.DecommissionStatusResponse{}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		req := &serverpb.DecommissionRequest{
			NodeIDs:         nodeIDs,
			Decommissioning: true,
		}
		resp, err := c.Decommission(ctx, req)
		if err != nil {
			fmt.Fprintln(stderr)
			return errors.Wrap(err, "while trying to mark as decommissioning")
		}

		if !reflect.DeepEqual(&prevResponse, resp) {
			fmt.Fprintln(stderr)
			if err := printDecommissionStatus(*resp); err != nil {
				return err
			}
			prevResponse = *resp
		} else {
			fmt.Fprintf(stderr, ".")
		}
		var replicaCount int64
		allDecommissioning := true
		for _, status := range resp.Status {
			if wait != nodeDecommissionWaitLive || status.IsLive {
				replicaCount += status.ReplicaCount
			}
			allDecommissioning = allDecommissioning && status.Decommissioning
		}
		if replicaCount == 0 && allDecommissioning {
			if wait == nodeDecommissionWaitAll {
				fmt.Fprintln(os.Stdout, "\nAll target nodes report that they hold no more data. "+
					"Please verify cluster health before removing the nodes.")
			} else {
				fmt.Fprintln(os.Stdout, "\nDecommissioning finished. Please verify cluster health "+
					"before removing the nodes.")
			}
			return nil
		}
		if wait == nodeDecommissionWaitNone {
			return nil
		}
		if replicaCount < minReplicaCount {
			minReplicaCount = replicaCount
			r.Reset()
		}
	}
	return errors.New("maximum number of retries exceeded")
}

func decommissionResponseAlignment() string {
	return "rcrcc"
}

// decommissionResponseValueToRows converts DecommissionStatusResponse_Status to
// SQL-like result rows, so that we can pretty-print them.
func decommissionResponseValueToRows(
	statuses []serverpb.DecommissionStatusResponse_Status,
) [][]string {
	// Create results that are like the results for SQL results, so that we can pretty-print them.
	var rows [][]string
	for _, node := range statuses {
		rows = append(rows, []string{
			strconv.FormatInt(int64(node.NodeID), 10),
			strconv.FormatBool(node.IsLive),
			strconv.FormatInt(node.ReplicaCount, 10),
			strconv.FormatBool(node.Decommissioning),
			strconv.FormatBool(node.Draining),
		})
	}
	return rows
}

var recommissionNodeCmd = &cobra.Command{
	Use:   "recommission <node id 1> [<node id 2> ...]",
	Short: "recommissions the node(s)",
	Long: `
For the nodes with the supplied IDs, resets the decommissioning states.
The target nodes must be restarted, at which point the change will take
effect and the nodes will participate in the cluster as regular nodes.
	`,
	Args: cobra.MinimumNArgs(1),
	RunE: MaybeDecorateGRPCError(runRecommissionNode),
}

func printDecommissionStatus(resp serverpb.DecommissionStatusResponse) error {
	return printQueryOutput(os.Stdout, decommissionNodesColumnHeaders,
		newRowSliceIter(decommissionResponseValueToRows(resp.Status), decommissionResponseAlignment()))
}

func runRecommissionNode(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeIDs, err := parseNodeIDs(args)
	if err != nil {
		return err
	}

	c, finish, err := getAdminClient(ctx)
	if err != nil {
		return err
	}
	defer finish()

	req := &serverpb.DecommissionRequest{
		NodeIDs:         nodeIDs,
		Decommissioning: false,
	}
	resp, err := c.Decommission(ctx, req)
	if err != nil {
		return err
	}
	if err := printDecommissionStatus(*resp); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "The affected nodes must be restarted for the change to take effect.")
	return nil
}

// Sub-commands for node command.
var nodeCmds = []*cobra.Command{
	lsNodesCmd,
	statusNodeCmd,
	decommissionNodeCmd,
	recommissionNodeCmd,
}

var nodeCmd = &cobra.Command{
	Use:   "node [command]",
	Short: "list, inspect or remove nodes",
	Long:  "List, inspect or remove nodes.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Usage()
	},
}

func init() {
	nodeCmd.AddCommand(nodeCmds...)
}
