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
//
// Author: Cuong Do (cdo@cockroachlabs.com)

package cli

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/util"
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
	Displays IDs for all nodes in cluster, which can be used with the status and stores
	commands.
	`,
	SilenceUsage: true,
	RunE:         runLsNodes,
}

func runLsNodes(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		mustUsage(cmd)
	}

	// Extract Node IDs from NodeStatuses.
	nodeStatuses := map[string][]status.NodeStatus{}
	if err := getJSON(cliContext.HTTPAddr, server.PathForNodeStatus(""), &nodeStatuses); err != nil {
		return err
	}

	var rows [][]string
	for _, nodeStatus := range nodeStatuses["d"] {
		rows = append(rows, []string{
			strconv.FormatInt(int64(nodeStatus.Desc.NodeID), 10),
		})
	}

	printQueryOutput(os.Stdout, lsNodesColumnHeaders, rows, "")
	return nil
}

var nodesColumnHeaders = []string{
	"id",
	"address",
	"build",
	"updated_at",
	"started_at",
	"live_bytes",
	"key_bytes",
	"value_bytes",
	"intent_bytes",
	"system_bytes",
	"leader_ranges",
	"repl_ranges", // Using abbreviations to avoid excessively wide output.
	"avail_ranges",
}

var statusNodeCmd = &cobra.Command{
	Use:   "status <optional node ID>",
	Short: "shows the status of a node or all nodes",
	Long: `
	If a node ID is specified, this will show the status for the corresponding node. If no node ID
	is specified, this will display the status for all nodes in the cluster.
	`,
	SilenceUsage: true,
	RunE:         runStatusNode,
}

func runStatusNode(cmd *cobra.Command, args []string) error {
	var nodeStatuses []status.NodeStatus

	switch len(args) {
	case 0:
		// Show status for all nodes.
		jsonResponse := map[string][]status.NodeStatus{}
		if err := getJSON(cliContext.HTTPAddr, server.PathForNodeStatus(""), &jsonResponse); err != nil {
			return err
		}
		nodeStatuses = jsonResponse["d"]

	case 1:
		nodeStatus := status.NodeStatus{}
		nodeID := args[0]
		if err := getJSON(cliContext.HTTPAddr, server.PathForNodeStatus(nodeID), &nodeStatus); err != nil {
			return err
		}
		if nodeStatus.Desc.NodeID == 0 {
			// I'm not sure why the status call doesn't return an error when the given NodeID doesn't
			// exist. This should be revisited.
			//
			// TODO(cdo): Look into why status call returns erroneous data when given node ID of 0.
			return fmt.Errorf("Error: node %s doesn't exist", nodeID)
		}
		nodeStatuses = []status.NodeStatus{nodeStatus}

	default:
		mustUsage(cmd)
		return util.Errorf("expected no arguments or a single node ID")
	}

	printQueryOutput(os.Stdout, nodesColumnHeaders, nodeStatusesToRows(nodeStatuses), "")
	return nil
}

// nodeStatusesToRows converts NodeStatuses to SQL-like result rows, so that we can pretty-print
// them.
func nodeStatusesToRows(statuses []status.NodeStatus) [][]string {
	// Create results that are like the results for SQL results, so that we can pretty-print them.
	var rows [][]string
	for _, nodeStatus := range statuses {
		hostPort := nodeStatus.Desc.Address.AddressField
		updatedAt := time.Unix(0, nodeStatus.UpdatedAt)
		updatedAtStr := updatedAt.Format(localTimeFormat)
		startedAt := time.Unix(0, nodeStatus.StartedAt)
		startedAtStr := startedAt.Format(localTimeFormat)
		build := nodeStatus.BuildInfo.Tag

		metricVals := map[string]float64{}
		for _, storeStatus := range nodeStatus.StoreStatuses {
			for key, val := range storeStatus.Metrics {
				metricVals[key] += val
			}
		}

		rows = append(rows, []string{
			strconv.FormatInt(int64(nodeStatus.Desc.NodeID), 10),
			hostPort,
			build,
			updatedAtStr,
			startedAtStr,
			strconv.FormatInt(int64(metricVals["livebytes"]), 10),
			strconv.FormatInt(int64(metricVals["keybytes"]), 10),
			strconv.FormatInt(int64(metricVals["valbytes"]), 10),
			strconv.FormatInt(int64(metricVals["intentbytes"]), 10),
			strconv.FormatInt(int64(metricVals["sysbytes"]), 10),
			strconv.FormatInt(int64(metricVals["ranges.leader"]), 10),
			strconv.FormatInt(int64(metricVals["ranges.replicated"]), 10),
			strconv.FormatInt(int64(metricVals["ranges.available"]), 10),
		})
	}
	return rows
}

// Sub-commands for node command.
var nodeCmds = []*cobra.Command{
	lsNodesCmd,
	statusNodeCmd,
}

var nodeCmd = &cobra.Command{
	Use:   "node [command]",
	Short: "list nodes and show their status\n",
	Long:  "List nodes and show their status.",
	Run: func(cmd *cobra.Command, args []string) {
		mustUsage(cmd)
	},
}

func init() {
	nodeCmd.AddCommand(nodeCmds...)
}
