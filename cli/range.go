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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package cli

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"

	"github.com/spf13/cobra"
)

// A lsRangesCmd command lists the ranges in a cluster.
var lsRangesCmd = &cobra.Command{
	Use:   "ls [options] [<start-key>]",
	Short: "lists the ranges",
	Long: `
Lists the ranges in a cluster.
`,
	Run: runLsRanges,
}

func runLsRanges(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		mustUsage(cmd)
		return
	}
	var startKey proto.Key
	if len(args) >= 1 {
		startKey = keys.RangeMetaKey(proto.Key(args[0]))
	} else {
		startKey = keys.Meta2Prefix
	}

	kvDB, stopper := makeDBClient()
	defer stopper.Stop()
	rows, err := kvDB.Scan(startKey, keys.Meta2Prefix.PrefixEnd(), maxResults)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scan failed: %s\n", err)
		osExit(1)
		return
	}

	for _, row := range rows {
		desc := &proto.RangeDescriptor{}
		if err := row.ValueProto(desc); err != nil {
			fmt.Fprintf(os.Stderr, "%s: unable to unmarshal range descriptor\n", row.Key)
			continue
		}
		fmt.Printf("%s-%s [%d]\n", desc.StartKey, desc.EndKey, desc.RangeID)
		for i, replica := range desc.Replicas {
			fmt.Printf("\t%d: node-id=%d store-id=%d\n",
				i, replica.NodeID, replica.StoreID)
		}
	}
	fmt.Printf("%d result(s)\n", len(rows))
}

// A splitRangeCmd command splits a range.
var splitRangeCmd = &cobra.Command{
	Use:   "split [options] <key>",
	Short: "splits a range",
	Long: `
Splits the range containing <key> at <key>.
`,
	Run: runSplitRange,
}

func runSplitRange(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}

	key := proto.Key(args[0])
	kvDB, stopper := makeDBClient()
	defer stopper.Stop()
	if err := kvDB.AdminSplit(key); err != nil {
		fmt.Fprintf(os.Stderr, "split failed: %s\n", err)
		osExit(1)
	}
}

// A mergeRangeCmd command merges a range.
var mergeRangeCmd = &cobra.Command{
	Use:   "merge [options] <key>",
	Short: "merges a range",
	Long: `
Merges the range containing <key> with the immediate successor range.
`,
	Run: runMergeRange,
}

func runMergeRange(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}

	kvDB, stopper := makeDBClient()
	defer stopper.Stop()
	if err := kvDB.AdminMerge(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "merge failed: %s\n", err)
		osExit(1)
	}
}

var rangeCmds = []*cobra.Command{
	lsRangesCmd,
	splitRangeCmd,
	mergeRangeCmd,
}

var rangeCmd = &cobra.Command{
	Use:   "range",
	Short: "list, split and merge ranges",
	Run: func(cmd *cobra.Command, args []string) {
		mustUsage(cmd)
	},
}

func init() {
	rangeCmd.AddCommand(rangeCmds...)
}
