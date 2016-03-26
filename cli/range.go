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
// Author: Peter Mattis (peter@cockroachlabs.com)

package cli

import (
	"fmt"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"

	"github.com/spf13/cobra"
)

// A lsRangesCmd command lists the ranges in a cluster.
var lsRangesCmd = &cobra.Command{
	Use:   "ls [options] [<start-key>]",
	Short: "lists the ranges",
	Long: `
Lists the ranges in a cluster.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runLsRanges),
}

func runLsRanges(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		mustUsage(cmd)
		return
	}

	var startKey roachpb.Key
	{
		k := roachpb.KeyMin.Next()
		if len(args) > 0 {
			k = roachpb.Key(args[0])
		}
		rk, err := keys.Addr(k)
		if err != nil {
			panic(err)
		}
		startKey = keys.RangeMetaKey(rk)
	}
	endKey := keys.Meta2Prefix.PrefixEnd()

	kvDB, stopper := makeDBClient()
	defer stopper.Stop()
	rows, err := kvDB.Scan(startKey, endKey, maxResults)
	if err != nil {
		panicf("scan failed: %s\n", err)
	}

	for _, row := range rows {
		desc := &roachpb.RangeDescriptor{}
		if err := row.ValueProto(desc); err != nil {
			panicf("%s: unable to unmarshal range descriptor\n", row.Key)
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
	SilenceUsage: true,
	RunE:         panicGuard(runSplitRange),
}

func runSplitRange(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}

	key := roachpb.Key(args[0])
	kvDB, stopper := makeDBClient()
	defer stopper.Stop()
	if err := kvDB.AdminSplit(key); err != nil {
		panicf("split failed: %s\n", err)
	}
}

var rangeCmds = []*cobra.Command{
	lsRangesCmd,
	splitRangeCmd,
}

var rangeCmd = &cobra.Command{
	Use:   "range",
	Short: "list and split ranges",
	Run: func(cmd *cobra.Command, args []string) {
		mustUsage(cmd)
	},
}

func init() {
	rangeCmd.AddCommand(rangeCmds...)
}
