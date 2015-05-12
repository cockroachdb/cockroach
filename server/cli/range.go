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
// Author: Peter Mattis (peter.mattis@gmail.com)

package cli

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"

	"github.com/spf13/cobra"
)

// A lsRangesCmd command lists the ranges in a cluster.
var lsRangesCmd = &cobra.Command{
	Use:   "ls [options] [<start-key>]",
	Short: "lists the ranges",
	Long: `
Lists the ranges in a cluster.

Caveat: Currently only lists up to 1000 rangges.
`,
	Run: runLsRanges,
}

func runLsRanges(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Usage()
		return
	}
	var startKey proto.Key
	if len(args) >= 1 {
		startKey = engine.RangeMetaKey(proto.Key(args[0]))
	} else {
		startKey = engine.KeyMeta2Prefix
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	r, err := kvDB.Scan(startKey, engine.KeyMeta2Prefix.PrefixEnd(), 1000)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scan failed: %s\n", err)
		osExit(1)
		return
	}

	for _, row := range r.Rows {
		desc := &proto.RangeDescriptor{}
		if err := row.ValueProto(desc); err != nil {
			fmt.Fprintf(os.Stderr, "%s: unable to unmarshal range descriptor\n", row.Key)
			continue
		}
		fmt.Printf("%s-%s [%d]\n", desc.StartKey, desc.EndKey, desc.RaftID)
		for i, replica := range desc.Replicas {
			fmt.Printf("\t%d: node-id=%d store-id=%d attrs=%v\n",
				i, replica.NodeID, replica.StoreID, replica.Attrs.Attrs)
		}
	}
}

// A splitRangeCmd command splits a range.
var splitRangeCmd = &cobra.Command{
	Use:   "split [options] <key> [<split-key>]",
	Short: "splits a range",
	Long: `
Splits the range containing <key>. If <split-key> is not specified a
key to split the range approximately in half will be automatically
chosen.
`,
	Run: runSplitRange,
}

func runSplitRange(cmd *cobra.Command, args []string) {
	if len(args) == 0 || len(args) > 2 {
		cmd.Usage()
		return
	}

	key := proto.Key(args[0])
	var splitKey proto.Key
	if len(args) >= 2 {
		splitKey = proto.Key(args[1])
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	if _, err := kvDB.AdminSplit(key, splitKey); err != nil {
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
		cmd.Usage()
		return
	}

	kvDB := makeDBClient()
	if kvDB == nil {
		return
	}
	if _, err := kvDB.AdminMerge(args[0]); err != nil {
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
		cmd.Usage()
	},
}

func init() {
	rangeCmd.AddCommand(rangeCmds...)
}
