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
	"flag"
	"fmt"
	"os"

	commander "code.google.com/p/go-commander"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	gogoproto "github.com/gogo/protobuf/proto"
)

// A lsRangesCmd command lists the ranges in a cluster.
var lsRangesCmd = &commander.Command{
	UsageLine: "ls-ranges [options] [<start-key>]",
	Short:     "lists the ranges",
	Long: `
Lists the ranges in a cluster.

Caveat: Currently only lists up to 1000 rangges.
`,
	Run:  runLsRanges,
	Flag: *flag.CommandLine,
}

func runLsRanges(cmd *commander.Command, args []string) {
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

	kv := makeKVClient()
	req := proto.ScanArgs(startKey, engine.KeyMeta2Prefix.PrefixEnd(), 1000)
	resp := &proto.ScanResponse{}
	if err := kv.Call(proto.Scan, req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "scan failed: %s\n", err)
		os.Exit(1)
	}

	for _, r := range resp.Rows {
		desc := &proto.RangeDescriptor{}
		if err := gogoproto.Unmarshal(r.Value.Bytes, desc); err != nil {
			fmt.Fprintf(os.Stderr, "%s: unable to unmarshal range descriptor\n", r.Key)
			continue
		}
		fmt.Printf("%s-%s [%d]\n", desc.StartKey, desc.EndKey, desc.RaftID)
		for i, r := range desc.Replicas {
			fmt.Printf("\t%d: node-id=%d store-id=%d attrs=%v\n",
				i, r.NodeID, r.StoreID, r.Attrs.Attrs)
		}
	}
}

// A splitRangeCmd command splits a range.
var splitRangeCmd = &commander.Command{
	UsageLine: "split-range [options] <key> [<split-key>]",
	Short:     "splits a range",
	Long: `
Splits the range containing <key>. If <split-key> is not specified a
key to split the range approximately in half will be automatically
chosen.
`,
	Run:  runSplitRange,
	Flag: *flag.CommandLine,
}

func runSplitRange(cmd *commander.Command, args []string) {
	if len(args) == 0 || len(args) > 2 {
		cmd.Usage()
		return
	}

	key := proto.Key(args[0])
	var splitKey proto.Key
	if len(args) >= 2 {
		splitKey = proto.Key(args[1])
	}

	kv := makeKVClient()
	req := &proto.AdminSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key: key,
		},
		SplitKey: splitKey,
	}
	resp := &proto.AdminSplitResponse{}
	if err := kv.Call(proto.AdminSplit, req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "split failed: %s\n", err)
		os.Exit(1)
	}
}

// A mergeRangeCmd command merges a range.
var mergeRangeCmd = &commander.Command{
	UsageLine: "merge-range [options] <key>",
	Short:     "merges a range\n",
	Long: `
Merges the range containing <key> with the immediate successor range.
`,
	Run:  runMergeRange,
	Flag: *flag.CommandLine,
}

func runMergeRange(cmd *commander.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}

	kv := makeKVClient()
	req := &proto.AdminMergeRequest{
		RequestHeader: proto.RequestHeader{
			Key: proto.Key(args[0]),
		},
	}
	resp := &proto.AdminMergeResponse{}
	if err := kv.Call(proto.AdminMerge, req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "merge failed: %s\n", err)
		os.Exit(1)
	}
}
