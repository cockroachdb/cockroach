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

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	gogoproto "github.com/gogo/protobuf/proto"

	"code.google.com/p/go-commander"
)

// A CmdLsRanges command lists the ranges in a cluster.
var CmdLsRanges = &commander.Command{
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
	defer kv.Close()

	req := proto.ScanArgs(startKey, engine.KeyMeta2Prefix.PrefixEnd(), 1000)
	resp := &proto.ScanResponse{}
	if err := kv.Call(proto.Scan, req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "scan failed: %s\n", err)
		os.Exit(2)
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

// A CmdSplitRange command splits a range.
var CmdSplitRange = &commander.Command{
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
	defer kv.Close()

	req := &proto.AdminSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key: key,
		},
		SplitKey: splitKey,
	}
	resp := &proto.AdminSplitResponse{}
	if err := kv.Call(proto.AdminSplit, req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "split failed: %s\n", err)
		os.Exit(2)
	}
}

// A CmdMergeRange command merges a range.
var CmdMergeRange = &commander.Command{
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

	key := proto.Key(args[0])

	kv := makeKVClient()
	defer kv.Close()

	scanReq := proto.ScanArgs(engine.RangeMetaKey(key), engine.KeyMeta2Prefix.PrefixEnd(), 2)
	scanResp := &proto.ScanResponse{}
	if err := kv.Call(proto.Scan, scanReq, scanResp); err != nil {
		fmt.Fprintf(os.Stderr, "scan failed: %s\n", err)
		os.Exit(2)
	}
	switch len(scanResp.Rows) {
	case 0:
		fmt.Fprintf(os.Stderr, "unable to find range descriptor containing: %s\n", key)
		os.Exit(1)
	case 1:
		fmt.Fprintf(os.Stderr, "range containing %s is the last range\n", key)
		os.Exit(1)
	case 2:
		break
	default:
		fmt.Fprintf(os.Stderr, "unexpected number of scanned rows: %d\n", len(scanResp.Rows))
		os.Exit(1)
	}

	req := &proto.AdminMergeRequest{
		RequestHeader: proto.RequestHeader{
			Key: key,
		},
	}
	if err := gogoproto.Unmarshal(scanResp.Rows[1].Value.Bytes, &req.SubsumedRange); err != nil {
		fmt.Fprintf(os.Stderr, "%s: unable to unmarshal range descriptor\n", scanResp.Rows[1].Key)
		os.Exit(1)
	}

	resp := &proto.AdminMergeResponse{}
	if err := kv.Call(proto.AdminMerge, req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "merge failed: %s\n", err)
		os.Exit(2)
	}
}
