// Copyright 2016 The Cockroach Authors.
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
// Author: Ben Darnell

package cli

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/kr/pretty"
	"github.com/spf13/cobra"
)

var debugKeysCmd = &cobra.Command{
	Use:   "keys [directory]",
	Short: "dump all the keys in a store",
	Long: `
Pretty-prints all keys in a store.
`,
	RunE: runDebugKeys,
}

func parseRangeID(arg string) (roachpb.RangeID, error) {
	rangeIDInt, err := strconv.ParseInt(arg, 10, 64)
	if err != nil {
		return 0, err
	}
	return roachpb.RangeID(rangeIDInt), nil
}

func openStore(cmd *cobra.Command, dir string, stopper *stop.Stopper) (engine.Engine, error) {
	initCacheSize()

	db := engine.NewRocksDB(roachpb.Attributes{}, dir,
		cliContext.CacheSize, cliContext.MemtableBudget, 0, stopper)
	if err := db.Open(); err != nil {
		return nil, err
	}
	return db, nil
}

func printKey(kv engine.MVCCKeyValue) (bool, error) {
	fmt.Printf("%q\n", kv.Key)

	return false, nil
}

func runDebugKeys(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 1 {
		return errors.New("one argument is required")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	if err := db.Iterate(engine.NilKey, engine.MVCCKeyMax, printKey); err != nil {
		return err
	}

	return nil
}

var debugRangeDescriptorsCmd = &cobra.Command{
	Use:   "range-descriptors [directory]",
	Short: "print all range descriptors in a store",
	Long: `
Prints all range descriptors in a store with a history of changes.
`,
	RunE: runDebugRangeDescriptors,
}

func printRangeDescriptor(kv engine.MVCCKeyValue) (bool, error) {
	startKey, suffix, _, err := keys.DecodeRangeKey(kv.Key.Key)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
		return false, nil
	}
	value := roachpb.Value{
		RawBytes: kv.Value,
	}
	var desc roachpb.RangeDescriptor
	if err := value.GetProto(&desc); err != nil {
		return false, err
	}
	fmt.Printf("Range descriptor %s at time %s\n\tCovers: [%s, %s)\n\tRaw:%s\n\n",
		startKey, kv.Key.Timestamp.GoTime(), desc.StartKey, desc.EndKey, &desc)
	return false, nil
}

func runDebugRangeDescriptors(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 1 {
		return errors.New("one argument is required")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	start := engine.MakeMVCCMetadataKey(keys.LocalRangePrefix)
	end := engine.MakeMVCCMetadataKey(keys.LocalRangeMax)

	if err := db.Iterate(start, end, printRangeDescriptor); err != nil {
		return err
	}
	return nil
}

var debugRaftLogCmd = &cobra.Command{
	Use:   "raft-log [directory] [range id]",
	Short: "print the raft log for a range",
	Long: `
Prints all log entries in a store for the given range.
`,
	RunE: runDebugRaftLog,
}

func printRaftLogEntry(kv engine.MVCCKeyValue) (bool, error) {
	var meta engine.MVCCMetadata
	if err := meta.Unmarshal(kv.Value); err != nil {
		return false, err
	}
	value := roachpb.Value{
		RawBytes: meta.RawBytes,
	}
	var ent raftpb.Entry
	if err := value.GetProto(&ent); err != nil {
		return false, err
	}
	if len(ent.Data) > 0 {
		_, cmdData := storage.DecodeRaftCommand(ent.Data)
		var cmd roachpb.RaftCommand
		if err := cmd.Unmarshal(cmdData); err != nil {
			return false, err
		}
		ent.Data = nil
		fmt.Printf("%s\n", &ent)
		fmt.Printf("%s\n", &cmd)
	} else {
		fmt.Printf("%s: EMPTY\n", &ent)
	}
	return false, nil
}

func runDebugRaftLog(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 2 {
		return errors.New("required arguments: dir range_id")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	rangeID, err := parseRangeID(args[1])
	if err != nil {
		return err
	}

	start := engine.MakeMVCCMetadataKey(keys.RaftLogPrefix(rangeID))
	end := engine.MakeMVCCMetadataKey(keys.RaftLogPrefix(rangeID).PrefixEnd())

	if err := db.Iterate(start, end, printRaftLogEntry); err != nil {
		return err
	}
	return nil
}

var debugGCCmd = &cobra.Command{
	Use:   "estimate-gc [directory] [range id]",
	Short: "find out what a GC run would do",
	Long: `
Sets up (but does not run) a GC collection cycle, giving insight into how much
work would be done (assuming all intent resolution and pushes succeed).

Without a RangeID specified on the command line, runs the analysis for all
ranges individually.

Uses a hard-coded GC policy with a 24 hour TTL for old versions.
`,
	RunE: runDebugGCCmd,
}

func runDebugGCCmd(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 1 {
		return errors.New("required arguments: dir")
	}

	var rangeID roachpb.RangeID
	if len(args) == 2 {
		var err error
		if rangeID, err = parseRangeID(args[1]); err != nil {
			return err
		}
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	start := keys.RangeDescriptorKey(roachpb.RKeyMin)
	end := keys.RangeDescriptorKey(roachpb.RKeyMax)

	var descs []roachpb.RangeDescriptor

	if _, err := engine.MVCCIterate(db, start, end, roachpb.MaxTimestamp,
		false /* !consistent */, nil, /* txn */
		false /* !reverse */, func(kv roachpb.KeyValue) (bool, error) {
			var desc roachpb.RangeDescriptor
			_, suffix, _, err := keys.DecodeRangeKey(kv.Key)
			if err != nil {
				return false, err
			}
			if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
				return false, nil
			}
			if err := kv.Value.GetProto(&desc); err != nil {
				return false, err
			}
			if desc.RangeID == rangeID || rangeID == 0 {
				descs = append(descs, desc)
			}
			return desc.RangeID == rangeID, nil
		}); err != nil {
		return err
	}

	if len(descs) == 0 {
		return fmt.Errorf("no range matching the criteria found")
	}

	for _, desc := range descs {
		snap := db.NewSnapshot()
		defer snap.Close()
		_, info, err := storage.RunGC(&desc, snap, roachpb.Timestamp{WallTime: timeutil.Now().UnixNano()},
			config.GCPolicy{TTLSeconds: 24 * 60 * 60 /* 1 day */}, func(_ roachpb.Timestamp, _ *roachpb.Transaction, _ roachpb.PushTxnType, wg *sync.WaitGroup) {
				wg.Done()
			}, func(_ []roachpb.Intent, _, _ bool) *roachpb.Error { return nil })
		if err != nil {
			return err
		}
		fmt.Printf("RangeID: %d [%s, %s):\n", desc.RangeID, desc.StartKey, desc.EndKey)
		_, _ = pretty.Println(info)
	}
	return nil
}

func init() {
	debugCmd.AddCommand(debugCmds...)
}

var debugCmds = []*cobra.Command{
	debugKeysCmd,
	debugRangeDescriptorsCmd,
	debugRaftLogCmd,
	debugGCCmd,
	kvCmd,
	rangeCmd,
}

var debugCmd = &cobra.Command{
	Use:   "debug [command]",
	Short: "debugging commands",
	Long: `Various commands for debugging.

These commands are useful for extracting data from the data files of a
process that has failed and cannot restart.
`,
	Run: func(cmd *cobra.Command, args []string) {
		mustUsage(cmd)
	},
}
