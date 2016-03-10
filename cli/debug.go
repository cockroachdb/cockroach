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

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft/raftpb"
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
	fmt.Printf("Range descriptor with start key %s at time %s\n%s\n", startKey, kv.Key.Timestamp.GoTime(), &desc)
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

	rangeIDInt, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return err
	}
	rangeID := roachpb.RangeID(rangeIDInt)

	start := engine.MakeMVCCMetadataKey(keys.RaftLogPrefix(rangeID))
	end := engine.MakeMVCCMetadataKey(keys.RaftLogPrefix(rangeID).PrefixEnd())

	if err := db.Iterate(start, end, printRaftLogEntry); err != nil {
		return err
	}
	return nil
}

var debugCmds = []*cobra.Command{
	debugKeysCmd,
	debugRangeDescriptorsCmd,
	debugRaftLogCmd,
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

func init() {
	debugCmd.AddCommand(debugCmds...)
}
