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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
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
	if rangeIDInt < 1 {
		return 0, fmt.Errorf("illegal RangeID: %d", rangeIDInt)
	}
	return roachpb.RangeID(rangeIDInt), nil
}

func openStore(cmd *cobra.Command, dir string, stopper *stop.Stopper) (engine.Engine, error) {
	setDefaultCacheSize(&cliContext.Context)

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

func printKeyValue(kv engine.MVCCKeyValue) (bool, error) {
	if kv.Key.Timestamp != roachpb.ZeroTimestamp {
		fmt.Printf("%s %q: ", kv.Key.Timestamp, kv.Key.Key)
	} else {
		fmt.Printf("%q: ", kv.Key.Key)
	}
	for _, decoder := range []func(kv engine.MVCCKeyValue) (string, error){tryRaftLogEntry, tryRangeDescriptor, tryMeta, tryAbort, tryTxn} {
		out, err := decoder(kv)
		if err != nil {
			continue
		}
		fmt.Println(out)
		return false, nil
	}
	// No better idea, just print raw bytes and hope that folks use `less -S`.
	fmt.Printf("%q\n\n", kv.Value)
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

	d := cliContext.debug

	from := engine.NilKey
	to := engine.MVCCKeyMax
	if d.raw {
		if len(d.startKey) > 0 {
			from = engine.MakeMVCCMetadataKey(roachpb.Key(d.startKey))
		}
		if len(d.endKey) > 0 {
			to = engine.MakeMVCCMetadataKey(roachpb.Key(d.endKey))
		}
	} else {
		if len(d.startKey) > 0 {
			startKey, err := keys.UglyPrint(d.startKey)
			if err != nil {
				return err
			}
			from = engine.MakeMVCCMetadataKey(startKey)
		}
		if len(d.endKey) > 0 {
			endKey, err := keys.UglyPrint(d.endKey)
			if err != nil {
				return err
			}
			to = engine.MakeMVCCMetadataKey(endKey)
		}
	}

	printer := printKey
	if d.values {
		printer = printKeyValue
	}

	if err := db.Iterate(from, to, printer); err != nil {
		return err
	}

	return nil
}

var debugSplitKeyCmd = &cobra.Command{
	Use:   "split-key [directory] [rangeid]",
	Short: "Compute a split key for the given key range",
	Long: `
Runs MVCCFindSplitKey on the given key range and prints debug information
and the obtained split key.
`,
	RunE: runDebugSplitKey,
}

func runDebugSplitKey(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 2 {
		return errors.New("store and rangeID must be specified")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}
	rangeID, err := parseRangeID(args[1])
	if err != nil {
		return err
	}

	snap := db.NewSnapshot()
	defer snap.Close()

	var desc roachpb.RangeDescriptor
	if err := storage.IterateRangeDescriptors(snap, func(descInside roachpb.RangeDescriptor) (bool, error) {
		if descInside.RangeID == rangeID {
			desc = descInside
			return true, nil
		}
		return false, nil
	}); err != nil {
		return err
	}

	if desc.RangeID != rangeID {
		return fmt.Errorf("range %d not found", rangeID)
	}

	if splitKey, err := engine.MVCCFindSplitKey(context.Background(), snap, rangeID,
		desc.StartKey, desc.EndKey, func(msg string, args ...interface{}) {
			fmt.Printf(msg+"\n", args...)
		}); err != nil {
		fmt.Println("No SplitKey found:", err)
	} else {
		fmt.Println("Computed SplitKey:", splitKey)
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

func descStr(desc roachpb.RangeDescriptor) string {
	return fmt.Sprintf("[%s, %s)\n\tRaw:%s\n",
		desc.StartKey, desc.EndKey, &desc)
}

func tryMeta(kv engine.MVCCKeyValue) (string, error) {
	if !bytes.HasPrefix(kv.Key.Key, keys.Meta1Prefix) && !bytes.HasPrefix(kv.Key.Key, keys.Meta2Prefix) {
		return "", errors.New("not a meta key")
	}
	value := roachpb.Value{
		Timestamp: kv.Key.Timestamp,
		RawBytes:  kv.Value,
	}
	var desc roachpb.RangeDescriptor
	if err := value.GetProto(&desc); err != nil {
		return "", err
	}
	return descStr(desc), nil
}

func maybeUnmarshalInline(v []byte, dest proto.Message) error {
	var meta engine.MVCCMetadata
	if err := meta.Unmarshal(v); err != nil {
		return err
	}
	value := roachpb.Value{
		RawBytes: meta.RawBytes,
	}
	return value.GetProto(dest)
}

func tryTxn(kv engine.MVCCKeyValue) (string, error) {
	var txn roachpb.Transaction
	if err := maybeUnmarshalInline(kv.Value, &txn); err != nil {
		return "", err
	}
	return txn.String() + "\n", nil
}

func tryAbort(kv engine.MVCCKeyValue) (string, error) {
	if kv.Key.Timestamp != roachpb.ZeroTimestamp {
		return "", errors.New("not an abort cache key")
	}
	_, err := keys.DecodeAbortCacheKey(kv.Key.Key, nil)
	if err != nil {
		return "", err
	}
	var dest roachpb.AbortCacheEntry
	if err := maybeUnmarshalInline(kv.Value, &dest); err != nil {
		return "", err
	}
	return fmt.Sprintf("key=%q, pri=%d\n", dest.Key, dest.Priority), nil
}

func tryRangeDescriptor(kv engine.MVCCKeyValue) (string, error) {
	_, suffix, _, err := keys.DecodeRangeKey(kv.Key.Key)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
		return "", fmt.Errorf("wrong suffix: %s", suffix)
	}
	value := roachpb.Value{
		RawBytes: kv.Value,
	}
	var desc roachpb.RangeDescriptor
	if err := value.GetProto(&desc); err != nil {
		return "", err
	}
	return descStr(desc), nil
}

func printRangeDescriptor(kv engine.MVCCKeyValue) (bool, error) {
	if out, err := tryRangeDescriptor(kv); err == nil {
		fmt.Printf("%s %q: %s\n", kv.Key.Timestamp, kv.Key.Key, out)
	}
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

func tryRaftLogEntry(kv engine.MVCCKeyValue) (string, error) {
	var ent raftpb.Entry
	if err := maybeUnmarshalInline(kv.Value, &ent); err != nil {
		return "", err
	}
	if ent.Type == raftpb.EntryNormal {
		if len(ent.Data) > 0 {
			_, cmdData := storage.DecodeRaftCommand(ent.Data)
			var cmd roachpb.RaftCommand
			if err := cmd.Unmarshal(cmdData); err != nil {
				return "", err
			}
			ent.Data = nil
			return fmt.Sprintf("%s by %v\n%s\n%s\n", &ent, cmd.OriginReplica, &cmd.Cmd, &cmd), nil
		}
		return fmt.Sprintf("%s: EMPTY\n", &ent), nil
	} else if ent.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(ent.Data); err != nil {
			return "", err
		}
		var ctx storage.ConfChangeContext
		if err := ctx.Unmarshal(cc.Context); err != nil {
			return "", err
		}
		var cmd roachpb.RaftCommand
		if err := cmd.Unmarshal(ctx.Payload); err != nil {
			return "", err
		}
		ent.Data = nil
		return fmt.Sprintf("%s\n%s\n", &ent, &cmd), nil
	}
	return "", fmt.Errorf("Unknown log entry type: %s\n", &ent)
}

func printRaftLogEntry(kv engine.MVCCKeyValue) (bool, error) {
	if out, err := tryRaftLogEntry(kv); err != nil {
		fmt.Printf("%q: %v\n\n", kv.Key.Key, err)
	} else {
		fmt.Printf("%q: %s\n", kv.Key.Key, out)
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

	if _, err := engine.MVCCIterate(context.Background(), db, start, end, roachpb.MaxTimestamp,
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
		_, info, err := storage.RunGC(context.Background(), &desc, snap, roachpb.Timestamp{WallTime: timeutil.Now().UnixNano()},
			config.GCPolicy{TTLSeconds: 24 * 60 * 60 /* 1 day */}, func(_ roachpb.Timestamp, _ *roachpb.Transaction, _ roachpb.PushTxnType) {
			}, func(_ []roachpb.Intent, _, _ bool) error { return nil })
		if err != nil {
			return err
		}
		fmt.Printf("RangeID: %d [%s, %s):\n", desc.RangeID, desc.StartKey, desc.EndKey)
		_, _ = pretty.Println(info)
	}
	return nil
}

var debugEnvCmd = &cobra.Command{
	Use:   "env",
	Short: "output environment settings",
	Long: `
Output environment variables that influence configuration.
`,
	Run: func(cmd *cobra.Command, args []string) {
		env := envutil.GetEnvReport()
		fmt.Print(env)
	},
}

func init() {
	debugCmd.AddCommand(debugCmds...)
}

var debugCmds = []*cobra.Command{
	debugKeysCmd,
	debugRangeDescriptorsCmd,
	debugRaftLogCmd,
	debugGCCmd,
	debugSplitKeyCmd,
	kvCmd,
	rangeCmd,
	debugEnvCmd,
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
