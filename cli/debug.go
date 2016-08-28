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
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/hlc"
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

func openStore(cmd *cobra.Command, dir string, stopper *stop.Stopper) (*engine.RocksDB, error) {
	cache := engine.NewRocksDBCache(512 << 20)
	defer cache.Release()
	maxOpenFiles, err := server.SetOpenFileLimitForOneStore()
	if err != nil {
		return nil, err
	}
	db := engine.NewRocksDB(
		roachpb.Attributes{},
		dir,
		cache,
		10<<20,
		0,
		maxOpenFiles,
		stopper,
	)
	if err := db.Open(); err != nil {
		return nil, err
	}
	return db, nil
}

func printKey(kv engine.MVCCKeyValue) (bool, error) {
	fmt.Printf("%s", kv.Key)
	if debugCtx.sizes {
		fmt.Printf(" %d %d", len(kv.Key.Key), len(kv.Value))
	}
	fmt.Printf("\n")
	return false, nil
}

func printKeyValue(kv engine.MVCCKeyValue) (bool, error) {
	if kv.Key.Timestamp != hlc.ZeroTimestamp {
		fmt.Printf("%s %s: ", kv.Key.Timestamp, kv.Key.Key)
	} else {
		fmt.Printf("%s: ", kv.Key.Key)
	}
	if debugCtx.sizes {
		fmt.Printf("%d %d: ", len(kv.Key.Key), len(kv.Value))
	}
	decoders := []func(kv engine.MVCCKeyValue) (string, error){
		tryRaftLogEntry,
		tryRangeDescriptor,
		tryMeta,
		tryTxn,
		tryRangeIDKey,
	}
	for _, decoder := range decoders {
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
		return errors.New("one argument required: dir")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	printer := printKey
	if debugCtx.values {
		printer = printKeyValue
	}

	if err := db.Iterate(debugCtx.startKey, debugCtx.endKey, printer); err != nil {
		return err
	}

	return nil
}

var debugRangeDataCmd = &cobra.Command{
	Use:   "range-data [directory] range-id",
	Short: "dump all the data in a range",
	Long: `
Pretty-prints all keys and values in a range. This includes all data covered
by the consistency checker.
`,
	RunE: runDebugRangeData,
}

func runDebugRangeData(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 2 {
		return errors.New("two arguments required: dir range_id")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	rangeID, err := parseRangeID(args[1])
	if err != nil {
		return err
	}

	desc, err := loadRangeDescriptor(db, rangeID)
	if err != nil {
		return err
	}

	iter := storage.NewReplicaDataIterator(&desc, db, true)
	for ; iter.Valid(); iter.Next() {
		if _, err := printKeyValue(engine.MVCCKeyValue{
			Key:   iter.Key(),
			Value: iter.Value(),
		}); err != nil {
			return err
		}
	}
	return iter.Error()
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
	var meta enginepb.MVCCMetadata
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

func tryRangeIDKey(kv engine.MVCCKeyValue) (string, error) {
	if kv.Key.Timestamp != hlc.ZeroTimestamp {
		return "", fmt.Errorf("range ID keys shouldn't have timestamps: %s", kv.Key)
	}
	_, _, suffix, _, err := keys.DecodeRangeIDKey(kv.Key.Key)
	if err != nil {
		return "", err
	}

	// All range ID keys are stored inline on the metadata.
	var meta enginepb.MVCCMetadata
	if err := meta.Unmarshal(kv.Value); err != nil {
		return "", err
	}
	value := roachpb.Value{RawBytes: meta.RawBytes}

	// Values encoded as protobufs set msg and continue outside the
	// switch. Other types are handled inside the switch and return.
	var msg proto.Message
	switch {
	case bytes.Equal(suffix, keys.LocalLeaseAppliedIndexSuffix):
		fallthrough
	case bytes.Equal(suffix, keys.LocalRaftAppliedIndexSuffix):
		i, err := value.GetInt()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(i, 10), nil

	case bytes.Equal(suffix, keys.LocalRangeFrozenStatusSuffix):
		b, err := value.GetBool()
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(b), nil

	case bytes.Equal(suffix, keys.LocalAbortCacheSuffix):
		msg = &roachpb.AbortCacheEntry{}

	case bytes.Equal(suffix, keys.LocalRangeLastGCSuffix):
		msg = &hlc.Timestamp{}

	case bytes.Equal(suffix, keys.LocalRaftTombstoneSuffix):
		msg = &roachpb.RaftTombstone{}

	case bytes.Equal(suffix, keys.LocalRaftTruncatedStateSuffix):
		msg = &roachpb.RaftTruncatedState{}

	case bytes.Equal(suffix, keys.LocalRangeLeaseSuffix):
		msg = &roachpb.Lease{}

	case bytes.Equal(suffix, keys.LocalRangeStatsSuffix):
		msg = &enginepb.MVCCStats{}

	case bytes.Equal(suffix, keys.LocalRaftHardStateSuffix):
		msg = &raftpb.HardState{}

	case bytes.Equal(suffix, keys.LocalRaftLastIndexSuffix):
		i, err := value.GetInt()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(i, 10), nil

	case bytes.Equal(suffix, keys.LocalRangeLastVerificationTimestampSuffix):
		msg = &hlc.Timestamp{}

	case bytes.Equal(suffix, keys.LocalRangeLastReplicaGCTimestampSuffix):
		msg = &hlc.Timestamp{}

	default:
		return "", fmt.Errorf("unknown raft id key %s", suffix)
	}

	if err := value.GetProto(msg); err != nil {
		return "", err
	}
	return msg.String(), nil
}

func checkRangeDescriptorKey(key engine.MVCCKey) error {
	_, suffix, _, err := keys.DecodeRangeKey(key.Key)
	if err != nil {
		return err
	}
	if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
		return fmt.Errorf("wrong suffix: %s", suffix)
	}
	return nil
}

func tryRangeDescriptor(kv engine.MVCCKeyValue) (string, error) {
	if err := checkRangeDescriptorKey(kv.Key); err != nil {
		return "", err
	}
	var desc roachpb.RangeDescriptor
	if err := getProtoValue(kv.Value, &desc); err != nil {
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

func getProtoValue(data []byte, msg proto.Message) error {
	value := roachpb.Value{
		RawBytes: data,
	}
	return value.GetProto(msg)
}

func loadRangeDescriptor(
	db engine.Engine, rangeID roachpb.RangeID,
) (roachpb.RangeDescriptor, error) {
	var desc roachpb.RangeDescriptor
	handleKV := func(kv engine.MVCCKeyValue) (bool, error) {
		if kv.Key.Timestamp == hlc.ZeroTimestamp {
			// We only want values, not MVCCMetadata.
			return false, nil
		}
		if err := checkRangeDescriptorKey(kv.Key); err != nil {
			// Range descriptor keys are interleaved with others, so if it
			// doesn't parse as a range descriptor just skip it.
			return false, nil
		}
		if err := getProtoValue(kv.Value, &desc); err != nil {
			return false, err
		}
		return desc.RangeID == rangeID, nil
	}

	// Range descriptors are stored by key, so we have to scan over the
	// range-local data to find the one for this RangeID.
	start := engine.MakeMVCCMetadataKey(keys.LocalRangePrefix)
	end := engine.MakeMVCCMetadataKey(keys.LocalRangeMax)

	if err := db.Iterate(start, end, handleKV); err != nil {
		return roachpb.RangeDescriptor{}, err
	}
	if desc.RangeID == rangeID {
		return desc, nil
	}
	return roachpb.RangeDescriptor{}, fmt.Errorf("range descriptor %d not found", rangeID)
}

func runDebugRangeDescriptors(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 1 {
		return errors.New("one argument required: dir")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	start := engine.MakeMVCCMetadataKey(keys.LocalRangePrefix)
	end := engine.MakeMVCCMetadataKey(keys.LocalRangeMax)

	return db.Iterate(start, end, printRangeDescriptor)
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
		return errors.New("two arguments required: dir range_id")
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

	return db.Iterate(start, end, printRaftLogEntry)
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
		return errors.New("one argument required: dir")
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

	if _, err := engine.MVCCIterate(context.Background(), db, start, end, hlc.MaxTimestamp,
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
		_, info, err := storage.RunGC(context.Background(), &desc, snap, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
			config.GCPolicy{TTLSeconds: 24 * 60 * 60 /* 1 day */}, func(_ hlc.Timestamp, _ *roachpb.Transaction, _ roachpb.PushTxnType) {
			}, func(_ []roachpb.Intent, _, _ bool) error { return nil })
		if err != nil {
			return err
		}
		fmt.Printf("RangeID: %d [%s, %s):\n", desc.RangeID, desc.StartKey, desc.EndKey)
		_, _ = pretty.Println(info)
	}
	return nil
}

var debugCheckStoreCmd = &cobra.Command{
	Use:   "check-store [directory]",
	Short: "consistency check for a single store",
	Long: `
Perform local consistency checks of a single store.

Capable of detecting the following errors:
* Raft logs that are inconsistent with their metadata
`,
	RunE: runDebugCheckStoreCmd,
}

type replicaCheckInfo struct {
	truncatedIndex uint64
	appliedIndex   uint64
	firstIndex     uint64
	lastIndex      uint64
}

func runDebugCheckStoreCmd(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 1 {
		return errors.New("one required argument: dir")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	// Iterate over the entire range-id-local space.
	start := roachpb.Key(keys.LocalRangeIDPrefix)
	end := start.PrefixEnd()

	replicaInfo := map[roachpb.RangeID]*replicaCheckInfo{}
	getReplicaInfo := func(rangeID roachpb.RangeID) *replicaCheckInfo {
		if info, ok := replicaInfo[rangeID]; ok {
			return info
		}
		replicaInfo[rangeID] = &replicaCheckInfo{}
		return replicaInfo[rangeID]
	}

	if _, err := engine.MVCCIterate(context.Background(), db, start, end, hlc.MaxTimestamp,
		false /* !consistent */, nil, /* txn */
		false /* !reverse */, func(kv roachpb.KeyValue) (bool, error) {
			rangeID, _, suffix, detail, err := keys.DecodeRangeIDKey(kv.Key)
			if err != nil {
				return false, err
			}

			switch {
			case bytes.Equal(suffix, keys.LocalRaftTruncatedStateSuffix):
				var trunc roachpb.RaftTruncatedState
				if err := kv.Value.GetProto(&trunc); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).truncatedIndex = trunc.Index
			case bytes.Equal(suffix, keys.LocalRaftAppliedIndexSuffix):
				idx, err := kv.Value.GetInt()
				if err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).appliedIndex = uint64(idx)
			case bytes.Equal(suffix, keys.LocalRaftLogSuffix):
				_, index, err := encoding.DecodeUint64Ascending(detail)
				if err != nil {
					return false, err
				}
				ri := getReplicaInfo(rangeID)
				if ri.firstIndex == 0 {
					ri.firstIndex = index
					ri.lastIndex = index
				} else {
					if index != ri.lastIndex+1 {
						fmt.Printf("range %s: log index anomaly: %v followed by %v\n",
							rangeID, ri.lastIndex, index)
					}
					ri.lastIndex = index
				}
			}

			return false, nil
		}); err != nil {
		return err
	}

	for rangeID, info := range replicaInfo {
		if info.truncatedIndex != info.firstIndex-1 {
			fmt.Printf("range %s: truncated index %v should equal first index %v - 1\n",
				rangeID, info.truncatedIndex, info.firstIndex)
		}
		if info.appliedIndex < info.firstIndex || info.appliedIndex > info.lastIndex {
			fmt.Printf("range %s: applied index %v should be between first index %v and last index %v\n",
				rangeID, info.appliedIndex, info.firstIndex, info.lastIndex)
		}
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

var debugCompactCmd = &cobra.Command{
	Use:   "compact [directory]",
	Short: "compact the sstables in a store",
	Long: `
Compact the sstables in a store.
`,
	RunE: runDebugCompact,
}

func runDebugCompact(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 1 {
		return errors.New("one argument is required")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	return db.Compact()
}

var debugSSTablesCmd = &cobra.Command{
	Use:   "sstables [directory]",
	Short: "list the sstables in a store",
	Long: `

List the sstables in a store. The output format is 1 or more lines of:

  level [ total size #files ]: file sizes

Only non-empty levels are shown. For levels greater than 0, the files span
non-overlapping ranges of the key space. Level-0 is special in that sstables
are created there by flushing the mem-table, thus every level-0 sstable must be
consulted to see if it contains a particular key. Within a level, the file
sizes are displayed in decreasing order and bucketed by the number of files of
that size. The following example shows 3-level output. In Level-3, there are 19
total files and 14 files that are 129 MiB in size.

  1 [   8M  3 ]: 7M 1M 63K
  2 [ 110M  7 ]: 31M 30M 13M[2] 10M 8M 5M
  3 [   2G 19 ]: 129M[14] 122M 93M 24M 18M 9M

The suffixes K, M, G and T are used for terseness to represent KiB, MiB, GiB
and TiB.
`,
	RunE: runDebugSSTables,
}

func runDebugSSTables(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop()

	if len(args) != 1 {
		return errors.New("one argument is required")
	}

	db, err := openStore(cmd, args[0], stopper)
	if err != nil {
		return err
	}

	fmt.Printf("%s", db.GetSSTables())
	return nil
}

func init() {
	debugCmd.AddCommand(debugCmds...)
}

var debugCmds = []*cobra.Command{
	debugKeysCmd,
	debugRangeDataCmd,
	debugRangeDescriptorsCmd,
	debugRaftLogCmd,
	debugGCCmd,
	debugCheckStoreCmd,
	debugCompactCmd,
	debugSSTablesCmd,
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
