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

package cli

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/synctest"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var debugKeysCmd = &cobra.Command{
	Use:   "keys <directory>",
	Short: "dump all the keys in a store",
	Long: `
Pretty-prints all keys in a store.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugKeys),
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

func openExistingStore(dir string, stopper *stop.Stopper, readOnly bool) (*engine.RocksDB, error) {
	cache := engine.NewRocksDBCache(server.DefaultCacheSize)
	defer cache.Release()
	maxOpenFiles, err := server.SetOpenFileLimitForOneStore()
	if err != nil {
		return nil, err
	}
	db, err := engine.NewRocksDB(
		engine.RocksDBConfig{
			Settings:     serverCfg.Settings,
			Dir:          dir,
			MaxOpenFiles: maxOpenFiles,
			MustExist:    true,
			ReadOnly:     readOnly,
		},
		cache,
	)
	if err != nil {
		return nil, err
	}
	stopper.AddCloser(db)
	return db, nil
}

func printKey(kv engine.MVCCKeyValue) (bool, error) {
	fmt.Printf("%s %s: ", kv.Key.Timestamp, kv.Key.Key)
	if debugCtx.sizes {
		fmt.Printf(" %d %d", len(kv.Key.Key), len(kv.Value))
	}
	fmt.Printf("\n")
	return false, nil
}

func printKeyValue(kv engine.MVCCKeyValue) (bool, error) {
	fmt.Printf("%s %s: ", kv.Key.Timestamp, kv.Key.Key)

	if debugCtx.sizes {
		fmt.Printf("%d %d: ", len(kv.Key.Key), len(kv.Value))
	}
	decoders := []func(kv engine.MVCCKeyValue) (string, error){
		tryRaftLogEntry,
		tryRangeDescriptor,
		tryMeta,
		tryTxn,
		tryRangeIDKey,
		tryIntent,
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
	defer stopper.Stop(context.Background())

	db, err := openExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	printer := printKey
	if debugCtx.values {
		printer = printKeyValue
	}

	return db.Iterate(debugCtx.startKey, debugCtx.endKey, printer)
}

var debugRangeDataCmd = &cobra.Command{
	Use:   "range-data <directory> <range id>",
	Short: "dump all the data in a range",
	Long: `
Pretty-prints all keys and values in a range. By default, includes unreplicated
state like the raft HardState. With --replicated, only includes data covered by
 the consistency checker.
`,
	Args: cobra.ExactArgs(2),
	RunE: MaybeDecorateGRPCError(runDebugRangeData),
}

func runDebugRangeData(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := openExistingStore(args[0], stopper, true /* readOnly */)
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

	iter := rditer.NewReplicaDataIterator(&desc, db, debugCtx.replicated)
	defer iter.Close()
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		if _, err := printKeyValue(engine.MVCCKeyValue{
			Key:   iter.Key(),
			Value: iter.Value(),
		}); err != nil {
			return err
		}
	}
	return nil
}

var debugRangeDescriptorsCmd = &cobra.Command{
	Use:   "range-descriptors <directory>",
	Short: "print all range descriptors in a store",
	Long: `
Prints all range descriptors in a store with a history of changes.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugRangeDescriptors),
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

func maybeUnmarshalInline(v []byte, dest protoutil.Message) error {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(v, &meta); err != nil {
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
	if kv.Key.Timestamp != (hlc.Timestamp{}) {
		return "", fmt.Errorf("range ID keys shouldn't have timestamps: %s", kv.Key)
	}
	_, _, suffix, _, err := keys.DecodeRangeIDKey(kv.Key.Key)
	if err != nil {
		return "", err
	}

	// All range ID keys are stored inline on the metadata.
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
		return "", err
	}
	value := roachpb.Value{RawBytes: meta.RawBytes}

	// Values encoded as protobufs set msg and continue outside the
	// switch. Other types are handled inside the switch and return.
	var msg protoutil.Message
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

	case bytes.Equal(suffix, keys.LocalAbortSpanSuffix):
		msg = &roachpb.AbortSpanEntry{}

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

	case bytes.Equal(suffix, keys.LocalRangeLastVerificationTimestampSuffixDeprecated):
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

func tryIntent(kv engine.MVCCKeyValue) (string, error) {
	if len(kv.Value) == 0 {
		return "", errors.New("empty")
	}
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
		return "", err
	}
	s := fmt.Sprintf("%+v", meta)
	if meta.Txn != nil {
		s = meta.Txn.Timestamp.String() + " " + s
	}
	return s, nil
}

func getProtoValue(data []byte, msg protoutil.Message) error {
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
		if kv.Key.Timestamp == (hlc.Timestamp{}) {
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
	defer stopper.Stop(context.Background())

	db, err := openExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	start := engine.MakeMVCCMetadataKey(keys.LocalRangePrefix)
	end := engine.MakeMVCCMetadataKey(keys.LocalRangeMax)

	return db.Iterate(start, end, printRangeDescriptor)
}

var debugDecodeKeyCmd = &cobra.Command{
	Use:   "decode-key",
	Short: "decode <key>",
	Long: `
Decode a hexadecimal-encoded key and pretty-print it. For example:

	$ decode-key BB89F902ADB43000151C2D1ED07DE6C009
	/Table/51/1/44938288/1521140384.514565824,0
`,
	Args: cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		for _, arg := range args {
			b, err := hex.DecodeString(arg)
			if err != nil {
				return err
			}
			k, err := engine.DecodeKey(b)
			if err != nil {
				return err
			}
			fmt.Println(k)
		}
		return nil
	},
}

var debugRaftLogCmd = &cobra.Command{
	Use:   "raft-log <directory> <range id>",
	Short: "print the raft log for a range",
	Long: `
Prints all log entries in a store for the given range.
`,
	Args: cobra.ExactArgs(2),
	RunE: MaybeDecorateGRPCError(runDebugRaftLog),
}

func tryRaftLogEntry(kv engine.MVCCKeyValue) (string, error) {
	var ent raftpb.Entry
	if err := maybeUnmarshalInline(kv.Value, &ent); err != nil {
		return "", err
	}
	if ent.Type == raftpb.EntryNormal {
		if len(ent.Data) > 0 {
			_, cmdData := storage.DecodeRaftCommand(ent.Data)
			var cmd storagebase.RaftCommand
			if err := protoutil.Unmarshal(cmdData, &cmd); err != nil {
				return "", err
			}
			ent.Data = nil
			var leaseStr string
			if l := cmd.DeprecatedProposerLease; l != nil {
				// Use the full lease, if available.
				leaseStr = l.String()
			} else {
				leaseStr = fmt.Sprintf("lease #%d", cmd.ProposerLeaseSequence)
			}
			return fmt.Sprintf("%s by %s\n%s\n", &ent, leaseStr, &cmd), nil
		}
		return fmt.Sprintf("%s: EMPTY\n", &ent), nil
	} else if ent.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := protoutil.Unmarshal(ent.Data, &cc); err != nil {
			return "", err
		}
		var ctx storage.ConfChangeContext
		if err := protoutil.Unmarshal(cc.Context, &ctx); err != nil {
			return "", err
		}
		var cmd storagebase.ReplicatedEvalResult
		if err := protoutil.Unmarshal(ctx.Payload, &cmd); err != nil {
			return "", err
		}
		ent.Data = nil
		return fmt.Sprintf("%s\n%s\n", &ent, &cmd), nil
	}
	return "", fmt.Errorf("unknown log entry type: %s", &ent)
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
	defer stopper.Stop(context.Background())

	db, err := openExistingStore(args[0], stopper, true /* readOnly */)
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
	Use:   "estimate-gc <directory> [range id]",
	Short: "find out what a GC run would do",
	Long: `
Sets up (but does not run) a GC collection cycle, giving insight into how much
work would be done (assuming all intent resolution and pushes succeed).

Without a RangeID specified on the command line, runs the analysis for all
ranges individually.

Uses a hard-coded GC policy with a 24 hour TTL for old versions.
`,
	Args: cobra.RangeArgs(1, 2),
	RunE: MaybeDecorateGRPCError(runDebugGCCmd),
}

func runDebugGCCmd(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	var rangeID roachpb.RangeID
	if len(args) == 2 {
		var err error
		if rangeID, err = parseRangeID(args[1]); err != nil {
			return err
		}
	}

	db, err := openExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	start := keys.RangeDescriptorKey(roachpb.RKeyMin)
	end := keys.RangeDescriptorKey(roachpb.RKeyMax)

	var descs []roachpb.RangeDescriptor

	if _, err := engine.MVCCIterate(context.Background(), db, start, end, hlc.MaxTimestamp,
		false /* consistent */, false /* tombstones */, nil, /* txn */
		false /* reverse */, func(kv roachpb.KeyValue) (bool, error) {
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
		info, err := storage.RunGC(
			context.Background(),
			&desc,
			snap,
			hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
			config.GCPolicy{TTLSeconds: 24 * 60 * 60 /* 1 day */},
			func(_ context.Context, _ [][]roachpb.GCRequest_GCKey, _ *storage.GCInfo) error { return nil },
			func(_ context.Context, _ []roachpb.Intent) error { return nil },
			func(_ context.Context, _ *roachpb.Transaction, _ []roachpb.Intent) error { return nil },
		)
		if err != nil {
			return err
		}
		fmt.Printf("RangeID: %d [%s, %s):\n", desc.RangeID, desc.StartKey, desc.EndKey)
		_, _ = pretty.Println(info)
	}
	return nil
}

var debugCheckStoreCmd = &cobra.Command{
	Use:   "check-store <directory>",
	Short: "consistency check for a single store",
	Long: `
Perform local consistency checks of a single store.

Capable of detecting the following errors:
* Raft logs that are inconsistent with their metadata
* MVCC stats that are inconsistent with the data within the range
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugCheckStoreCmd),
}

type replicaCheckInfo struct {
	truncatedIndex uint64
	appliedIndex   uint64
	firstIndex     uint64
	lastIndex      uint64
}

func runDebugCheckStoreCmd(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	ctx := context.Background()

	db, err := openExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}
	if err := runDebugCheckStoreRaft(ctx, db); err != nil {
		return err
	}
	return runDebugCheckStoreDescriptors(ctx, db)
}

func runDebugCheckStoreDescriptors(ctx context.Context, db *engine.RocksDB) error {
	fmt.Println("checking MVCC stats")
	defer fmt.Println()

	var failed bool
	if err := storage.IterateRangeDescriptors(ctx, db,
		func(desc roachpb.RangeDescriptor) (bool, error) {
			claimedMS, err := stateloader.Make(serverCfg.Settings, desc.RangeID).LoadMVCCStats(ctx, db)
			if err != nil {
				return false, err
			}
			ms, err := rditer.ComputeStatsForRange(&desc, db, claimedMS.LastUpdateNanos)
			if err != nil {
				return false, err
			}

			if !ms.Equal(claimedMS) {
				var prefix string
				if !claimedMS.ContainsEstimates {
					failed = true
				} else {
					prefix = "(ignored) "
				}
				fmt.Printf("\n%s%+v: diff(actual, claimed): %s\n", prefix, desc, strings.Join(pretty.Diff(ms, claimedMS), "\n"))
			} else {
				fmt.Print(".")
			}
			return false, nil
		}); err != nil {
		return err
	}
	if failed {
		return errors.New("check failed")
	}
	return nil
}

func runDebugCheckStoreRaft(ctx context.Context, db *engine.RocksDB) error {
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

	if _, err := engine.MVCCIterate(ctx, db, start, end, hlc.MaxTimestamp,
		false /* consistent */, false /* tombstones */, nil, /* txn */
		false /* reverse */, func(kv roachpb.KeyValue) (bool, error) {
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

var debugRocksDBCmd = &cobra.Command{
	Use:   "rocksdb",
	Short: "run the RocksDB 'ldb' tool",
	Long: `
Runs the RocksDB 'ldb' tool, which provides various subcommands for examining
raw store data. 'cockroach debug rocksdb' accepts the same arguments and flags
as 'ldb'.

https://github.com/facebook/rocksdb/wiki/Administration-and-Data-Access-Tool#ldb-tool
`,
	// LDB does its own flag parsing.
	DisableFlagParsing: true,
	Run: func(cmd *cobra.Command, args []string) {
		engine.RunLDB(args)
	},
}

var debugEnvCmd = &cobra.Command{
	Use:   "env",
	Short: "output environment settings",
	Long: `
Output environment variables that influence configuration.
`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		env := envutil.GetEnvReport()
		fmt.Print(env)
	},
}

var debugCompactCmd = &cobra.Command{
	Use:   "compact <directory>",
	Short: "compact the sstables in a store",
	Long: `
Compact the sstables in a store.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugCompact),
}

func runDebugCompact(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := openExistingStore(args[0], stopper, false /* readOnly */)
	if err != nil {
		return err
	}

	{
		approxBytesBefore, err := db.ApproximateDiskBytes(roachpb.KeyMin, roachpb.KeyMax)
		if err != nil {
			return errors.Wrap(err, "while computing approximate size before compaction")
		}
		fmt.Printf("approximate reported database size before compaction: %s\n", humanizeutil.IBytes(int64(approxBytesBefore)))
	}

	if err := db.Compact(); err != nil {
		return errors.Wrap(err, "while compacting")
	}

	{
		approxBytesAfter, err := db.ApproximateDiskBytes(roachpb.KeyMin, roachpb.KeyMax)
		if err != nil {
			return errors.Wrap(err, "while computing approximate size after compaction")
		}
		fmt.Printf("approximate reported database size after compaction: %s\n", humanizeutil.IBytes(int64(approxBytesAfter)))
	}
	return nil
}

var debugSSTablesCmd = &cobra.Command{
	Use:   "sstables <directory>",
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
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugSSTables),
}

func runDebugSSTables(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := openExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	fmt.Printf("%s", db.GetSSTables())
	return nil
}

var debugGossipValuesCmd = &cobra.Command{
	Use:   "gossip-values <directory>",
	Short: "dump all the values in a node's gossip instance",
	Long: `
Pretty-prints the values in a node's gossip instance.

Can connect to a running server to get the values or can be provided with
a JSON file captured from a node's /_status/gossip/ debug endpoint.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugGossipValues),
}

func runDebugGossipValues(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// If a file is provided, use it. Otherwise, try talking to the running node.
	var gossipInfo *gossip.InfoStatus
	if debugCtx.inputFile != "" {
		file, err := os.Open(debugCtx.inputFile)
		if err != nil {
			return err
		}
		defer file.Close()
		gossipInfo = new(gossip.InfoStatus)
		if err := jsonpb.Unmarshal(file, gossipInfo); err != nil {
			return errors.Wrap(err, "failed to parse provided file as gossip.InfoStatus")
		}
	} else {
		conn, _, finish, err := getClientGRPCConn(ctx)
		if err != nil {
			return err
		}
		defer finish()

		status := serverpb.NewStatusClient(conn)
		gossipInfo, err = status.Gossip(ctx, &serverpb.GossipRequest{})
		if err != nil {
			return errors.Wrap(err, "failed to retrieve gossip from server")
		}
	}

	output, err := parseGossipValues(gossipInfo)
	if err != nil {
		return err
	}
	fmt.Println(output)
	return nil
}

func parseGossipValues(gossipInfo *gossip.InfoStatus) (string, error) {
	var output []string
	for key, info := range gossipInfo.Infos {
		bytes, err := info.Value.GetBytes()
		if err != nil {
			return "", errors.Wrapf(err, "failed to extract bytes for key %q", key)
		}
		if key == gossip.KeyClusterID || key == gossip.KeySentinel {
			clusterID, err := uuid.FromBytes(bytes)
			if err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %v", key, clusterID))
		} else if key == gossip.KeySystemConfig {
			if debugCtx.printSystemConfig {
				var config config.SystemConfig
				if err := protoutil.Unmarshal(bytes, &config); err != nil {
					return "", errors.Wrapf(err, "failed to parse value for key %q", key)
				}
				output = append(output, fmt.Sprintf("%q: %+v", key, config))
			} else {
				output = append(output, fmt.Sprintf("%q: omitted", key))
			}
		} else if key == gossip.KeyFirstRangeDescriptor {
			var desc roachpb.RangeDescriptor
			if err := protoutil.Unmarshal(bytes, &desc); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %v", key, desc))
		} else if gossip.IsNodeIDKey(key) {
			var desc roachpb.NodeDescriptor
			if err := protoutil.Unmarshal(bytes, &desc); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, desc))
		} else if strings.HasPrefix(key, gossip.KeyStorePrefix) {
			var desc roachpb.StoreDescriptor
			if err := protoutil.Unmarshal(bytes, &desc); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, desc))
		} else if strings.HasPrefix(key, gossip.KeyNodeLivenessPrefix) {
			var liveness storage.Liveness
			if err := protoutil.Unmarshal(bytes, &liveness); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, liveness))
		} else if strings.HasPrefix(key, gossip.KeyDeadReplicasPrefix) {
			var deadReplicas roachpb.StoreDeadReplicas
			if err := protoutil.Unmarshal(bytes, &deadReplicas); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, deadReplicas))
		}
	}

	sort.Strings(output)
	return strings.Join(output, "\n"), nil
}

var debugSyncTestCmd = &cobra.Command{
	Use:   "synctest [directory]",
	Short: "Run a performance test for WAL sync speed",
	Long: `
`,
	Args:   cobra.MaximumNArgs(1),
	Hidden: true,
	RunE:   MaybeDecorateGRPCError(runDebugSyncTest),
}

var syncTestOpts = synctest.Options{
	Concurrency: 1,
	Duration:    10 * time.Second,
	LogOnly:     true,
}

func runDebugSyncTest(cmd *cobra.Command, args []string) error {
	syncTestOpts.Dir = "./testdb"
	if len(args) == 1 {
		syncTestOpts.Dir = args[0]
	}
	return synctest.Run(syncTestOpts)
}

func init() {
	debugCmd.AddCommand(debugCmds...)

	f := debugSyncTestCmd.Flags()
	f.IntVarP(&syncTestOpts.Concurrency, "concurrency", "c", syncTestOpts.Concurrency,
		"number of concurrent writers")
	f.DurationVarP(&syncTestOpts.Duration, "duration", "d", syncTestOpts.Duration,
		"duration to run the test for")
	f.BoolVarP(&syncTestOpts.LogOnly, "log-only", "l", syncTestOpts.LogOnly,
		"only write to the WAL, not to sstables")
}

var debugCmds = []*cobra.Command{
	debugKeysCmd,
	debugRangeDataCmd,
	debugRangeDescriptorsCmd,
	debugDecodeKeyCmd,
	debugRaftLogCmd,
	debugGCCmd,
	debugCheckStoreCmd,
	debugRocksDBCmd,
	debugCompactCmd,
	debugSSTablesCmd,
	debugGossipValuesCmd,
	debugSyncTestCmd,
	debugEnvCmd,
	debugZipCmd,
}

var debugCmd = &cobra.Command{
	Use:   "debug [command]",
	Short: "debugging commands",
	Long: `Various commands for debugging.

These commands are useful for extracting data from the data files of a
process that has failed and cannot restart.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Usage()
	},
}
