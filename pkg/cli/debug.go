// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bufio"
	"bytes"
	"context"
	gohex "encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/syncbench"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/flagutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble/tool"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/raft/raftpb"
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

var debugBallastCmd = &cobra.Command{
	Use:   "ballast <file>",
	Short: "create a ballast file",
	Long: `
Create a ballast file to fill the store directory up to a given amount
`,
	Args: cobra.ExactArgs(1),
	RunE: runDebugBallast,
}

// PopulateRocksDBConfigHook is a callback set by CCL code.
// It populates any needed fields in the RocksDBConfig.
// It must do nothing in OSS code.
var PopulateRocksDBConfigHook func(*engine.RocksDBConfig) error

func parsePositiveInt(arg string) (int64, error) {
	i, err := strconv.ParseInt(arg, 10, 64)
	if err != nil {
		return 0, err
	}
	if i < 1 {
		return 0, fmt.Errorf("illegal val: %d < 1", i)
	}
	return i, nil
}

func parseRangeID(arg string) (roachpb.RangeID, error) {
	rangeIDInt, err := parsePositiveInt(arg)
	if err != nil {
		return 0, err
	}
	return roachpb.RangeID(rangeIDInt), nil
}

// OpenEngineOptions tunes the behavior of OpenEngine.
type OpenEngineOptions struct {
	ReadOnly  bool
	MustExist bool
}

// OpenExistingStore opens the rocksdb engine rooted at 'dir'.
// If 'readOnly' is true, opens the store in read-only mode.
func OpenExistingStore(dir string, stopper *stop.Stopper, readOnly bool) (*engine.RocksDB, error) {
	return OpenEngine(dir, stopper, OpenEngineOptions{ReadOnly: readOnly, MustExist: true})
}

// OpenEngine opens the RocksDB engine at 'dir'. Depending on the supplied options,
// an empty engine might be initialized.
func OpenEngine(
	dir string, stopper *stop.Stopper, opts OpenEngineOptions,
) (*engine.RocksDB, error) {
	cache := engine.NewRocksDBCache(server.DefaultCacheSize)
	defer cache.Release()
	maxOpenFiles, err := server.SetOpenFileLimitForOneStore()
	if err != nil {
		return nil, err
	}

	cfg := engine.RocksDBConfig{
		Settings:     serverCfg.Settings,
		Dir:          dir,
		MaxOpenFiles: maxOpenFiles,
		MustExist:    opts.MustExist,
		ReadOnly:     opts.ReadOnly,
	}

	if PopulateRocksDBConfigHook != nil {
		if err := PopulateRocksDBConfigHook(&cfg); err != nil {
			return nil, err
		}
	}

	db, err := engine.NewRocksDB(cfg, cache)
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

func runDebugKeys(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := OpenExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	printer := printKey
	if debugCtx.values {
		printer = func(kv engine.MVCCKeyValue) (bool, error) {
			storage.PrintKeyValue(kv)
			return false, nil
		}
	}

	return db.Iterate(debugCtx.startKey, debugCtx.endKey, printer)
}

func runDebugBallast(cmd *cobra.Command, args []string) error {
	ballastFile := args[0] // we use cobra.ExactArgs(1)
	dataDirectory := filepath.Dir(ballastFile)

	fs, err := sysutil.StatFS(dataDirectory)
	if err != nil {
		return errors.Wrapf(err, "failed to stat filesystem %s", dataDirectory)
	}
	total := fs.TotalBlocks * fs.BlockSize
	free := fs.AvailBlocks * fs.BlockSize

	used := total - free
	var targetUsage int64
	p := debugCtx.ballastSize.Percent
	if math.Abs(p) > 100 {
		return errors.Errorf("absolute percentage value %f greater than 100", p)
	}
	b := debugCtx.ballastSize.InBytes
	if p != 0 && b != 0 {
		return errors.New("expected exactly one of percentage or bytes non-zero, found both")
	}
	switch {
	case p > 0:
		fillRatio := p / float64(100)
		targetUsage = used + int64((fillRatio)*float64(total))
	case p < 0:
		// Negative means leave the absolute %age of disk space.
		fillRatio := 1.0 + (p / float64(100))
		targetUsage = int64((fillRatio) * float64(total))
	case b > 0:
		targetUsage = used + b
	case b < 0:
		// Negative means leave that many bytes of disk space.
		targetUsage = total + b
	default:
		return errors.New("expected exactly one of percentage or bytes non-zero, found none")
	}
	if used > targetUsage {
		return errors.Errorf(
			"Used space %s already more than needed to be filled %s\n",
			humanizeutil.IBytes(used),
			humanizeutil.IBytes(targetUsage),
		)
	}
	if used == targetUsage {
		return nil
	}
	ballastSize := targetUsage - used
	if err := sysutil.CreateLargeFile(ballastFile, ballastSize); err != nil {
		return errors.Wrap(err, "failed to fallocate to ballast file")
	}
	return nil
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

	db, err := OpenExistingStore(args[0], stopper, true /* readOnly */)
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
		storage.PrintKeyValue(engine.MVCCKeyValue{
			Key:   iter.Key(),
			Value: iter.Value(),
		})
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

func loadRangeDescriptor(
	db engine.Engine, rangeID roachpb.RangeID,
) (roachpb.RangeDescriptor, error) {
	var desc roachpb.RangeDescriptor
	handleKV := func(kv engine.MVCCKeyValue) (bool, error) {
		if kv.Key.Timestamp == (hlc.Timestamp{}) {
			// We only want values, not MVCCMetadata.
			return false, nil
		}
		if err := storage.IsRangeDescriptorKey(kv.Key); err != nil {
			// Range descriptor keys are interleaved with others, so if it
			// doesn't parse as a range descriptor just skip it.
			return false, nil
		}
		if len(kv.Value) == 0 {
			// RangeDescriptor was deleted (range merged away).
			return false, nil
		}
		if err := (roachpb.Value{RawBytes: kv.Value}).GetProto(&desc); err != nil {
			log.Warningf(context.Background(), "ignoring range descriptor due to error %s: %+v", err, kv)
			return false, nil
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

	db, err := OpenExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	start := engine.MakeMVCCMetadataKey(keys.LocalRangePrefix)
	end := engine.MakeMVCCMetadataKey(keys.LocalRangeMax)

	return db.Iterate(start, end, func(kv engine.MVCCKeyValue) (bool, error) {
		if storage.IsRangeDescriptorKey(kv.Key) != nil {
			return false, nil
		}
		storage.PrintKeyValue(kv)
		return false, nil
	})
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
			b, err := gohex.DecodeString(arg)
			if err != nil {
				return err
			}
			k, err := engine.DecodeMVCCKey(b)
			if err != nil {
				return err
			}
			fmt.Println(k)
		}
		return nil
	},
}

var debugDecodeValueCmd = &cobra.Command{
	Use:   "decode-value",
	Short: "decode-value <key> <value>",
	Long: `
Decode and print a hexadecimal-encoded key-value pair.

	$ decode-value <TBD> <TBD>
	<TBD>
`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		var bs [][]byte
		for _, arg := range args {
			b, err := gohex.DecodeString(arg)
			if err != nil {
				return err
			}
			bs = append(bs, b)
		}

		isTS := bytes.HasPrefix(bs[0], keys.TimeseriesPrefix)
		k, err := engine.DecodeMVCCKey(bs[0])
		if err != nil {
			// Older versions of the consistency checker give you diffs with a raw_key that
			// is already a roachpb.Key, so make a half-assed attempt to support both.
			if !isTS {
				fmt.Printf("unable to decode key: %v, assuming it's a roachpb.Key with fake timestamp;\n"+
					"if the result below looks like garbage, then it likely is:\n\n", err)
			}
			k = engine.MVCCKey{
				Key:       bs[0],
				Timestamp: hlc.Timestamp{WallTime: 987654321},
			}
		}

		storage.PrintKeyValue(engine.MVCCKeyValue{
			Key:   k,
			Value: bs[1],
		})
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

func runDebugRaftLog(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := OpenExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	rangeID, err := parseRangeID(args[1])
	if err != nil {
		return err
	}

	start := engine.MakeMVCCMetadataKey(keys.RaftLogPrefix(rangeID))
	end := engine.MakeMVCCMetadataKey(keys.RaftLogPrefix(rangeID).PrefixEnd())
	fmt.Printf("Printing keys %s -> %s (RocksDB keys: %#x - %#x )\n",
		start, end, string(engine.EncodeKey(start)), string(engine.EncodeKey(end)))

	return db.Iterate(start, end, func(kv engine.MVCCKeyValue) (bool, error) {
		storage.PrintKeyValue(kv)
		return false, nil
	})
}

var debugGCCmd = &cobra.Command{
	Use:   "estimate-gc <directory> [range id] [ttl-in-seconds]",
	Short: "find out what a GC run would do",
	Long: `
Sets up (but does not run) a GC collection cycle, giving insight into how much
work would be done (assuming all intent resolution and pushes succeed).

Without a RangeID specified on the command line, runs the analysis for all
ranges individually.

Uses a configurable GC policy, with a default 24 hour TTL, for old versions.
`,
	Args: cobra.RangeArgs(1, 2),
	RunE: MaybeDecorateGRPCError(runDebugGCCmd),
}

func runDebugGCCmd(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	var rangeID roachpb.RangeID
	gcTTLInSeconds := int64((24 * time.Hour).Seconds())
	switch len(args) {
	case 3:
		var err error
		if rangeID, err = parseRangeID(args[1]); err != nil {
			return errors.Wrapf(err, "unable to parse %v as range ID", args[1])
		}
		if gcTTLInSeconds, err = parsePositiveInt(args[2]); err != nil {
			return errors.Wrapf(err, "unable to parse %v as TTL", args[2])
		}

	case 2:
		var err error
		if rangeID, err = parseRangeID(args[1]); err != nil {
			return err
		}
	}

	db, err := OpenExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	start := keys.RangeDescriptorKey(roachpb.RKeyMin)
	end := keys.RangeDescriptorKey(roachpb.RKeyMax)

	var descs []roachpb.RangeDescriptor

	if _, err := engine.MVCCIterate(context.Background(), db, start, end, hlc.MaxTimestamp,
		engine.MVCCScanOptions{Inconsistent: true}, func(kv roachpb.KeyValue) (bool, error) {
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
			zonepb.GCPolicy{TTLSeconds: int32(gcTTLInSeconds)},
			storage.NoopGCer{},
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
	committedIndex uint64
}

func runDebugCheckStoreCmd(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	ctx := context.Background()

	db, err := OpenExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}
	var hasError bool
	if err := runDebugCheckStoreRaft(ctx, db); err != nil {
		hasError = true
		log.Warning(ctx, err)
	}
	if err := runDebugCheckStoreDescriptors(ctx, db); err != nil {
		hasError = true
		log.Warning(ctx, err)
	}
	if hasError {
		return errors.New("errors detected")
	}
	return nil
}

func runDebugCheckStoreDescriptors(ctx context.Context, db *engine.RocksDB) error {
	fmt.Println("checking MVCC stats")
	defer fmt.Println()

	var failed bool
	if err := storage.IterateRangeDescriptors(ctx, db,
		func(desc roachpb.RangeDescriptor) (bool, error) {
			claimedMS, err := stateloader.Make(desc.RangeID).LoadMVCCStats(ctx, db)
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
					ms.ContainsEstimates = true
					prefix = "(ignored) "
				}
				fmt.Printf("\n%s%+v: diff(actual, claimed): %s\n", prefix, desc, strings.Join(pretty.Diff(ms, claimedMS), "\n"))
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

	var hasError bool

	if _, err := engine.MVCCIterate(ctx, db, start, end, hlc.MaxTimestamp,
		engine.MVCCScanOptions{Inconsistent: true}, func(kv roachpb.KeyValue) (bool, error) {
			rangeID, _, suffix, detail, err := keys.DecodeRangeIDKey(kv.Key)
			if err != nil {
				return false, err
			}

			switch {
			case bytes.Equal(suffix, keys.LocalRaftHardStateSuffix):
				var hs raftpb.HardState
				if err := kv.Value.GetProto(&hs); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).committedIndex = hs.Commit
			case bytes.Equal(suffix, keys.LocalRaftTruncatedStateLegacySuffix):
				var trunc roachpb.RaftTruncatedState
				if err := kv.Value.GetProto(&trunc); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).truncatedIndex = trunc.Index
			case bytes.Equal(suffix, keys.LocalRangeAppliedStateSuffix):
				var state enginepb.RangeAppliedState
				if err := kv.Value.GetProto(&state); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).appliedIndex = state.RaftAppliedIndex
			case bytes.Equal(suffix, keys.LocalRaftAppliedIndexLegacySuffix):
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
						hasError = true
					}
					ri.lastIndex = index
				}
			}

			return false, nil
		}); err != nil {
		return err
	}

	for rangeID, info := range replicaInfo {
		if info.truncatedIndex != 0 && info.truncatedIndex != info.firstIndex-1 {
			hasError = true
			fmt.Printf("range %s: truncated index %v should equal first index %v - 1\n",
				rangeID, info.truncatedIndex, info.firstIndex)
		}
		if info.firstIndex > info.lastIndex {
			hasError = true
			fmt.Printf("range %s: [first index, last index] is [%d, %d]\n",
				rangeID, info.firstIndex, info.lastIndex)
		}
		if info.appliedIndex < info.firstIndex || info.appliedIndex > info.lastIndex {
			hasError = true
			fmt.Printf("range %s: applied index %v should be between first index %v and last index %v\n",
				rangeID, info.appliedIndex, info.firstIndex, info.lastIndex)
		}
		if info.appliedIndex > info.committedIndex {
			hasError = true
			fmt.Printf("range %s: committed index %d must not trail applied index %d\n",
				rangeID, info.committedIndex, info.appliedIndex)
		}
		if info.committedIndex > info.lastIndex {
			hasError = true
			fmt.Printf("range %s: committed index %d ahead of last index  %d\n",
				rangeID, info.committedIndex, info.lastIndex)
		}
	}
	if hasError {
		return errors.New("anomalies detected in Raft state")
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
	// TODO(mberhault): support encrypted stores.
	DisableFlagParsing: true,
	Run: func(cmd *cobra.Command, args []string) {
		engine.RunLDB(args)
	},
}

var debugPebbleCmd = &cobra.Command{
	Use:   "pebble [command]",
	Short: "run a Pebble introspection tool command",
	Long: `
Allows the use of pebble tools, such as to introspect manifests, SSTables, etc.
`,
}

var debugSSTDumpCmd = &cobra.Command{
	Use:   "sst_dump",
	Short: "run the RocksDB 'sst_dump' tool",
	Long: `
Runs the RocksDB 'sst_dump' tool
`,
	// sst_dump does its own flag parsing.
	// TODO(mberhault): support encrypted stores.
	DisableFlagParsing: true,
	Run: func(cmd *cobra.Command, args []string) {
		engine.RunSSTDump(args)
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

	db, err := OpenExistingStore(args[0], stopper, false /* readOnly */)
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

	db, err := OpenExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	fmt.Printf("%s", db.GetSSTables())
	return nil
}

var debugGossipValuesCmd = &cobra.Command{
	Use:   "gossip-values",
	Short: "dump all the values in a node's gossip instance",
	Long: `
Pretty-prints the values in a node's gossip instance.

Can connect to a running server to get the values or can be provided with
a JSON file captured from a node's /_status/gossip/ debug endpoint.
`,
	Args: cobra.NoArgs,
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
				var config config.SystemConfigEntries
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
			var liveness storagepb.Liveness
			if err := protoutil.Unmarshal(bytes, &liveness); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, liveness))
		} else if strings.HasPrefix(key, gossip.KeyNodeHealthAlertPrefix) {
			var healthAlert statuspb.HealthCheckResult
			if err := protoutil.Unmarshal(bytes, &healthAlert); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, healthAlert))
		} else if strings.HasPrefix(key, gossip.KeyDistSQLNodeVersionKeyPrefix) {
			var version execinfrapb.DistSQLVersionGossipInfo
			if err := protoutil.Unmarshal(bytes, &version); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, version))
		} else if strings.HasPrefix(key, gossip.KeyDistSQLDrainingPrefix) {
			var drainingInfo execinfrapb.DistSQLDrainingInfo
			if err := protoutil.Unmarshal(bytes, &drainingInfo); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, drainingInfo))
		} else if strings.HasPrefix(key, gossip.KeyTableStatAddedPrefix) {
			gossipedTime := timeutil.Unix(0, info.OrigStamp)
			output = append(output, fmt.Sprintf("%q: %v", key, gossipedTime))
		} else if strings.HasPrefix(key, gossip.KeyGossipClientsPrefix) {
			output = append(output, fmt.Sprintf("%q: %v", key, string(bytes)))
		}
	}

	sort.Strings(output)
	return strings.Join(output, "\n"), nil
}

var debugTimeSeriesDumpCmd = &cobra.Command{
	Use:   "tsdump",
	Short: "dump all the raw timeseries values in a cluster",
	Long: `
Dumps all of the raw timeseries values in a cluster.
`,
	RunE: MaybeDecorateGRPCError(runTimeSeriesDump),
}

func runTimeSeriesDump(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, _, finish, err := getClientGRPCConn(ctx)
	if err != nil {
		return err
	}
	defer finish()

	tsClient := tspb.NewTimeSeriesClient(conn)
	stream, err := tsClient.Dump(context.Background(), &tspb.DumpRequest{})
	if err != nil {
		log.Fatal(context.Background(), err)
	}

	var name, source string
	for {
		data, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}
		if name != data.Name || source != data.Source {
			name, source = data.Name, data.Source
			fmt.Printf("%s %s\n", name, source)
		}
		for _, d := range data.Datapoints {
			fmt.Printf("%d %v\n", d.TimestampNanos, d.Value)
		}
	}
}

var debugSyncBenchCmd = &cobra.Command{
	Use:   "syncbench [directory]",
	Short: "Run a performance test for WAL sync speed",
	Long: `
`,
	Args:   cobra.MaximumNArgs(1),
	Hidden: true,
	RunE:   MaybeDecorateGRPCError(runDebugSyncBench),
}

var syncBenchOpts = syncbench.Options{
	Concurrency: 1,
	Duration:    10 * time.Second,
	LogOnly:     true,
}

func runDebugSyncBench(cmd *cobra.Command, args []string) error {
	syncBenchOpts.Dir = "./testdb"
	if len(args) == 1 {
		syncBenchOpts.Dir = args[0]
	}
	return syncbench.Run(syncBenchOpts)
}

var debugUnsafeRemoveDeadReplicasCmd = &cobra.Command{
	Use:   "unsafe-remove-dead-replicas --dead-store-ids=[store ID,...] [path]",
	Short: "Unsafely remove all other replicas from the given range",
	Long: `

This command is UNSAFE and should only be used with the supervision of
a Cockroach Labs engineer. It is a last-resort option to recover data
after multiple node failures. The recovered data is not guaranteed to
be consistent.

The --dead-store-ids flag takes a comma-separated list of dead store
IDs and scans this store for any ranges whose only live replica is on
this store. These range descriptors will be edited to forcibly remove
the dead stores, allowing the range to recover from this single
replica.

Must only be used when the dead stores are lost and unrecoverable. If
the dead stores were to rejoin the cluster after this command was
used, data may be corrupted.

This comand will prompt for confirmation before committing its changes.

`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugUnsafeRemoveDeadReplicas),
}

var removeDeadReplicasOpts struct {
	deadStoreIDs []int
}

func runDebugUnsafeRemoveDeadReplicas(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := OpenExistingStore(args[0], stopper, false /* readOnly */)
	if err != nil {
		return err
	}
	defer db.Close()

	deadStoreIDs := map[roachpb.StoreID]struct{}{}
	for _, id := range removeDeadReplicasOpts.deadStoreIDs {
		deadStoreIDs[roachpb.StoreID(id)] = struct{}{}
	}
	batch, err := removeDeadReplicas(db, deadStoreIDs)
	if err != nil {
		return err
	} else if batch == nil {
		fmt.Printf("Nothing to do\n")
		return nil
	}
	defer batch.Close()

	fmt.Printf("Proceed with the above rewrites? [y/N] ")

	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	fmt.Printf("\n")
	if line[0] == 'y' || line[0] == 'Y' {
		fmt.Printf("Committing\n")
		if err := batch.Commit(true); err != nil {
			return err
		}
	} else {
		fmt.Printf("Aborting\n")
	}
	return nil
}

func removeDeadReplicas(
	db engine.Engine, deadStoreIDs map[roachpb.StoreID]struct{},
) (engine.Batch, error) {
	clock := hlc.NewClock(hlc.UnixNano, 0)

	ctx := context.Background()

	storeIdent, err := storage.ReadStoreIdent(ctx, db)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Scanning replicas on store %s for dead peers %v\n", storeIdent.String(),
		removeDeadReplicasOpts.deadStoreIDs)

	if _, ok := deadStoreIDs[storeIdent.StoreID]; ok {
		return nil, errors.Errorf("This store's ID (%s) marked as dead, aborting", storeIdent.StoreID)
	}

	var newDescs []roachpb.RangeDescriptor

	err = storage.IterateRangeDescriptors(ctx, db, func(desc roachpb.RangeDescriptor) (bool, error) {
		hasSelf := false
		numDeadPeers := 0
		allReplicas := desc.Replicas().All()
		maxLivePeer := roachpb.StoreID(-1)
		for _, rep := range allReplicas {
			if rep.StoreID == storeIdent.StoreID {
				hasSelf = true
			}
			if _, ok := deadStoreIDs[rep.StoreID]; ok {
				numDeadPeers++
			} else {
				if rep.StoreID > maxLivePeer {
					maxLivePeer = rep.StoreID
				}
			}
		}
		if hasSelf && numDeadPeers > 0 && storeIdent.StoreID == maxLivePeer {
			canMakeProgress := desc.Replicas().CanMakeProgress(func(rep roachpb.ReplicaDescriptor) bool {
				_, ok := deadStoreIDs[rep.StoreID]
				return !ok
			})
			if canMakeProgress {
				return false, nil
			}

			// Rewrite the range as having a single replica. The winning
			// replica is picked arbitrarily: the one with the highest store
			// ID. This is not always the best option: it may lose writes
			// that were committed on another surviving replica that had
			// applied more of the raft log. However, in practice when we
			// have multiple surviving replicas but still need this tool
			// (because the replication factor was 4 or higher), we see that
			// the logs are nearly always in sync and the choice doesn't
			// matter. Correctly picking the replica with the longer log
			// would complicate the use of this tool.
			newDesc := desc
			// Rewrite the replicas list. Bump the replica ID so that in
			// case there are other surviving nodes that were members of the
			// old incarnation of the range, they no longer recognize this
			// revived replica (because they are not in sync with it).
			replicas := []roachpb.ReplicaDescriptor{{
				NodeID:    storeIdent.NodeID,
				StoreID:   storeIdent.StoreID,
				ReplicaID: desc.NextReplicaID,
			}}
			newDesc.SetReplicas(roachpb.MakeReplicaDescriptors(replicas))
			newDesc.NextReplicaID++
			fmt.Printf("Replica %s -> %s\n", &desc, &newDesc)
			newDescs = append(newDescs, newDesc)
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	if len(newDescs) == 0 {
		return nil, nil
	}

	batch := db.NewBatch()
	for _, desc := range newDescs {
		// Write the rewritten descriptor to the range-local descriptor
		// key. We do not update the meta copies of the descriptor.
		// Instead, we leave them in a temporarily inconsistent state and
		// they will be overwritten when the cluster recovers and
		// up-replicates this range from its single copy to multiple
		// copies. We rely on the fact that all range descriptor updates
		// start with a CPut on the range-local copy followed by a blind
		// Put to the meta copy.
		//
		// For example, if we have replicas on s1-s4 but s3 and s4 are
		// dead, we will rewrite the replica on s2 to have s2 as its only
		// member only. When the cluster is restarted (and the dead nodes
		// remain dead), the rewritten replica will be the only one able
		// to make progress. It will elect itself leader and upreplicate.
		//
		// The old replica on s1 is untouched by this process. It will
		// eventually either be overwritten by a new replica when s2
		// upreplicates, or it will be destroyed by the replica GC queue
		// after upreplication has happened and s1 is no longer a member.
		// (Note that in the latter case, consistency between s1 and s2 no
		// longer matters; the consistency checker will only run on nodes
		// that the new leader believes are members of the range).
		//
		// Note that this tool does not guarantee fully consistent
		// results; the most recent writes to the raft log may have been
		// lost. In the most unfortunate cases, this means that we would
		// be "winding back" a split or a merge, which is almost certainly
		// to result in irrecoverable corruption (for example, not only
		// will individual values stored in the meta ranges diverge, but
		// there will be keys not represented by any ranges or vice
		// versa).
		key := keys.RangeDescriptorKey(desc.StartKey)
		sl := stateloader.Make(desc.RangeID)
		ms, err := sl.LoadMVCCStats(ctx, batch)
		if err != nil {
			return nil, errors.Wrap(err, "loading MVCCStats")
		}
		err = engine.MVCCPutProto(ctx, batch, &ms, key, clock.Now(), nil /* txn */, &desc)
		if wiErr, ok := err.(*roachpb.WriteIntentError); ok {
			if len(wiErr.Intents) != 1 {
				return nil, errors.Errorf("expected 1 intent, found %d: %s", len(wiErr.Intents), wiErr)
			}
			intent := wiErr.Intents[0]
			// We rely on the property that transactions involving the range
			// descriptor always start on the range-local descriptor's key.
			// This guarantees that when the transaction commits, the intent
			// will be resolved synchronously. If we see an intent on this
			// key, we know that the transaction did not commit and we can
			// abort it.
			//
			// TODO(nvanbenschoten): This need updating for parallel
			// commits. If the transaction record is in the STAGING state,
			// we can't just delete it. Simplest solution to this is to
			// avoid parallel commits for membership change transactions; if
			// we can't do that I don't think we'll be able to recover them
			// with an offline tool.
			fmt.Printf("Conflicting intent found on %s. Aborting txn %s to resolve.\n", key, intent.Txn.ID)

			// A crude form of the intent resolution process: abort the
			// transaction by deleting its record.
			txnKey := keys.TransactionKey(intent.Txn.Key, intent.Txn.ID)
			if err := engine.MVCCDelete(ctx, batch, &ms, txnKey, hlc.Timestamp{}, nil); err != nil {
				return nil, err
			}
			intent.Status = roachpb.ABORTED
			if err := engine.MVCCResolveWriteIntent(ctx, batch, &ms, intent); err != nil {
				return nil, err
			}
			// With the intent resolved, we can try again.
			if err := engine.MVCCPutProto(ctx, batch, &ms, key, clock.Now(),
				nil /* txn */, &desc); err != nil {
				return nil, err
			}
		} else if err != nil {
			batch.Close()
			return nil, err
		}
		if err := sl.SetMVCCStats(ctx, batch, &ms); err != nil {
			return nil, errors.Wrap(err, "updating MVCCStats")
		}
	}

	return batch, nil
}

var debugMergeLogsCommand = &cobra.Command{
	Use:   "merge-logs <log file globs>",
	Short: "merge multiple log files from different machines into a single stream",
	Long: `
Takes a list of glob patterns (not left exclusively to the shell because of
MAX_ARG_STRLEN, usually 128kB) pointing to log files and merges them into a
single stream printed to stdout. Files not matching the log file name pattern
are ignored. If log lines appear out of order within a file (which happens), the
timestamp is ratcheted to the highest value seen so far. The command supports
efficient time filtering as well as multiline regexp pattern matching via flags.
If the filter regexp contains captures, such as '^abc(hello)def(world)', only
the captured parts will be printed.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: runDebugMergeLogs,
}

var debugMergeLogsOpts = struct {
	from    time.Time
	to      time.Time
	filter  *regexp.Regexp
	program *regexp.Regexp
	file    *regexp.Regexp
	prefix  string
}{
	program: regexp.MustCompile("^cockroach.*$"),
	file:    regexp.MustCompile(log.FilePattern),
}

func runDebugMergeLogs(cmd *cobra.Command, args []string) error {
	o := debugMergeLogsOpts
	s, err := newMergedStreamFromPatterns(context.Background(),
		args, o.file, o.program, o.from, o.to)
	if err != nil {
		return err
	}
	return writeLogStream(s, cmd.OutOrStdout(), o.filter, o.prefix)
}

// DebugCmdsForRocksDB lists debug commands that access rocksdb through the engine
// and need encryption flags (injected by CCL code).
// Note: do NOT include commands that just call rocksdb code without setting up an engine.
var DebugCmdsForRocksDB = []*cobra.Command{
	debugCheckStoreCmd,
	debugCompactCmd,
	debugGCCmd,
	debugKeysCmd,
	debugRaftLogCmd,
	debugRangeDataCmd,
	debugRangeDescriptorsCmd,
	debugSSTablesCmd,
}

// All other debug commands go here.
var debugCmds = append(DebugCmdsForRocksDB,
	debugBallastCmd,
	debugDecodeKeyCmd,
	debugDecodeValueCmd,
	debugRocksDBCmd,
	debugSSTDumpCmd,
	debugGossipValuesCmd,
	debugTimeSeriesDumpCmd,
	debugSyncBenchCmd,
	debugSyncTestCmd,
	debugUnsafeRemoveDeadReplicasCmd,
	debugEnvCmd,
	debugZipCmd,
	debugMergeLogsCommand,
)

// DebugCmd is the root of all debug commands. Exported to allow modification by CCL code.
var DebugCmd = &cobra.Command{
	Use:   "debug [command]",
	Short: "debugging commands",
	Long: `Various commands for debugging.

These commands are useful for extracting data from the data files of a
process that has failed and cannot restart.
`,
	RunE: usageAndErr,
}

func init() {
	DebugCmd.AddCommand(debugCmds...)

	pebbleTool := tool.New()
	// To be able to read Cockroach-written RocksDB manifests/SSTables, comparator
	// and merger functions must be specified to pebble that match the ones used
	// to write those files.
	pebbleTool.RegisterMerger(engine.MVCCMerger)
	pebbleTool.RegisterComparer(engine.MVCCComparer)
	debugPebbleCmd.AddCommand(pebbleTool.Commands...)
	DebugCmd.AddCommand(debugPebbleCmd)

	f := debugSyncBenchCmd.Flags()
	f.IntVarP(&syncBenchOpts.Concurrency, "concurrency", "c", syncBenchOpts.Concurrency,
		"number of concurrent writers")
	f.DurationVarP(&syncBenchOpts.Duration, "duration", "d", syncBenchOpts.Duration,
		"duration to run the test for")
	f.BoolVarP(&syncBenchOpts.LogOnly, "log-only", "l", syncBenchOpts.LogOnly,
		"only write to the WAL, not to sstables")

	f = debugUnsafeRemoveDeadReplicasCmd.Flags()
	f.IntSliceVar(&removeDeadReplicasOpts.deadStoreIDs, "dead-store-ids", nil,
		"list of dead store IDs")

	f = debugMergeLogsCommand.Flags()
	f.Var(flagutil.Time(&debugMergeLogsOpts.from), "from",
		"time before which messages should be filtered")
	f.Var(flagutil.Time(&debugMergeLogsOpts.to), "to",
		"time after which messages should be filtered")
	f.Var(flagutil.Regexp(&debugMergeLogsOpts.filter), "filter",
		"re which filters log messages")
	f.Var(flagutil.Regexp(&debugMergeLogsOpts.file), "file-pattern",
		"re which filters log files based on path, also used with prefix and program-filter")
	f.Var(flagutil.Regexp(&debugMergeLogsOpts.program), "program-filter",
		"re which filter log files that operates on the capture group named \"program\" in file-pattern, "+
			"if no such group exists, program-filter is ignored")
	f.StringVar(&debugMergeLogsOpts.prefix, "prefix", "${host}> ",
		"expansion template (see regexp.Expand) used as prefix to merged log messages evaluated on file-pattern")
}
