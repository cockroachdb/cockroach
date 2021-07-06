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
	"encoding/base64"
	gohex "encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/syncbench"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/flagutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/tool"
	"github.com/cockroachdb/pebble/vfs"
	humanize "github.com/dustin/go-humanize"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/kr/pretty"
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
var PopulateRocksDBConfigHook func(*base.StorageConfig) error

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

func parsePositiveDuration(arg string) (time.Duration, error) {
	duration, err := time.ParseDuration(arg)
	if err != nil {
		return 0, err
	}
	if duration <= 0 {
		return 0, fmt.Errorf("illegal val: %v <= 0", duration)
	}
	return duration, nil
}

// OpenEngineOptions tunes the behavior of OpenEngine.
type OpenEngineOptions struct {
	ReadOnly  bool
	MustExist bool
}

// OpenExistingStore opens the rocksdb engine rooted at 'dir'.
// If 'readOnly' is true, opens the store in read-only mode.
func OpenExistingStore(dir string, stopper *stop.Stopper, readOnly bool) (storage.Engine, error) {
	return OpenEngine(dir, stopper, OpenEngineOptions{ReadOnly: readOnly, MustExist: true})
}

// OpenEngine opens the engine at 'dir'. Depending on the supplied options,
// an empty engine might be initialized.
func OpenEngine(dir string, stopper *stop.Stopper, opts OpenEngineOptions) (storage.Engine, error) {
	maxOpenFiles, err := server.SetOpenFileLimitForOneStore()
	if err != nil {
		return nil, err
	}

	storageConfig := base.StorageConfig{
		Settings:  serverCfg.Settings,
		Dir:       dir,
		MustExist: opts.MustExist,
	}
	if PopulateRocksDBConfigHook != nil {
		if err := PopulateRocksDBConfigHook(&storageConfig); err != nil {
			return nil, err
		}
	}

	var db storage.Engine

	cfg := storage.PebbleConfig{
		StorageConfig: storageConfig,
		Opts:          storage.DefaultPebbleOptions(),
	}
	cfg.Opts.Cache = pebble.NewCache(server.DefaultCacheSize)
	defer cfg.Opts.Cache.Unref()

	cfg.Opts.MaxOpenFiles = int(maxOpenFiles)
	cfg.Opts.ReadOnly = opts.ReadOnly

	db, err = storage.NewPebble(context.Background(), cfg)

	if err != nil {
		return nil, err
	}

	stopper.AddCloser(db)
	return db, nil
}

func printKey(kv storage.MVCCKeyValue) (bool, error) {
	fmt.Printf("%s %s: ", kv.Key.Timestamp, kv.Key.Key)
	if debugCtx.sizes {
		fmt.Printf(" %d %d", len(kv.Key.Key), len(kv.Value))
	}
	fmt.Printf("\n")
	return false, nil
}

func transactionPredicate(kv storage.MVCCKeyValue) bool {
	if kv.Key.IsValue() {
		return false
	}
	_, suffix, _, err := keys.DecodeRangeKey(kv.Key.Key)
	if err != nil {
		return false
	}
	return keys.LocalTransactionSuffix.Equal(suffix)
}

func intentPredicate(kv storage.MVCCKeyValue) bool {
	if kv.Key.IsValue() {
		return false
	}
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
		return false
	}
	return meta.Txn != nil
}

var keyTypeParams = map[keyTypeFilter]struct {
	predicate      func(kv storage.MVCCKeyValue) bool
	minKey, maxKey storage.MVCCKey
}{
	showAll: {
		predicate: func(kv storage.MVCCKeyValue) bool { return true },
		minKey:    storage.NilKey,
		maxKey:    storage.MVCCKeyMax,
	},
	showTxns: {
		predicate: transactionPredicate,
		minKey:    storage.NilKey,
		maxKey:    storage.MVCCKey{Key: keys.LocalMax},
	},
	showValues: {
		predicate: func(kv storage.MVCCKeyValue) bool {
			return kv.Key.IsValue()
		},
		minKey: storage.NilKey,
		maxKey: storage.MVCCKeyMax,
	},
	showIntents: {
		predicate: intentPredicate,
		minKey:    storage.NilKey,
		maxKey:    storage.MVCCKeyMax,
	},
}

func runDebugKeys(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := OpenExistingStore(args[0], stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	if debugCtx.decodeAsTableDesc != "" {
		bytes, err := base64.StdEncoding.DecodeString(debugCtx.decodeAsTableDesc)
		if err != nil {
			return err
		}
		var desc descpb.Descriptor
		if err := protoutil.Unmarshal(bytes, &desc); err != nil {
			return err
		}
		b := catalogkv.NewBuilder(&desc)
		if b == nil || b.DescriptorType() != catalog.Table {
			return errors.Newf("expected a table descriptor")
		}
		table := b.BuildImmutable().(catalog.TableDescriptor)

		fn := func(kv storage.MVCCKeyValue) (string, error) {
			var v roachpb.Value
			v.RawBytes = kv.Value
			_, names, values, err := row.DecodeRowInfo(context.Background(), table, kv.Key.Key, &v, true)
			if err != nil {
				return "", err
			}
			pairs := make([]string, len(names))
			for i := range pairs {
				pairs[i] = fmt.Sprintf("%s=%s", names[i], values[i])
			}
			return strings.Join(pairs, ", "), nil
		}
		kvserver.DebugSprintKeyValueDecoders = append(kvserver.DebugSprintKeyValueDecoders, fn)
	}
	printer := printKey
	if debugCtx.values {
		printer = func(kv storage.MVCCKeyValue) (bool, error) {
			kvserver.PrintKeyValue(kv)
			return false, nil
		}
	}

	keyTypeOptions := keyTypeParams[debugCtx.keyTypes]
	if debugCtx.startKey.Equal(storage.NilKey) {
		debugCtx.startKey = keyTypeOptions.minKey
	}
	if debugCtx.endKey.Equal(storage.NilKey) {
		debugCtx.endKey = keyTypeOptions.maxKey
	}

	results := 0
	iterFunc := func(kv storage.MVCCKeyValue) error {
		if !keyTypeOptions.predicate(kv) {
			return nil
		}
		done, err := printer(kv)
		if err != nil {
			return err
		}
		if done {
			return iterutil.StopIteration()
		}
		results++
		if results == debugCtx.maxResults {
			return iterutil.StopIteration()
		}
		return nil
	}
	endKey := debugCtx.endKey.Key
	splitScan := false
	// If the startKey is local and the endKey is global, split into two parts
	// to do the scan. This is because MVCCKeyAndIntentsIterKind cannot span
	// across the two key kinds.
	if (len(debugCtx.startKey.Key) == 0 || keys.IsLocal(debugCtx.startKey.Key)) && !(keys.IsLocal(endKey) || bytes.Equal(endKey, keys.LocalMax)) {
		splitScan = true
		endKey = keys.LocalMax
	}
	if err := db.MVCCIterate(
		debugCtx.startKey.Key, endKey, storage.MVCCKeyAndIntentsIterKind, iterFunc); err != nil {
		return err
	}
	if splitScan {
		if err := db.MVCCIterate(keys.LocalMax, debugCtx.endKey.Key, storage.MVCCKeyAndIntentsIterKind,
			iterFunc); err != nil {
			return err
		}
	}
	return nil
}

func runDebugBallast(cmd *cobra.Command, args []string) error {
	ballastFile := args[0] // we use cobra.ExactArgs(1)
	dataDirectory := filepath.Dir(ballastFile)

	du, err := vfs.Default.GetDiskUsage(dataDirectory)
	if err != nil {
		return errors.Wrapf(err, "failed to stat filesystem %s", dataDirectory)
	}

	// Use a 'usedBytes' calculation that counts disk space reserved for the
	// root user as used. The UsedBytes value returned by GetDiskUsage is
	// the true count of currently allocated bytes.
	usedBytes := du.TotalBytes - du.AvailBytes

	var targetUsage uint64
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
		targetUsage = usedBytes + uint64((fillRatio)*float64(du.TotalBytes))
	case p < 0:
		// Negative means leave the absolute %age of disk space.
		fillRatio := 1.0 + (p / float64(100))
		targetUsage = uint64((fillRatio) * float64(du.TotalBytes))
	case b > 0:
		targetUsage = usedBytes + uint64(b)
	case b < 0:
		// Negative means leave that many bytes of disk space.
		targetUsage = du.TotalBytes - uint64(-b)
	default:
		return errors.New("expected exactly one of percentage or bytes non-zero, found none")
	}
	if usedBytes > targetUsage {
		return errors.Errorf(
			"Used space %s already more than needed to be filled %s\n",
			humanize.IBytes(usedBytes),
			humanize.IBytes(targetUsage),
		)
	}
	if usedBytes == targetUsage {
		return nil
	}
	ballastSize := targetUsage - usedBytes

	// Note: We intentionally fail if the target file already exists. This is
	// a feature; we have seen users mistakenly applying the `ballast` command
	// directly to block devices, thereby trashing their filesystem.
	if _, err := os.Stat(ballastFile); err == nil {
		return os.ErrExist
	} else if !oserror.IsNotExist(err) {
		return errors.Wrap(err, "stating ballast file")
	}

	if err := sysutil.ResizeLargeFile(ballastFile, int64(ballastSize)); err != nil {
		return errors.Wrap(err, "error allocating ballast file")
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

	iter := rditer.NewReplicaEngineDataIterator(&desc, db, debugCtx.replicated)
	defer iter.Close()
	results := 0
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		kvserver.PrintEngineKeyValue(iter.UnsafeKey(), iter.UnsafeValue())
		results++
		if results == debugCtx.maxResults {
			break
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

func loadRangeDescriptor(
	db storage.Engine, rangeID roachpb.RangeID,
) (roachpb.RangeDescriptor, error) {
	var desc roachpb.RangeDescriptor
	handleKV := func(kv storage.MVCCKeyValue) error {
		if kv.Key.Timestamp.IsEmpty() {
			// We only want values, not MVCCMetadata.
			return nil
		}
		if err := kvserver.IsRangeDescriptorKey(kv.Key); err != nil {
			// Range descriptor keys are interleaved with others, so if it
			// doesn't parse as a range descriptor just skip it.
			return nil //nolint:returnerrcheck
		}
		if len(kv.Value) == 0 {
			// RangeDescriptor was deleted (range merged away).
			return nil
		}
		if err := (roachpb.Value{RawBytes: kv.Value}).GetProto(&desc); err != nil {
			log.Warningf(context.Background(), "ignoring range descriptor due to error %s: %+v", err, kv)
			return nil
		}
		if desc.RangeID == rangeID {
			return iterutil.StopIteration()
		}
		return nil
	}

	// Range descriptors are stored by key, so we have to scan over the
	// range-local data to find the one for this RangeID.
	start := keys.LocalRangePrefix
	end := keys.LocalRangeMax

	// NB: Range descriptor keys can have intents.
	if err := db.MVCCIterate(start, end, storage.MVCCKeyAndIntentsIterKind, handleKV); err != nil {
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

	start := keys.LocalRangePrefix
	end := keys.LocalRangeMax

	// NB: Range descriptor keys can have intents.
	return db.MVCCIterate(start, end, storage.MVCCKeyAndIntentsIterKind, func(kv storage.MVCCKeyValue) error {
		if kvserver.IsRangeDescriptorKey(kv.Key) != nil {
			return nil
		}
		kvserver.PrintKeyValue(kv)
		return nil
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
			k, err := storage.DecodeMVCCKey(b)
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
		k, err := storage.DecodeMVCCKey(bs[0])
		if err != nil {
			// - Could be an EngineKey.
			// - Older versions of the consistency checker give you diffs with a raw_key that
			//   is already a roachpb.Key, so make a half-assed attempt to support both.
			if !isTS {
				if k, ok := storage.DecodeEngineKey(bs[0]); ok {
					kvserver.PrintEngineKeyValue(k, bs[1])
					return nil
				}
				fmt.Printf("unable to decode key: %v, assuming it's a roachpb.Key with fake timestamp;\n"+
					"if the result below looks like garbage, then it likely is:\n\n", err)
			}
			k = storage.MVCCKey{
				Key:       bs[0],
				Timestamp: hlc.Timestamp{WallTime: 987654321},
			}
		}

		kvserver.PrintKeyValue(storage.MVCCKeyValue{
			Key:   k,
			Value: bs[1],
		})
		return nil
	},
}

var debugDecodeProtoName string
var debugDecodeProtoEmitDefaults bool
var debugDecodeProtoCmd = &cobra.Command{
	Use:   "decode-proto",
	Short: "decode-proto <proto> --name=<fully qualified proto name>",
	Long: `
Read from stdin and attempt to decode any hex or base64 encoded proto fields and
output them as JSON. All other fields will be outputted unchanged. Output fields
will be separated by tabs.
	
The default value for --schema is 'cockroach.sql.sqlbase.Descriptor'.
For example:

$ decode-proto < cat debug/system.decsriptor.txt
id	descriptor	hex_descriptor
1	\022!\012\006system\020\001\032\025\012\011\012\005admin\0200\012\010\012\004root\0200	{"database": {"id": 1, "modificationTime": {}, "name": "system", "privileges": {"users": [{"privileges": 48, "user": "admin"}, {"privileges": 48, "user": "root"}]}}}
...	
`,
	Args: cobra.ArbitraryArgs,
	RunE: runDebugDecodeProto,
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

	start := keys.RaftLogPrefix(rangeID)
	end := keys.RaftLogPrefix(rangeID).PrefixEnd()
	fmt.Printf("Printing keys %s -> %s (RocksDB keys: %#x - %#x )\n",
		start, end,
		string(storage.EncodeKey(storage.MakeMVCCMetadataKey(start))),
		string(storage.EncodeKey(storage.MakeMVCCMetadataKey(end))))

	// NB: raft log does not have intents.
	return db.MVCCIterate(start, end, storage.MVCCKeyIterKind, func(kv storage.MVCCKeyValue) error {
		kvserver.PrintKeyValue(kv)
		return nil
	})
}

var debugGCCmd = &cobra.Command{
	Use:   "estimate-gc <directory> [range id] [ttl-in-seconds] [intent-age-as-duration]",
	Short: "find out what a GC run would do",
	Long: `
Sets up (but does not run) a GC collection cycle, giving insight into how much
work would be done (assuming all intent resolution and pushes succeed).

Without a RangeID specified on the command line, runs the analysis for all
ranges individually.

Uses a configurable GC policy, with a default 24 hour TTL, for old versions and
2 hour intent resolution threshold.
`,
	Args: cobra.RangeArgs(1, 4),
	RunE: MaybeDecorateGRPCError(runDebugGCCmd),
}

func runDebugGCCmd(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	var rangeID roachpb.RangeID
	gcTTLInSeconds := int64((24 * time.Hour).Seconds())
	intentAgeThreshold := gc.IntentAgeThreshold.Default()

	if len(args) > 3 {
		var err error
		if intentAgeThreshold, err = parsePositiveDuration(args[3]); err != nil {
			return errors.Wrapf(err, "unable to parse %v as intent age threshold", args[3])
		}
	}
	if len(args) > 2 {
		var err error
		if gcTTLInSeconds, err = parsePositiveInt(args[2]); err != nil {
			return errors.Wrapf(err, "unable to parse %v as TTL", args[2])
		}
	}
	if len(args) > 1 {
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

	if _, err := storage.MVCCIterate(context.Background(), db, start, end, hlc.MaxTimestamp,
		storage.MVCCScanOptions{Inconsistent: true}, func(kv roachpb.KeyValue) error {
			var desc roachpb.RangeDescriptor
			_, suffix, _, err := keys.DecodeRangeKey(kv.Key)
			if err != nil {
				return err
			}
			if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
				return nil
			}
			if err := kv.Value.GetProto(&desc); err != nil {
				return err
			}
			if desc.RangeID == rangeID || rangeID == 0 {
				descs = append(descs, desc)
			}
			if desc.RangeID == rangeID {
				return iterutil.StopIteration()
			}
			return nil
		}); err != nil {
		return err
	}

	if len(descs) == 0 {
		return fmt.Errorf("no range matching the criteria found")
	}

	for _, desc := range descs {
		snap := db.NewSnapshot()
		defer snap.Close()
		policy := zonepb.GCPolicy{TTLSeconds: int32(gcTTLInSeconds)}
		now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		thresh := gc.CalculateThreshold(now, policy)
		info, err := gc.Run(
			context.Background(),
			&desc, snap,
			now, thresh, intentAgeThreshold, policy,
			gc.NoopGCer{},
			func(_ context.Context, _ []roachpb.Intent) error { return nil },
			func(_ context.Context, _ *roachpb.Transaction) error { return nil },
		)
		if err != nil {
			return err
		}
		fmt.Printf("RangeID: %d [%s, %s):\n", desc.RangeID, desc.StartKey, desc.EndKey)
		_, _ = pretty.Println(info)
	}
	return nil
}

// DebugPebbleCmd is the root of all debug pebble commands.
// Exported to allow modification by CCL code.
var DebugPebbleCmd = &cobra.Command{
	Use:   "pebble [command]",
	Short: "run a Pebble introspection tool command",
	Long: `
Allows the use of pebble tools, such as to introspect manifests, SSTables, etc.
`,
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

	// Begin compacting the store in a separate goroutine.
	errCh := make(chan error, 1)
	go func() {
		errCh <- errors.Wrap(db.Compact(), "while compacting")
	}()

	// Print the current LSM every minute.
	ticker := time.NewTicker(time.Minute)
	for done := false; !done; {
		select {
		case <-ticker.C:
			fmt.Printf("%s\n", db.GetMetrics())
		case err := <-errCh:
			ticker.Stop()
			if err != nil {
				return err
			}
			done = true
		}
	}
	fmt.Printf("%s\n", db.GetMetrics())

	{
		approxBytesAfter, err := db.ApproximateDiskBytes(roachpb.KeyMin, roachpb.KeyMax)
		if err != nil {
			return errors.Wrap(err, "while computing approximate size after compaction")
		}
		fmt.Printf("approximate reported database size after compaction: %s\n", humanizeutil.IBytes(int64(approxBytesAfter)))
	}
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
		conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
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
			var liveness livenesspb.Liveness
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
be consistent. If a suitable backup exists, restore it instead of
using this tool.

The --dead-store-ids flag takes a comma-separated list of dead store
IDs and scans this store for any ranges whose only live replica is on
this store. These range descriptors will be edited to forcibly remove
the dead stores, allowing the range to recover from this single
replica.

This command will prompt for confirmation before committing its changes.

It is safest to run this command while all nodes are stopped. In some
circumstances it may be possible to run it while some nodes are still
running provided all nodes containing replicas of nodes that have lost
quorum are stopped.

It is recommended to take a filesystem-level backup or snapshot of the
nodes to be affected before running this command (remember that it is
not safe to take a filesystem-level backup of a running node, but it is
possible while the node is stopped)

WARNINGS

This tool will cause previously committed data to be lost. It does not
preserve atomicity of transactions, so further inconsistencies and
undefined behavior may result. Before proceeding at the yes/no prompt,
review the ranges that are affected to consider the possible impact
of inconsistencies. Further remediation may be necessary after running
this tool, including dropping and recreating affected indexes, or in the
worst case creating a new backup or export of this cluster's data for
restoration into a brand new cluster. Because of the latter possibilities,
this tool is a slower means of disaster recovery than restoring from
a backup.

Must only be used when the dead stores are lost and unrecoverable. If
the dead stores were to rejoin the cluster after this command was
used, data may be corrupted.

After this command is used, the node should not be restarted until at
least 10 seconds have passed since it was stopped. Restarting it too
early may lead to things getting stuck (if it happens, it can be fixed
by restarting a second time).
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
	db storage.Engine, deadStoreIDs map[roachpb.StoreID]struct{},
) (storage.Batch, error) {
	clock := hlc.NewClock(hlc.UnixNano, 0)

	ctx := context.Background()

	storeIdent, err := kvserver.ReadStoreIdent(ctx, db)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Scanning replicas on store %s for dead peers %v\n", storeIdent.String(),
		removeDeadReplicasOpts.deadStoreIDs)

	if _, ok := deadStoreIDs[storeIdent.StoreID]; ok {
		return nil, errors.Errorf("this store's ID (%s) marked as dead, aborting", storeIdent.StoreID)
	}

	var newDescs []roachpb.RangeDescriptor

	err = kvserver.IterateRangeDescriptors(ctx, db, func(desc roachpb.RangeDescriptor) error {
		hasSelf := false
		numDeadPeers := 0
		allReplicas := desc.Replicas().Descriptors()
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
				return nil
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
			newDesc.SetReplicas(roachpb.MakeReplicaSet(replicas))
			newDesc.NextReplicaID++
			fmt.Printf("Replica %s -> %s\n", &desc, &newDesc)
			newDescs = append(newDescs, newDesc)
		}
		return nil
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
		err = storage.MVCCPutProto(ctx, batch, &ms, key, clock.Now(), nil /* txn */, &desc)
		if wiErr := (*roachpb.WriteIntentError)(nil); errors.As(err, &wiErr) {
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
			if err := storage.MVCCDelete(ctx, batch, &ms, txnKey, hlc.Timestamp{}, nil); err != nil {
				return nil, err
			}
			update := roachpb.LockUpdate{
				Span:   roachpb.Span{Key: intent.Key},
				Txn:    intent.Txn,
				Status: roachpb.ABORTED,
			}
			if _, err := storage.MVCCResolveWriteIntent(ctx, batch, &ms, update); err != nil {
				return nil, err
			}
			// With the intent resolved, we can try again.
			if err := storage.MVCCPutProto(ctx, batch, &ms, key, clock.Now(),
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
MAX_ARG_STRLEN, usually 128kB) which will be walked and whose contained log
files and merged them into a single stream printed to stdout. Files not matching
the log file name pattern are ignored. If log lines appear out of order within
a file (which happens), the timestamp is ratcheted to the highest value seen so far.
The command supports efficient time filtering as well as multiline regexp pattern
matching via flags. If the filter regexp contains captures, such as
'^abc(hello)def(world)', only the captured parts will be printed.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: runDebugMergeLogs,
}

// TODO(knz): this struct belongs elsewhere.
// See: https://github.com/cockroachdb/cockroach/issues/49509
var debugMergeLogsOpts = struct {
	from           time.Time
	to             time.Time
	filter         *regexp.Regexp
	program        *regexp.Regexp
	file           *regexp.Regexp
	prefix         string
	keepRedactable bool
	redactInput    bool
}{
	program:        nil, // match everything
	file:           regexp.MustCompile(log.FilePattern),
	keepRedactable: true,
	redactInput:    false,
}

func runDebugMergeLogs(cmd *cobra.Command, args []string) error {
	o := debugMergeLogsOpts

	inputEditMode := log.SelectEditMode(o.redactInput, o.keepRedactable)

	s, err := newMergedStreamFromPatterns(context.Background(),
		args, o.file, o.program, o.from, o.to, inputEditMode)
	if err != nil {
		return err
	}
	return writeLogStream(s, cmd.OutOrStdout(), o.filter, o.prefix, o.keepRedactable)
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
}

// All other debug commands go here.
var debugCmds = append(DebugCmdsForRocksDB,
	debugBallastCmd,
	debugCheckLogConfigCmd,
	debugDecodeKeyCmd,
	debugDecodeValueCmd,
	debugDecodeProtoCmd,
	debugGossipValuesCmd,
	debugTimeSeriesDumpCmd,
	debugSyncBenchCmd,
	debugSyncTestCmd,
	debugUnsafeRemoveDeadReplicasCmd,
	debugEnvCmd,
	debugZipCmd,
	debugMergeLogsCommand,
	debugListFilesCmd,
	debugResetQuorumCmd,
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

// mvccValueFormatter is a fmt.Formatter for MVCC values.
type mvccValueFormatter struct {
	kv  storage.MVCCKeyValue
	err error
}

// Format implements the fmt.Formatter interface.
func (m mvccValueFormatter) Format(f fmt.State, c rune) {
	if m.err != nil {
		errors.FormatError(m.err, f, c)
		return
	}
	fmt.Fprint(f, kvserver.SprintKeyValue(m.kv, false /* printKey */))
}

// lockValueFormatter is a fmt.Formatter for lock values.
type lockValueFormatter struct {
	value []byte
}

// Format implements the fmt.Formatter interface.
func (m lockValueFormatter) Format(f fmt.State, c rune) {
	fmt.Fprint(f, kvserver.SprintIntent(m.value))
}

// pebbleToolFS is the vfs.FS that the pebble tool should use.
// It is necessary because an FS must be passed to tool.New before
// the command line flags are parsed (i.e. before we can determine
// if we have an encrypted FS).
var pebbleToolFS = &swappableFS{vfs.Default}

func init() {
	DebugCmd.AddCommand(debugCmds...)

	// Note: we hook up FormatValue here in order to avoid a circular dependency
	// between kvserver and storage.
	storage.EngineComparer.FormatValue = func(key, value []byte) fmt.Formatter {
		decoded, ok := storage.DecodeEngineKey(key)
		if !ok {
			return mvccValueFormatter{err: errors.Errorf("invalid encoded engine key: %x", key)}
		}
		if decoded.IsMVCCKey() {
			mvccKey, err := decoded.ToMVCCKey()
			if err != nil {
				return mvccValueFormatter{err: err}
			}
			return mvccValueFormatter{kv: storage.MVCCKeyValue{Key: mvccKey, Value: value}}
		}
		return lockValueFormatter{value: value}
	}

	// To be able to read Cockroach-written RocksDB manifests/SSTables, comparator
	// and merger functions must be specified to pebble that match the ones used
	// to write those files.
	pebbleTool := tool.New(tool.Mergers(storage.MVCCMerger),
		tool.DefaultComparer(storage.EngineComparer),
		tool.FS(&absoluteFS{pebbleToolFS}),
	)
	DebugPebbleCmd.AddCommand(pebbleTool.Commands...)
	initPebbleCmds(DebugPebbleCmd)
	DebugCmd.AddCommand(DebugPebbleCmd)

	doctorExamineCmd.AddCommand(doctorExamineClusterCmd, doctorExamineZipDirCmd)
	doctorRecreateCmd.AddCommand(doctorRecreateClusterCmd, doctorRecreateZipDirCmd)
	debugDoctorCmd.AddCommand(doctorExamineCmd, doctorRecreateCmd, doctorExamineFallbackClusterCmd, doctorExamineFallbackZipDirCmd)
	DebugCmd.AddCommand(debugDoctorCmd)

	DebugCmd.AddCommand(debugJobTraceFromClusterCmd)

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
	// TODO(knz): the "to" should be named "until" - it's a time boundary, not a space boundary.
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
	f.BoolVar(&debugMergeLogsOpts.keepRedactable, "redactable-output", debugMergeLogsOpts.keepRedactable,
		"keep the output log file redactable")
	f.BoolVar(&debugMergeLogsOpts.redactInput, "redact", debugMergeLogsOpts.redactInput,
		"redact the input files to remove sensitive information")

	f = debugDecodeProtoCmd.Flags()
	f.StringVar(&debugDecodeProtoName, "schema", "cockroach.sql.sqlbase.Descriptor",
		"fully qualified name of the proto to decode")
	f.BoolVar(&debugDecodeProtoEmitDefaults, "emit-defaults", true,
		"encode default values for every field")

	f = debugCheckLogConfigCmd.Flags()
	f.Var(&debugLogChanSel, "only-channels", "selection of channels to include in the output diagram.")

	f = debugTimeSeriesDumpCmd.Flags()
	f.Var(&debugTimeSeriesDumpOpts.format, "format", "output format (text, csv, tsv, raw)")
}

func initPebbleCmds(cmd *cobra.Command) {
	for _, c := range cmd.Commands() {
		wrapped := c.PreRunE
		c.PreRunE = func(cmd *cobra.Command, args []string) error {
			if wrapped != nil {
				if err := wrapped(cmd, args); err != nil {
					return err
				}
			}
			return pebbleCryptoInitializer()
		}
		initPebbleCmds(c)
	}
}

func pebbleCryptoInitializer() error {
	storageConfig := base.StorageConfig{
		Settings: serverCfg.Settings,
		Dir:      serverCfg.Stores.Specs[0].Path,
	}

	if PopulateRocksDBConfigHook != nil {
		if err := PopulateRocksDBConfigHook(&storageConfig); err != nil {
			return err
		}
	}

	cfg := storage.PebbleConfig{
		StorageConfig: storageConfig,
		Opts:          storage.DefaultPebbleOptions(),
	}

	// This has the side effect of storing the encrypted FS into cfg.Opts.FS.
	_, _, err := storage.ResolveEncryptedEnvOptions(&cfg)
	if err != nil {
		return err
	}

	pebbleToolFS.set(cfg.Opts.FS)
	return nil
}
