// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	gohex "encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/syncbench"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
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
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/tool"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/ttycolor"
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
	RunE: clierrorplus.MaybeDecorateError(runDebugKeys),
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

type keyFormat int

const (
	hexKey = iota
	base64Key
)

func (f *keyFormat) Set(value string) error {
	switch value {
	case "hex":
		*f = hexKey
	case "base64":
		*f = base64Key
	default:
		return errors.Errorf("unsupported format %s", value)
	}
	return nil
}

func (f *keyFormat) String() string {
	switch *f {
	case hexKey:
		return "hex"
	case base64Key:
		return "base64"
	default:
		panic(errors.AssertionFailedf("invalid format value %d", *f))
	}
}

func (f *keyFormat) Type() string {
	return "hex|base64"
}

// OpenFilesystemEnv opens the filesystem environment at 'dir'. Note that
// opening the fs.Env will acquire the directory lock and prevent opening of the
// engine through [OpenEngine]. If the caller wishes to then open the storage
// engine, they should manually open it using storage.Open. The returned Env has
// 1 reference and the caller must ensure it's closed.
func OpenFilesystemEnv(dir string, rw fs.RWMode) (*fs.Env, error) {
	envConfig := fs.EnvConfig{RW: rw}
	if err := fillEncryptionOptionsForStore(dir, &envConfig); err != nil {
		return nil, err
	}
	return fs.InitEnv(context.Background(), vfs.Default, dir, envConfig, nil /* diskWriteStats */)
}

// OpenEngine opens the engine at 'dir'. Depending on the supplied options,
// an empty engine might be initialized.
func OpenEngine(
	dir string, stopper *stop.Stopper, rw fs.RWMode, opts ...storage.ConfigOption,
) (storage.Engine, error) {
	env, err := OpenFilesystemEnv(dir, rw)
	if err != nil {
		return nil, err
	}
	maxOpenFiles, err := server.SetOpenFileLimitForOneStore()
	if err != nil {
		return nil, err
	}
	db, err := storage.Open(
		context.Background(),
		env,
		serverCfg.Settings,
		storage.MaxOpenFiles(int(maxOpenFiles)),
		storage.CacheSize(server.DefaultCacheSize),
		storage.CombineOptions(opts...))
	if err != nil {
		env.Close()
		return nil, err
	}

	stopper.AddCloser(db)
	return db, nil
}

func printKey(kv storage.MVCCKeyValue) {
	fmt.Printf("%s %s: ", kv.Key.Timestamp, kv.Key.Key)
	if debugCtx.sizes {
		fmt.Printf(" %d %d", len(kv.Key.Key), len(kv.Value))
	}
	fmt.Printf("\n")
}

func printRangeKey(rkv storage.MVCCRangeKeyValue) {
	fmt.Printf("%s %s: ", rkv.RangeKey.Timestamp, rkv.RangeKey.Bounds())
	if debugCtx.sizes {
		fmt.Printf(" %d %d", len(rkv.RangeKey.StartKey)+len(rkv.RangeKey.EndKey), len(rkv.Value))
	}
	fmt.Printf("\n")
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
	predicate         func(kv storage.MVCCKeyValue) bool
	rangeKeyPredicate func(rkv storage.MVCCRangeKeyValue) bool
	minKey, maxKey    storage.MVCCKey
}{
	showAll: {
		predicate:         func(kv storage.MVCCKeyValue) bool { return true },
		rangeKeyPredicate: func(storage.MVCCRangeKeyValue) bool { return true },
		minKey:            storage.NilKey,
		maxKey:            storage.MVCCKeyMax,
	},
	showTxns: {
		predicate:         transactionPredicate,
		rangeKeyPredicate: func(storage.MVCCRangeKeyValue) bool { return false },
		minKey:            storage.NilKey,
		maxKey:            storage.MVCCKey{Key: keys.LocalMax},
	},
	showValues: {
		predicate: func(kv storage.MVCCKeyValue) bool {
			return kv.Key.IsValue()
		},
		rangeKeyPredicate: func(storage.MVCCRangeKeyValue) bool { return true },
		minKey:            storage.NilKey,
		maxKey:            storage.MVCCKeyMax,
	},
	showIntents: {
		predicate:         intentPredicate,
		rangeKeyPredicate: func(storage.MVCCRangeKeyValue) bool { return false },
		minKey:            storage.NilKey,
		maxKey:            storage.MVCCKeyMax,
	},
	showRangeKeys: {
		predicate:         func(storage.MVCCKeyValue) bool { return false },
		rangeKeyPredicate: func(storage.MVCCRangeKeyValue) bool { return true },
		minKey:            storage.NilKey,
		maxKey:            storage.MVCCKeyMax,
	},
}

func runDebugKeys(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := OpenEngine(args[0], stopper, fs.ReadOnly, storage.MustExist)
	if err != nil {
		return err
	}

	if debugCtx.decodeAsTableDesc != "" {
		bytes, err := base64.StdEncoding.DecodeString(debugCtx.decodeAsTableDesc)
		if err != nil {
			return err
		}
		b, err := descbuilder.FromBytesAndMVCCTimestamp(bytes, hlc.Timestamp{})
		if err != nil {
			return err
		}
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
		kvserver.DebugSprintMVCCKeyValueDecoders = append(kvserver.DebugSprintMVCCKeyValueDecoders, fn)
	}
	printer := printKey
	rangeKeyPrinter := printRangeKey
	if debugCtx.values {
		printer = kvserver.PrintMVCCKeyValue
		rangeKeyPrinter = kvserver.PrintMVCCRangeKeyValue
	}

	keyTypeOptions := keyTypeParams[debugCtx.keyTypes]
	if debugCtx.startKey.Equal(storage.NilKey) {
		debugCtx.startKey = keyTypeOptions.minKey
	}
	if debugCtx.endKey.Equal(storage.NilKey) {
		debugCtx.endKey = keyTypeOptions.maxKey
	}

	results := 0
	var lastRangeKey roachpb.Key
	iterFunc := func(kv storage.MVCCKeyValue, rangeKeys storage.MVCCRangeKeyStack) error {
		// MVCC range keys.
		if !rangeKeys.IsEmpty() && !rangeKeys.Bounds.Key.Equal(lastRangeKey) {
			lastRangeKey = rangeKeys.Bounds.Key.Clone()
			for _, v := range rangeKeys.Versions {
				rkv := rangeKeys.AsRangeKeyValue(v)
				if keyTypeOptions.rangeKeyPredicate(rkv) {
					rangeKeyPrinter(rangeKeys.AsRangeKeyValue(v))
					results++
					if results == debugCtx.maxResults {
						return iterutil.StopIteration()
					}
				}
			}
		}

		// MVCC point keys.
		if len(kv.Key.Key) > 0 && keyTypeOptions.predicate(kv) {
			printer(kv)
			results++
			if results == debugCtx.maxResults {
				return iterutil.StopIteration()
			}
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
		cmd.Context(), debugCtx.startKey.Key, endKey, storage.MVCCKeyAndIntentsIterKind,
		storage.IterKeyTypePointsAndRanges, fs.UnknownReadCategory, iterFunc); err != nil {
		return err
	}
	if splitScan {
		if err := db.MVCCIterate(cmd.Context(), keys.LocalMax, debugCtx.endKey.Key,
			storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsAndRanges,
			fs.UnknownReadCategory, iterFunc); err != nil {
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
	b := debugCtx.ballastSize.Capacity
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
	RunE: clierrorplus.MaybeDecorateError(runDebugRangeData),
}

func runDebugRangeData(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	earlyBootAccessor := cloud.NewEarlyBootExternalStorageAccessor(serverCfg.Settings, serverCfg.ExternalIODirConfig, cidr.NewLookup(&serverCfg.Settings.SV))
	opts := []storage.ConfigOption{storage.MustExist, storage.RemoteStorageFactory(earlyBootAccessor)}
	if serverCfg.StorageConfig.SharedStorage.URI != "" {
		es, err := cloud.ExternalStorageFromURI(ctx, serverCfg.StorageConfig.SharedStorage.URI,
			base.ExternalIODirConfig{}, serverCfg.Settings, nil, username.RootUserName(), nil,
			nil, cloud.NilMetrics)
		if err != nil {
			return err
		}
		opts = append(opts, storage.SharedStorage(es))
	}
	db, err := OpenEngine(args[0], stopper, fs.ReadOnly, opts...)
	if err != nil {
		return err
	}

	rangeID, err := parseRangeID(args[1])
	if err != nil {
		return err
	}

	desc, err := loadRangeDescriptor(cmd.Context(), db, rangeID)
	if err != nil {
		return err
	}

	snapshot := db.NewSnapshot()
	defer snapshot.Close()

	var results int
	return rditer.IterateReplicaKeySpans(cmd.Context(), &desc, snapshot, debugCtx.replicated,
		rditer.ReplicatedSpansAll,
		func(iter storage.EngineIterator, _ roachpb.Span) error {
			for ok := true; ok && err == nil; ok, err = iter.NextEngineKey() {
				hasPoint, hasRange := iter.HasPointAndRange()
				if hasPoint {
					key, err := iter.UnsafeEngineKey()
					if err != nil {
						return err
					}
					v, err := iter.UnsafeValue()
					if err != nil {
						return err
					}
					kvserver.PrintEngineKeyValue(key, v)
					results++
					if results == debugCtx.maxResults {
						return iterutil.StopIteration()
					}
				}

				if hasRange && iter.RangeKeyChanged() {
					bounds, err := iter.EngineRangeBounds()
					if err != nil {
						return err
					}
					for _, v := range iter.EngineRangeKeys() {
						kvserver.PrintEngineRangeKeyValue(bounds, v)
						results++
						if results == debugCtx.maxResults {
							return iterutil.StopIteration()
						}
					}
				}
			}
			return err
		})
}

var debugRangeDescriptorsCmd = &cobra.Command{
	Use:   "range-descriptors <directory>",
	Short: "print all range descriptors in a store",
	Long: `
Prints all range descriptors in a store with a history of changes.
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runDebugRangeDescriptors),
}

func loadRangeDescriptor(
	ctx context.Context, db storage.Engine, rangeID roachpb.RangeID,
) (roachpb.RangeDescriptor, error) {
	var desc roachpb.RangeDescriptor
	handleKV := func(kv storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
		if kv.Key.Timestamp.IsEmpty() {
			// We only want values, not MVCCMetadata.
			return nil
		}
		if err := kvserver.IsRangeDescriptorKey(kv.Key); err != nil {
			// Range descriptor keys are interleaved with others, so if it
			// doesn't parse as a range descriptor just skip it.
			return nil //nolint:returnerrcheck
		}
		v, err := storage.DecodeMVCCValue(kv.Value)
		if err != nil {
			log.Warningf(context.Background(), "ignoring range descriptor due to error %s: %+v", err, kv)
		}
		if v.IsTombstone() {
			// RangeDescriptor was deleted (range merged away).
			return nil
		}
		if err := v.Value.GetProto(&desc); err != nil {
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
	if err := db.MVCCIterate(
		ctx, start, end, storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsOnly,
		fs.UnknownReadCategory, handleKV); err != nil {
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

	db, err := OpenEngine(args[0], stopper, fs.ReadOnly, storage.MustExist)
	if err != nil {
		return err
	}

	start := keys.LocalRangePrefix
	end := keys.LocalRangeMax

	// NB: Range descriptor keys can have intents.
	return db.MVCCIterate(cmd.Context(), start, end, storage.MVCCKeyAndIntentsIterKind,
		storage.IterKeyTypePointsOnly, fs.UnknownReadCategory,
		func(kv storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
			if kvserver.IsRangeDescriptorKey(kv.Key) != nil {
				return nil
			}
			kvserver.PrintMVCCKeyValue(kv)
			return nil
		})
}

var decodeKeyOptions struct {
	encoding keyFormat
	userKey  bool
}

var debugDecodeKeyCmd = &cobra.Command{
	Use:   "decode-key",
	Short: "decode <key>",
	Long: `
Decode encoded keys provided as command arguments and pretty-print them.
Decode command could be used with either encoded engine keys that contain
timestamp or user keys used in range descriptors, range keys etc.
Key encoding type could be changed using encoding flag.
For example:

	$ cockroach debug decode-key BB89F902ADB43000151C2D1ED07DE6C009
	/Table/51/1/44938288/1521140384.514565824,0
`,
	Args: cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		for _, arg := range args {
			var b []byte
			var err error
			switch decodeKeyOptions.encoding {
			case hexKey:
				b, err = gohex.DecodeString(arg)
			case base64Key:
				b, err = base64.StdEncoding.DecodeString(arg)
			default:
				return errors.Errorf("unsupported key format %d", decodeKeyOptions.encoding)
			}
			if err != nil {
				return err
			}
			if decodeKeyOptions.userKey {
				fmt.Println(roachpb.Key(b))
			} else {
				k, err := storage.DecodeMVCCKey(b)
				if err != nil {
					return err
				}
				fmt.Println(k)
			}
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

		kvserver.PrintMVCCKeyValue(storage.MVCCKeyValue{
			Key:   k,
			Value: bs[1],
		})
		return nil
	},
}

var debugDecodeProtoName string
var debugDecodeProtoEmitDefaults bool
var debugDecodeProtoSingleProto bool
var debugDecodeProtoBinaryOutput bool
var debugDecodeProtoOutputFile string
var debugDecodeProtoCmd = &cobra.Command{
	Use:   "decode-proto",
	Short: "decode-proto <proto> --name=<fully qualified proto name>",
	Long: `
Read from stdin and attempt to decode any hex, base64, or C-escaped encoded
protos and output them as JSON. If --single is specified, the input is expected
to consist of a single encoded proto. Otherwise, the input can consist of
multiple fields, separated by new lines and tabs. Each field is attempted to be
decoded and, if that's unsuccessful, is echoed as is.

The default value for --schema is 'cockroach.sql.sqlbase.Descriptor'.
For example:

$ cat debug/system.descriptor.txt | cockroach debug decode-proto
id	descriptor
1	{"database": {"id": 1, "modificationTime": {}, "name": "system", "privileges": {"users": [{"privileges": 48, "user": "admin"}, {"privileges": 48, "user": "root"}]}}}
...

decode-proto can be used to decode protos as captured by Chrome Dev
Tools from HTTP network requests ("Copy as cURL"). Chrome captures these as
UTF8-encoded raw bytes, which are then rendered as C-escaped strings. The UTF8
encoding breaks the proto encoding, so the curl command doesn't work as Chrome
presents it. To rectify that, take the string argument passed to "curl --data" and pass it to
"cockroach decode-proto --single --binary --out=<file>". Then, to replay the HTTP
request, do something like:
$ curl -X POST  'http://localhost:8080/ts/query' \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/x-protobuf' \
  --data-binary @<file>
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
	RunE: clierrorplus.MaybeDecorateError(runDebugRaftLog),
}

func runDebugRaftLog(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := OpenEngine(args[0], stopper, fs.ReadOnly, storage.MustExist)
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
		string(storage.EncodeMVCCKey(storage.MakeMVCCMetadataKey(start))),
		string(storage.EncodeMVCCKey(storage.MakeMVCCMetadataKey(end))))

	// NB: raft log does not have intents.
	return db.MVCCIterate(cmd.Context(), start, end, storage.MVCCKeyIterKind,
		storage.IterKeyTypePointsOnly, fs.UnknownReadCategory,
		func(kv storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
			kvserver.PrintMVCCKeyValue(kv)
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
	RunE: clierrorplus.MaybeDecorateError(runDebugGCCmd),
}

func runDebugGCCmd(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	var rangeID roachpb.RangeID
	gcTTL := 24 * time.Hour
	lockAgeThreshold := gc.LockAgeThreshold.Default()
	lockBatchSize := gc.MaxLocksPerCleanupBatch.Default()
	txnCleanupThreshold := gc.TxnCleanupThreshold.Default()

	if len(args) > 3 {
		var err error
		if lockAgeThreshold, err = parsePositiveDuration(args[3]); err != nil {
			return errors.Wrapf(err, "unable to parse %v as lock age threshold", args[3])
		}
	}
	if len(args) > 2 {
		gcTTLInSeconds, err := parsePositiveInt(args[2])
		if err != nil {
			return errors.Wrapf(err, "unable to parse %v as TTL", args[2])
		}
		gcTTL = time.Duration(gcTTLInSeconds) * time.Second
	}
	if len(args) > 1 {
		var err error
		if rangeID, err = parseRangeID(args[1]); err != nil {
			return err
		}
	}

	db, err := OpenEngine(args[0], stopper, fs.ReadOnly, storage.MustExist)
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
		//nolint:deferloop TODO(#137605)
		defer snap.Close()
		now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		thresh := gc.CalculateThreshold(now, gcTTL)
		info, err := gc.Run(
			context.Background(),
			&desc, snap,
			now, thresh,
			gc.RunOptions{
				LockAgeThreshold:              lockAgeThreshold,
				MaxLocksPerIntentCleanupBatch: lockBatchSize,
				TxnCleanupThreshold:           txnCleanupThreshold,
			},
			gcTTL, gc.NoopGCer{},
			func(_ context.Context, _ []roachpb.Lock) error { return nil },
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

var debugPebbleOpts = struct {
	sharedStorageURI   string
	exciseTenantID     uint64
	exciseTableID      uint32
	exciseEntireTenant bool
}{}

// debugPebbleCmd is the root of all debug pebble commands.
var debugPebbleCmd = &cobra.Command{
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

var debugCompactOpts = struct {
	maxConcurrency int
}{maxConcurrency: runtime.GOMAXPROCS(0)}

var debugCompactCmd = &cobra.Command{
	Use:   "compact <directory>",
	Short: "compact the sstables in a store",
	Long: `
Compact the sstables in a store.
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runDebugCompact),
}

func runDebugCompact(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := OpenEngine(args[0], stopper, fs.ReadWrite,
		storage.MustExist,
		storage.DisableAutomaticCompactions,
		storage.MaxConcurrentCompactions(debugCompactOpts.maxConcurrency),
		// Currently, any concurrency over 0 enables Writer parallelism.
		storage.MaxWriterConcurrency(1),
		// Force Writer Parallelism will allow Writer parallelism to
		// be enabled without checking the CPU.
		storage.ForceWriterParallelism,
	)
	if err != nil {
		return err
	}

	{
		approxBytesBefore, _, _, err := db.ApproximateDiskBytes(roachpb.KeyMin, roachpb.KeyMax)
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
		approxBytesAfter, _, _, err := db.ApproximateDiskBytes(roachpb.KeyMin, roachpb.KeyMax)
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
	RunE: clierrorplus.MaybeDecorateError(runDebugGossipValues),
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
		status, finish, err := getStatusClient(ctx, serverCfg)
		if err != nil {
			return err
		}
		defer finish()

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
		} else if key == gossip.KeyDeprecatedSystemConfig {
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
		} else if gossip.IsNodeDescKey(key) {
			var desc roachpb.NodeDescriptor
			if err := protoutil.Unmarshal(bytes, &desc); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, desc))
		} else if strings.HasPrefix(key, gossip.KeyStoreDescPrefix) {
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
		} else if strings.HasPrefix(key, gossip.KeyDistSQLDrainingPrefix) {
			var drainingInfo execinfrapb.DistSQLDrainingInfo
			if err := protoutil.Unmarshal(bytes, &drainingInfo); err != nil {
				return "", errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, drainingInfo))
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
	RunE:   clierrorplus.MaybeDecorateError(runDebugSyncBench),
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

var debugMergeLogsCmd = &cobra.Command{
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

// filePattern matches log file paths. Redeclared here from the log package
// due to significant test breakage when adding the fpath named capture group.
const logFilePattern = "^(?:(?P<fpath>.*)/)?" + log.FileNamePattern + "$"

// TODO(knz): this struct belongs elsewhere.
// See: https://github.com/cockroachdb/cockroach/issues/49509
var debugMergeLogsOpts = struct {
	from            time.Time
	to              time.Time
	filter          *regexp.Regexp
	program         *regexp.Regexp
	file            *regexp.Regexp
	keepRedactable  bool
	prefix          string
	redactInput     bool
	format          string
	useColor        forceColor
	tenantIDsFilter []string
}{
	program:        nil, // match everything
	file:           regexp.MustCompile(logFilePattern),
	keepRedactable: true,
	redactInput:    false,
}

func runDebugMergeLogs(cmd *cobra.Command, args []string) error {
	o := debugMergeLogsOpts
	p := newFilePrefixer(withTemplate(o.prefix))

	inputEditMode := log.SelectEditMode(o.redactInput, o.keepRedactable)

	s, err := newMergedStreamFromPatterns(context.Background(),
		args, o.file, o.program, o.from, o.to, inputEditMode, o.format, p)
	if err != nil {
		return err
	}

	// Only auto-detect if auto-detection is needed, as it may fail with an error.
	autoDetect := func(outStream io.Writer) (ttycolor.Profile, error) {
		if f, ok := outStream.(*os.File); ok {
			// If the output is a terminal, auto-detect the color scheme based
			// on that.
			return ttycolor.DetectProfile(f)
		}
		return nil, nil
	}
	outStream := cmd.OutOrStdout()
	var cp ttycolor.Profile
	// Now choose the color profile depending on the user option.
	switch o.useColor {
	case forceColorOff:
		// Nothing to do, cp stays nil.
	case forceColorOn:
		// If there was a color profile auto-detected, we want
		// to use that as it will be tailored to the output terminal.
		var err error
		cp, err = autoDetect(outStream)
		if err != nil || cp == nil {
			// The user requested "forcing" the color mode but
			// auto-detection failed. Ignore the error and use a best guess.
			cp = ttycolor.Profile8
		}
	case forceColorAuto:
		var err error
		cp, err = autoDetect(outStream)
		if err != nil {
			return err
		}
	}

	// Validate tenantIDsFilter
	if len(o.tenantIDsFilter) != 0 {
		for _, tID := range o.tenantIDsFilter {
			number, err := strconv.ParseUint(tID, 10, 64)
			if err != nil {
				return errors.Wrapf(err,
					"invalid tenant ID provided in filter: %s. Tenant IDs must be integers >= 0", tID)
			}
			_, err = roachpb.MakeTenantID(number)
			if err != nil {
				return errors.Wrapf(err,
					"invalid tenant ID provided in filter: %s. Unable to parse into roachpb.TenantID", tID)
			}
		}
	}

	return writeLogStream(s, outStream, o.filter, o.keepRedactable, cp, o.tenantIDsFilter)
}

var debugIntentCount = &cobra.Command{
	Use:   "intent-count <store directory>",
	Short: "return a count of intents in directory",
	Long: `
Returns a count of intents in the store directory. Used to investigate stores
with lots of unresolved intents.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: runDebugIntentCount,
}

func runDebugIntentCount(cmd *cobra.Command, args []string) error {
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	db, err := OpenEngine(args[0], stopper, fs.ReadOnly, storage.MustExist)
	if err != nil {
		return err
	}
	defer db.Close()

	var intentCount int
	var keysCount uint64
	var wg sync.WaitGroup
	closer := make(chan bool)

	wg.Add(1)
	_ = stopper.RunAsyncTask(ctx, "intent-count-progress-indicator", func(ctx context.Context) {
		defer wg.Done()
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		select {
		case <-ticker.C:
			fmt.Printf("scanned %d keys\n", atomic.LoadUint64(&keysCount))
		case <-ctx.Done():
			return
		case <-closer:
			return
		}
	})

	iter, err := db.NewEngineIterator(ctx, storage.IterOptions{
		LowerBound: keys.LockTableSingleKeyStart,
		UpperBound: keys.LockTableSingleKeyEnd,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	seekKey := storage.EngineKey{Key: keys.LockTableSingleKeyStart}

	var valid bool
	for valid, err = iter.SeekEngineKeyGE(seekKey); valid && err == nil; valid, err = iter.NextEngineKey() {
		key, err := iter.EngineKey()
		if err != nil {
			return err
		}
		atomic.AddUint64(&keysCount, 1)
		if key.IsLockTableKey() {
			intentCount++
		}
	}
	if err != nil {
		return err
	}
	close(closer)
	wg.Wait()
	fmt.Printf("intents: %d\n", intentCount)
	return nil
}

// DebugCommandsRequiringEncryption lists debug commands that access Pebble through the engine
// and need encryption flags (injected by CCL code).
// Note: do NOT include commands that just call Pebble code without setting up an engine.
var DebugCommandsRequiringEncryption = []*cobra.Command{
	debugCheckStoreCmd,
	debugCompactCmd,
	debugGCCmd,
	debugIntentCount,
	debugKeysCmd,
	debugRaftLogCmd,
	debugRangeDataCmd,
	debugRangeDescriptorsCmd,
	debugRecoverCollectInfoCmd,
	debugRecoverExecuteCmd,
}

// Debug commands. All commands in this list to be added to root debug command.
var debugCmds = []*cobra.Command{
	debugCheckStoreCmd,
	debugCompactCmd,
	debugGCCmd,
	debugIntentCount,
	debugKeysCmd,
	debugRaftLogCmd,
	debugRangeDataCmd,
	debugRangeDescriptorsCmd,
	debugBallastCmd,
	debugCheckLogConfigCmd,
	debugDecodeKeyCmd,
	debugDecodeValueCmd,
	debugDecodeProtoCmd,
	debugGossipValuesCmd,
	debugTimeSeriesDumpCmd,
	debugSyncBenchCmd,
	debugSyncTestCmd,
	debugEnvCmd,
	debugZipCmd,
	debugMergeLogsCmd,
	debugListFilesCmd,
	debugResetQuorumCmd,
	debugSendKVBatchCmd,
	debugRecoverCmd,
}

// DebugCmd is the root of all debug commands.
var DebugCmd = &cobra.Command{
	Use:   "debug [command]",
	Short: "debugging commands",
	Long: `Various commands for debugging.

These commands are useful for extracting data from the data files of a
process that has failed and cannot restart.
`,
	RunE: UsageAndErr,
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
	fmt.Fprint(f, kvserver.SprintMVCCKeyValue(m.kv, false /* printKey */))
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
var pebbleToolFS = &autoDecryptFS{}

func init() {
	debugZipCmd.AddCommand(debugZipUploadCmd)
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

	// To be able to read Cockroach-written Pebble manifests/SSTables, comparator
	// and merger functions must be specified to pebble that match the ones used
	// to write those files.
	pebbleTool := tool.New(
		tool.Mergers(storage.MVCCMerger),
		tool.DefaultComparer(&storage.EngineComparer),
		tool.KeySchema(storage.DefaultKeySchema),
		tool.KeySchemas(storage.KeySchemas...),
		tool.FS(pebbleToolFS),
		tool.OpenErrEnhancer(func(err error) error {
			if pebble.IsCorruptionError(err) {
				// None of the wrappers provided by the error library allow adding a
				// message that shows up with "%s" after the original message.
				// nolint:errwrap
				return errors.Newf("%v\nIf this is an encrypted store, make sure the correct encryption key is set.", err)
			}
			return err
		}),
		tool.OpenOptions(pebbleOpenOptionLockDir{pebbleToolFS}),
		tool.WithDBExciseSpanFn(pebbleExciseSpanFn),
	)
	debugPebbleCmd.AddCommand(pebbleTool.Commands...)
	f := debugPebbleCmd.PersistentFlags()
	f.StringVarP(&debugPebbleOpts.sharedStorageURI, cliflags.SharedStorage.Name, cliflags.SharedStorage.Shorthand, "", cliflags.SharedStorage.Usage())
	initPebbleCmds(debugPebbleCmd, pebbleTool)
	DebugCmd.AddCommand(debugPebbleCmd)

	doctorExamineCmd.AddCommand(doctorExamineClusterCmd, doctorExamineZipDirCmd)
	doctorRecreateCmd.AddCommand(doctorRecreateClusterCmd, doctorRecreateZipDirCmd)
	debugDoctorCmd.AddCommand(doctorExamineCmd, doctorRecreateCmd, doctorExamineFallbackClusterCmd, doctorExamineFallbackZipDirCmd)
	DebugCmd.AddCommand(debugDoctorCmd)

	DebugCmd.AddCommand(declarativeValidateCorpus)
	DebugCmd.AddCommand(declarativePrintRules)

	debugStatementBundleCmd.AddCommand(statementBundleRecreateCmd)
	DebugCmd.AddCommand(debugStatementBundleCmd)

	DebugCmd.AddCommand(debugJobTraceFromClusterCmd)
	DebugCmd.AddCommand(debugJobCleanupInfoRows)
	f = debugJobCleanupInfoRows.PersistentFlags()
	f.IntVar(&jobCleanupInfoRowOpts.PageSize, "page-size", jobCleanupInfoRowOpts.PageSize,
		"number of deletes to perform per query",
	)
	f.DurationVar(&jobCleanupInfoRowOpts.Age, "age", jobCleanupInfoRowOpts.Age,
		"minimum age of job_info rows to delete; rows younger than this will not be deleted")

	f = debugSyncBenchCmd.Flags()
	f.IntVarP(&syncBenchOpts.Concurrency, "concurrency", "c", syncBenchOpts.Concurrency,
		"number of concurrent writers")
	f.DurationVarP(&syncBenchOpts.Duration, "duration", "d", syncBenchOpts.Duration,
		"duration to run the test for")
	f.BoolVarP(&syncBenchOpts.LogOnly, "log-only", "l", syncBenchOpts.LogOnly,
		"only write to the WAL, not to sstables")

	f = debugCompactCmd.Flags()
	f.IntVarP(&debugCompactOpts.maxConcurrency, "max-concurrency", "c", debugCompactOpts.maxConcurrency,
		"maximum number of concurrent compactions")

	f = debugRecoverCollectInfoCmd.Flags()
	f.VarP(&debugRecoverCollectInfoOpts.Stores, cliflags.RecoverStore.Name, cliflags.RecoverStore.Shorthand, cliflags.RecoverStore.Usage())
	f.IntVarP(&debugRecoverCollectInfoOpts.maxConcurrency, "max-concurrency", "c", debugRecoverDefaultMaxConcurrency,
		"maximum concurrency when fanning out RPCs to nodes in the cluster")

	f = debugRecoverPlanCmd.Flags()
	f.StringVarP(&debugRecoverPlanOpts.outputFileName, "plan", "o", "",
		"filename to write plan to")
	f.IntSliceVar(&debugRecoverPlanOpts.deadStoreIDs, "dead-store-ids", nil,
		"list of dead store IDs (can't be used together with dead-node-ids)")
	f.IntSliceVar(&debugRecoverPlanOpts.deadNodeIDs, "dead-node-ids", nil,
		"list of dead node IDs (can't be used together with dead-store-ids)")
	f.VarP(&debugRecoverPlanOpts.confirmAction, cliflags.ConfirmActions.Name, cliflags.ConfirmActions.Shorthand,
		cliflags.ConfirmActions.Usage())
	f.BoolVar(&debugRecoverPlanOpts.force, "force", false,
		"force creation of plan even when problems were encountered; applying this plan may "+
			"result in additional problems and should be done only with care and as a last resort")
	f.IntVarP(&debugRecoverPlanOpts.maxConcurrency, "max-concurrency", "c", debugRecoverDefaultMaxConcurrency,
		"maximum concurrency when fanning out RPCs to nodes in the cluster")
	f.UintVar(&formatHelper.maxPrintedKeyLength, cliflags.PrintKeyLength.Name,
		formatHelper.maxPrintedKeyLength, cliflags.PrintKeyLength.Usage())

	f = debugRecoverExecuteCmd.Flags()
	f.VarP(&debugRecoverExecuteOpts.Stores, cliflags.RecoverStore.Name, cliflags.RecoverStore.Shorthand, cliflags.RecoverStore.Usage())
	f.VarP(&debugRecoverExecuteOpts.confirmAction, cliflags.ConfirmActions.Name, cliflags.ConfirmActions.Shorthand,
		cliflags.ConfirmActions.Usage())
	f.UintVar(&formatHelper.maxPrintedKeyLength, cliflags.PrintKeyLength.Name,
		formatHelper.maxPrintedKeyLength, cliflags.PrintKeyLength.Usage())
	f.BoolVar(&debugRecoverExecuteOpts.ignoreInternalVersion, cliflags.RecoverIgnoreInternalVersion.Name,
		debugRecoverExecuteOpts.ignoreInternalVersion, cliflags.RecoverIgnoreInternalVersion.Usage())
	f.IntVarP(&debugRecoverExecuteOpts.maxConcurrency, "max-concurrency", "c", debugRecoverDefaultMaxConcurrency,
		"maximum concurrency when fanning out RPCs to nodes in the cluster")

	f = debugRecoverVerifyCmd.Flags()
	f.IntVarP(&debugRecoverVerifyOpts.maxConcurrency, "max-concurrency", "c", debugRecoverDefaultMaxConcurrency,
		"maximum concurrency when fanning out RPCs to nodes in the cluster")

	f = debugMergeLogsCmd.Flags()
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
	f.StringVar(&debugMergeLogsOpts.format, "format", "",
		"log format of the input files")
	f.Var(&debugMergeLogsOpts.useColor, "color",
		"force use of TTY escape codes to colorize the output")
	f.StringSliceVar(&debugMergeLogsOpts.tenantIDsFilter, "tenant-ids", nil,
		"tenant IDs to filter logs by")

	f = debugZipUploadCmd.Flags()
	f.StringVar(&debugZipUploadOpts.ddAPIKey, "dd-api-key", getEnvOrDefault(datadogAPIKeyEnvVar, ""),
		"Datadog API key to use to send debug.zip artifacts to datadog")
	f.StringVar(&debugZipUploadOpts.ddAPPKey, "dd-app-key", getEnvOrDefault(datadogAPPKeyEnvVar, ""),
		"Datadog APP key to use to send debug.zip artifacts to datadog")
	f.StringVar(&debugZipUploadOpts.ddSite, "dd-site", getEnvOrDefault(datadogSiteEnvVar, defaultDDSite),
		"Datadog site to use to send debug.zip artifacts to datadog")
	f.StringSliceVar(&debugZipUploadOpts.include, "include", nil,
		"The debug zip artifacts to include. Possible values: "+strings.Join(zipArtifactTypes, ", "))
	f.StringSliceVar(&debugZipUploadOpts.tags, "tags", nil,
		"Tags to attach to the debug zip artifacts. This can be used to annotate the artifacts with details about the customer."+
			"\nExample: --tags \"env:prod,customer:xyz\"")
	f.StringVar(&debugZipUploadOpts.clusterName, "cluster", "",
		"Name of the cluster to associate with the debug zip artifacts. This can be used to identify data in the upstream observability tool.")
	f.Var(&debugZipUploadOpts.from, "from", "oldest timestamp to include (inclusive)")
	f.Var(&debugZipUploadOpts.to, "to", "newest timestamp to include (inclusive)")
	f.StringVar(&debugZipUploadOpts.logFormat, "log-format", "crdb-v1",
		"log format of the input files")
	// the log-format flag is depricated. It will
	// eventually be removed completely. keeping it hidden for now incase we ever
	// need to specify the log format
	f.Lookup("log-format").Hidden = true
	f.StringVar(&debugZipUploadOpts.gcpProjectID, "gcp-project-id",
		defaultGCPProjectID, "GCP project ID to use to send debug.zip logs to GCS")

	f = debugDecodeKeyCmd.Flags()
	f.Var(&decodeKeyOptions.encoding, "encoding", "key argument encoding")
	f.BoolVar(&decodeKeyOptions.userKey, "user-key", false, "key type")

	f = debugDecodeProtoCmd.Flags()
	f.StringVar(&debugDecodeProtoName, "schema", "cockroach.sql.sqlbase.Descriptor",
		"fully qualified name of the proto to decode")
	f.BoolVar(&debugDecodeProtoEmitDefaults, "emit-defaults", false,
		"encode default values for every field")
	f.BoolVar(&debugDecodeProtoSingleProto, "single", false,
		"treat the input as a single field")
	f.BoolVar(&debugDecodeProtoBinaryOutput, "binary", false,
		"output the protos as binary instead of JSON. If specified, --out also needs to be specified.")
	f.StringVar(&debugDecodeProtoOutputFile, "out", "",
		"path to output file. If not specified, output goes to stdout.")

	f = debugCheckLogConfigCmd.Flags()
	f.Var(&debugLogChanSel, "only-channels", "selection of channels to include in the output diagram.")

	f = debugTimeSeriesDumpCmd.Flags()
	f.Var(&debugTimeSeriesDumpOpts.format, "format", "output format (text, csv, tsv, raw, openmetrics)")
	f.Var(&debugTimeSeriesDumpOpts.from, "from", "oldest timestamp to include (inclusive)")
	f.Var(&debugTimeSeriesDumpOpts.to, "to", "newest timestamp to include (inclusive)")
	f.StringVar(&debugTimeSeriesDumpOpts.clusterLabel, "cluster-label",
		"", "prometheus label for cluster name")
	f.StringVar(&debugTimeSeriesDumpOpts.yaml, "yaml", debugTimeSeriesDumpOpts.yaml, "full path to create the tsdump.yaml with storeID: nodeID mappings (raw format only). This file is required when loading the raw tsdump for troubleshooting.")
	f.StringVar(&debugTimeSeriesDumpOpts.targetURL, "target-url", "", "target URL to send openmetrics data over HTTP")
	f.StringVar(&debugTimeSeriesDumpOpts.ddSite, "dd-site", getEnvOrDefault(datadogSiteEnvVar, defaultDDSite),
		"Datadog site to use to send tsdump artifacts to datadog")
	f.StringVar(&debugTimeSeriesDumpOpts.ddApiKey, "dd-api-key", getEnvOrDefault(datadogAPIKeyEnvVar, ""),
		"Datadog API key to use to send to the datadog formatter")
	f.StringVar(&debugTimeSeriesDumpOpts.httpToken, "http-token", "", "HTTP header to use with the json export format")
	f.StringVar(&debugTimeSeriesDumpOpts.clusterID, "cluster-id", "", "cluster ID to use in datadog upload")
	f.StringVar(&debugTimeSeriesDumpOpts.zendeskTicket, "zendesk-ticket", "", "zendesk ticket to use in datadog upload")
	f.StringVar(&debugTimeSeriesDumpOpts.organizationName, "org-name", "", "organization name to use in datadog upload")
	f.StringVar(&debugTimeSeriesDumpOpts.userName, "user-name", "", "name of the user to perform datadog upload")

	f = debugSendKVBatchCmd.Flags()
	f.StringVar(&debugSendKVBatchContext.traceFormat, "trace", debugSendKVBatchContext.traceFormat,
		"which format to use for the trace output (off, text, jaeger)")
	f.BoolVar(&debugSendKVBatchContext.keepCollectedSpans, "keep-collected-spans", debugSendKVBatchContext.keepCollectedSpans,
		"whether to keep the CollectedSpans field on the response, to learn about how traces work")
	f.StringVar(&debugSendKVBatchContext.traceFile, "trace-output", debugSendKVBatchContext.traceFile,
		"the output file to use for the trace. If left empty, output to stderr.")
}

func initPebbleCmds(cmd *cobra.Command, pebbleTool *tool.T) {
	for _, c := range cmd.Commands() {
		wrapped := c.PreRunE
		c.PreRunE = func(cmd *cobra.Command, args []string) error {
			if wrapped != nil {
				if err := wrapped(cmd, args); err != nil {
					return err
				}
			}
			if debugPebbleOpts.sharedStorageURI != "" {
				es, err := cloud.ExternalStorageFromURI(
					context.Background(),
					debugPebbleOpts.sharedStorageURI,
					base.ExternalIODirConfig{},
					cluster.MakeClusterSettings(),
					nil, /* blobClientFactory: */
					username.PublicRoleName(),
					nil, /* db */
					nil, /* limiters */
					cloud.NilMetrics,
				)
				if err != nil {
					return err
				}
				wrapper := storage.MakeExternalStorageWrapper(context.Background(), es)
				factory := remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
					"": wrapper,
				})
				pebbleTool.ConfigureSharedStorage(factory, remote.CreateOnSharedLower, "" /* createOnSharedLocator */)
			}
			pebbleCryptoInitializer(cmd.Context())
			return nil
		}
		if c.Name() == "excise" {
			f := c.Flags()
			f.Uint64Var(&debugPebbleOpts.exciseTenantID, "tenant-id", 0, "tenant ID for table to excise (must be used in conjunction with --table-id or --entire-tenant)")
			f.Uint32Var(&debugPebbleOpts.exciseTableID, "table-id", 0, "table ID to excise (must be used in conjunction with --tenant-id)")
			f.BoolVar(&debugPebbleOpts.exciseEntireTenant, "entire-tenant", false, "excise the entire tenant (must be used in conjunction with --tenant-id)")
		}
		initPebbleCmds(c, pebbleTool)
	}
}

// pebbleExciseSpanFn implements tool.DBExciseSpanFn. It returns a span
// for a table if the table/tenant ID flags are used.
func pebbleExciseSpanFn() (pebble.KeyRange, error) {
	tenant := debugPebbleOpts.exciseTenantID
	tableID := debugPebbleOpts.exciseTableID

	if tableID != 0 && debugPebbleOpts.exciseEntireTenant {
		return pebble.KeyRange{}, errors.Errorf("--table-id and --entire-tenant cannot be used together")
	}

	if tenant == 0 {
		if tableID != 0 {
			return pebble.KeyRange{}, errors.Errorf("--table-id must be used with --tenant-id")
		}
		if debugPebbleOpts.exciseEntireTenant {
			return pebble.KeyRange{}, errors.Errorf("--entire-tenant must be used with --tenant-id")
		}
		// Fall back to using the normal flags.
		return pebble.KeyRange{}, nil
	}

	if tableID == 0 && !debugPebbleOpts.exciseEntireTenant {
		return pebble.KeyRange{}, errors.Errorf("--tenant-id must be used with either --table-id or --entire-tenant")
	}
	tenantID, err := roachpb.MakeTenantID(tenant)
	if err != nil {
		return pebble.KeyRange{}, err
	}
	codec := keys.MakeSQLCodec(tenantID)
	var start, end storage.EngineKey
	if tableID != 0 {
		start.Key = codec.TablePrefix(tableID)
		end.Key = start.Key.PrefixEnd()
	} else {
		if codec.ForSystemTenant() {
			return pebble.KeyRange{}, errors.Errorf("cannot excise the entire system tenant")
		}
		start.Key = codec.TenantPrefix()
		end.Key = codec.TenantEndKey()
	}
	return pebble.KeyRange{
		Start: start.Encode(),
		End:   end.Encode(),
	}, nil
}

func pebbleCryptoInitializer(ctx context.Context) {
	var encryptedPaths []string
	for _, spec := range encryptionSpecs.Specs {
		encryptedPaths = append(encryptedPaths, spec.Path)
	}
	resolveFn := func(dir string) (*fs.Env, error) {
		var envConfig fs.EnvConfig
		if err := fillEncryptionOptionsForStore(dir, &envConfig); err != nil {
			return nil, err
		}
		env, err := fs.InitEnv(ctx, vfs.Default, dir, envConfig, nil /* diskWriteStats */)
		if err != nil {
			return nil, err
		}
		return env, nil
	}
	pebbleToolFS.Init(encryptedPaths, resolveFn)
}
