// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/pebbleiter"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/replay"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	humanize "github.com/dustin/go-humanize"
)

// ValueBlocksEnabled controls whether older versions of MVCC keys in the same
// sstable will have their values written to value blocks. This only affects
// sstables that will be written in the future, as part of flushes or
// compactions, and does not eagerly change the encoding of existing sstables.
// Reads can correctly read both kinds of sstables.
var ValueBlocksEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel, // used for temp storage in virtual cluster servers
	"storage.value_blocks.enabled",
	"set to true to enable writing of value blocks in sstables",
	util.ConstantWithMetamorphicTestBool(
		"storage.value_blocks.enabled", true),
	settings.WithPublic)

// UseEFOS controls whether uses of pebble Snapshots should use
// EventuallyFileOnlySnapshots instead. This reduces write-amp with the main
// tradeoff being higher space-amp. Note that UseExciseForSnapshot, if true,
// effectively causes EventuallyFileOnlySnapshots to be used as well.
//
// Note: Do NOT read this setting directly. Use ShouldUseEFOS() instead.
var UseEFOS = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"storage.experimental.eventually_file_only_snapshots.enabled",
	"set to false to disable eventually-file-only-snapshots (kv.snapshot_receiver.excise.enabled must also be false)",
	util.ConstantWithMetamorphicTestBool(
		"storage.experimental.eventually_file_only_snapshots.enabled", true), /* defaultValue */
	settings.WithPublic)

// UseExciseForSnapshots controls whether virtual-sstable-based excises should
// be used instead of range deletions for clearing out replica contents as part
// of a rebalance/recovery snapshot application. Applied on the receiver side.
// Note that setting this setting to true also effectively causes UseEFOS above
// to become true. This interaction is why this setting is defined in the
// storage package even though it mostly affects KV.
var UseExciseForSnapshots = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.snapshot_receiver.excise.enabled",
	"set to false to disable excises in place of range deletions for KV snapshots",
	util.ConstantWithMetamorphicTestBool(
		"kv.snapshot_receiver.excise.enabled", true), /* defaultValue */
	settings.WithPublic,
)

// IngestSplitEnabled controls whether ingest-time splitting is enabled in
// Pebble. This feature allows for existing sstables to be split into multiple
// virtual sstables at ingest time if that allows for an ingestion sstable to go
// into a lower level than it would otherwise be in. No keys are masked with
// this split; it only happens if there are no keys in that existing sstable
// in the span of the incoming sstable.
var IngestSplitEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"storage.ingest_split.enabled",
	"set to false to disable ingest-time splitting that lowers write-amplification",
	util.ConstantWithMetamorphicTestBool(
		"storage.ingest_split.enabled", true), /* defaultValue */
	settings.WithPublic,
)

// IngestAsFlushable controls whether ingested sstables that overlap the
// memtable may be lazily ingested: written to the WAL and enqueued in the list
// of flushables (eg, memtables, large batches and now lazily-ingested
// sstables). This only affects sstables that are ingested in the future. If a
// sstable was already lazily ingested but not flushed, a crash and subsequent
// recovery will still enqueue the sstables as flushable when the ingest's WAL
// entry is replayed.
//
// This cluster setting will be removed in a subsequent release.
var IngestAsFlushable = settings.RegisterBoolSetting(
	settings.ApplicationLevel, // used to init temp storage in virtual cluster servers
	"storage.ingest_as_flushable.enabled",
	"set to true to enable lazy ingestion of sstables",
	util.ConstantWithMetamorphicTestBool(
		"storage.ingest_as_flushable.enabled", true))

const (
	compressionAlgorithmSnappy int64 = 1
	compressionAlgorithmZstd   int64 = 2
)

// compressionAlgorithm determines the compression algorithm used to compress
// data blocks when writing sstables. Users should call getCompressionAlgorithm
// rather than calling compressionAlgorithm.Get directly.
var compressionAlgorithm = settings.RegisterEnumSetting(
	// NB: We can't use settings.SystemOnly today because we may need to read the
	// value from within a tenant building an sstable for AddSSTable.
	settings.SystemVisible,
	"storage.sstable.compression_algorithm",
	`determines the compression algorithm to use when compressing sstable data blocks;`+
		` supported values: "snappy", "zstd"`,
	// TODO(jackson): Consider using a metamorphic constant here, but many tests
	// will need to override it because they depend on a deterministic sstable
	// size.
	"snappy",
	map[int64]string{
		compressionAlgorithmSnappy: "snappy",
		compressionAlgorithmZstd:   "zstd",
	},
	settings.WithPublic,
)

func getCompressionAlgorithm(ctx context.Context, settings *cluster.Settings) pebble.Compression {
	switch compressionAlgorithm.Get(&settings.SV) {
	case compressionAlgorithmSnappy:
		return pebble.SnappyCompression
	case compressionAlgorithmZstd:
		// Pre-24.1 Pebble's implementation of zstd had bugs that could cause
		// in-memory corruption. We require that the cluster version is 24.1 which
		// implies that all nodes are running 24.1 code and will never run code
		// < 24.1 again.
		if settings.Version.ActiveVersionOrEmpty(ctx).IsActive(clusterversion.V24_1) {
			return pebble.ZstdCompression
		}
		return pebble.DefaultCompression
	default:
		return pebble.DefaultCompression
	}
}

// DO NOT set storage.single_delete.crash_on_invariant_violation.enabled or
// storage.single_delete.crash_on_ineffectual.enabled to true.
//
// Pebble's delete-only compactions can cause a recent RANGEDEL to peek below
// an older SINGLEDEL and delete an arbitrary subset of data below that
// SINGLEDEL. When that SINGLEDEL gets compacted (without the RANGEDEL), any
// of these callbacks can happen, without it being a real correctness problem.
//
// Example 1:
// RANGEDEL [a, c)#10 in L0
// SINGLEDEL b#5 in L1
// SET b#3 in L6
//
// If the L6 file containing the SET is narrow and the L1 file containing the
// SINGLEDEL is wide, a delete-only compaction can remove the file in L2
// before the SINGLEDEL is compacted down. Then when the SINGLEDEL is
// compacted down, it will not find any SET to delete, resulting in the
// ineffectual callback.
//
// Example 2:
// RANGEDEL [a, z)#60 in L0
// SINGLEDEL g#50 in L1
// SET g#40 in L2
// RANGEDEL [g,h)#30 in L3
// SET g#20 in L6
//
// In this example, the two SETs represent the same intent, and the RANGEDELs
// are caused by the CRDB range being dropped. That is, the transaction wrote
// the intent once, range was dropped, then added back, which caused the SET
// again, then the transaction committed, causing a SINGLEDEL, and then the
// range was dropped again. The older RANGEDEL can get fragmented due to
// compactions it has been part of. Say this L3 file containing the RANGEDEL
// is very narrow, while the L1, L2, L6 files are wider than the RANGEDEL in
// L0. Then the RANGEDEL in L3 can be dropped using a delete-only compaction,
// resulting in an LSM with state:
//
// RANGEDEL [a, z)#60 in L0
// SINGLEDEL g#50 in L1
// SET g#40 in L2
// SET g#20 in L6
//
// A multi-level compaction involving L1, L2, L6 will cause the invariant
// violation callback. This example doesn't need multi-level compactions: say
// there was a Pebble snapshot at g#21 preventing g#20 from being dropped when
// it meets g#40 in a compaction. That snapshot will not save RANGEDEL
// [g,h)#30, so we can have:
//
// SINGLEDEL g#50 in L1
// SET g#40, SET g#20 in L6
//
// And say the snapshot is removed and then the L1 and L6 compaction happens,
// resulting in the invariant violation callback.
//
// TODO(sumeer): remove these cluster settings or figure out a way to bring
// back some invariant checking.

var SingleDeleteCrashOnInvariantViolation = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"storage.single_delete.crash_on_invariant_violation.enabled",
	"set to true to crash if the single delete invariant is violated",
	false,
	settings.WithVisibility(settings.Reserved),
)

var SingleDeleteCrashOnIneffectual = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"storage.single_delete.crash_on_ineffectual.enabled",
	"set to true to crash if the single delete was ineffectual",
	false,
	settings.WithVisibility(settings.Reserved),
)

var walFailoverUnhealthyOpThreshold = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"storage.wal_failover.unhealthy_op_threshold",
	"the latency of a WAL write considered unhealthy and triggers a failover to a secondary WAL location",
	100*time.Millisecond,
	settings.WithPublic,
)

// ShouldUseEFOS returns true if either of the UseEFOS or UseExciseForSnapshots
// cluster settings are enabled, and EventuallyFileOnlySnapshots must be used
// to guarantee snapshot-like semantics.
func ShouldUseEFOS(settings *settings.Values) bool {
	return UseEFOS.Get(settings) || UseExciseForSnapshots.Get(settings)
}

// EngineKeyCompare compares cockroach keys, including the version (which
// could be MVCC timestamps).
func EngineKeyCompare(a, b []byte) int {
	// NB: For performance, this routine manually splits the key into the
	// user-key and version components rather than using DecodeEngineKey. In
	// most situations, use DecodeEngineKey or GetKeyPartFromEngineKey or
	// SplitMVCCKey instead of doing this.
	aEnd := len(a) - 1
	bEnd := len(b) - 1
	if aEnd < 0 || bEnd < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Compare(a, b)
	}

	// Compute the index of the separator between the key and the version. If the
	// separator is found to be at -1 for both keys, then we are comparing bare
	// suffixes without a user key part. Pebble requires bare suffixes to be
	// comparable with the same ordering as if they had a common user key.
	aSep := aEnd - int(a[aEnd])
	bSep := bEnd - int(b[bEnd])
	if aSep == -1 && bSep == -1 {
		aSep, bSep = 0, 0 // comparing bare suffixes
	}
	if aSep < 0 || bSep < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Compare(a, b)
	}

	// Compare the "user key" part of the key.
	if c := bytes.Compare(a[:aSep], b[:bSep]); c != 0 {
		return c
	}

	// Compare the version part of the key. Note that when the version is a
	// timestamp, the timestamp encoding causes byte comparison to be equivalent
	// to timestamp comparison.
	aVer := a[aSep:aEnd]
	bVer := b[bSep:bEnd]
	if len(aVer) == 0 {
		if len(bVer) == 0 {
			return 0
		}
		return -1
	} else if len(bVer) == 0 {
		return 1
	}
	aVer = normalizeEngineKeyVersionForCompare(aVer)
	bVer = normalizeEngineKeyVersionForCompare(bVer)
	return bytes.Compare(bVer, aVer)
}

// EngineKeyEqual checks for equality of cockroach keys, including the version
// (which could be MVCC timestamps).
func EngineKeyEqual(a, b []byte) bool {
	// NB: For performance, this routine manually splits the key into the
	// user-key and version components rather than using DecodeEngineKey. In
	// most situations, use DecodeEngineKey or GetKeyPartFromEngineKey or
	// SplitMVCCKey instead of doing this.
	aEnd := len(a) - 1
	bEnd := len(b) - 1
	if aEnd < 0 || bEnd < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Equal(a, b)
	}

	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	aVerLen := int(a[aEnd])
	bVerLen := int(b[bEnd])

	// Fast-path. If the key version is empty or contains only a walltime
	// component then normalizeEngineKeyVersionForCompare is a no-op, so we don't
	// need to split the "user key" from the version suffix before comparing to
	// compute equality. Instead, we can check for byte equality immediately.
	const withWall = mvccEncodedTimeSentinelLen + mvccEncodedTimeWallLen
	const withLockTableLen = mvccEncodedTimeSentinelLen + engineKeyVersionLockTableLen
	if (aVerLen <= withWall && bVerLen <= withWall) || (aVerLen == withLockTableLen && bVerLen == withLockTableLen) {
		return bytes.Equal(a, b)
	}

	// Compute the index of the separator between the key and the version. If the
	// separator is found to be at -1 for both keys, then we are comparing bare
	// suffixes without a user key part. Pebble requires bare suffixes to be
	// comparable with the same ordering as if they had a common user key.
	aSep := aEnd - aVerLen
	bSep := bEnd - bVerLen
	if aSep == -1 && bSep == -1 {
		aSep, bSep = 0, 0 // comparing bare suffixes
	}
	if aSep < 0 || bSep < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Equal(a, b)
	}

	// Compare the "user key" part of the key.
	if !bytes.Equal(a[:aSep], b[:bSep]) {
		return false
	}

	// Compare the version part of the key.
	aVer := a[aSep:aEnd]
	bVer := b[bSep:bEnd]
	aVer = normalizeEngineKeyVersionForCompare(aVer)
	bVer = normalizeEngineKeyVersionForCompare(bVer)
	return bytes.Equal(aVer, bVer)
}

var zeroLogical [mvccEncodedTimeLogicalLen]byte

//gcassert:inline
func normalizeEngineKeyVersionForCompare(a []byte) []byte {
	// In general, the version could also be a non-timestamp version, but we know
	// that engineKeyVersionLockTableLen+mvccEncodedTimeSentinelLen is a different
	// constant than the above, so there is no danger here of stripping parts from
	// a non-timestamp version.
	const withWall = mvccEncodedTimeSentinelLen + mvccEncodedTimeWallLen
	const withLogical = withWall + mvccEncodedTimeLogicalLen
	const withSynthetic = withLogical + mvccEncodedTimeSyntheticLen
	if len(a) == withSynthetic {
		// Strip the synthetic bit component from the timestamp version. The
		// presence of the synthetic bit does not affect key ordering or equality.
		a = a[:withLogical]
	}
	if len(a) == withLogical {
		// If the timestamp version contains a logical timestamp component that is
		// zero, strip the component. encodeMVCCTimestampToBuf will typically omit
		// the entire logical component in these cases as an optimization, but it
		// does not guarantee to never include a zero logical component.
		// Additionally, we can fall into this case after stripping off other
		// components of the key version earlier on in this function.
		if bytes.Equal(a[withWall:], zeroLogical[:]) {
			a = a[:withWall]
		}
	}
	return a
}

// EngineComparer is a pebble.Comparer object that implements MVCC-specific
// comparator settings for use with Pebble.
var EngineComparer = &pebble.Comparer{
	Compare: EngineKeyCompare,

	Equal: EngineKeyEqual,

	AbbreviatedKey: func(k []byte) uint64 {
		key, ok := GetKeyPartFromEngineKey(k)
		if !ok {
			return 0
		}
		return pebble.DefaultComparer.AbbreviatedKey(key)
	},

	FormatKey: func(k []byte) fmt.Formatter {
		decoded, ok := DecodeEngineKey(k)
		if !ok {
			return mvccKeyFormatter{err: errors.Errorf("invalid encoded engine key: %x", k)}
		}
		if decoded.IsMVCCKey() {
			mvccKey, err := decoded.ToMVCCKey()
			if err != nil {
				return mvccKeyFormatter{err: err}
			}
			return mvccKeyFormatter{key: mvccKey}
		}
		return EngineKeyFormatter{key: decoded}
	},

	Separator: func(dst, a, b []byte) []byte {
		aKey, ok := GetKeyPartFromEngineKey(a)
		if !ok {
			return append(dst, a...)
		}
		bKey, ok := GetKeyPartFromEngineKey(b)
		if !ok {
			return append(dst, a...)
		}
		// If the keys are the same just return a.
		if bytes.Equal(aKey, bKey) {
			return append(dst, a...)
		}
		n := len(dst)
		// Engine key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Separator implementation.
		dst = pebble.DefaultComparer.Separator(dst, aKey, bKey)
		// Did it pick a separator different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The separator is > aKey, so we only need to add the sentinel.
		return append(dst, 0)
	},

	Successor: func(dst, a []byte) []byte {
		aKey, ok := GetKeyPartFromEngineKey(a)
		if !ok {
			return append(dst, a...)
		}
		n := len(dst)
		// Engine key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's Successor implementation.
		dst = pebble.DefaultComparer.Successor(dst, aKey)
		// Did it pick a successor different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The successor is > aKey, so we only need to add the sentinel.
		return append(dst, 0)
	},

	ImmediateSuccessor: func(dst, a []byte) []byte {
		// The key `a` is guaranteed to be a bare prefix: It's a
		// `engineKeyNoVersion` key without a version—just a trailing 0-byte to
		// signify the length of the version. For example the user key "foo" is
		// encoded as: "foo\0". We need to encode the immediate successor to
		// "foo", which in the natural byte ordering is "foo\0".  Append a
		// single additional zero, to encode the user key "foo\0" with a
		// zero-length version.
		return append(append(dst, a...), 0)
	},

	Split: func(k []byte) int {
		keyLen := len(k)
		if keyLen == 0 {
			return 0
		}
		// Last byte is the version length + 1 when there is a version,
		// else it is 0.
		versionLen := int(k[keyLen-1])
		// keyPartEnd points to the sentinel byte.
		keyPartEnd := keyLen - 1 - versionLen
		if keyPartEnd < 0 {
			return keyLen
		}
		// Pebble requires that keys generated via a split be comparable with
		// normal encoded engine keys. Encoded engine keys have a suffix
		// indicating the number of bytes of version data. Engine keys without a
		// version have a suffix of 0. We're careful in EncodeKey to make sure
		// that the user-key always has a trailing 0. If there is no version this
		// falls out naturally. If there is a version we prepend a 0 to the
		// encoded version data.
		return keyPartEnd + 1
	},

	Name: "cockroach_comparator",
}

// MVCCMerger is a pebble.Merger object that implements the merge operator used
// by Cockroach.
var MVCCMerger = &pebble.Merger{
	Name: "cockroach_merge_operator",
	Merge: func(_, value []byte) (pebble.ValueMerger, error) {
		res := &MVCCValueMerger{}
		err := res.MergeNewer(value)
		if err != nil {
			return nil, err
		}
		return res, nil
	},
}

var _ sstable.BlockIntervalSyntheticReplacer = MVCCBlockIntervalSyntheticReplacer{}

type MVCCBlockIntervalSyntheticReplacer struct{}

func (mbsr MVCCBlockIntervalSyntheticReplacer) AdjustIntervalWithSyntheticSuffix(
	lower uint64, upper uint64, suffix []byte,
) (adjustedLower uint64, adjustedUpper uint64, err error) {
	synthDecoded, err := DecodeMVCCTimestampSuffix(suffix)
	if err != nil {
		return 0, 0, errors.AssertionFailedf("could not decode synthetic suffix")
	}
	synthDecodedWalltime := uint64(synthDecoded.WallTime)
	if upper >= synthDecodedWalltime {
		return 0, 0, errors.AssertionFailedf("the synthetic suffix %d is less than or equal to the original upper bound %d", synthDecoded, upper)
	}
	// The returned bound includes the synthetic suffix, regardless of its logical
	// component.
	return synthDecodedWalltime, synthDecodedWalltime + 1, nil
}

// pebbleDataBlockMVCCTimeIntervalPointCollector implements
// pebble.DataBlockIntervalCollector for point keys.
type pebbleDataBlockMVCCTimeIntervalPointCollector struct {
	pebbleDataBlockMVCCTimeIntervalCollector
}

var (
	_ sstable.DataBlockIntervalCollector      = (*pebbleDataBlockMVCCTimeIntervalPointCollector)(nil)
	_ sstable.SuffixReplaceableBlockCollector = (*pebbleDataBlockMVCCTimeIntervalPointCollector)(nil)
)

func (tc *pebbleDataBlockMVCCTimeIntervalPointCollector) Add(
	key pebble.InternalKey, _ []byte,
) error {
	return tc.add(key.UserKey)
}

// pebbleDataBlockMVCCTimeIntervalRangeCollector implements
// pebble.DataBlockIntervalCollector for range keys.
type pebbleDataBlockMVCCTimeIntervalRangeCollector struct {
	pebbleDataBlockMVCCTimeIntervalCollector
}

var (
	_ sstable.DataBlockIntervalCollector      = (*pebbleDataBlockMVCCTimeIntervalRangeCollector)(nil)
	_ sstable.SuffixReplaceableBlockCollector = (*pebbleDataBlockMVCCTimeIntervalRangeCollector)(nil)
)

func (tc *pebbleDataBlockMVCCTimeIntervalRangeCollector) Add(
	key pebble.InternalKey, value []byte,
) error {
	// TODO(erikgrinaker): should reuse a buffer for keysDst, but keyspan.Key is
	// not exported by Pebble.
	span, err := rangekey.Decode(key, value, nil)
	if err != nil {
		return errors.Wrapf(err, "decoding range key at %s", key)
	}
	for _, k := range span.Keys {
		if err := tc.add(k.Suffix); err != nil {
			return errors.Wrapf(err, "recording suffix %x for range key at %s", k.Suffix, key)
		}
	}
	return nil
}

// pebbleDataBlockMVCCTimeIntervalCollector is a helper for a
// pebble.DataBlockIntervalCollector that is used to construct a
// pebble.BlockPropertyCollector. This provides per-block filtering, which
// also gets aggregated to the sstable-level and filters out sstables. It must
// only be used for MVCCKeyIterKind iterators, since it will ignore
// blocks/sstables that contain intents (and any other key that is not a real
// MVCC key).
//
// This is wrapped by structs for point or range key collection, which actually
// implement pebble.DataBlockIntervalCollector.
type pebbleDataBlockMVCCTimeIntervalCollector struct {
	// min, max are the encoded timestamps.
	min, max []byte
}

// add collects the given slice in the collector. The slice may be an entire
// encoded MVCC key, or the bare suffix of an encoded key.
func (tc *pebbleDataBlockMVCCTimeIntervalCollector) add(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(b[len(b)-1])
	if versionLen == 0 {
		// This is not an MVCC key that we can collect.
		return nil
	}
	// prefixPartEnd points to the sentinel byte, unless this is a bare suffix, in
	// which case the index is -1.
	prefixPartEnd := len(b) - 1 - versionLen
	// Sanity check: the index should be >= -1. Additionally, if the index is >=
	// 0, it should point to the sentinel byte, as this is a full EngineKey.
	if prefixPartEnd < -1 || (prefixPartEnd >= 0 && b[prefixPartEnd] != sentinel) {
		return errors.Errorf("invalid key %s", roachpb.Key(b).String())
	}
	// We don't need the last byte (the version length).
	versionLen--
	// Only collect if this looks like an MVCC timestamp.
	if versionLen == engineKeyVersionWallTimeLen ||
		versionLen == engineKeyVersionWallAndLogicalTimeLen ||
		versionLen == engineKeyVersionWallLogicalAndSyntheticTimeLen {
		// INVARIANT: -1 <= prefixPartEnd < len(b) - 1.
		// Version consists of the bytes after the sentinel and before the length.
		b = b[prefixPartEnd+1 : len(b)-1]
		// Lexicographic comparison on the encoded timestamps is equivalent to the
		// comparison on decoded timestamps, so delay decoding.
		if len(tc.min) == 0 || bytes.Compare(b, tc.min) < 0 {
			tc.min = append(tc.min[:0], b...)
		}
		if len(tc.max) == 0 || bytes.Compare(b, tc.max) > 0 {
			tc.max = append(tc.max[:0], b...)
		}
	}
	return nil
}

func decodeWallTime(ts []byte) uint64 {
	return binary.BigEndian.Uint64(ts[0:engineKeyVersionWallTimeLen])
}

func (tc *pebbleDataBlockMVCCTimeIntervalCollector) FinishDataBlock() (
	lower uint64,
	upper uint64,
	err error,
) {
	if len(tc.min) == 0 {
		// No calls to Add that contained a timestamped key.
		return 0, 0, nil
	}
	// Construct a [lower, upper) walltime that will contain all the
	// hlc.Timestamps in this block.
	lower = decodeWallTime(tc.min)
	// Remember that we have to reset tc.min and tc.max to get ready for the
	// next data block, as specified in the DataBlockIntervalCollector interface
	// help and help too.
	tc.min = tc.min[:0]
	// The actual value encoded into walltime is an int64, so +1 will not
	// overflow.
	//
	// Note that the timestamp with a wall time tc.max+1 and no logical component is
	// a valid exclusive upper bound for timestamps with wall time tc.max and any
	// logical component.
	upper = decodeWallTime(tc.max) + 1
	tc.max = tc.max[:0]
	if lower >= upper {
		return 0, 0,
			errors.Errorf("corrupt timestamps lower %d >= upper %d", lower, upper)
	}
	return lower, upper, nil
}

func (tc *pebbleDataBlockMVCCTimeIntervalCollector) UpdateKeySuffixes(
	_ []byte, _, newSuffix []byte,
) error {
	return tc.add(newSuffix)
}

const mvccWallTimeIntervalCollector = "MVCCTimeInterval"

var _ pebble.BlockPropertyFilterMask = (*mvccWallTimeIntervalRangeKeyMask)(nil)

type mvccWallTimeIntervalRangeKeyMask struct {
	sstable.BlockIntervalFilter
}

// SetSuffix implements the pebble.BlockPropertyFilterMask interface.
func (m *mvccWallTimeIntervalRangeKeyMask) SetSuffix(suffix []byte) error {
	if len(suffix) == 0 {
		// This is currently impossible, because the only range key Cockroach
		// writes today is the MVCC Delete Range that's always suffixed.
		return nil
	}
	ts, err := DecodeMVCCTimestampSuffix(suffix)
	if err != nil {
		return err
	}
	m.BlockIntervalFilter.SetInterval(uint64(ts.WallTime), math.MaxUint64)
	return nil
}

// PebbleBlockPropertyCollectors is the list of functions to construct
// BlockPropertyCollectors.
var PebbleBlockPropertyCollectors = []func() pebble.BlockPropertyCollector{
	func() pebble.BlockPropertyCollector {
		return sstable.NewBlockIntervalCollector(
			mvccWallTimeIntervalCollector,
			&pebbleDataBlockMVCCTimeIntervalPointCollector{},
			&pebbleDataBlockMVCCTimeIntervalRangeCollector{},
		)
	},
}

// MinimumSupportedFormatVersion is the version that provides features that the
// Cockroach code relies on unconditionally (like range keys). New stores are by
// default created with this version. It should correspond to the minimum
// supported binary version.
const MinimumSupportedFormatVersion = pebble.FormatFlushableIngest

// DefaultPebbleOptions returns the default pebble options.
func DefaultPebbleOptions() *pebble.Options {
	opts := &pebble.Options{
		Comparer: EngineComparer,
		FS:       vfs.Default,
		// A value of 2 triggers a compaction when there is 1 sub-level.
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 1000,
		LBaseMaxBytes:         64 << 20, // 64 MB
		Levels:                make([]pebble.LevelOptions, 7),
		// NB: Options.MaxConcurrentCompactions may be overidden in NewPebble to
		// allow overriding the max at runtime through
		// Engine.SetCompactionConcurrency.
		MaxConcurrentCompactions:    getMaxConcurrentCompactions,
		MemTableSize:                64 << 20, // 64 MB
		MemTableStopWritesThreshold: 4,
		Merger:                      MVCCMerger,
		BlockPropertyCollectors:     PebbleBlockPropertyCollectors,
		FormatMajorVersion:          MinimumSupportedFormatVersion,
	}
	opts.Experimental.L0CompactionConcurrency = l0SubLevelCompactionConcurrency
	// Automatically flush 10s after the first range tombstone is added to a
	// memtable. This ensures that we can reclaim space even when there's no
	// activity on the database generating flushes.
	opts.FlushDelayDeleteRange = 10 * time.Second
	// Automatically flush 10s after the first range key is added to a memtable.
	// This ensures that range keys are quickly flushed, allowing use of lazy
	// combined iteration within Pebble.
	opts.FlushDelayRangeKey = 10 * time.Second
	// Enable deletion pacing. This helps prevent disk slowness events on some
	// SSDs, that kick off an expensive GC if a lot of files are deleted at
	// once.
	opts.TargetByteDeletionRate = 128 << 20 // 128 MB
	// Validate min/max keys in each SSTable when performing a compaction. This
	// serves as a simple protection against corruption or programmer-error in
	// Pebble.
	opts.Experimental.KeyValidationFunc = func(userKey []byte) error {
		engineKey, ok := DecodeEngineKey(userKey)
		if !ok {
			return errors.Newf("key %s could not be decoded as an EngineKey", string(userKey))
		}
		if err := engineKey.Validate(); err != nil {
			return err
		}
		return nil
	}
	opts.Experimental.ShortAttributeExtractor = shortAttributeExtractorForValues
	opts.Experimental.RequiredInPlaceValueBound = pebble.UserKeyPrefixBound{
		Lower: keys.LocalRangeLockTablePrefix,
		Upper: keys.LocalRangeLockTablePrefix.PrefixEnd(),
	}

	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}

	return opts
}

func shortAttributeExtractorForValues(
	key []byte, keyPrefixLen int, value []byte,
) (pebble.ShortAttribute, error) {
	suffixLen := len(key) - keyPrefixLen
	const lockTableSuffixLen = engineKeyVersionLockTableLen + sentinelLen
	if suffixLen == engineKeyNoVersion || suffixLen == lockTableSuffixLen {
		// Not a versioned MVCC value.
		return 0, nil
	}
	isTombstone, err := EncodedMVCCValueIsTombstone(value)
	if err != nil {
		return 0, err
	}
	if isTombstone {
		return 1, nil
	}
	return 0, nil
}

type pebbleLogger struct {
	ctx   context.Context
	depth int
}

var _ pebble.LoggerAndTracer = pebbleLogger{}

func (l pebbleLogger) Infof(format string, args ...interface{}) {
	log.Storage.InfofDepth(l.ctx, l.depth, format, args...)
}

func (l pebbleLogger) Fatalf(format string, args ...interface{}) {
	log.Storage.FatalfDepth(l.ctx, l.depth, format, args...)
}

func (l pebbleLogger) Eventf(ctx context.Context, format string, args ...interface{}) {
	log.Eventf(ctx, format, args...)
}

func (l pebbleLogger) IsTracingEnabled(ctx context.Context) bool {
	return log.HasSpan(ctx)
}

func (l pebbleLogger) Errorf(format string, args ...interface{}) {
	log.Storage.ErrorfDepth(l.ctx, l.depth, format, args...)
}

// PebbleConfig holds all configuration parameters and knobs used in setting up
// a new Pebble instance.
//
// TODO(jackson): Unexport and delete base.StorageConfig.
type PebbleConfig struct {
	// StorageConfig contains storage configs for all storage engines.
	// A non-nil cluster.Settings must be provided in the StorageConfig for a
	// Pebble instance that will be used to write intents.
	base.StorageConfig
	// Env holds the initialized virtual filesystem that the Engine should use.
	Env *fs.Env
	// Pebble specific options.
	Opts *pebble.Options
	// SharedStorage is a cloud.ExternalStorage that can be used by all Pebble
	// stores on this node and on other nodes to store sstables.
	SharedStorage cloud.ExternalStorage

	// RemoteStorageFactory is used to pass the ExternalStorage factory.
	RemoteStorageFactory *cloud.EarlyBootExternalStorageAccessor

	// onClose is a slice of functions to be invoked before the engine is closed.
	onClose []func(*Pebble)

	DiskWriteStatsCollector *vfs.DiskWriteStatsCollector
}

// Pebble is a wrapper around a Pebble database instance.
type Pebble struct {
	atomic struct {
		// compactionConcurrency is the current compaction concurrency set on
		// the Pebble store. The compactionConcurrency option in the Pebble
		// Options struct is a closure which will return
		// Pebble.atomic.compactionConcurrency.
		//
		// This mechanism allows us to change the Pebble compactionConcurrency
		// on the fly without restarting Pebble.
		compactionConcurrency uint64
	}

	db *pebble.DB

	closed      bool
	readOnly    bool
	path        string
	auxDir      string
	ballastPath string
	ballastSize int64
	maxSize     int64
	attrs       roachpb.Attributes
	properties  roachpb.StoreProperties
	settings    *cluster.Settings
	env         *fs.Env

	// Stats updated by pebble.EventListener invocations, and returned in
	// GetMetrics. Updated and retrieved atomically.
	writeStallCount                  int64
	writeStallDuration               time.Duration
	writeStallStartNanos             int64
	diskSlowCount                    int64
	diskStallCount                   int64
	singleDelInvariantViolationCount int64
	singleDelIneffectualCount        int64
	sharedBytesRead                  int64
	sharedBytesWritten               int64
	iterStats                        struct {
		syncutil.Mutex
		AggregatedIteratorStats
	}
	batchCommitStats struct {
		syncutil.Mutex
		AggregatedBatchCommitStats
	}
	diskWriteStatsCollector *vfs.DiskWriteStatsCollector
	// Relevant options copied over from pebble.Options.
	logCtx        context.Context
	logger        pebble.LoggerAndTracer
	eventListener *pebble.EventListener
	mu            struct {
		// This mutex is the lowest in any lock ordering.
		syncutil.Mutex
		flushCompletedCallback func()
	}
	asyncDone sync.WaitGroup

	// minVersion is the minimum CockroachDB version that can open this store.
	minVersion roachpb.Version

	// onClose is a slice of functions to be invoked before the engine closes.
	onClose []func(*Pebble)

	storeIDPebbleLog *base.StoreIDContainer
	replayer         *replay.WorkloadCollector

	singleDelLogEvery log.EveryN
}

// WorkloadCollector implements an workloadCollectorGetter and returns the
// workload collector stored on Pebble. This method is invoked following a
// successful cast of an Engine to a `workloadCollectorGetter` type. This method
// allows for pebble exclusive functionality to be used without modifying the
// Engine interface.
func (p *Pebble) WorkloadCollector() *replay.WorkloadCollector {
	return p.replayer
}

var _ Engine = &Pebble{}

// WorkloadCollectorEnabled specifies if the workload collector will be enabled
var WorkloadCollectorEnabled = envutil.EnvOrDefaultBool("COCKROACH_STORAGE_WORKLOAD_COLLECTOR", false)

// SetCompactionConcurrency will return the previous compaction concurrency.
func (p *Pebble) SetCompactionConcurrency(n uint64) uint64 {
	prevConcurrency := atomic.SwapUint64(&p.atomic.compactionConcurrency, n)
	return prevConcurrency
}

// AdjustCompactionConcurrency adjusts the compaction concurrency up or down by
// the passed delta, down to a minimum of 1.
func (p *Pebble) AdjustCompactionConcurrency(delta int64) uint64 {
	for {
		current := atomic.LoadUint64(&p.atomic.compactionConcurrency)
		adjusted := int64(current) + delta
		if adjusted < 1 {
			adjusted = 1
		}
		if atomic.CompareAndSwapUint64(&p.atomic.compactionConcurrency, current, uint64(adjusted)) {
			return uint64(adjusted)
		}
	}
}

// SetStoreID adds the store id to pebble logs.
func (p *Pebble) SetStoreID(ctx context.Context, storeID int32) error {
	if p == nil {
		return nil
	}
	p.storeIDPebbleLog.Set(ctx, storeID)
	// Note that SetCreatorID only does something if remote storage is configured
	// in the pebble options.
	if storeID != base.TempStoreID {
		if err := p.db.SetCreatorID(uint64(storeID)); err != nil {
			return err
		}
	}
	return nil
}

// GetStoreID returns to configured store ID.
func (p *Pebble) GetStoreID() (int32, error) {
	if p == nil {
		return 0, errors.AssertionFailedf("GetStoreID requires non-nil Pebble")
	}
	if p.storeIDPebbleLog == nil {
		return 0, errors.AssertionFailedf("GetStoreID requires an initialized store ID container")
	}
	storeID := p.storeIDPebbleLog.Get()
	if storeID == 0 {
		return 0, errors.AssertionFailedf("GetStoreID must be called after calling SetStoreID")
	}
	return storeID, nil
}

func (p *Pebble) Download(ctx context.Context, span roachpb.Span, copy bool) error {
	const copySpanName, rewriteSpanName = "pebble.Download", "pebble.DownloadRewrite"
	spanName := rewriteSpanName
	if copy {
		spanName = copySpanName
	}
	ctx, sp := tracing.ChildSpan(ctx, spanName)
	defer sp.Finish()
	if p == nil {
		return nil
	}
	downloadSpan := pebble.DownloadSpan{
		StartKey:               span.Key,
		EndKey:                 span.EndKey,
		ViaBackingFileDownload: copy,
	}
	return p.db.Download(ctx, []pebble.DownloadSpan{downloadSpan})
}

type remoteStorageAdaptor struct {
	p       *Pebble
	ctx     context.Context
	factory *cloud.EarlyBootExternalStorageAccessor
}

func (r remoteStorageAdaptor) CreateStorage(locator remote.Locator) (remote.Storage, error) {
	es, err := r.factory.OpenURL(r.ctx, string(locator))
	return &externalStorageWrapper{p: r.p, ctx: r.ctx, es: es}, err
}

// ConfigureForSharedStorage is used to configure a pebble Options for shared
// storage.
var ConfigureForSharedStorage func(opts *pebble.Options, storage remote.Storage) error

// newPebble creates a new Pebble instance, at the specified path.
// Do not use directly (except in test); use Open instead.
//
// Direct users of NewPebble: cfs.opts.{Logger,LoggerAndTracer} must not be
// set.
func newPebble(ctx context.Context, cfg PebbleConfig) (p *Pebble, err error) {
	if cfg.Settings == nil {
		return nil, errors.AssertionFailedf("NewPebble requires cfg.Settings to be set")
	}

	var opts *pebble.Options
	if cfg.Opts == nil {
		opts = DefaultPebbleOptions()
	} else {
		// Open also causes DefaultPebbleOptions before calling NewPebble, so we
		// are tolerant of Logger being set to pebble.DefaultLogger.
		if cfg.Opts.Logger != nil && cfg.Opts.Logger != pebble.DefaultLogger {
			return nil, errors.AssertionFailedf("Options.Logger is set to unexpected value")
		}
		// Clone the given options so that we are free to modify them.
		opts = cfg.Opts.Clone()
	}
	if opts.FormatMajorVersion < MinimumSupportedFormatVersion {
		return nil, errors.AssertionFailedf(
			"FormatMajorVersion is %d, should be at least %d",
			opts.FormatMajorVersion, MinimumSupportedFormatVersion,
		)
	}
	opts.FS = cfg.Env
	opts.Lock = cfg.Env.DirectoryLock
	for _, l := range opts.Levels {
		l.Compression = func() sstable.Compression {
			return getCompressionAlgorithm(ctx, cfg.Settings)
		}
	}
	opts.EnsureDefaults()

	// The context dance here is done so that we have a clean context without
	// timeouts that has a copy of the log tags.
	logCtx := logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	// The store id, could not necessarily be determined when this function
	// is called. Therefore, we use a container for the store id.
	storeIDContainer := &base.StoreIDContainer{}
	logCtx = logtags.AddTag(logCtx, "s", storeIDContainer)
	logCtx = logtags.AddTag(logCtx, "pebble", nil)

	opts.ErrorIfNotExists = cfg.MustExist
	opts.WALMinSyncInterval = func() time.Duration {
		return minWALSyncInterval.Get(&cfg.Settings.SV)
	}
	opts.Experimental.EnableValueBlocks = func() bool {
		return ValueBlocksEnabled.Get(&cfg.Settings.SV)
	}
	opts.Experimental.DisableIngestAsFlushable = func() bool {
		// Disable flushable ingests if shared storage is enabled. This is because
		// flushable ingests currently do not support Excise operations.
		//
		// TODO(bilal): Remove the first part of this || statement when
		// https://github.com/cockroachdb/pebble/issues/2676 is completed, or when
		// Pebble has better guards against this.
		return cfg.SharedStorage != nil || !IngestAsFlushable.Get(&cfg.Settings.SV)
	}
	// Multi-level compactions were discovered to cause excessively large
	// compactions that can have adverse affects. We disable these types of
	// compactions for now.
	// See https://github.com/cockroachdb/pebble/issues/3120
	// TODO(travers): Re-enable, once the issues are resolved.
	opts.Experimental.MultiLevelCompactionHeuristic = pebble.NoMultiLevel{}
	opts.Experimental.IngestSplit = func() bool {
		return IngestSplitEnabled.Get(&cfg.Settings.SV)
	}

	auxDir := opts.FS.PathJoin(cfg.Dir, base.AuxiliaryDir)
	if !cfg.Env.IsReadOnly() {
		if err := opts.FS.MkdirAll(auxDir, 0755); err != nil {
			return nil, err
		}
	}
	ballastPath := base.EmergencyBallastFile(opts.FS.PathJoin, cfg.Dir)

	opts.Logger = nil // Defensive, since LoggerAndTracer will be used.
	if opts.LoggerAndTracer == nil {
		opts.LoggerAndTracer = pebbleLogger{
			ctx:   logCtx,
			depth: 1,
		}
	}
	// Else, already have a LoggerAndTracer. This only occurs in unit tests.

	// Establish the emergency ballast if we can. If there's not sufficient
	// disk space, the ballast will be reestablished from Capacity when the
	// store's capacity is queried periodically.
	if !opts.ReadOnly {
		du, err := cfg.Env.UnencryptedFS.GetDiskUsage(cfg.Dir)
		// If the FS is an in-memory FS, GetDiskUsage returns
		// vfs.ErrUnsupported and we skip ballast creation.
		if err != nil && !errors.Is(err, vfs.ErrUnsupported) {
			return nil, errors.Wrap(err, "retrieving disk usage")
		} else if err == nil {
			resized, err := maybeEstablishBallast(cfg.Env.UnencryptedFS, ballastPath, cfg.BallastSize, du)
			if err != nil {
				return nil, errors.Wrap(err, "resizing ballast")
			}
			if resized {
				opts.LoggerAndTracer.Infof("resized ballast %s to size %s",
					ballastPath, humanizeutil.IBytes(cfg.BallastSize))
			}
		}
	}

	p = &Pebble{
		readOnly:                opts.ReadOnly,
		path:                    cfg.Dir,
		auxDir:                  auxDir,
		ballastPath:             ballastPath,
		ballastSize:             cfg.BallastSize,
		maxSize:                 cfg.MaxSize,
		attrs:                   cfg.Attrs,
		properties:              computeStoreProperties(ctx, cfg),
		settings:                cfg.Settings,
		env:                     cfg.Env,
		logger:                  opts.LoggerAndTracer,
		logCtx:                  logCtx,
		storeIDPebbleLog:        storeIDContainer,
		onClose:                 cfg.onClose,
		replayer:                replay.NewWorkloadCollector(cfg.StorageConfig.Dir),
		singleDelLogEvery:       log.Every(5 * time.Minute),
		diskWriteStatsCollector: cfg.DiskWriteStatsCollector,
	}

	opts.Experimental.SingleDeleteInvariantViolationCallback = func(userKey []byte) {
		logFunc := func(ctx context.Context, format string, args ...interface{}) {}
		if SingleDeleteCrashOnInvariantViolation.Get(&cfg.Settings.SV) {
			logFunc = log.Fatalf
		} else if p.singleDelLogEvery.ShouldLog() {
			logFunc = log.Infof
		}
		logFunc(logCtx, "SingleDel invariant violation callback (can be false positive) on key %s", roachpb.Key(userKey))
		atomic.AddInt64(&p.singleDelInvariantViolationCount, 1)
	}
	opts.Experimental.IneffectualSingleDeleteCallback = func(userKey []byte) {
		logFunc := func(ctx context.Context, format string, args ...interface{}) {}
		if SingleDeleteCrashOnInvariantViolation.Get(&cfg.Settings.SV) {
			logFunc = log.Fatalf
		} else if p.singleDelLogEvery.ShouldLog() {
			logFunc = log.Infof
		}
		logFunc(logCtx, "Ineffectual SingleDel callback (can be false positive) on key %s", roachpb.Key(userKey))
		atomic.AddInt64(&p.singleDelIneffectualCount, 1)
	}

	// MaxConcurrentCompactions can be set by multiple sources, but all the
	// sources will eventually call NewPebble. So, we override
	// Opts.MaxConcurrentCompactions to a closure which will return
	// Pebble.atomic.compactionConcurrency. This will allow us to both honor
	// the compactions concurrency which has already been set and allow us
	// to update the compactionConcurrency on the fly by changing the
	// Pebble.atomic.compactionConcurrency variable.
	p.atomic.compactionConcurrency = uint64(opts.MaxConcurrentCompactions())
	opts.MaxConcurrentCompactions = func() int {
		return int(atomic.LoadUint64(&p.atomic.compactionConcurrency))
	}

	// NB: The ordering of the event listeners passed to TeeEventListener is
	// deliberate. The listener returned by makeMetricEtcEventListener is
	// responsible for crashing the process if a DiskSlow event indicates the
	// disk is stalled. While the logging subsystem should also be robust to
	// stalls and crash the process if unable to write logs, there's less risk
	// to sequencing the crashing listener first.
	//
	// For the same reason, make the logging call asynchronous for DiskSlow events.
	// This prevents slow logging calls during a disk slow/stall event from holding
	// up Pebble's internal disk health checking, and better obeys the
	// EventListener contract for not having any functions block or take a while to
	// run. Creating goroutines is acceptable given their low cost, and the low
	// write concurrency to Pebble's FS (Pebble compactions + flushes + SQL
	// spilling to disk). If the maximum concurrency of DiskSlow events increases
	// significantly in the future, we can improve the logic here by queueing up
	// most of the logging work (except for the Fatalf call), and have it be done
	// by a single goroutine.
	// TODO(jackson): Refactor this indirection; there's no need the DiskSlow
	// callback needs to go through the EventListener and this structure is
	// confusing.
	cfg.Env.RegisterOnDiskSlow(func(info pebble.DiskSlowInfo) {
		el := opts.EventListener
		p.async(func() { el.DiskSlow(info) })
	})
	el := pebble.TeeEventListener(
		p.makeMetricEtcEventListener(logCtx),
		pebble.MakeLoggingEventListener(pebbleLogger{
			ctx:   logCtx,
			depth: 2, // skip over the EventListener stack frame
		}),
	)

	p.eventListener = &el
	opts.EventListener = &el

	// If both cfg.SharedStorage and cfg.RemoteStorageFactory are set, CRDB uses
	// cfg.SharedStorage. Note that eventually we will enable using both at the
	// same time, but we don't have the right abstractions in place to do that
	// today.
	//
	// We prefer cfg.SharedStorage, since the Locator -> Storage mapping contained
	// in it is needed for CRDB to function properly.
	if cfg.SharedStorage != nil {
		esWrapper := &externalStorageWrapper{p: p, es: cfg.SharedStorage, ctx: ctx}
		if ConfigureForSharedStorage == nil {
			return nil, errors.New("shared storage requires CCL features")
		}
		if err := ConfigureForSharedStorage(opts, esWrapper); err != nil {
			return nil, errors.Wrap(err, "error when configuring shared storage")
		}
	} else {
		if cfg.RemoteStorageFactory != nil {
			opts.Experimental.RemoteStorage = remoteStorageAdaptor{p: p, ctx: ctx, factory: cfg.RemoteStorageFactory}
		}
	}

	// Read the current store cluster version.
	storeClusterVersion, minVerFileExists, err := getMinVersion(p.env.UnencryptedFS, cfg.Dir)
	if err != nil {
		return nil, err
	}
	if minVerFileExists {
		// Avoid running a binary too new for this store. This is what you'd catch
		// if, say, you restarted directly from v21.2 into v22.2 (bumping the min
		// version) without going through v22.1 first.
		//
		// Note that "going through" above means that v22.1 successfully upgrades
		// all existing stores. If v22.1 crashes half-way through the startup
		// sequence (so now some stores have v21.2, but others v22.1) you are
		// expected to run v22.1 again (hopefully without the crash this time) which
		// would then rewrite all the stores.
		if v := cfg.Settings.Version; storeClusterVersion.Less(v.MinSupportedVersion()) {
			if storeClusterVersion.Major < clusterversion.DevOffset && v.LatestVersion().Major >= clusterversion.DevOffset {
				return nil, errors.Errorf(
					"store last used with cockroach non-development version v%s "+
						"cannot be opened by development version v%s",
					storeClusterVersion, v.LatestVersion(),
				)
			}
			return nil, errors.Errorf(
				"store last used with cockroach version v%s "+
					"is too old for running version v%s (which requires data from v%s or later)",
				storeClusterVersion, v.LatestVersion(), v.MinSupportedVersion(),
			)
		}
		opts.ErrorIfNotExists = true
	} else {
		if opts.ErrorIfNotExists || opts.ReadOnly {
			// Make sure the message is not confusing if the store does exist but
			// there is no min version file.
			filename := p.env.UnencryptedFS.PathJoin(cfg.Dir, MinVersionFilename)
			return nil, errors.Errorf(
				"pebble: database %q does not exist (missing required file %q)",
				cfg.StorageConfig.Dir, filename,
			)
		}
		// If there is no min version file, there should be no store. If there is
		// one, it's either 1) a store from a very old version (which we don't want
		// to open) or 2) an empty store that was created from a previous bootstrap
		// attempt that failed right before writing out the min version file. We set
		// a flag to disallow the open in case 1.
		opts.ErrorIfNotPristine = true
	}

	if WorkloadCollectorEnabled {
		p.replayer.Attach(opts)
	}

	db, err := pebble.Open(cfg.StorageConfig.Dir, opts)
	if err != nil {
		// Decorate the errors caused by the flags we set above.
		if minVerFileExists && errors.Is(err, pebble.ErrDBDoesNotExist) {
			err = errors.Wrap(err, "min version file exists but store doesn't")
		}
		if !minVerFileExists && errors.Is(err, pebble.ErrDBNotPristine) {
			err = errors.Wrap(err, "store has no min-version file; this can "+
				"happen if the store was created by an old CockroachDB version that is no "+
				"longer supported")
		}
		return nil, err
	}
	p.db = db

	if !minVerFileExists {
		storeClusterVersion = cfg.Settings.Version.ActiveVersionOrEmpty(ctx).Version
		if storeClusterVersion == (roachpb.Version{}) {
			// If there is no active version, use the minimum supported version.
			storeClusterVersion = cfg.Settings.Version.MinSupportedVersion()
		}
	}

	// The storage engine performs its own internal migrations
	// through the setting of the store cluster version. When
	// storage's min version is set, SetMinVersion writes to disk to
	// commit to the new store cluster version. Then it idempotently
	// applies any internal storage engine migrations necessitated
	// or enabled by the new store cluster version. If we crash
	// after committing the new store cluster version but before
	// applying the internal migrations, we're left in an in-between
	// state.
	//
	// To account for this, after the engine is open,
	// unconditionally set the min cluster version again. If any
	// storage engine state has not been updated, the call to
	// SetMinVersion will update it.  If all storage engine state is
	// already updated, SetMinVersion is a noop.
	if err := p.SetMinVersion(storeClusterVersion); err != nil {
		p.Close()
		return nil, err
	}

	return p, nil
}

// async launches the provided function in a new goroutine. It uses a wait group
// to synchronize with (*Pebble).Close to ensure all launched goroutines have
// exited before Close returns.
func (p *Pebble) async(fn func()) {
	p.asyncDone.Add(1)
	go func() {
		defer p.asyncDone.Done()
		fn()
	}()
}

// writePreventStartupFile creates a file that will prevent nodes from automatically restarting after
// experiencing sstable corruption.
func (p *Pebble) writePreventStartupFile(ctx context.Context, corruptionError error) {
	auxDir := p.GetAuxiliaryDir()
	path := base.PreventedStartupFile(auxDir)

	preventStartupMsg := fmt.Sprintf(`ATTENTION:

  this node is terminating because of sstable corruption.
	Corruption may be a consequence of a hardware error.

	Error: %s

  A file preventing this node from restarting was placed at:
  %s`, corruptionError.Error(), path)

	if err := fs.WriteFile(p.env.UnencryptedFS, path, []byte(preventStartupMsg), fs.UnspecifiedWriteCategory); err != nil {
		log.Warningf(ctx, "%v", err)
	}
}

func (p *Pebble) makeMetricEtcEventListener(ctx context.Context) pebble.EventListener {
	return pebble.EventListener{
		BackgroundError: func(err error) {
			if errors.Is(err, pebble.ErrCorruption) {
				p.writePreventStartupFile(ctx, err)
				log.Fatalf(ctx, "local corruption detected: %v", err)
			}
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			atomic.AddInt64(&p.writeStallCount, 1)
			startNanos := timeutil.Now().UnixNano()
			atomic.StoreInt64(&p.writeStallStartNanos, startNanos)
		},
		WriteStallEnd: func() {
			startNanos := atomic.SwapInt64(&p.writeStallStartNanos, 0)
			if startNanos == 0 {
				// Should not happen since these callbacks are registered when Pebble
				// is opened, but just in case we miss the WriteStallBegin, lets not
				// corrupt the metric.
				return
			}
			stallDuration := timeutil.Now().UnixNano() - startNanos
			if stallDuration < 0 {
				return
			}
			atomic.AddInt64((*int64)(&p.writeStallDuration), stallDuration)
		},
		DiskSlow: func(info pebble.DiskSlowInfo) {
			maxSyncDuration := fs.MaxSyncDuration.Get(&p.settings.SV)
			fatalOnExceeded := fs.MaxSyncDurationFatalOnExceeded.Get(&p.settings.SV)
			if info.Duration.Seconds() >= maxSyncDuration.Seconds() {
				atomic.AddInt64(&p.diskStallCount, 1)
				// Note that the below log messages go to the main cockroach log, not
				// the pebble-specific log.
				//
				// Run non-fatal log.* calls in separate goroutines as they could block
				// if the logging device is also slow/stalling, preventing pebble's disk
				// health checking from functioning correctly. See the comment in
				// pebble.EventListener on why it's important for this method to return
				// quickly.
				if fatalOnExceeded {
					// The write stall may prevent the process from exiting. If
					// the process won't exit, we can at least terminate all our
					// RPC connections first.
					//
					// See pkg/cli.runStart for where this function is hooked
					// up.
					log.MakeProcessUnavailable()

					log.Fatalf(ctx, "disk stall detected: %s", info)
				} else {
					p.async(func() { log.Errorf(ctx, "disk stall detected: %s", info) })
				}
				return
			}
			atomic.AddInt64(&p.diskSlowCount, 1)
		},
		FlushEnd: func(info pebble.FlushInfo) {
			if info.Err != nil {
				return
			}
			p.mu.Lock()
			cb := p.mu.flushCompletedCallback
			p.mu.Unlock()
			if cb != nil {
				cb()
			}
		},
	}
}

// Env implements Engine.
func (p *Pebble) Env() *fs.Env { return p.env }

func (p *Pebble) String() string {
	dir := p.path
	if dir == "" {
		dir = "<in-mem>"
	}
	attrs := p.attrs.String()
	if attrs == "" {
		attrs = "<no-attributes>"
	}
	return fmt.Sprintf("%s=%s", attrs, dir)
}

// Close implements the Engine interface.
func (p *Pebble) Close() {
	if p.closed {
		p.logger.Infof("closing unopened pebble instance")
		return
	}
	for _, closeFunc := range p.onClose {
		closeFunc(p)
	}

	p.closed = true

	// Wait for any asynchronous goroutines to exit.
	p.asyncDone.Wait()

	handleErr := func(err error) {
		if err == nil {
			return
		}
		// Allow unclean close in production builds for now. We refrain from
		// Fatal-ing on an unclean close because Cockroach opens and closes
		// ephemeral engines at time, and an error in those codepaths should not
		// fatal the process.
		//
		// TODO(jackson): Propagate the error to call sites without fataling:
		// This is tricky, because the Reader interface requires Close return
		// nothing.
		if buildutil.CrdbTestBuild {
			log.Fatalf(p.logCtx, "error during engine close: %s\n", err)
		} else {
			log.Errorf(p.logCtx, "error during engine close: %s\n", err)
		}
	}

	handleErr(p.db.Close())
	if p.env != nil {
		p.env.Close()
		p.env = nil
	}
}

// aggregateIterStats is propagated to all of an engine's iterators, aggregating
// iterator stats when an iterator is closed or its stats are reset. These
// aggregated stats are exposed through GetMetrics.
func (p *Pebble) aggregateIterStats(stats IteratorStats) {
	p.iterStats.Lock()
	defer p.iterStats.Unlock()
	p.iterStats.BlockBytes += stats.Stats.InternalStats.BlockBytes
	p.iterStats.BlockBytesInCache += stats.Stats.InternalStats.BlockBytesInCache
	p.iterStats.BlockReadDuration += stats.Stats.InternalStats.BlockReadDuration
	p.iterStats.ExternalSeeks += stats.Stats.ForwardSeekCount[pebble.InterfaceCall] + stats.Stats.ReverseSeekCount[pebble.InterfaceCall]
	p.iterStats.ExternalSteps += stats.Stats.ForwardStepCount[pebble.InterfaceCall] + stats.Stats.ReverseStepCount[pebble.InterfaceCall]
	p.iterStats.InternalSeeks += stats.Stats.ForwardSeekCount[pebble.InternalIterCall] + stats.Stats.ReverseSeekCount[pebble.InternalIterCall]
	p.iterStats.InternalSteps += stats.Stats.ForwardStepCount[pebble.InternalIterCall] + stats.Stats.ReverseStepCount[pebble.InternalIterCall]
}

func (p *Pebble) aggregateBatchCommitStats(stats BatchCommitStats) {
	p.batchCommitStats.Lock()
	p.batchCommitStats.Count++
	p.batchCommitStats.TotalDuration += stats.TotalDuration
	p.batchCommitStats.SemaphoreWaitDuration += stats.SemaphoreWaitDuration
	p.batchCommitStats.WALQueueWaitDuration += stats.WALQueueWaitDuration
	p.batchCommitStats.MemTableWriteStallDuration += stats.MemTableWriteStallDuration
	p.batchCommitStats.L0ReadAmpWriteStallDuration += stats.L0ReadAmpWriteStallDuration
	p.batchCommitStats.WALRotationDuration += stats.WALRotationDuration
	p.batchCommitStats.CommitWaitDuration += stats.CommitWaitDuration
	p.batchCommitStats.Unlock()
}

// Closed implements the Engine interface.
func (p *Pebble) Closed() bool {
	return p.closed
}

// MVCCIterate implements the Engine interface.
func (p *Pebble) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(ctx, r, start, end, iterKind, keyTypes, readCategory, f)
		r.Free()
		return err
	}
	return iterateOnReader(ctx, p, start, end, iterKind, keyTypes, readCategory, f)
}

// NewMVCCIterator implements the Engine interface.
func (p *Pebble) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(ctx, iterKind, opts)
		r.Free()
		if err != nil {
			return nil, err
		}
		return maybeWrapInUnsafeIter(iter), nil
	}

	iter, err := newPebbleIterator(ctx, p.db, opts, StandardDurability, p)
	if err != nil {
		return nil, err
	}
	return maybeWrapInUnsafeIter(iter), nil
}

// NewEngineIterator implements the Engine interface.
func (p *Pebble) NewEngineIterator(ctx context.Context, opts IterOptions) (EngineIterator, error) {
	return newPebbleIterator(ctx, p.db, opts, StandardDurability, p)
}

// ScanInternal implements the Engine interface.
func (p *Pebble) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum uint64) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	rawLower := EngineKey{Key: lower}.Encode()
	rawUpper := EngineKey{Key: upper}.Encode()
	// TODO(sumeer): set CategoryAndQoS.
	return p.db.ScanInternal(ctx, sstable.CategoryAndQoS{}, rawLower, rawUpper, visitPointKey,
		visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

// ConsistentIterators implements the Engine interface.
func (p *Pebble) ConsistentIterators() bool {
	return false
}

// PinEngineStateForIterators implements the Engine interface.
func (p *Pebble) PinEngineStateForIterators(fs.ReadCategory) error {
	return errors.AssertionFailedf(
		"PinEngineStateForIterators must not be called when ConsistentIterators returns false")
}

// ApplyBatchRepr implements the Engine interface.
func (p *Pebble) ApplyBatchRepr(repr []byte, sync bool) error {
	// batch.SetRepr takes ownership of the underlying slice, so make a copy.
	reprCopy := make([]byte, len(repr))
	copy(reprCopy, repr)

	batch := p.db.NewBatch()
	if err := batch.SetRepr(reprCopy); err != nil {
		return err
	}

	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	return batch.Commit(opts)
}

// ClearMVCC implements the Engine interface.
func (p *Pebble) ClearMVCC(key MVCCKey, opts ClearOptions) error {
	if key.Timestamp.IsEmpty() {
		panic("ClearMVCC timestamp is empty")
	}
	return p.clear(key, opts)
}

// ClearUnversioned implements the Engine interface.
func (p *Pebble) ClearUnversioned(key roachpb.Key, opts ClearOptions) error {
	return p.clear(MVCCKey{Key: key}, opts)
}

// ClearEngineKey implements the Engine interface.
func (p *Pebble) ClearEngineKey(key EngineKey, opts ClearOptions) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	if !opts.ValueSizeKnown || !p.settings.Version.ActiveVersionOrEmpty(context.TODO()).
		IsActive(clusterversion.V23_2_UseSizedPebblePointTombstones) {
		return p.db.Delete(key.Encode(), pebble.Sync)
	}
	return p.db.DeleteSized(key.Encode(), opts.ValueSize, pebble.Sync)
}

func (p *Pebble) clear(key MVCCKey, opts ClearOptions) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	if !opts.ValueSizeKnown || !p.settings.Version.ActiveVersionOrEmpty(context.TODO()).
		IsActive(clusterversion.V23_2_UseSizedPebblePointTombstones) {
		return p.db.Delete(EncodeMVCCKey(key), pebble.Sync)
	}
	// Use DeleteSized to propagate the value size.
	return p.db.DeleteSized(EncodeMVCCKey(key), opts.ValueSize, pebble.Sync)
}

// SingleClearEngineKey implements the Engine interface.
func (p *Pebble) SingleClearEngineKey(key EngineKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.SingleDelete(key.Encode(), pebble.Sync)
}

// ClearRawRange implements the Engine interface.
func (p *Pebble) ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	startRaw, endRaw := EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode()
	if pointKeys {
		if err := p.db.DeleteRange(startRaw, endRaw, pebble.Sync); err != nil {
			return err
		}
	}
	if rangeKeys {
		if err := p.db.RangeKeyDelete(startRaw, endRaw, pebble.Sync); err != nil {
			return err
		}
	}
	return nil
}

// ClearMVCCRange implements the Engine interface.
func (p *Pebble) ClearMVCCRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	// Write all the tombstones in one batch.
	batch := p.NewUnindexedBatch()
	defer batch.Close()

	if err := batch.ClearMVCCRange(start, end, pointKeys, rangeKeys); err != nil {
		return err
	}
	return batch.Commit(true)
}

// ClearMVCCVersions implements the Engine interface.
func (p *Pebble) ClearMVCCVersions(start, end MVCCKey) error {
	return p.db.DeleteRange(EncodeMVCCKey(start), EncodeMVCCKey(end), pebble.Sync)
}

// ClearMVCCIteratorRange implements the Engine interface.
func (p *Pebble) ClearMVCCIteratorRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	// Write all the tombstones in one batch.
	batch := p.NewUnindexedBatch()
	defer batch.Close()

	if err := batch.ClearMVCCIteratorRange(start, end, pointKeys, rangeKeys); err != nil {
		return err
	}
	return batch.Commit(true)
}

// ClearMVCCRangeKey implements the Engine interface.
func (p *Pebble) ClearMVCCRangeKey(rangeKey MVCCRangeKey) error {
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	return p.ClearEngineRangeKey(
		rangeKey.StartKey, rangeKey.EndKey, EncodeMVCCTimestampSuffix(rangeKey.Timestamp))
}

// PutMVCCRangeKey implements the Engine interface.
func (p *Pebble) PutMVCCRangeKey(rangeKey MVCCRangeKey, value MVCCValue) error {
	// NB: all MVCC APIs currently assume all range keys are range tombstones.
	if !value.IsTombstone() {
		return errors.New("range keys can only be MVCC range tombstones")
	}
	valueRaw, err := EncodeMVCCValue(value)
	if err != nil {
		return errors.Wrapf(err, "failed to encode MVCC value for range key %s", rangeKey)
	}
	return p.PutRawMVCCRangeKey(rangeKey, valueRaw)
}

// PutRawMVCCRangeKey implements the Engine interface.
func (p *Pebble) PutRawMVCCRangeKey(rangeKey MVCCRangeKey, value []byte) error {
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	return p.PutEngineRangeKey(
		rangeKey.StartKey, rangeKey.EndKey, EncodeMVCCTimestampSuffix(rangeKey.Timestamp), value)
}

// Merge implements the Engine interface.
func (p *Pebble) Merge(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Merge(EncodeMVCCKey(key), value, pebble.Sync)
}

// PutMVCC implements the Engine interface.
func (p *Pebble) PutMVCC(key MVCCKey, value MVCCValue) error {
	if key.Timestamp.IsEmpty() {
		panic("PutMVCC timestamp is empty")
	}
	encValue, err := EncodeMVCCValue(value)
	if err != nil {
		return err
	}
	return p.put(key, encValue)
}

// PutRawMVCC implements the Engine interface.
func (p *Pebble) PutRawMVCC(key MVCCKey, value []byte) error {
	if key.Timestamp.IsEmpty() {
		panic("PutRawMVCC timestamp is empty")
	}
	return p.put(key, value)
}

// PutUnversioned implements the Engine interface.
func (p *Pebble) PutUnversioned(key roachpb.Key, value []byte) error {
	return p.put(MVCCKey{Key: key}, value)
}

// PutEngineKey implements the Engine interface.
func (p *Pebble) PutEngineKey(key EngineKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Set(key.Encode(), value, pebble.Sync)
}

func (p *Pebble) put(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return p.db.Set(EncodeMVCCKey(key), value, pebble.Sync)
}

// PutEngineRangeKey implements the Engine interface.
func (p *Pebble) PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte) error {
	return p.db.RangeKeySet(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, value, pebble.Sync)
}

// ClearEngineRangeKey implements the Engine interface.
func (p *Pebble) ClearEngineRangeKey(start, end roachpb.Key, suffix []byte) error {
	return p.db.RangeKeyUnset(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, pebble.Sync)
}

// LogData implements the Engine interface.
func (p *Pebble) LogData(data []byte) error {
	return p.db.LogData(data, pebble.Sync)
}

// LogLogicalOp implements the Engine interface.
func (p *Pebble) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

// LocalTimestampsEnabled controls whether local timestamps are written in MVCC
// values. A true setting is also gated on clusterversion.LocalTimestamps. After
// all nodes in a cluster are at or beyond clusterversion.LocalTimestamps,
// different nodes will see the version state transition at different times.
// Nodes that have not yet seen the transition may remove the local timestamp
// from an intent that has one during intent resolution. This will not cause
// problems.
//
// TODO(nvanbenschoten): remove this cluster setting and its associated plumbing
// when removing the cluster version, once we're confident in the efficacy and
// stability of local timestamps.
var LocalTimestampsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"storage.transaction.local_timestamps.enabled",
	"if enabled, MVCC keys will be written with local timestamps",
	true,
)

func shouldWriteLocalTimestamps(ctx context.Context, settings *cluster.Settings) bool {
	return LocalTimestampsEnabled.Get(&settings.SV)
}

// ShouldWriteLocalTimestamps implements the Writer interface.
func (p *Pebble) ShouldWriteLocalTimestamps(ctx context.Context) bool {
	// This is not fast. Pebble should not be used by writers that want
	// performance. They should use pebbleBatch.
	return shouldWriteLocalTimestamps(ctx, p.settings)
}

// Attrs implements the Engine interface.
func (p *Pebble) Attrs() roachpb.Attributes {
	return p.attrs
}

// Properties implements the Engine interface.
func (p *Pebble) Properties() roachpb.StoreProperties {
	return p.properties
}

// Capacity implements the Engine interface.
func (p *Pebble) Capacity() (roachpb.StoreCapacity, error) {
	dir := p.path
	if dir != "" {
		var err error
		// Eval directory if it is a symbolic links.
		if dir, err = filepath.EvalSymlinks(dir); err != nil {
			return roachpb.StoreCapacity{}, err
		}
	}
	du, err := p.env.UnencryptedFS.GetDiskUsage(dir)
	if errors.Is(err, vfs.ErrUnsupported) {
		// This is an in-memory instance. Pretend we're empty since we
		// don't know better and only use this for testing. Using any
		// part of the actual file system here can throw off allocator
		// rebalancing in a hard-to-trace manner. See #7050.
		return roachpb.StoreCapacity{
			Capacity:  p.maxSize,
			Available: p.maxSize,
		}, nil
	} else if err != nil {
		return roachpb.StoreCapacity{}, err
	}

	if du.TotalBytes > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(du.TotalBytes), humanizeutil.IBytes(math.MaxInt64))
	}
	if du.AvailBytes > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(du.AvailBytes), humanizeutil.IBytes(math.MaxInt64))
	}
	fsuTotal := int64(du.TotalBytes)
	fsuAvail := int64(du.AvailBytes)

	// If the emergency ballast isn't appropriately sized, try to resize it.
	// This is a no-op if the ballast is already sized or if there's not
	// enough available capacity to resize it. Capacity is called periodically
	// by the kvserver, and that drives the automatic resizing of the ballast.
	if !p.readOnly {
		resized, err := maybeEstablishBallast(p.env.UnencryptedFS, p.ballastPath, p.ballastSize, du)
		if err != nil {
			return roachpb.StoreCapacity{}, errors.Wrap(err, "resizing ballast")
		}
		if resized {
			p.logger.Infof("resized ballast %s to size %s",
				p.ballastPath, humanizeutil.IBytes(p.ballastSize))
			du, err = p.env.UnencryptedFS.GetDiskUsage(dir)
			if err != nil {
				return roachpb.StoreCapacity{}, err
			}
		}
	}

	// Pebble has detailed accounting of its own disk space usage, and it's
	// incrementally updated which helps avoid O(# files) work here.
	m := p.db.Metrics()
	totalUsedBytes := int64(m.DiskSpaceUsage())

	// We don't have incremental accounting of the disk space usage of files
	// in the auxiliary directory. Walk the auxiliary directory and all its
	// subdirectories, adding to the total used bytes.
	if errOuter := filepath.Walk(p.auxDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// This can happen if CockroachDB removes files out from under us -
			// just keep going to get the best estimate we can.
			if oserror.IsNotExist(err) {
				return nil
			}
			// Special-case: if the store-dir is configured using the root of some fs,
			// e.g. "/mnt/db", we might have special fs-created files like lost+found
			// that we can't read, so just ignore them rather than crashing.
			if oserror.IsPermission(err) && filepath.Base(path) == "lost+found" {
				return nil
			}
			return err
		}
		if path == p.ballastPath {
			// Skip the ballast. Counting it as used is likely to confuse
			// users, and it's more akin to space that is just unavailable
			// like disk space often restricted to a root user.
			return nil
		}
		if info.Mode().IsRegular() {
			totalUsedBytes += info.Size()
		}
		return nil
	}); errOuter != nil {
		return roachpb.StoreCapacity{}, errOuter
	}

	// If no size limitation have been placed on the store size or if the
	// limitation is greater than what's available, just return the actual
	// totals.
	if p.maxSize == 0 || p.maxSize >= fsuTotal || p.path == "" {
		return roachpb.StoreCapacity{
			Capacity:  fsuTotal,
			Available: fsuAvail,
			Used:      totalUsedBytes,
		}, nil
	}

	available := p.maxSize - totalUsedBytes
	if available > fsuAvail {
		available = fsuAvail
	}
	if available < 0 {
		available = 0
	}

	return roachpb.StoreCapacity{
		Capacity:  p.maxSize,
		Available: available,
		Used:      totalUsedBytes,
	}, nil
}

// Flush implements the Engine interface.
func (p *Pebble) Flush() error {
	return p.db.Flush()
}

// GetMetrics implements the Engine interface.
func (p *Pebble) GetMetrics() Metrics {
	m := Metrics{
		Metrics:                          p.db.Metrics(),
		WriteStallCount:                  atomic.LoadInt64(&p.writeStallCount),
		WriteStallDuration:               time.Duration(atomic.LoadInt64((*int64)(&p.writeStallDuration))),
		DiskSlowCount:                    atomic.LoadInt64(&p.diskSlowCount),
		DiskStallCount:                   atomic.LoadInt64(&p.diskStallCount),
		SingleDelInvariantViolationCount: atomic.LoadInt64(&p.singleDelInvariantViolationCount),
		SingleDelIneffectualCount:        atomic.LoadInt64(&p.singleDelIneffectualCount),
		SharedStorageReadBytes:           atomic.LoadInt64(&p.sharedBytesRead),
		SharedStorageWriteBytes:          atomic.LoadInt64(&p.sharedBytesWritten),
	}
	p.iterStats.Lock()
	m.Iterator = p.iterStats.AggregatedIteratorStats
	p.iterStats.Unlock()
	p.batchCommitStats.Lock()
	m.BatchCommitStats = p.batchCommitStats.AggregatedBatchCommitStats
	p.batchCommitStats.Unlock()
	m.DiskWriteStats = p.diskWriteStatsCollector.GetStats()
	return m
}

// GetEncryptionRegistries implements the Engine interface.
func (p *Pebble) GetEncryptionRegistries() (*fs.EncryptionRegistries, error) {
	rv := &fs.EncryptionRegistries{}
	var err error
	if p.env.Encryption != nil {
		rv.KeyRegistry, err = p.env.Encryption.StatsHandler.GetDataKeysRegistry()
		if err != nil {
			return nil, err
		}
	}
	if p.env.Registry != nil {
		rv.FileRegistry, err = protoutil.Marshal(p.env.Registry.GetRegistrySnapshot())
		if err != nil {
			return nil, err
		}
	}
	return rv, nil
}

// GetEnvStats implements the Engine interface.
func (p *Pebble) GetEnvStats() (*fs.EnvStats, error) {
	// TODO(sumeer): make the stats complete. There are no bytes stats. The TotalFiles is missing
	// files that are not in the registry (from before encryption was enabled).
	stats := &fs.EnvStats{}
	if p.env.Encryption == nil {
		return stats, nil
	}
	stats.EncryptionType = p.env.Encryption.StatsHandler.GetActiveStoreKeyType()
	var err error
	stats.EncryptionStatus, err = p.env.Encryption.StatsHandler.GetEncryptionStatus()
	if err != nil {
		return nil, err
	}
	fr := p.env.Registry.GetRegistrySnapshot()
	activeKeyID, err := p.env.Encryption.StatsHandler.GetActiveDataKeyID()
	if err != nil {
		return nil, err
	}

	m := p.db.Metrics()
	stats.TotalFiles = 3 /* CURRENT, MANIFEST, OPTIONS */
	stats.TotalFiles += uint64(m.WAL.Files + m.Table.ZombieCount + m.WAL.ObsoleteFiles + m.Table.ObsoleteCount)
	stats.TotalBytes = m.WAL.Size + m.Table.ZombieSize + m.Table.ObsoleteSize
	for _, l := range m.Levels {
		stats.TotalFiles += uint64(l.NumFiles)
		stats.TotalBytes += uint64(l.Size)
	}

	sstSizes := make(map[pebble.FileNum]uint64)
	sstInfos, err := p.db.SSTables()
	if err != nil {
		return nil, err
	}
	for _, ssts := range sstInfos {
		for _, sst := range ssts {
			sstSizes[sst.FileNum] = sst.Size
		}
	}

	for filePath, entry := range fr.Files {
		keyID, err := p.env.Encryption.StatsHandler.GetKeyIDFromSettings(entry.EncryptionSettings)
		if err != nil {
			return nil, err
		}
		if len(keyID) == 0 {
			keyID = "plain"
		}
		if keyID != activeKeyID {
			continue
		}
		stats.ActiveKeyFiles++

		filename := p.env.PathBase(filePath)
		numStr := strings.TrimSuffix(filename, ".sst")
		if len(numStr) == len(filename) {
			continue // not a sstable
		}
		u, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing filename %q", errors.Safe(filename))
		}
		stats.ActiveKeyBytes += sstSizes[pebble.FileNum(u)]
	}

	// Ensure that encryption percentage does not exceed 100%.
	frFileLen := uint64(len(fr.Files))
	if stats.TotalFiles < frFileLen {
		stats.TotalFiles = frFileLen
	}

	if stats.TotalBytes < stats.ActiveKeyBytes {
		stats.TotalBytes = stats.ActiveKeyBytes
	}

	return stats, nil
}

// GetAuxiliaryDir implements the Engine interface.
func (p *Pebble) GetAuxiliaryDir() string {
	return p.auxDir
}

// NewBatch implements the Engine interface.
func (p *Pebble) NewBatch() Batch {
	return newPebbleBatch(p.db, p.db.NewIndexedBatch(), p.settings, p, p)
}

// NewReader implements the Engine interface.
func (p *Pebble) NewReader(durability DurabilityRequirement) Reader {
	return newPebbleReadOnly(p, durability)
}

// NewReadOnly implements the Engine interface.
func (p *Pebble) NewReadOnly(durability DurabilityRequirement) ReadWriter {
	return newPebbleReadOnly(p, durability)
}

// NewUnindexedBatch implements the Engine interface.
func (p *Pebble) NewUnindexedBatch() Batch {
	return newPebbleBatch(p.db, p.db.NewBatch(), p.settings, p, p)
}

// NewWriteBatch implements the Engine interface.
func (p *Pebble) NewWriteBatch() WriteBatch {
	return newWriteBatch(p.db, p.db.NewBatch(), p.settings, p, p)
}

// NewSnapshot implements the Engine interface.
func (p *Pebble) NewSnapshot() Reader {
	return &pebbleSnapshot{
		snapshot: p.db.NewSnapshot(),
		parent:   p,
	}
}

// NewEventuallyFileOnlySnapshot implements the Engine interface.
func (p *Pebble) NewEventuallyFileOnlySnapshot(keyRanges []roachpb.Span) EventuallyFileOnlyReader {
	engineKeyRanges := make([]pebble.KeyRange, len(keyRanges))
	for i := range keyRanges {
		engineKeyRanges[i].Start = EngineKey{Key: keyRanges[i].Key}.Encode()
		engineKeyRanges[i].End = EngineKey{Key: keyRanges[i].EndKey}.Encode()
	}
	efos := p.db.NewEventuallyFileOnlySnapshot(engineKeyRanges)
	return &pebbleEFOS{
		efos:      efos,
		parent:    p,
		keyRanges: keyRanges,
	}
}

// Type implements the Engine interface.
func (p *Pebble) Type() enginepb.EngineType {
	return enginepb.EngineTypePebble
}

// IngestLocalFiles implements the Engine interface.
func (p *Pebble) IngestLocalFiles(ctx context.Context, paths []string) error {
	return p.db.Ingest(paths)
}

// IngestLocalFilesWithStats implements the Engine interface.
func (p *Pebble) IngestLocalFilesWithStats(
	ctx context.Context, paths []string,
) (pebble.IngestOperationStats, error) {
	return p.db.IngestWithStats(paths)
}

// IngestAndExciseFiles implements the Engine interface.
func (p *Pebble) IngestAndExciseFiles(
	ctx context.Context,
	paths []string,
	shared []pebble.SharedSSTMeta,
	external []pebble.ExternalFile,
	exciseSpan roachpb.Span,
) (pebble.IngestOperationStats, error) {
	rawSpan := pebble.KeyRange{
		Start: EngineKey{Key: exciseSpan.Key}.Encode(),
		End:   EngineKey{Key: exciseSpan.EndKey}.Encode(),
	}
	return p.db.IngestAndExcise(paths, shared, external, rawSpan)
}

// IngestExternalFiles implements the Engine interface.
func (p *Pebble) IngestExternalFiles(
	ctx context.Context, external []pebble.ExternalFile,
) (pebble.IngestOperationStats, error) {
	return p.db.IngestExternalFiles(external)
}

// PreIngestDelay implements the Engine interface.
func (p *Pebble) PreIngestDelay(ctx context.Context) {
	preIngestDelay(ctx, p, p.settings)
}

// GetTableMetrics implements the Engine interface.
func (p *Pebble) GetTableMetrics(start, end roachpb.Key) ([]enginepb.SSTableMetricsInfo, error) {
	tableInfo, err := p.db.SSTables(pebble.WithKeyRangeFilter(start, end), pebble.WithProperties(), pebble.WithApproximateSpanBytes())

	if err != nil {
		return []enginepb.SSTableMetricsInfo{}, err
	}

	var totalTables int
	for _, info := range tableInfo {
		totalTables += len(info)
	}

	var metricsInfo []enginepb.SSTableMetricsInfo

	for level, sstableInfos := range tableInfo {
		for _, sstableInfo := range sstableInfos {
			marshalTableInfo, err := json.Marshal(sstableInfo)

			if err != nil {
				return []enginepb.SSTableMetricsInfo{}, err
			}

			tableID := sstableInfo.TableInfo.FileNum
			approximateSpanBytes, err := strconv.ParseUint(sstableInfo.Properties.UserProperties["approximate-span-bytes"], 10, 64)
			if err != nil {
				return []enginepb.SSTableMetricsInfo{}, err
			}
			metricsInfo = append(metricsInfo, enginepb.SSTableMetricsInfo{TableID: uint64(tableID), Level: int32(level), ApproximateSpanBytes: approximateSpanBytes, TableInfoJSON: marshalTableInfo})
		}
	}
	return metricsInfo, nil
}

// ScanStorageInternalKeys implements the Engine interface.
func (p *Pebble) ScanStorageInternalKeys(
	start, end roachpb.Key, megabytesPerSecond int64,
) ([]enginepb.StorageInternalKeysMetrics, error) {
	stats, err := p.db.ScanStatistics(context.TODO(), start, end, pebble.ScanStatisticsOptions{LimitBytesPerSecond: 1000000 * megabytesPerSecond})
	if err != nil {
		return []enginepb.StorageInternalKeysMetrics{}, err
	}
	setMetricsFromStats := func(
		level int, stats *pebble.KeyStatistics, m *enginepb.StorageInternalKeysMetrics) {
		*m = enginepb.StorageInternalKeysMetrics{
			Level:                       int32(level),
			SnapshotPinnedKeys:          uint64(stats.SnapshotPinnedKeys),
			SnapshotPinnedKeysBytes:     stats.SnapshotPinnedKeysBytes,
			PointKeyDeleteCount:         uint64(stats.KindsCount[pebble.InternalKeyKindDelete]),
			PointKeySetCount:            uint64(stats.KindsCount[pebble.InternalKeyKindSet]),
			RangeDeleteCount:            uint64(stats.KindsCount[pebble.InternalKeyKindRangeDelete]),
			RangeKeySetCount:            uint64(stats.KindsCount[pebble.InternalKeyKindRangeKeySet]),
			RangeKeyDeleteCount:         uint64(stats.KindsCount[pebble.InternalKeyKindRangeKeyDelete]),
			PointKeyDeleteIsLatestCount: uint64(stats.LatestKindsCount[pebble.InternalKeyKindDelete]),
			PointKeySetIsLatestCount:    uint64(stats.LatestKindsCount[pebble.InternalKeyKindSet]),
		}
	}
	var metrics []enginepb.StorageInternalKeysMetrics
	for level := 0; level < 7; level++ {
		var m enginepb.StorageInternalKeysMetrics
		setMetricsFromStats(level, &stats.Levels[level], &m)
		metrics = append(metrics, m)
	}
	var m enginepb.StorageInternalKeysMetrics
	setMetricsFromStats(-1 /* level */, &stats.Accumulated, &m)
	metrics = append(metrics, m)
	return metrics, nil
}

// ApproximateDiskBytes implements the Engine interface.
func (p *Pebble) ApproximateDiskBytes(
	from, to roachpb.Key,
) (bytes, remoteBytes, externalBytes uint64, _ error) {
	fromEncoded := EngineKey{Key: from}.Encode()
	toEncoded := EngineKey{Key: to}.Encode()
	bytes, remoteBytes, externalBytes, err := p.db.EstimateDiskUsageByBackingType(fromEncoded, toEncoded)
	if err != nil {
		return 0, 0, 0, err
	}
	return bytes, remoteBytes, externalBytes, nil
}

// Compact implements the Engine interface.
func (p *Pebble) Compact() error {
	return p.db.Compact(nil, EncodeMVCCKey(MVCCKeyMax), true /* parallel */)
}

// CompactRange implements the Engine interface.
func (p *Pebble) CompactRange(start, end roachpb.Key) error {
	bufStart := EncodeMVCCKey(MVCCKey{start, hlc.Timestamp{}})
	bufEnd := EncodeMVCCKey(MVCCKey{end, hlc.Timestamp{}})
	return p.db.Compact(bufStart, bufEnd, true /* parallel */)
}

// RegisterFlushCompletedCallback implements the Engine interface.
func (p *Pebble) RegisterFlushCompletedCallback(cb func()) {
	p.mu.Lock()
	p.mu.flushCompletedCallback = cb
	p.mu.Unlock()
}

func checkpointSpansNote(spans []roachpb.Span) []byte {
	note := "CRDB spans:\n"
	for _, span := range spans {
		note += span.String() + "\n"
	}
	return []byte(note)
}

// CreateCheckpoint implements the Engine interface.
func (p *Pebble) CreateCheckpoint(dir string, spans []roachpb.Span) error {
	opts := []pebble.CheckpointOption{
		pebble.WithFlushedWAL(),
	}
	if l := len(spans); l > 0 {
		s := make([]pebble.CheckpointSpan, 0, l)
		for _, span := range spans {
			s = append(s, pebble.CheckpointSpan{
				Start: EngineKey{Key: span.Key}.Encode(),
				End:   EngineKey{Key: span.EndKey}.Encode(),
			})
		}
		opts = append(opts, pebble.WithRestrictToSpans(s))
	}
	if err := p.db.Checkpoint(dir, opts...); err != nil {
		return err
	}

	// Write out the min version file.
	if err := writeMinVersionFile(p.env.UnencryptedFS, dir, p.MinVersion()); err != nil {
		return errors.Wrapf(err, "writing min version file for checkpoint")
	}

	// TODO(#90543, cockroachdb/pebble#2285): move spans info to Pebble manifest.
	if len(spans) > 0 {
		if err := fs.SafeWriteToFile(
			p.env, dir, p.env.PathJoin(dir, "checkpoint.txt"),
			checkpointSpansNote(spans),
			fs.UnspecifiedWriteCategory,
		); err != nil {
			return err
		}
	}

	return nil
}

// pebbleFormatVersionMap maps cluster versions to the corresponding pebble
// format version. For a given cluster version, the entry with the latest
// version that is not newer than the given version is chosen.
//
// This map needs to include the "final" version for each supported previous
// release, and the in-development versions of the current release and the
// previous one (for experimental version skipping during upgrade).
//
// Pebble has a concept of format major versions, similar to cluster versions.
// Backwards incompatible changes to Pebble's on-disk format are gated behind
// new format major versions. Bumping the storage engine's format major version
// is tied to a CockroachDB cluster version.
//
// Format major versions and cluster versions both only ratchet upwards. Here we
// map the persisted cluster version to the corresponding format major version,
// ratcheting Pebble's format major version if necessary.
//
// The pebble versions are advanced when nodes enter the "fence" version for the
// named cluster version, if there is one, so that if *any* node moves into the
// named version, it can be assumed all *nodes* have ratcheted to the pebble
// version associated with it, since they did so during the fence version.
var pebbleFormatVersionMap = map[clusterversion.Key]pebble.FormatMajorVersion{
	clusterversion.V23_1: pebble.FormatFlushableIngest,
	clusterversion.V23_2_PebbleFormatDeleteSizedAndObsolete: pebble.FormatDeleteSizedAndObsolete,
	clusterversion.V23_2_PebbleFormatVirtualSSTables:        pebble.FormatVirtualSSTables,
	clusterversion.V24_1_PebbleFormatSyntheticPrefixSuffix:  pebble.FormatSyntheticPrefixSuffix,
}

// pebbleFormatVersionKeys contains the keys in the map above, in descending order.
var pebbleFormatVersionKeys []clusterversion.Key = func() []clusterversion.Key {
	versionKeys := make([]clusterversion.Key, 0, len(pebbleFormatVersionMap))
	for k := range pebbleFormatVersionMap {
		versionKeys = append(versionKeys, k)
	}
	// Sort the keys in reverse order.
	sort.Slice(versionKeys, func(i, j int) bool {
		return versionKeys[i] > versionKeys[j]
	})
	return versionKeys
}()

// pebbleFormatVersion finds the most recent pebble format version supported by
// the given cluster version.
func pebbleFormatVersion(clusterVersion roachpb.Version) pebble.FormatMajorVersion {
	// pebbleFormatVersionKeys are sorted in descending order; find the first one
	// that is not newer than clusterVersion.
	for _, k := range pebbleFormatVersionKeys {
		if clusterVersion.AtLeast(k.FenceVersion()) {
			return pebbleFormatVersionMap[k]
		}
	}
	// This should never happen in production. But we tolerate tests creating
	// imaginary older versions; we must still use the earliest supported
	// format.
	return MinimumSupportedFormatVersion
}

// SetMinVersion implements the Engine interface.
func (p *Pebble) SetMinVersion(version roachpb.Version) error {
	p.minVersion = version

	if p.readOnly {
		// Don't make any on-disk changes.
		return nil
	}

	// NB: SetMinVersion must be idempotent. It may called multiple
	// times with the same version.

	// Writing the min version file commits this storage engine to the
	// provided cluster version.
	if err := writeMinVersionFile(p.env.UnencryptedFS, p.path, version); err != nil {
		return err
	}

	// Set the shared object creator ID .
	if storeID := p.storeIDPebbleLog.Get(); storeID != 0 && storeID != base.TempStoreID {
		if err := p.db.SetCreatorID(uint64(storeID)); err != nil {
			return err
		}
	}

	formatVers := pebbleFormatVersion(version)
	if p.db.FormatMajorVersion() < formatVers {
		if err := p.db.RatchetFormatMajorVersion(formatVers); err != nil {
			return errors.Wrap(err, "ratcheting format major version")
		}
	}
	return nil
}

// MinVersion implements the Engine interface.
func (p *Pebble) MinVersion() roachpb.Version {
	return p.minVersion
}

// BufferedSize implements the Engine interface.
func (p *Pebble) BufferedSize() int {
	return 0
}

// ConvertFilesToBatchAndCommit implements the Engine interface.
func (p *Pebble) ConvertFilesToBatchAndCommit(
	_ context.Context, paths []string, clearedSpans []roachpb.Span,
) error {
	files := make([]sstable.ReadableFile, len(paths))
	closeFiles := func() {
		for i := range files {
			if files[i] != nil {
				files[i].Close()
			}
		}
	}
	for i, fileName := range paths {
		f, err := p.env.Open(fileName)
		if err != nil {
			closeFiles()
			return err
		}
		files[i] = f
	}
	iter, err := NewSSTEngineIterator(
		[][]sstable.ReadableFile{files},
		IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: roachpb.KeyMin,
			UpperBound: roachpb.KeyMax,
		})
	if err != nil {
		// TODO(sumeer): we don't call closeFiles() since in the error case some
		// of the files may be closed. See the code in
		// https://github.com/cockroachdb/pebble/blob/master/external_iterator.go#L104-L113
		// which closes the opened readers. At this point in the code we don't
		// know which files are already closed. The callee needs to be fixed to
		// not close any of the files or close all the files in the error case.
		// The natural behavior would be to not close any file. Fix this in
		// Pebble, and then adjust the code here if needed.
		return err
	}
	defer iter.Close()

	batch := p.NewWriteBatch()
	for i := range clearedSpans {
		err :=
			batch.ClearRawRange(clearedSpans[i].Key, clearedSpans[i].EndKey, true, true)
		if err != nil {
			return err
		}
	}
	valid, err := iter.SeekEngineKeyGE(EngineKey{Key: roachpb.KeyMin})
	for valid {
		hasPoint, hasRange := iter.HasPointAndRange()
		if hasPoint {
			var k EngineKey
			if k, err = iter.UnsafeEngineKey(); err != nil {
				break
			}
			var v []byte
			if v, err = iter.UnsafeValue(); err != nil {
				break
			}
			if err = batch.PutEngineKey(k, v); err != nil {
				break
			}
		}
		if hasRange && iter.RangeKeyChanged() {
			var rangeBounds roachpb.Span
			if rangeBounds, err = iter.EngineRangeBounds(); err != nil {
				break
			}
			rangeKeys := iter.EngineRangeKeys()
			for i := range rangeKeys {
				if err = batch.PutEngineRangeKey(rangeBounds.Key, rangeBounds.EndKey, rangeKeys[i].Version,
					rangeKeys[i].Value); err != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		valid, err = iter.NextEngineKey()
	}
	if err != nil {
		batch.Close()
		return err
	}
	return batch.Commit(true)
}

type pebbleReadOnly struct {
	parent *Pebble
	// The iterator reuse optimization in pebbleReadOnly is for servicing a
	// BatchRequest, such that the iterators get reused across different
	// requests in the batch.
	// Reuse iterators for {normal,prefix} x {MVCCKey,EngineKey} iteration. We
	// need separate iterators for EngineKey and MVCCKey iteration since
	// iterators that make separated locks/intents look as interleaved need to
	// use both simultaneously.
	// When the first iterator is initialized, or when
	// PinEngineStateForIterators is called (whichever happens first), the
	// underlying *pebble.Iterator is stashed in iter, so that subsequent
	// iterator initialization can use Iterator.Clone to use the same underlying
	// engine state. This relies on the fact that all pebbleIterators created
	// here are marked as reusable, which causes pebbleIterator.Close to not
	// close iter. iter will be closed when pebbleReadOnly.Close is called.
	prefixIter       pebbleIterator
	normalIter       pebbleIterator
	prefixEngineIter pebbleIterator
	normalEngineIter pebbleIterator

	iter       pebbleiter.Iterator
	iterUsed   bool // avoids cloning after PinEngineStateForIterators()
	durability DurabilityRequirement
	closed     bool
}

var _ ReadWriter = &pebbleReadOnly{}

var pebbleReadOnlyPool = sync.Pool{
	New: func() interface{} {
		return &pebbleReadOnly{
			// Defensively set reusable=true. One has to be careful about this since
			// an accidental false value would cause these iterators, that are value
			// members of pebbleReadOnly, to be put in the pebbleIterPool.
			prefixIter:       pebbleIterator{reusable: true},
			normalIter:       pebbleIterator{reusable: true},
			prefixEngineIter: pebbleIterator{reusable: true},
			normalEngineIter: pebbleIterator{reusable: true},
		}
	},
}

// Instantiates a new pebbleReadOnly.
func newPebbleReadOnly(parent *Pebble, durability DurabilityRequirement) *pebbleReadOnly {
	p := pebbleReadOnlyPool.Get().(*pebbleReadOnly)
	// When p is a reused pebbleReadOnly from the pool, the iter fields preserve
	// the original reusable=true that was set above in pebbleReadOnlyPool.New(),
	// and some buffers that are safe to reuse. Everything else has been reset by
	// pebbleIterator.destroy().
	*p = pebbleReadOnly{
		parent:           parent,
		prefixIter:       p.prefixIter,
		normalIter:       p.normalIter,
		prefixEngineIter: p.prefixEngineIter,
		normalEngineIter: p.normalEngineIter,
		durability:       durability,
	}
	return p
}

func (p *pebbleReadOnly) Close() {
	if p.closed {
		panic("closing an already-closed pebbleReadOnly")
	}
	p.closed = true
	if p.iter != nil && !p.iterUsed {
		err := p.iter.Close()
		if err != nil {
			panic(err)
		}
	}

	// Setting iter to nil is sufficient since it will be closed by one of the
	// subsequent destroy calls.
	p.iter = nil
	p.prefixIter.destroy()
	p.normalIter.destroy()
	p.prefixEngineIter.destroy()
	p.normalEngineIter.destroy()
	p.durability = StandardDurability

	pebbleReadOnlyPool.Put(p)
}

func (p *pebbleReadOnly) Closed() bool {
	return p.closed
}

func (p *pebbleReadOnly) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(ctx, r, start, end, iterKind, keyTypes, readCategory, f)
		r.Free()
		return err
	}
	return iterateOnReader(ctx, p, start, end, iterKind, keyTypes, readCategory, f)
}

// NewMVCCIterator implements the Engine interface.
func (p *pebbleReadOnly) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}

	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(ctx, iterKind, opts)
		r.Free()
		if err != nil {
			return nil, err
		}
		return maybeWrapInUnsafeIter(iter), nil
	}

	iter := &p.normalIter
	if opts.Prefix {
		iter = &p.prefixIter
	}
	if iter.inuse {
		return newPebbleIteratorByCloning(ctx, CloneContext{
			rawIter: p.iter,
			engine:  p.parent,
		}, opts, p.durability), nil
	}

	if iter.iter != nil {
		iter.setOptions(ctx, opts, p.durability)
	} else {
		if err := iter.initReuseOrCreate(
			ctx, p.parent.db, p.iter, p.iterUsed, opts, p.durability, p.parent); err != nil {
			return nil, err
		}
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		p.iterUsed = true
		iter.reusable = true
	}

	iter.inuse = true
	return maybeWrapInUnsafeIter(iter), nil
}

// NewEngineIterator implements the Engine interface.
func (p *pebbleReadOnly) NewEngineIterator(
	ctx context.Context, opts IterOptions,
) (EngineIterator, error) {
	if p.closed {
		panic("using a closed pebbleReadOnly")
	}

	iter := &p.normalEngineIter
	if opts.Prefix {
		iter = &p.prefixEngineIter
	}
	if iter.inuse {
		return newPebbleIteratorByCloning(ctx, CloneContext{
			rawIter: p.iter,
			engine:  p.parent,
		}, opts, p.durability), nil
	}

	if iter.iter != nil {
		iter.setOptions(ctx, opts, p.durability)
	} else {
		err := iter.initReuseOrCreate(
			ctx, p.parent.db, p.iter, p.iterUsed, opts, p.durability, p.parent)
		if err != nil {
			return nil, err
		}
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		p.iterUsed = true
		iter.reusable = true
	}

	iter.inuse = true
	return iter, nil
}

// ConsistentIterators implements the Engine interface.
func (p *pebbleReadOnly) ConsistentIterators() bool {
	return true
}

// PinEngineStateForIterators implements the Engine interface.
func (p *pebbleReadOnly) PinEngineStateForIterators(readCategory fs.ReadCategory) error {
	if p.iter == nil {
		o := &pebble.IterOptions{CategoryAndQoS: fs.GetCategoryAndQoS(readCategory)}
		if p.durability == GuaranteedDurability {
			o.OnlyReadGuaranteedDurable = true
		}
		iter, err := p.parent.db.NewIter(o)
		if err != nil {
			return err
		}
		p.iter = pebbleiter.MaybeWrap(iter)
		// NB: p.iterUsed == false avoids cloning this in NewMVCCIterator(), since
		// we've just created it.
	}
	return nil
}

// ScanInternal implements the Reader interface.
func (p *pebbleReadOnly) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum uint64) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	return p.parent.ScanInternal(ctx, lower, upper, visitPointKey, visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

// Writer methods are not implemented for pebbleReadOnly. Ideally, the code
// could be refactored so that a Reader could be supplied to evaluateBatch

// Writer is the write interface to an engine's data.

func (p *pebbleReadOnly) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCC(key MVCCKey, opts ClearOptions) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearUnversioned(key roachpb.Key, opts ClearOptions) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearEngineKey(key EngineKey, opts ClearOptions) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) SingleClearEngineKey(key EngineKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCVersions(start, end MVCCKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCIteratorRange(
	start, end roachpb.Key, pointKeys, rangeKeys bool,
) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutMVCCRangeKey(MVCCRangeKey, MVCCValue) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutRawMVCCRangeKey(MVCCRangeKey, []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutEngineRangeKey(roachpb.Key, roachpb.Key, []byte, []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearEngineRangeKey(roachpb.Key, roachpb.Key, []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) ClearMVCCRangeKey(MVCCRangeKey) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) Merge(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutMVCC(key MVCCKey, value MVCCValue) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutRawMVCC(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutUnversioned(key roachpb.Key, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) PutEngineKey(key EngineKey, value []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) LogData(data []byte) error {
	panic("not implemented")
}

func (p *pebbleReadOnly) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	panic("not implemented")
}

func (p *pebbleReadOnly) ShouldWriteLocalTimestamps(ctx context.Context) bool {
	panic("not implemented")
}

func (p *pebbleReadOnly) BufferedSize() int {
	panic("not implemented")
}

// pebbleSnapshot represents a snapshot created using Pebble.NewSnapshot().
type pebbleSnapshot struct {
	snapshot *pebble.Snapshot
	parent   *Pebble
	closed   bool
}

var _ Reader = &pebbleSnapshot{}

// Close implements the Reader interface.
func (p *pebbleSnapshot) Close() {
	_ = p.snapshot.Close()
	p.closed = true
}

// Closed implements the Reader interface.
func (p *pebbleSnapshot) Closed() bool {
	return p.closed
}

// MVCCIterate implements the Reader interface.
func (p *pebbleSnapshot) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(ctx, r, start, end, iterKind, keyTypes, readCategory, f)
		r.Free()
		return err
	}
	return iterateOnReader(ctx, p, start, end, iterKind, keyTypes, readCategory, f)
}

// NewMVCCIterator implements the Reader interface.
func (p *pebbleSnapshot) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(ctx, iterKind, opts)
		r.Free()
		if err != nil {
			return nil, err
		}
		return maybeWrapInUnsafeIter(iter), nil
	}

	iter, err := newPebbleIterator(ctx, p.snapshot, opts, StandardDurability, p.parent)
	if err != nil {
		return nil, err
	}
	return maybeWrapInUnsafeIter(MVCCIterator(iter)), nil
}

// NewEngineIterator implements the Reader interface.
func (p pebbleSnapshot) NewEngineIterator(
	ctx context.Context, opts IterOptions,
) (EngineIterator, error) {
	return newPebbleIterator(ctx, p.snapshot, opts, StandardDurability, p.parent)
}

// ConsistentIterators implements the Reader interface.
func (p pebbleSnapshot) ConsistentIterators() bool {
	return true
}

// PinEngineStateForIterators implements the Reader interface.
func (p *pebbleSnapshot) PinEngineStateForIterators(fs.ReadCategory) error {
	// Snapshot already pins state, so nothing to do.
	return nil
}

// ScanInternal implements the Reader interface.
func (p *pebbleSnapshot) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum uint64) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	rawLower := EngineKey{Key: lower}.Encode()
	rawUpper := EngineKey{Key: upper}.Encode()
	// TODO(sumeer): set CategoryAndQoS.
	return p.snapshot.ScanInternal(ctx, sstable.CategoryAndQoS{}, rawLower, rawUpper, visitPointKey,
		visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

// pebbleEFOS represents an eventually file-only snapshot created using
// NewEventuallyFileOnlySnapshot.
type pebbleEFOS struct {
	efos      *pebble.EventuallyFileOnlySnapshot
	parent    *Pebble
	keyRanges []roachpb.Span
	closed    bool
}

var _ EventuallyFileOnlyReader = &pebbleEFOS{}

// Close implements the Reader interface.
func (p *pebbleEFOS) Close() {
	_ = p.efos.Close()
	p.closed = true
}

// Closed implements the Reader interface.
func (p *pebbleEFOS) Closed() bool {
	return p.closed
}

// MVCCIterate implements the Reader interface.
func (p *pebbleEFOS) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(ctx, r, start, end, iterKind, keyTypes, readCategory, f)
		r.Free()
		return err
	}
	return iterateOnReader(ctx, p, start, end, iterKind, keyTypes, readCategory, f)
}

// WaitForFileOnly implements the EventuallyFileOnlyReader interface.
func (p *pebbleEFOS) WaitForFileOnly(
	ctx context.Context, gracePeriodBeforeFlush time.Duration,
) error {
	return p.efos.WaitForFileOnlySnapshot(ctx, gracePeriodBeforeFlush)
}

// NewMVCCIterator implements the Reader interface.
func (p *pebbleEFOS) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	// Check if the bounds fall within the EFOS' keyRanges. We can only do this
	// check for non-prefix iterators as prefix iterators often don't specify
	// any bounds.
	if !opts.Prefix {
		if opts.LowerBound == nil || opts.UpperBound == nil {
			return nil, errors.AssertionFailedf("cannot create iterators on EFOS without bounds")
		}
		var found bool
		boundSpan := roachpb.Span{Key: opts.LowerBound, EndKey: opts.UpperBound}
		for i := range p.keyRanges {
			if p.keyRanges[i].Contains(boundSpan) {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.AssertionFailedf("iterator bounds exceed eventually-file-only-snapshot key ranges: %s", boundSpan.String())
		}
	}
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(ctx, iterKind, opts)
		r.Free()
		if err != nil {
			return nil, err
		}
		return maybeWrapInUnsafeIter(iter), nil
	}

	iter, err := newPebbleIterator(ctx, p.efos, opts, StandardDurability, p.parent)
	if err != nil {
		return nil, err
	}
	return maybeWrapInUnsafeIter(MVCCIterator(iter)), nil
}

// NewEngineIterator implements the Reader interface.
func (p *pebbleEFOS) NewEngineIterator(
	ctx context.Context, opts IterOptions,
) (EngineIterator, error) {
	return newPebbleIterator(ctx, p.efos, opts, StandardDurability, p.parent)
}

// ConsistentIterators implements the Reader interface.
func (p *pebbleEFOS) ConsistentIterators() bool {
	return true
}

// PinEngineStateForIterators implements the Reader interface.
func (p *pebbleEFOS) PinEngineStateForIterators(fs.ReadCategory) error {
	// Snapshot already pins state, so nothing to do.
	return nil
}

// ScanInternal implements the Reader interface.
func (p *pebbleEFOS) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum uint64) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	rawLower := EngineKey{Key: lower}.Encode()
	rawUpper := EngineKey{Key: upper}.Encode()
	// TODO(sumeer): set CategoryAndQoS.
	return p.efos.ScanInternal(ctx, sstable.CategoryAndQoS{}, rawLower, rawUpper, visitPointKey,
		visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

// ExceedMaxSizeError is the error returned when an export request
// fails due the export size exceeding the budget. This can be caused
// by large KVs that have many revisions.
type ExceedMaxSizeError struct {
	reached int64
	maxSize uint64
}

var _ error = &ExceedMaxSizeError{}

func (e *ExceedMaxSizeError) Error() string {
	return fmt.Sprintf("export size (%d bytes) exceeds max size (%d bytes)", e.reached, e.maxSize)
}
