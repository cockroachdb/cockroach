// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

const (
	// MVCCVersionTimestampSize is the size of the timestamp portion of MVCC
	// version keys (used to update stats).
	MVCCVersionTimestampSize int64 = 12
	// RecommendedMaxOpenFiles is the recommended value for RocksDB's
	// max_open_files option.
	RecommendedMaxOpenFiles = 10000
	// MinimumMaxOpenFiles is the minimum value that RocksDB's max_open_files
	// option can be set to. While this should be set as high as possible, the
	// minimum total for a single store node must be under 2048 for Windows
	// compatibility.
	MinimumMaxOpenFiles = 1700
	// MaxIntentsPerWriteIntentErrorDefault is the default value for maximum
	// number of intents reported by ExportToSST and Scan operations in
	// WriteIntentError is set to half of the maximum lock table size.
	// This value is subject to tuning in real environment as we have more data
	// available.
	MaxIntentsPerWriteIntentErrorDefault = 5000
)

var minWALSyncInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"rocksdb.min_wal_sync_interval",
	"minimum duration between syncs of the RocksDB WAL",
	0*time.Millisecond,
)

// MVCCRangeTombstonesEnabled enables writing of MVCC range tombstones.
// Currently, this is used for schema GC and import cancellation rollbacks.
//
// Note that any executing jobs may not pick up this change, so these need to be
// waited out before being certain that the setting has taken effect.
//
// If disabled after being enabled, this will prevent new range tombstones from
// being written, but already written tombstones will remain until GCed. The
// above note on jobs also applies in this case.
var MVCCRangeTombstonesEnabled = settings.RegisterBoolSetting(
	settings.TenantReadOnly,
	"storage.mvcc.range_tombstones.enabled",
	"enables the use of MVCC range tombstones",
	true)

// CanUseMVCCRangeTombstones returns true if the caller can begin writing
// MVCC range tombstones, by setting DeleteRangeRequest.UseRangeTombstone.
// It requires the MVCCRangeTombstones version gate to be active, and the
// setting storage.mvcc.range_tombstones.enabled to be enabled.
func CanUseMVCCRangeTombstones(ctx context.Context, st *cluster.Settings) bool {
	return st.Version.IsActive(ctx, clusterversion.V22_2MVCCRangeTombstones) &&
		MVCCRangeTombstonesEnabled.Get(&st.SV)
}

// MaxIntentsPerWriteIntentError sets maximum number of intents returned in
// WriteIntentError in operations that return multiple intents per error.
// Currently it is used in Scan, ReverseScan, and ExportToSST.
var MaxIntentsPerWriteIntentError = settings.RegisterIntSetting(
	settings.TenantWritable,
	"storage.mvcc.max_intents_per_error",
	"maximum number of intents returned in error during export of scan requests",
	MaxIntentsPerWriteIntentErrorDefault,
)

var rocksdbConcurrency = envutil.EnvOrDefaultInt(
	"COCKROACH_ROCKSDB_CONCURRENCY", func() int {
		// Use up to min(numCPU, 4) threads for background RocksDB compactions per
		// store.
		const max = 4
		if n := runtime.GOMAXPROCS(0); n <= max {
			return n
		}
		return max
	}())

// MakeValue returns the inline value.
func MakeValue(meta enginepb.MVCCMetadata) roachpb.Value {
	return roachpb.Value{RawBytes: meta.RawBytes}
}

func emptyKeyError() error {
	return errors.Errorf("attempted access to empty key")
}

// MVCCKeyValue contains the raw bytes of the value for a key.
type MVCCKeyValue struct {
	Key MVCCKey
	// if Key.IsValue(), Value is an encoded MVCCValue.
	//             else, Value is an encoded MVCCMetadata.
	Value []byte
}

// MVCCRangeKeyValue contains the raw bytes of the value for a key.
type MVCCRangeKeyValue struct {
	RangeKey MVCCRangeKey
	Value    []byte
}

// Clone returns a copy of the MVCCRangeKeyValue.
func (r MVCCRangeKeyValue) Clone() MVCCRangeKeyValue {
	r.RangeKey = r.RangeKey.Clone()
	if r.Value != nil {
		r.Value = append([]byte{}, r.Value...)
	}
	return r
}

// optionalValue represents an optional roachpb.Value. It is preferred
// over a *roachpb.Value to avoid the forced heap allocation.
type optionalValue struct {
	roachpb.Value
	exists bool
}

func makeOptionalValue(v roachpb.Value) optionalValue {
	return optionalValue{Value: v, exists: true}
}

func (v *optionalValue) IsPresent() bool {
	return v.exists && v.Value.IsPresent()
}

func (v *optionalValue) IsTombstone() bool {
	return v.exists && !v.Value.IsPresent()
}

func (v *optionalValue) ToPointer() *roachpb.Value {
	if !v.exists {
		return nil
	}
	// Copy to prevent forcing receiver onto heap.
	cpy := v.Value
	return &cpy
}

// isSysLocal returns whether the key is system-local.
func isSysLocal(key roachpb.Key) bool {
	return key.Compare(keys.LocalMax) < 0
}

// isAbortSpanKey returns whether the key is an abort span key.
func isAbortSpanKey(key roachpb.Key) bool {
	if !bytes.HasPrefix(key, keys.LocalRangeIDPrefix) {
		return false
	}

	_ /* rangeID */, infix, suffix, _ /* detail */, err := keys.DecodeRangeIDKey(key)
	if err != nil {
		return false
	}
	hasAbortSpanSuffix := infix.Equal(keys.LocalRangeIDReplicatedInfix) && suffix.Equal(keys.LocalAbortSpanSuffix)
	return hasAbortSpanSuffix
}

// updateStatsForInline updates stat counters for an inline value
// (abort span entries for example). These are simpler as they don't
// involve intents, multiple versions, or MVCC range tombstones.
func updateStatsForInline(
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	origMetaKeySize, origMetaValSize, metaKeySize, metaValSize int64,
) {
	sys := isSysLocal(key)
	// Remove counts for this key if the original size is non-zero.
	if origMetaKeySize != 0 {
		if sys {
			ms.SysBytes -= (origMetaKeySize + origMetaValSize)
			ms.SysCount--
			// We only do this check in updateStatsForInline since
			// abort span keys are always inlined - we don't associate
			// timestamps with them.
			if isAbortSpanKey(key) {
				ms.AbortSpanBytes -= (origMetaKeySize + origMetaValSize)
			}
		} else {
			ms.LiveBytes -= (origMetaKeySize + origMetaValSize)
			ms.LiveCount--
			ms.KeyBytes -= origMetaKeySize
			ms.ValBytes -= origMetaValSize
			ms.KeyCount--
			ms.ValCount--
		}
	}
	// Add counts for this key if the new size is non-zero.
	if metaKeySize != 0 {
		if sys {
			ms.SysBytes += metaKeySize + metaValSize
			ms.SysCount++
			if isAbortSpanKey(key) {
				ms.AbortSpanBytes += metaKeySize + metaValSize
			}
		} else {
			ms.LiveBytes += metaKeySize + metaValSize
			ms.LiveCount++
			ms.KeyBytes += metaKeySize
			ms.ValBytes += metaValSize
			ms.KeyCount++
			ms.ValCount++
		}
	}
}

// updateStatsOnMerge updates metadata stats while merging inlined
// values. Unfortunately, we're unable to keep accurate stats on merges as the
// actual details of the merge play out asynchronously during compaction. We
// actually undercount by only adding the size of the value.RawBytes byte slice
// (and eliding MVCCVersionTimestampSize, corresponding to the metadata overhead,
// even for the very "first" write). These errors are corrected during splits and
// merges.
func updateStatsOnMerge(key roachpb.Key, valSize, nowNanos int64) enginepb.MVCCStats {
	var ms enginepb.MVCCStats
	sys := isSysLocal(key)
	ms.AgeTo(nowNanos)

	ms.ContainsEstimates = 1

	if sys {
		ms.SysBytes += valSize
	} else {
		ms.LiveBytes += valSize
		ms.ValBytes += valSize
	}
	return ms
}

// updateStatsOnPut updates stat counters for a newly put value,
// including both the metadata key & value bytes and the mvcc
// versioned value's key & value bytes. If the value is not a
// deletion tombstone, updates the live stat counters as well.
// If this value is an intent, updates the intent counters.
func updateStatsOnPut(
	key roachpb.Key,
	prevIsValue bool,
	prevValSize int64,
	origMetaKeySize, origMetaValSize, metaKeySize, metaValSize int64,
	orig, meta *enginepb.MVCCMetadata,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats

	if isSysLocal(key) {
		// Handling system-local keys is straightforward because
		// we don't track ageable quantities for them (we
		// could, but don't). Remove the contributions from the
		// original, if any, and add in the new contributions.
		if orig != nil {
			ms.SysBytes -= origMetaKeySize + origMetaValSize
			if orig.Txn != nil {
				// If the original value was an intent, we're replacing the
				// intent. Note that since it's a system key, it doesn't affect
				// IntentByte, IntentCount, and correspondingly, IntentAge.
				ms.SysBytes -= orig.KeyBytes + orig.ValBytes
			}
			ms.SysCount--
		}
		ms.SysBytes += meta.KeyBytes + meta.ValBytes + metaKeySize + metaValSize
		ms.SysCount++
		return ms
	}

	// Handle non-sys keys. This follows the same scheme: if there was a previous
	// value, perhaps even an intent, subtract its contributions, and then add the
	// new contributions. The complexity here is that we need to properly update
	// GCBytesAge and IntentAge, which don't follow the same semantics. The difference
	// between them is that an intent accrues IntentAge from its own timestamp on,
	// while GCBytesAge is accrued by versions according to the following rules:
	// 1. a (non-tombstone) value that is shadowed by a newer write accrues age at
	// 	  the point in time at which it is shadowed (i.e. the newer write's timestamp).
	// 2. a tombstone value accrues age at its own timestamp (note that this means
	//    the tombstone's own contribution only -- the actual write that was deleted
	//    is then shadowed by this tombstone, and will thus also accrue age from
	//    the tombstone's value on, as per 1).
	//
	// This seems relatively straightforward, but only because it omits pesky
	// details, which have been relegated to the comments below.

	// Remove current live counts for this key.
	if orig != nil {
		ms.KeyCount--

		// Move the (so far empty) stats to the timestamp at which the
		// previous entry was created, which is where we wish to reclassify
		// its contributions.
		ms.AgeTo(orig.Timestamp.WallTime)

		// If the original metadata for this key was an intent, subtract
		// its contribution from stat counters as it's being replaced.
		if orig.Txn != nil {
			// Subtract counts attributable to intent we're replacing.
			ms.ValCount--
			ms.IntentBytes -= (orig.KeyBytes + orig.ValBytes)
			ms.IntentCount--
			ms.SeparatedIntentCount--
		}

		// If the original intent is a deletion, we're removing the intent. This
		// means removing its contribution at the *old* timestamp because it has
		// accrued GCBytesAge that we need to offset (rule 2).
		//
		// Note that there is a corresponding block for the case of a non-deletion
		// (rule 1) below, at meta.Timestamp.
		if orig.Deleted {
			ms.KeyBytes -= origMetaKeySize
			ms.ValBytes -= origMetaValSize

			if orig.Txn != nil {
				ms.KeyBytes -= orig.KeyBytes
				ms.ValBytes -= orig.ValBytes
			}
		}

		// Rule 1 implies that sometimes it's not only the old meta and the new meta
		// that matter, but also the version below both of them. For example, take
		// a version at t=1 and an intent over it at t=2 that is now being replaced
		// (t=3). Then orig.Timestamp will be 2, and meta.Timestamp will be 3, but
		// rule 1 tells us that for the interval [2,3) we have already accrued
		// GCBytesAge for the version at t=1 that is now moot, because the intent
		// at t=2 is moving to t=3; we have to emit a GCBytesAge offset to that effect.
		//
		// The code below achieves this by making the old version live again at
		// orig.Timestamp, and then marking it as shadowed at meta.Timestamp below.
		// This only happens when that version wasn't a tombstone, in which case it
		// contributes from its own timestamp on anyway, and doesn't need adjustment.
		//
		// Note that when meta.Timestamp equals orig.Timestamp, the computation is
		// moot, which is something our callers may exploit (since retrieving the
		// previous version is not for free).
		if prevIsValue {
			// If the previous value (exists and) was not a deletion tombstone, make it
			// live at orig.Timestamp. We don't have to do anything if there is a
			// previous value that is a tombstone: according to rule two its age
			// contributions are anchored to its own timestamp, so moving some values
			// higher up doesn't affect the contributions tied to that key.
			ms.LiveBytes += MVCCVersionTimestampSize + prevValSize
		}

		// Note that there is an interesting special case here: it's possible that
		// meta.Timestamp.WallTime < orig.Timestamp.WallTime. This wouldn't happen
		// outside of tests (due to our semantics of txn.ReadTimestamp, which never
		// decreases) but it sure does happen in randomized testing. An earlier
		// version of the code used `Forward` here, which is incorrect as it would be
		// a no-op and fail to subtract out the intent bytes/GC age incurred due to
		// removing the meta entry at `orig.Timestamp` (when `orig != nil`).
		ms.AgeTo(meta.Timestamp.WallTime)

		if prevIsValue {
			// Make the previous non-deletion value non-live again, as explained in the
			// sibling block above.
			ms.LiveBytes -= MVCCVersionTimestampSize + prevValSize
		}

		// If the original version wasn't a deletion, it becomes non-live at meta.Timestamp
		// as this is where it is shadowed.
		if !orig.Deleted {
			ms.LiveBytes -= orig.KeyBytes + orig.ValBytes
			ms.LiveBytes -= origMetaKeySize + origMetaValSize
			ms.LiveCount--

			ms.KeyBytes -= origMetaKeySize
			ms.ValBytes -= origMetaValSize

			if orig.Txn != nil {
				ms.KeyBytes -= orig.KeyBytes
				ms.ValBytes -= orig.ValBytes
			}
		}
	} else {
		ms.AgeTo(meta.Timestamp.WallTime)
	}

	// If the new version isn't a deletion tombstone, add it to live counters.
	if !meta.Deleted {
		ms.LiveBytes += meta.KeyBytes + meta.ValBytes + metaKeySize + metaValSize
		ms.LiveCount++
	}
	ms.KeyBytes += meta.KeyBytes + metaKeySize
	ms.ValBytes += meta.ValBytes + metaValSize
	ms.KeyCount++
	ms.ValCount++
	if meta.Txn != nil {
		ms.IntentBytes += meta.KeyBytes + meta.ValBytes
		ms.IntentCount++
		ms.SeparatedIntentCount++
	}
	return ms
}

// updateStatsOnResolve updates stat counters with the difference
// between the original and new metadata sizes. The size of the
// resolved value (key & bytes) are subtracted from the intents
// counters if commit=true.
func updateStatsOnResolve(
	key roachpb.Key,
	prevIsValue bool,
	prevValSize int64,
	origMetaKeySize, origMetaValSize, metaKeySize, metaValSize int64,
	orig, meta *enginepb.MVCCMetadata,
	commit bool,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats

	if isSysLocal(key) {
		// Straightforward: old contribution goes, new contribution comes, and we're done.
		ms.SysBytes -= origMetaKeySize + origMetaValSize + orig.KeyBytes + orig.ValBytes
		ms.SysBytes += metaKeySize + metaValSize + meta.KeyBytes + meta.ValBytes
		return ms
	}

	// An intent can't turn from deleted to non-deleted and vice versa while being
	// resolved.
	if orig.Deleted != meta.Deleted {
		log.Fatalf(context.TODO(), "on resolve, original meta was deleted=%t, but new one is deleted=%t",
			orig.Deleted, meta.Deleted)
	}

	// In the main case, we had an old intent at orig.Timestamp, and a new intent
	// or value at meta.Timestamp. We'll walk through the contributions below,
	// taking special care for IntentAge and GCBytesAge.
	//
	// Jump into the method below for extensive commentary on their semantics
	// and "rules one and two".
	_ = updateStatsOnPut

	ms.AgeTo(orig.Timestamp.WallTime)

	// At orig.Timestamp, the original meta key disappears. Fortunately, the
	// GCBytesAge computations are fairly transparent because the intent is either
	// not a deletion in which case it is always live (it's the most recent value,
	// so it isn't shadowed -- see rule 1), or it *is* a deletion, in which case
	// its own timestamp is where it starts accruing GCBytesAge (rule 2).
	ms.KeyBytes -= origMetaKeySize + orig.KeyBytes
	ms.ValBytes -= origMetaValSize + orig.ValBytes

	// If the old intent is a deletion, then the key already isn't tracked
	// in LiveBytes any more (and the new intent/value is also a deletion).
	// If we're looking at a non-deletion intent/value, update the live
	// bytes to account for the difference between the previous intent and
	// the new intent/value.
	if !meta.Deleted {
		ms.LiveBytes -= origMetaKeySize + origMetaValSize
		ms.LiveBytes -= orig.KeyBytes + orig.ValBytes
	}

	// IntentAge is always accrued from the intent's own timestamp on.
	ms.IntentBytes -= orig.KeyBytes + orig.ValBytes
	ms.IntentCount--
	ms.SeparatedIntentCount--

	// If there was a previous value (before orig.Timestamp), and it was not a
	// deletion tombstone, then we have to adjust its GCBytesAge contribution
	// which was previously anchored at orig.Timestamp and now has to move to
	// meta.Timestamp. Paralleling very similar code in the method below, this
	// is achieved by making the previous key live between orig.Timestamp and
	// meta.Timestamp. When the two are equal, this will be a zero adjustment,
	// and so in that case the caller may simply pass prevValSize=0 and can
	// skip computing that quantity in the first place.
	_ = updateStatsOnPut

	if prevIsValue {
		ms.LiveBytes += MVCCVersionTimestampSize + prevValSize
	}

	ms.AgeTo(meta.Timestamp.WallTime)

	if prevIsValue {
		// The previous non-deletion value becomes non-live at meta.Timestamp.
		// See the sibling code above.
		ms.LiveBytes -= MVCCVersionTimestampSize + prevValSize
	}

	// At meta.Timestamp, the new meta key appears.
	ms.KeyBytes += metaKeySize + meta.KeyBytes
	ms.ValBytes += metaValSize + meta.ValBytes

	// The new meta key appears.
	if !meta.Deleted {
		ms.LiveBytes += (metaKeySize + metaValSize) + (meta.KeyBytes + meta.ValBytes)
	}

	if !commit {
		// If not committing, the intent reappears (but at meta.Timestamp).
		//
		// This is the case in which an intent is pushed (a similar case
		// happens when an intent is overwritten, but that's handled in
		// updateStatsOnPut, not this method).
		ms.IntentBytes += meta.KeyBytes + meta.ValBytes
		ms.IntentCount++
		ms.SeparatedIntentCount++
	}
	return ms
}

// updateStatsOnRangeKeyClear updates MVCCStats for clearing an entire
// range key stack.
func updateStatsOnRangeKeyClear(rangeKeys MVCCRangeKeyStack) enginepb.MVCCStats {
	var ms enginepb.MVCCStats
	ms.Subtract(updateStatsOnRangeKeyPut(rangeKeys))
	return ms
}

// updateStatsOnRangeKeyClearVersion updates MVCCStats for clearing a single
// version in a range key stack. The given range key stack must be before the
// clear.
func updateStatsOnRangeKeyClearVersion(
	rangeKeys MVCCRangeKeyStack, version MVCCRangeKeyVersion,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats

	// If we're removing the newest version, hide it from the slice such that we
	// can invert the put contribution.
	if version.Timestamp.Equal(rangeKeys.Newest()) {
		if rangeKeys.Len() == 1 {
			ms.Add(updateStatsOnRangeKeyClear(rangeKeys))
			return ms
		}
		rangeKeys.Versions = rangeKeys.Versions[1:]
	}

	ms.Subtract(updateStatsOnRangeKeyPutVersion(rangeKeys, version))
	return ms
}

// updateStatsOnRangeKeyPut updates MVCCStats for writing a new range key stack.
func updateStatsOnRangeKeyPut(rangeKeys MVCCRangeKeyStack) enginepb.MVCCStats {
	var ms enginepb.MVCCStats
	ms.AgeTo(rangeKeys.Newest().WallTime)
	ms.RangeKeyCount++
	ms.RangeKeyBytes += int64(EncodedMVCCKeyPrefixLength(rangeKeys.Bounds.Key)) +
		int64(EncodedMVCCKeyPrefixLength(rangeKeys.Bounds.EndKey))
	for _, v := range rangeKeys.Versions {
		ms.AgeTo(v.Timestamp.WallTime)
		ms.RangeKeyBytes += int64(EncodedMVCCTimestampSuffixLength(v.Timestamp))
		ms.RangeValCount++
		ms.RangeValBytes += int64(len(v.Value))
	}
	return ms
}

// updateStatsOnRangeKeyPutVersion updates MVCCStats for writing a new range key
// version in an existing range key stack. The given range key stack must be
// before the put.
func updateStatsOnRangeKeyPutVersion(
	rangeKeys MVCCRangeKeyStack, version MVCCRangeKeyVersion,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats

	// We currently assume all range keys are MVCC range tombstones. We therefore
	// have to move the GCBytesAge contribution of the key up from the latest
	// version to the new version if it's written at the top.
	if rangeKeys.Newest().Less(version.Timestamp) {
		keyBytes := int64(EncodedMVCCKeyPrefixLength(rangeKeys.Bounds.Key)) +
			int64(EncodedMVCCKeyPrefixLength(rangeKeys.Bounds.EndKey))
		ms.AgeTo(rangeKeys.Newest().WallTime)
		ms.RangeKeyBytes -= keyBytes
		ms.AgeTo(version.Timestamp.WallTime)
		ms.RangeKeyBytes += keyBytes
	}

	// Account for the new version.
	ms.AgeTo(version.Timestamp.WallTime)
	ms.RangeKeyBytes += int64(EncodedMVCCTimestampSuffixLength(version.Timestamp))
	ms.RangeValCount++
	ms.RangeValBytes += int64(len(version.Value))

	return ms
}

// updateStatsOnRangeKeyCover updates MVCCStats for when an MVCC range key
// covers an MVCC point key at the given timestamp. The valueLen and
// isTombstone are attributes of the point key.
func updateStatsOnRangeKeyCover(
	ts hlc.Timestamp, key MVCCKey, valueLen int, isTombstone bool,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats
	ms.AgeTo(ts.WallTime)
	if !isTombstone {
		ms.LiveCount--
		ms.LiveBytes -= int64(key.EncodedSize()) + int64(valueLen)
	}
	return ms
}

// updateStatsOnRangeKeyCoverStats updates MVCCStats for when an MVCC range
// tombstone covers existing data whose stats are already known.
func updateStatsOnRangeKeyCoverStats(ts hlc.Timestamp, cur enginepb.MVCCStats) enginepb.MVCCStats {
	var ms enginepb.MVCCStats
	ms.AgeTo(ts.WallTime)
	ms.ContainsEstimates += cur.ContainsEstimates
	ms.LiveCount -= cur.LiveCount
	ms.LiveBytes -= cur.LiveBytes
	return ms
}

// updateStatsOnRangeKeyMerge updates MVCCStats for a merge of two MVCC range
// key stacks. Both sides of the merge must have identical versions. The merge
// can happen either to the right or the left, only the merge key (i.e. the key
// where the stacks abut) is needed. versions can't be empty.
func updateStatsOnRangeKeyMerge(
	mergeKey roachpb.Key, versions MVCCRangeKeyVersions,
) enginepb.MVCCStats {
	// A merge is simply the inverse of a split.
	var ms enginepb.MVCCStats
	ms.Subtract(UpdateStatsOnRangeKeySplit(mergeKey, versions))
	return ms
}

// UpdateStatsOnRangeKeySplit updates MVCCStats for the split/fragmentation of a
// range key stack at a given split key. versions can't be empty.
func UpdateStatsOnRangeKeySplit(
	splitKey roachpb.Key, versions MVCCRangeKeyVersions,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats

	// Account for the creation of one of the range key stacks, and the key
	// contribution of the end and start keys of the split stacks.
	ms.AgeTo(versions[0].Timestamp.WallTime)
	ms.RangeKeyCount++
	ms.RangeKeyBytes += 2 * int64(EncodedMVCCKeyPrefixLength(splitKey))

	// Account for the creation of all versions in new new stack.
	for _, v := range versions {
		ms.AgeTo(v.Timestamp.WallTime)
		ms.RangeValCount++
		ms.RangeKeyBytes += int64(EncodedMVCCTimestampSuffixLength(v.Timestamp))
		ms.RangeValBytes += int64(len(v.Value))
	}

	return ms
}

// updateStatsOnClear updates stat counters by subtracting a
// cleared value's key and value byte sizes. If an earlier version
// was restored, the restored values are added to live bytes and
// count if the restored value isn't a deletion tombstone.
func updateStatsOnClear(
	key roachpb.Key,
	origMetaKeySize, origMetaValSize, restoredMetaKeySize, restoredMetaValSize int64,
	orig, restored *enginepb.MVCCMetadata,
	restoredNanos int64,
) enginepb.MVCCStats {

	var ms enginepb.MVCCStats

	if isSysLocal(key) {
		if restored != nil {
			ms.SysBytes += restoredMetaKeySize + restoredMetaValSize
			ms.SysCount++
		}

		ms.SysBytes -= (orig.KeyBytes + orig.ValBytes) + (origMetaKeySize + origMetaValSize)
		ms.SysCount--
		return ms
	}

	// If we're restoring a previous value (which is thus not an intent), there are
	// two main cases:
	//
	// 1. the previous value is a tombstone, so according to rule 2 it accrues
	// GCBytesAge from its own timestamp on (we need to adjust only for the
	// implicit meta key that "pops up" at that timestamp), -- or --
	// 2. it is not, and it has been shadowed by the key we are clearing,
	// in which case we need to offset its GCBytesAge contribution from
	// restoredNanos to orig.Timestamp (rule 1).
	if restored != nil {
		if restored.Txn != nil {
			panic("restored version should never be an intent")
		}

		ms.AgeTo(restoredNanos)

		if restored.Deleted {
			// The new meta key will be implicit and at restoredNanos. It needs to
			// catch up on the GCBytesAge from that point on until orig.Timestamp
			// (rule 2).
			ms.KeyBytes += restoredMetaKeySize
			ms.ValBytes += restoredMetaValSize
		}

		ms.AgeTo(orig.Timestamp.WallTime)

		ms.KeyCount++

		if !restored.Deleted {
			// At orig.Timestamp, make the non-deletion version live again.
			// Note that there's no need to explicitly age to the "present time"
			// after.
			ms.KeyBytes += restoredMetaKeySize
			ms.ValBytes += restoredMetaValSize

			ms.LiveBytes += restored.KeyBytes + restored.ValBytes
			ms.LiveCount++
			ms.LiveBytes += restoredMetaKeySize + restoredMetaValSize
		}
	} else {
		ms.AgeTo(orig.Timestamp.WallTime)
	}

	if !orig.Deleted {
		ms.LiveBytes -= (orig.KeyBytes + orig.ValBytes) + (origMetaKeySize + origMetaValSize)
		ms.LiveCount--
	}
	ms.KeyBytes -= (orig.KeyBytes + origMetaKeySize)
	ms.ValBytes -= (orig.ValBytes + origMetaValSize)
	ms.KeyCount--
	ms.ValCount--
	if orig.Txn != nil {
		ms.IntentBytes -= (orig.KeyBytes + orig.ValBytes)
		ms.IntentCount--
		ms.SeparatedIntentCount--
	}
	return ms
}

// updateStatsOnGC updates stat counters after garbage collection
// by subtracting key and value byte counts, updating key and
// value counts, and updating the GC'able bytes age. If metaKey is
// true, then the value being GC'd is the mvcc metadata and we
// decrement the key count.
//
// nonLiveMS is the timestamp at which the value became non-live.
// For a deletion tombstone this will be its own timestamp (rule two
// in updateStatsOnPut) and for a regular version it will be the closest
// newer version's (rule one).
func updateStatsOnGC(
	key roachpb.Key, keySize, valSize int64, metaKey bool, nonLiveMS int64,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats

	if isSysLocal(key) {
		ms.SysBytes -= keySize + valSize
		if metaKey {
			ms.SysCount--
		}
		return ms
	}

	ms.AgeTo(nonLiveMS)
	ms.KeyBytes -= keySize
	ms.ValBytes -= valSize
	if metaKey {
		ms.KeyCount--
	} else {
		ms.ValCount--
	}
	return ms
}

// MVCCGetProto fetches the value at the specified key and unmarshals it into
// msg if msg is non-nil. Returns true on success or false if the key was not
// found.
//
// See the documentation for MVCCGet for the semantics of the MVCCGetOptions.
func MVCCGetProto(
	ctx context.Context,
	reader Reader,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	msg protoutil.Message,
	opts MVCCGetOptions,
) (bool, error) {
	// TODO(tschottdorf): Consider returning skipped intents to the caller.
	valueRes, mvccGetErr := MVCCGet(ctx, reader, key, timestamp, opts)
	found := valueRes.Value != nil
	// If we found a result, parse it regardless of the error returned by MVCCGet.
	if found && msg != nil {
		// If the unmarshal failed, return its result. Otherwise, pass
		// through the underlying error (which may be a WriteIntentError
		// to be handled specially alongside the returned value).
		if err := valueRes.Value.GetProto(msg); err != nil {
			return found, err
		}
	}
	return found, mvccGetErr
}

// MVCCPutProto sets the given key to the protobuf-serialized byte
// string of msg and the provided timestamp.
func MVCCPutProto(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	txn *roachpb.Transaction,
	msg protoutil.Message,
) error {
	value := roachpb.Value{}
	if err := value.SetProto(msg); err != nil {
		return err
	}
	value.InitChecksum(key)
	return MVCCPut(ctx, rw, ms, key, timestamp, localTimestamp, value, txn)
}

// MVCCBlindPutProto sets the given key to the protobuf-serialized byte string
// of msg and the provided timestamp. See MVCCBlindPut for a discussion on this
// fast-path and when it is appropriate to use.
func MVCCBlindPutProto(
	ctx context.Context,
	writer Writer,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	msg protoutil.Message,
	txn *roachpb.Transaction,
) error {
	value := roachpb.Value{}
	if err := value.SetProto(msg); err != nil {
		return err
	}
	value.InitChecksum(key)
	return MVCCBlindPut(ctx, writer, ms, key, timestamp, localTimestamp, value, txn)
}

// LockTableView is a transaction-bound view into an in-memory collections of
// key-level locks. The set of per-key locks stored in the in-memory lock table
// structure overlaps with those stored in the persistent lock table keyspace
// (i.e. intents produced by an MVCCKeyAndIntentsIterKind iterator), but one is
// not a subset of the other. There are locks only stored in the in-memory lock
// table (i.e. unreplicated locks) and locks only stored in the persistent lock
// table keyspace (i.e. replicated locks that have yet to be "discovered").
type LockTableView interface {
	// IsKeyLockedByConflictingTxn returns whether the specified key is locked or
	// reserved (see lockTable "reservations") by a conflicting transaction, given
	// the caller's own desired locking strength. If so, true is returned. If the
	// key is locked, the lock holder is also returned. Otherwise, if the key is
	// reserved, nil is also returned. A transaction's own lock or reservation
	// does not appear to be locked to itself (false is returned). The method is
	// used by requests in conjunction with the SkipLocked wait policy to
	// determine which keys they should skip over during evaluation.
	IsKeyLockedByConflictingTxn(roachpb.Key, lock.Strength) (bool, *enginepb.TxnMeta)
}

// MVCCGetOptions bundles options for the MVCCGet family of functions.
type MVCCGetOptions struct {
	// See the documentation for MVCCGet for information on these parameters.
	Inconsistent     bool
	SkipLocked       bool
	Tombstones       bool
	FailOnMoreRecent bool
	Txn              *roachpb.Transaction
	Uncertainty      uncertainty.Interval
	// MemoryAccount is used for tracking memory allocations.
	MemoryAccount *mon.BoundAccount
	// LockTable is used to determine whether keys are locked in the in-memory
	// lock table when scanning with the SkipLocked option.
	LockTable LockTableView
	// DontInterleaveIntents, when set, makes it such that intent metadata is not
	// interleaved with the results of the scan. Setting this option means that
	// the underlying pebble iterator will only scan over the MVCC keyspace and
	// will not use an `intentInterleavingIter`. It is only appropriate to use
	// this when the caller does not need to know whether a given key is an intent
	// or not. It is usually set by read-only requests that have resolved their
	// conflicts before they begin their MVCC scan.
	DontInterleaveIntents bool
	// MaxKeys is the maximum number of kv pairs returned from this operation.
	// The non-negative value represents an unbounded Get. The value -1 returns
	// no keys in the result and a ResumeSpan equal to the request span is
	// returned.
	MaxKeys int64
	// TargetBytes is a byte threshold to limit the amount of data pulled into
	// memory during a Get operation. The zero value indicates no limit. The
	// value -1 returns no keys in the result. A positive value represents an
	// unbounded Get unless AllowEmpty is set. If an empty result is returned,
	// then a ResumeSpan equal to the request span is returned.
	TargetBytes int64
	// AllowEmpty will return an empty result if the request key exceeds the
	// TargetBytes limit.
	AllowEmpty bool
}

// MVCCGetResult bundles return values for the MVCCGet family of functions.
type MVCCGetResult struct {
	// The most recent value for the specified key whose timestamp is less than
	// or equal to the supplied timestamp. If no such value exists, nil is
	// returned instead.
	Value *roachpb.Value
	// In inconsistent mode, the intent if an intent is encountered. In
	// consistent mode, an intent will generate a WriteIntentError with the
	// intent embedded within and the intent parameter will be nil.
	Intent *roachpb.Intent
	// See the documentation for roachpb.ResponseHeader for information on
	// these parameters.
	ResumeSpan      *roachpb.Span
	ResumeReason    roachpb.ResumeReason
	ResumeNextBytes int64
	NumKeys         int64
	NumBytes        int64
}

func (opts *MVCCGetOptions) validate() error {
	if opts.Inconsistent && opts.Txn != nil {
		return errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if opts.Inconsistent && opts.SkipLocked {
		return errors.Errorf("cannot allow inconsistent reads with skip locked option")
	}
	if opts.Inconsistent && opts.FailOnMoreRecent {
		return errors.Errorf("cannot allow inconsistent reads with fail on more recent option")
	}
	if opts.DontInterleaveIntents && opts.SkipLocked {
		return errors.Errorf("cannot disable interleaved intents with skip locked option")
	}
	return nil
}

func (opts *MVCCGetOptions) errOnIntents() bool {
	return !opts.Inconsistent && !opts.SkipLocked
}

// MVCCResolveWriteIntentOptions bundles options for the MVCCResolveWriteIntent
// function.
type MVCCResolveWriteIntentOptions struct {
	// See the documentation for MVCCResolveWriteIntent for information on these
	// parameters.
	TargetBytes int64
}

// MVCCResolveWriteIntentRangeOptions bundles options for the
// MVCCResolveWriteIntentRange function.
type MVCCResolveWriteIntentRangeOptions struct {
	// See the documentation for MVCCResolveWriteIntentRange for information on
	// these parameters.
	MaxKeys     int64
	TargetBytes int64
}

// newMVCCIterator sets up a suitable iterator for high-level MVCC operations
// operating at the given timestamp. If timestamp is empty or if
// `noInterleavedIntents` is set, the iterator is considered to be used for
// inline values, disabling intents and range keys. If rangeKeyMasking is true,
// IterOptions.RangeKeyMaskingBelow is set to the given timestamp.
func newMVCCIterator(
	reader Reader,
	timestamp hlc.Timestamp,
	rangeKeyMasking bool,
	noInterleavedIntents bool,
	opts IterOptions,
) MVCCIterator {
	// If reading inline then just return a plain MVCCIterator without intents.
	// However, we allow the caller to enable range keys, since they may be needed
	// for conflict checks when writing inline values.
	if timestamp.IsEmpty() {
		return reader.NewMVCCIterator(MVCCKeyIterKind, opts)
	}
	// Enable range key masking if requested.
	if rangeKeyMasking && opts.KeyTypes != IterKeyTypePointsOnly &&
		opts.RangeKeyMaskingBelow.IsEmpty() {
		opts.RangeKeyMaskingBelow = timestamp
	}
	iterKind := MVCCKeyAndIntentsIterKind
	if noInterleavedIntents {
		iterKind = MVCCKeyIterKind
	}
	return reader.NewMVCCIterator(iterKind, opts)
}

// MVCCGet returns a MVCCGetResult.
//
// The first field of MVCCGetResult contains the most recent value for the
// specified key whose timestamp is less than or equal to the supplied
// timestamp. If no such value exists, nil is returned instead.
//
// In tombstones mode, if the most recent value is a deletion tombstone, the
// result will be a non-nil roachpb.Value whose RawBytes field is nil.
// Otherwise, a deletion tombstone results in a nil roachpb.Value. MVCC range
// tombstones are emitted as synthetic point tombstones, even if there is no
// existing point key at the position.
//
// NB: Synthetic tombstones generated for MVCC range tombstones may not be
// visible to an MVCCScan, since MVCCScan will only synthesize point tombstones
// above existing point keys.
//
// In inconsistent mode, if an intent is encountered, it will be placed in the
// intent field. By contrast, in consistent mode, an intent will generate a
// WriteIntentError with the intent embedded within, and the intent result
// parameter will be nil.
//
// Note that transactional gets must be consistent. Put another way, only
// non-transactional gets may be inconsistent.
//
// If the timestamp is specified as hlc.Timestamp{}, the value is expected to be
// "inlined". See MVCCPut().
//
// When reading in "skip locked" mode, a key that is locked by a transaction
// other than the reader is not included in the result set and does not result
// in a WriteIntentError. Instead, the key is included in the encountered intent
// result parameter so that it can be resolved asynchronously. In this mode, the
// LockTableView provided in the options is consulted any observed key to
// determine whether it is locked with an unreplicated lock.
//
// When reading in "fail on more recent" mode, a WriteTooOldError will be
// returned if the read observes a version with a timestamp above the read
// timestamp. Similarly, a WriteIntentError will be returned if the read
// observes another transaction's intent, even if it has a timestamp above
// the read timestamp.
func MVCCGet(
	ctx context.Context, reader Reader, key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (MVCCGetResult, error) {
	res, _, err := MVCCGetWithValueHeader(ctx, reader, key, timestamp, opts)
	return res, err
}

// MVCCGetWithValueHeader is like MVCCGet, but in addition returns the
// MVCCValueHeader for the value.
func MVCCGetWithValueHeader(
	ctx context.Context, reader Reader, key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (MVCCGetResult, enginepb.MVCCValueHeader, error) {
	var result MVCCGetResult
	if opts.MaxKeys < 0 || opts.TargetBytes < 0 {
		// Receipt of a GetRequest with negative MaxKeys or TargetBytes indicates
		// that the request was part of a batch that has already exhausted its
		// limit, which means that we should *not* serve the request and return a
		// ResumeSpan for this GetRequest.
		result.ResumeSpan = &roachpb.Span{Key: key}
		if opts.MaxKeys < 0 {
			result.ResumeReason = roachpb.RESUME_KEY_LIMIT
		} else if opts.TargetBytes < 0 {
			result.ResumeReason = roachpb.RESUME_BYTE_LIMIT
		}
		return result, enginepb.MVCCValueHeader{}, nil
	}
	iter := newMVCCIterator(
		reader, timestamp, false /* rangeKeyMasking */, opts.DontInterleaveIntents, IterOptions{
			KeyTypes: IterKeyTypePointsAndRanges,
			Prefix:   true,
		},
	)
	defer iter.Close()
	value, intent, vh, err := mvccGetWithValueHeader(ctx, iter, key, timestamp, opts)
	val := value.ToPointer()
	if err == nil && val != nil {
		// NB: This calculation is different from Scan, since Scan responses include
		// the key/value pair while Get only includes the value.
		numBytes := int64(len(val.RawBytes))
		if opts.TargetBytes > 0 && opts.AllowEmpty && numBytes > opts.TargetBytes {
			result.ResumeSpan = &roachpb.Span{Key: key}
			result.ResumeReason = roachpb.RESUME_BYTE_LIMIT
			result.ResumeNextBytes = numBytes
			return result, enginepb.MVCCValueHeader{}, nil
		}
		result.NumKeys = 1
		result.NumBytes = numBytes
	}
	result.Value = val
	result.Intent = intent
	return result, vh, err
}

// gcassert:inline
func mvccGet(
	ctx context.Context,
	iter MVCCIterator,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCGetOptions,
) (value optionalValue, intent *roachpb.Intent, err error) {
	value, intent, _, err = mvccGetWithValueHeader(ctx, iter, key, timestamp, opts)
	return value, intent, err
}

func mvccGetWithValueHeader(
	ctx context.Context,
	iter MVCCIterator,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCGetOptions,
) (value optionalValue, intent *roachpb.Intent, vh enginepb.MVCCValueHeader, err error) {
	if len(key) == 0 {
		return optionalValue{}, nil, enginepb.MVCCValueHeader{}, emptyKeyError()
	}
	if timestamp.WallTime < 0 {
		return optionalValue{}, nil, enginepb.MVCCValueHeader{}, errors.Errorf("cannot write to %q at timestamp %s", key, timestamp)
	}
	if util.RaceEnabled && !iter.IsPrefix() {
		return optionalValue{}, nil, enginepb.MVCCValueHeader{}, errors.AssertionFailedf("mvccGet called with non-prefix iterator")
	}
	if err := opts.validate(); err != nil {
		return optionalValue{}, nil, enginepb.MVCCValueHeader{}, err
	}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer mvccScanner.release()

	// MVCCGet is implemented as an MVCCScan where we retrieve a single key. We
	// specify an empty key for the end key which will ensure we don't retrieve a
	// key different than the start key. This is a bit of a hack.
	*mvccScanner = pebbleMVCCScanner{
		parent:           iter,
		memAccount:       opts.MemoryAccount,
		lockTable:        opts.LockTable,
		start:            key,
		ts:               timestamp,
		maxKeys:          1,
		inconsistent:     opts.Inconsistent,
		skipLocked:       opts.SkipLocked,
		tombstones:       opts.Tombstones,
		failOnMoreRecent: opts.FailOnMoreRecent,
		keyBuf:           mvccScanner.keyBuf,
	}

	var results pebbleResults
	mvccScanner.init(opts.Txn, opts.Uncertainty, &results)
	mvccScanner.get(ctx)

	// If we have a trace, emit the scan stats that we produced.
	recordIteratorStats(ctx, mvccScanner.parent)

	if mvccScanner.err != nil {
		return optionalValue{}, nil, enginepb.MVCCValueHeader{}, mvccScanner.err
	}
	intents, err := buildScanIntents(mvccScanner.intentsRepr())
	if err != nil {
		return optionalValue{}, nil, enginepb.MVCCValueHeader{}, err
	}
	if opts.errOnIntents() && len(intents) > 0 {
		return optionalValue{}, nil, enginepb.MVCCValueHeader{}, &roachpb.WriteIntentError{Intents: intents}
	}

	if len(intents) > 1 {
		return optionalValue{}, nil, enginepb.MVCCValueHeader{}, errors.Errorf("expected 0 or 1 intents, got %d", len(intents))
	} else if len(intents) == 1 {
		intent = &intents[0]
	}

	if len(results.repr) == 0 {
		return optionalValue{}, intent, enginepb.MVCCValueHeader{}, nil
	}

	mvccKey, rawValue, _, err := MVCCScanDecodeKeyValue(results.repr)
	if err != nil {
		return optionalValue{}, nil, enginepb.MVCCValueHeader{}, err
	}

	value = makeOptionalValue(roachpb.Value{
		RawBytes:  rawValue,
		Timestamp: mvccKey.Timestamp,
	})
	// NB: we may return MVCCValueHeader out of curUnsafeValue because that
	// type does not contain any pointers. A comment on MVCCValueHeader ensures
	// that this stays true.
	return value, intent, mvccScanner.curUnsafeValue.MVCCValueHeader, nil
}

// MVCCGetAsTxn constructs a temporary transaction from the given transaction
// metadata and calls MVCCGet as that transaction. This method is required
// only for reading intents of a transaction when only its metadata is known
// and should rarely be used.
//
// The read is carried out without the chance of uncertainty restarts.
func MVCCGetAsTxn(
	ctx context.Context,
	reader Reader,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	txnMeta enginepb.TxnMeta,
) (MVCCGetResult, error) {
	return MVCCGet(ctx, reader, key, timestamp, MVCCGetOptions{
		Txn: &roachpb.Transaction{
			TxnMeta:                txnMeta,
			Status:                 roachpb.PENDING,
			ReadTimestamp:          txnMeta.WriteTimestamp,
			GlobalUncertaintyLimit: txnMeta.WriteTimestamp,
		}})
}

// mvccGetMetadata returns or reconstructs the meta key for the given key.
// A prefix scan using the iterator is performed, resulting in one of the
// following successful outcomes:
//
//  1. iterator finds nothing; returns (false, 0, 0, nil).
//  2. iterator finds an explicit meta key; unmarshals and returns its size.
//     ok is set to true.
//  3. iterator finds a value, i.e. the meta key is implicit.
//     In this case, it accounts for the size of the key with the portion
//     of the user key found which is not the MVCC timestamp suffix (since
//     that is the usual contribution of the meta key). The value size returned
//     will be zero, as there is no stored MVCCMetadata.
//     ok is set to true.
//  4. iterator finds an MVCC range tombstone above a value. In this case,
//     metadata for a synthetic point tombstone is returned.
//
// The timestamp where the real point key last changed is also returned, if a
// real point key was found. This may differ from the metadata timestamp when a
// point key is covered by multiple MVCC range tombstones (in which case the
// point key disappeared at the _lowest_ range tombstone above it), or when a
// point tombstone is covered by a range tombstone (in which case the point key
// disappeared at the point tombstone). It is needed to correctly account for
// the GCBytesAge contribution of the key prefix, which is not affected by MVCC
// range tombstones, and would be incorrect if we used the synthetic point
// tombstone of the newest MVCC range tombstone instead.
//
// The passed in MVCCMetadata must not be nil. If the supplied iterator is nil,
// no seek operation is performed. This is used by the Blind{Put,ConditionalPut}
// operations to avoid seeking when the metadata is known not to exist.
func mvccGetMetadata(
	iter MVCCIterator, metaKey MVCCKey, meta *enginepb.MVCCMetadata,
) (ok bool, keyBytes, valBytes int64, realKeyChanged hlc.Timestamp, err error) {
	if iter == nil {
		return false, 0, 0, hlc.Timestamp{}, nil
	}
	iter.SeekGE(metaKey)
	if ok, err = iter.Valid(); !ok {
		return false, 0, 0, hlc.Timestamp{}, err
	}
	unsafeKey := iter.UnsafeKey()
	if !unsafeKey.Key.Equal(metaKey.Key) {
		return false, 0, 0, hlc.Timestamp{}, nil
	}
	hasPoint, hasRange := iter.HasPointAndRange()

	// Check for existing intent metadata. Intents will be emitted colocated with
	// a covering range key when seeking to it, and always located above range
	// keys, so we don't need to check for range keys here.
	if hasPoint && !unsafeKey.IsValue() {
		if err := iter.ValueProto(meta); err != nil {
			return false, 0, 0, hlc.Timestamp{}, err
		}
		return true, int64(unsafeKey.EncodedSize()), int64(iter.ValueLen()),
			meta.Timestamp.ToTimestamp(), nil
	}

	// Synthesize point key metadata. For values, the size of keys is always
	// accounted for as MVCCVersionTimestampSize. The size of the metadata key is
	// accounted for separately.
	meta.Reset()
	meta.KeyBytes = MVCCVersionTimestampSize

	// If we land on a (bare) range key, step to look for a colocated point key.
	if hasRange && !hasPoint {
		rkTimestamp := iter.RangeKeys().Versions[0].Timestamp

		iter.Next()
		if ok, err = iter.Valid(); err != nil {
			return false, 0, 0, hlc.Timestamp{}, err
		} else if ok {
			// NB: For !ok, hasPoint is already false.
			hasPoint, hasRange = iter.HasPointAndRange()
			unsafeKey = iter.UnsafeKey()
		}
		// If only a bare range tombstone was found at the seek key, synthesize
		// point tombstone metadata for it. realKeyChanged is empty since there
		// was no real point key here.
		if !hasPoint || !unsafeKey.Key.Equal(metaKey.Key) {
			meta.Deleted = true
			meta.Timestamp = rkTimestamp.ToLegacyTimestamp()
			return true, 0, 0, hlc.Timestamp{}, nil
		}
	}

	// We're now on a point key.
	valueLen, isTombstone, err := iter.MVCCValueLenAndIsTombstone()
	if err != nil {
		return false, 0, 0, hlc.Timestamp{}, err
	}

	// Check if the point key is covered by an MVCC range tombstone, and
	// synthesize point tombstone metadata for it in that case. realKeyChanged is
	// set to the timestamp where the point key ceased to exist -- either the
	// lowest range tombstone above the key (not the highest which is used for
	// metadata), or the point version's timestamp if it was a tombstone.
	if hasRange {
		rangeKeys := iter.RangeKeys()
		if v, ok := rangeKeys.FirstAtOrAbove(unsafeKey.Timestamp); ok {
			meta.Deleted = true
			meta.Timestamp = rangeKeys.Versions[0].Timestamp.ToLegacyTimestamp()
			keyLastSeen := v.Timestamp
			if isTombstone {
				keyLastSeen = unsafeKey.Timestamp
			}
			return true, int64(EncodedMVCCKeyPrefixLength(metaKey.Key)), 0, keyLastSeen, nil
		}
	}

	// Synthesize metadata for a regular point key.
	meta.ValBytes = int64(valueLen)
	meta.Deleted = isTombstone
	meta.Timestamp = unsafeKey.Timestamp.ToLegacyTimestamp()

	return true, int64(EncodedMVCCKeyPrefixLength(metaKey.Key)), 0, unsafeKey.Timestamp, nil
}

// putBuffer holds pointer data needed by mvccPutInternal. Bundling
// this data into a single structure reduces memory
// allocations. Managing this temporary buffer using a sync.Pool
// completely eliminates allocation from the put common path.
type putBuffer struct {
	meta    enginepb.MVCCMetadata
	newMeta enginepb.MVCCMetadata
	ts      hlc.LegacyTimestamp // avoids heap allocations
	tmpbuf  []byte              // avoids heap allocations
}

var putBufferPool = sync.Pool{
	New: func() interface{} {
		return &putBuffer{}
	},
}

func newPutBuffer() *putBuffer {
	return putBufferPool.Get().(*putBuffer)
}

func (b *putBuffer) release() {
	*b = putBuffer{tmpbuf: b.tmpbuf[:0]}
	putBufferPool.Put(b)
}

func (b *putBuffer) marshalMeta(meta *enginepb.MVCCMetadata) (_ []byte, err error) {
	size := meta.Size()
	data := b.tmpbuf
	if cap(data) < size {
		data = make([]byte, size)
	} else {
		data = data[:size]
	}
	n, err := protoutil.MarshalTo(meta, data)
	if err != nil {
		return nil, err
	}
	b.tmpbuf = data
	return data[:n], nil
}

func (b *putBuffer) putInlineMeta(
	writer Writer, key MVCCKey, meta *enginepb.MVCCMetadata,
) (keyBytes, valBytes int64, err error) {
	bytes, err := b.marshalMeta(meta)
	if err != nil {
		return 0, 0, err
	}
	if err := writer.PutUnversioned(key.Key, bytes); err != nil {
		return 0, 0, err
	}
	return int64(key.EncodedSize()), int64(len(bytes)), nil
}

var trueValue = true

func (b *putBuffer) putIntentMeta(
	ctx context.Context, writer Writer, key MVCCKey, meta *enginepb.MVCCMetadata, alreadyExists bool,
) (keyBytes, valBytes int64, err error) {
	if meta.Txn != nil && meta.Timestamp.ToTimestamp() != meta.Txn.WriteTimestamp {
		// The timestamps are supposed to be in sync. If they weren't, it wouldn't
		// be clear for readers which one to use for what.
		return 0, 0, errors.AssertionFailedf(
			"meta.Timestamp != meta.Txn.WriteTimestamp: %s != %s", meta.Timestamp, meta.Txn.WriteTimestamp)
	}
	if alreadyExists {
		// Absence represents false.
		meta.TxnDidNotUpdateMeta = nil
	} else {
		meta.TxnDidNotUpdateMeta = &trueValue
	}
	bytes, err := b.marshalMeta(meta)
	if err != nil {
		return 0, 0, err
	}
	if err = writer.PutIntent(ctx, key.Key, bytes, meta.Txn.ID); err != nil {
		return 0, 0, err
	}
	return int64(key.EncodedSize()), int64(len(bytes)), nil
}

// MVCCPut sets the value for a specified key. It will save the value
// with different versions according to its timestamp and update the
// key metadata. The timestamp must be passed as a parameter; using
// the Timestamp field on the value results in an error.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter is
// confusing and redundant. See the comment on mvccPutInternal for details.
//
// If the timestamp is specified as hlc.Timestamp{}, the value is
// inlined instead of being written as a timestamp-versioned value. A
// zero timestamp write to a key precludes a subsequent write using a
// non-zero timestamp and vice versa. Inlined values require only a
// single row and never accumulate more than a single value. Successive
// zero timestamp writes to a key replace the value and deletes clear
// the value. In addition, zero timestamp values may be merged.
func MVCCPut(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
) error {
	// If we're not tracking stats for the key and we're writing a non-versioned
	// key we can utilize a blind put to avoid reading any existing value.
	var iter MVCCIterator
	blind := ms == nil && timestamp.IsEmpty()
	if !blind {
		iter = newMVCCIterator(
			rw, timestamp, false /* rangeKeyMasking */, false /* noInterleavedIntents */, IterOptions{
				KeyTypes: IterKeyTypePointsAndRanges,
				Prefix:   true,
			},
		)
		defer iter.Close()
	}
	return mvccPutUsingIter(ctx, rw, iter, ms, key, timestamp, localTimestamp, value, txn, nil)
}

// MVCCBlindPut is a fast-path of MVCCPut. See the MVCCPut comments for details
// of the semantics. MVCCBlindPut skips retrieving the existing metadata for
// the key requiring the caller to guarantee no versions for the key currently
// exist in order for stats to be updated properly. If a previous version of
// the key does exist it is up to the caller to properly account for their
// existence in updating the stats.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCBlindPut(
	ctx context.Context,
	writer Writer,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
) error {
	return mvccPutUsingIter(ctx, writer, nil, ms, key, timestamp, localTimestamp, value, txn, nil)
}

// MVCCDelete marks the key deleted so that it will not be returned in
// future get responses.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter is
// confusing and redundant. See the comment on mvccPutInternal for details.
//
// foundKey indicates whether the key that was passed in had a value already in
// the database.
func MVCCDelete(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	txn *roachpb.Transaction,
) (foundKey bool, err error) {
	iter := newMVCCIterator(
		rw, timestamp, false /* rangeKeyMasking */, false /* noInterleavedIntents */, IterOptions{
			KeyTypes: IterKeyTypePointsAndRanges,
			Prefix:   true,
		},
	)
	defer iter.Close()

	buf := newPutBuffer()
	defer buf.release()

	// TODO(yuzefovich): can we avoid the put if the key does not exist?
	return mvccPutInternal(
		ctx, rw, iter, ms, key, timestamp, localTimestamp, noValue, txn, buf, nil)
}

var noValue = roachpb.Value{}

// mvccPutUsingIter sets the value for a specified key using the provided
// MVCCIterator. The function takes a value and a valueFn, only one of which
// should be provided. If the valueFn is nil, value's raw bytes will be set
// for the key, else the bytes provided by the valueFn will be used.
func mvccPutUsingIter(
	ctx context.Context,
	writer Writer,
	iter MVCCIterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
	valueFn func(optionalValue) (roachpb.Value, error),
) error {
	buf := newPutBuffer()
	defer buf.release()

	// Most callers don't care about the returned exReplaced value. The ones that
	// do can call mvccPutInternal directly.
	_, err := mvccPutInternal(
		ctx, writer, iter, ms, key, timestamp, localTimestamp, value, txn, buf, valueFn)
	return err
}

// maybeGetValue returns either value (if valueFn is nil) or else the
// result of calling valueFn on the data read at readTimestamp. The
// function uses a non-transactional read, so uncertainty does not apply
// and any intents (even the caller's own if the caller is operating on
// behalf of a transaction), will result in a WriteIntentError. Because
// of this, the function is only called from places where intents have
// already been considered.
func maybeGetValue(
	ctx context.Context,
	iter MVCCIterator,
	key roachpb.Key,
	value roachpb.Value,
	exists bool,
	readTimestamp hlc.Timestamp,
	valueFn func(optionalValue) (roachpb.Value, error),
) (roachpb.Value, error) {
	// If a valueFn is specified, read existing value using the iter.
	if valueFn == nil {
		return value, nil
	}
	var exVal optionalValue
	if exists {
		var err error
		exVal, _, err = mvccGet(ctx, iter, key, readTimestamp, MVCCGetOptions{Tombstones: true})
		if err != nil {
			return roachpb.Value{}, err
		}
	}
	return valueFn(exVal)
}

// MVCCScanDecodeKeyValue decodes a key/value pair returned in an MVCCScan
// "batch" (this is not the RocksDB batch repr format), returning both the
// key/value and the suffix of data remaining in the batch.
func MVCCScanDecodeKeyValue(repr []byte) (key MVCCKey, value []byte, orepr []byte, err error) {
	k, ts, value, orepr, err := enginepb.ScanDecodeKeyValue(repr)
	return MVCCKey{k, ts}, value, orepr, err
}

// MVCCScanDecodeKeyValues decodes all key/value pairs returned in one or more
// MVCCScan "batches" (this is not the RocksDB batch repr format). The provided
// function is called for each key/value pair.
func MVCCScanDecodeKeyValues(repr [][]byte, fn func(key MVCCKey, rawBytes []byte) error) error {
	return enginepb.ScanDecodeKeyValues(repr, func(k []byte, ts hlc.Timestamp, rawBytes []byte) error {
		return fn(MVCCKey{k, ts}, rawBytes)
	})
}

// replayTransactionalWrite performs a transactional write under the assumption
// that the transactional write was already executed before. Essentially a replay.
// Since transactions should be idempotent, we must be particularly careful
// about writing an intent if it was already written. If the sequence of the
// transaction is at or below one found in `meta.Txn` then we should
// simply assert the value we're trying to add against the value that was
// previously written at that sequence.
//
// 1) Firstly, we find the value previously written as part of the same sequence.
// 2) We then figure out the value the transaction is trying to write (either the
// value itself or the valueFn applied to the right previous value).
// 3) We assert that the transactional write is idempotent.
//
// Ensure all intents are found and that the values are always accurate for
// transactional idempotency.
func replayTransactionalWrite(
	ctx context.Context,
	iter MVCCIterator,
	meta *enginepb.MVCCMetadata,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
	valueFn func(optionalValue) (roachpb.Value, error),
) error {
	var writtenValue optionalValue
	var err error
	if txn.Sequence == meta.Txn.Sequence {
		// This is a special case. This is when the intent hasn't made it
		// to the intent history yet. We must now assert the value written
		// in the intent to the value we're trying to write.
		writtenValue, _, err = mvccGet(ctx, iter, key, timestamp, MVCCGetOptions{
			Txn:        txn,
			Tombstones: true,
		})
		if err != nil {
			return err
		}
	} else {
		// Get the value from the intent history.
		if intentValRaw, ok := meta.GetIntentValue(txn.Sequence); ok {
			intentVal, err := DecodeMVCCValue(intentValRaw)
			if err != nil {
				return err
			}
			writtenValue = makeOptionalValue(intentVal.Value)
		}
	}
	if !writtenValue.exists {
		// NB: This error may be due to a batched `DelRange` operation that, upon being replayed, finds a new key to delete.
		// See issue #71236 for more explanation.
		err := errors.AssertionFailedf("transaction %s with sequence %d missing an intent with lower sequence %d",
			txn.ID, meta.Txn.Sequence, txn.Sequence)
		errWithIssue := errors.WithIssueLink(err,
			errors.IssueLink{
				IssueURL: "https://github.com/cockroachdb/cockroach/issues/71236",
				Detail: "This error may be caused by `DelRange` operation in a batch that also contains a " +
					"write on an intersecting key, as in the case the other write hits a `WriteTooOld` " +
					"error, it is possible for the `DelRange` operation to pick up a new key to delete on " +
					"replay, which will cause sanity checks of intent history to fail as the first iteration " +
					"of the operation would not have placed an intent on this new key.",
			})
		return errWithIssue
	}

	// If the valueFn is specified, we must apply it to the would-be value at the key.
	if valueFn != nil {
		var exVal optionalValue

		// If there's an intent history, use that.
		prevIntent, prevValueWritten := meta.GetPrevIntentSeq(txn.Sequence, txn.IgnoredSeqNums)
		if prevValueWritten {
			// If the previous value was found in the IntentHistory,
			// simply apply the value function to the historic value
			// to get the would-be value.
			prevIntentVal, err := DecodeMVCCValue(prevIntent.Value)
			if err != nil {
				return err
			}
			exVal = makeOptionalValue(prevIntentVal.Value)
		} else {
			// If the previous value at the key wasn't written by this
			// transaction, or it was hidden by a rolled back seqnum, we look at
			// last committed value on the key. Since we want the last committed
			// value on the key, we must make an inconsistent read so we ignore
			// our previous intents here.
			exVal, _, err = mvccGet(ctx, iter, key, timestamp, MVCCGetOptions{
				Inconsistent: true,
				Tombstones:   true,
			})
			if err != nil {
				return err
			}
		}

		value, err = valueFn(exVal)
		if err != nil {
			return err
		}
	}

	// To ensure the transaction is idempotent, we must assert that the
	// calculated value on this replay is the same as the one we've previously
	// written.
	if !bytes.Equal(value.RawBytes, writtenValue.RawBytes) {
		return errors.AssertionFailedf("transaction %s with sequence %d has a different value %+v after recomputing from what was written: %+v",
			txn.ID, txn.Sequence, value.RawBytes, writtenValue.RawBytes)
	}
	return nil
}

// mvccPutInternal adds a new timestamped value to the specified key.
// If value is nil, creates a deletion tombstone value. valueFn is
// an optional alternative to supplying value directly. It is passed
// the existing value (or nil if none exists) and returns the value
// to write or an error. If valueFn is supplied, value should be nil
// and vice versa. valueFn can delete by returning nil. Returning
// []byte{} will write an empty value, not delete.
//
// The returned boolean indicates whether the put replaced an existing live key
// (including one written previously by the same transaction). This is evaluated
// at the transaction's read timestamp, and may not be valid when
// `WriteTooOldError` is returned having written at a higher timestamp.
// TODO(erikgrinaker): This return value exists solely because valueFn incurs an
// additional seek via maybeGetValue(). In most cases we have already read the
// value via other means, e.g. mvccGetMetadata(). We should restructure the code
// such that valueFn omits unnecessary reads in the common case and then use it
// rather than the returned boolean where needed. See also:
// https://github.com/cockroachdb/cockroach/issues/90609
//
// The given iter must surface range keys to correctly account for
// MVCC range tombstones in MVCC stats.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter
// is redundant. Specifically, the intent is written at the txn's
// provisional commit timestamp, txn.WriteTimestamp, unless it is
// forwarded by an existing committed value above that timestamp.
// However, reads (e.g., for a ConditionalPut) are performed at the
// txn's read timestamp (txn.ReadTimestamp) to ensure that the
// client sees a consistent snapshot of the database. Any existing
// committed writes that are newer than the read timestamp will thus
// generate a WriteTooOld error.
//
// In an attempt to reduce confusion about which timestamp applies, when writing
// transactionally, the timestamp parameter must be equal to the transaction's
// read timestamp. (One could imagine instead requiring that the timestamp
// parameter be set to hlc.Timestamp{} when writing transactionally, but
// hlc.Timestamp{} is already used as a sentinel for inline puts.)
//
// The local timestamp parameter dictates the local clock timestamp
// assigned to the key-value. It should be taken from the local HLC
// clock on the leaseholder that is performing the write and must be
// below the leaseholder's lease expiration. If the supplied local
// timestamp is empty (hlc.ClockTimestamp{}), the value will not be
// assigned an explicit local timestamp. The effect of this is that
// readers treat the local timestamp as being equal to the version
// timestamp.
func mvccPutInternal(
	ctx context.Context,
	writer Writer,
	iter MVCCIterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
	buf *putBuffer,
	valueFn func(optionalValue) (roachpb.Value, error),
) (bool, error) {
	if len(key) == 0 {
		return false, emptyKeyError()
	}
	if timestamp.WallTime < 0 {
		return false, errors.Errorf("cannot write to %q at timestamp %s", key, timestamp)
	}
	if !value.Timestamp.IsEmpty() {
		return false, errors.Errorf("cannot have timestamp set in value")
	}

	metaKey := MakeMVCCMetadataKey(key)
	ok, origMetaKeySize, origMetaValSize, origRealKeyChanged, err :=
		mvccGetMetadata(iter, metaKey, &buf.meta)
	if err != nil {
		return false, err
	}

	// Verify we're not mixing inline and non-inline values.
	putIsInline := timestamp.IsEmpty()
	if ok && putIsInline != buf.meta.IsInline() {
		return false, errors.Errorf("%q: put is inline=%t, but existing value is inline=%t",
			metaKey, putIsInline, buf.meta.IsInline())
	}
	// Handle inline put. No IntentHistory is required for inline writes as they
	// aren't allowed within transactions. MVCC range tombstones cannot exist
	// across them either.
	if putIsInline {
		if txn != nil {
			return false, errors.Errorf("%q: inline writes not allowed within transactions", metaKey)
		}
		var metaKeySize, metaValSize int64
		if value, err = maybeGetValue(ctx, iter, key, value, ok, timestamp, valueFn); err != nil {
			return false, err
		}
		if !value.IsPresent() {
			metaKeySize, metaValSize, err = 0, 0, writer.ClearUnversioned(metaKey.Key)
		} else {
			buf.meta = enginepb.MVCCMetadata{RawBytes: value.RawBytes}
			metaKeySize, metaValSize, err = buf.putInlineMeta(writer, metaKey, &buf.meta)
		}
		if ms != nil {
			updateStatsForInline(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize)
		}
		return ok && !buf.meta.Deleted, err
	}

	// Determine the read and write timestamps for the write. For a
	// non-transactional write, these will be identical. For a transactional
	// write, we read at the transaction's read timestamp but write intents at its
	// provisional commit timestamp. See the comment on the txn.WriteTimestamp field
	// definition for rationale.
	readTimestamp := timestamp
	writeTimestamp := timestamp
	if txn != nil {
		readTimestamp = txn.ReadTimestamp
		if readTimestamp != timestamp {
			return false, errors.AssertionFailedf(
				"mvccPutInternal: txn's read timestamp %s does not match timestamp %s",
				readTimestamp, timestamp)
		}
		writeTimestamp = txn.WriteTimestamp
	}

	timestamp = hlc.Timestamp{} // prevent accidental use below

	// Determine what the logical operation is. Are we writing an intent
	// or a value directly?
	logicalOp := MVCCWriteValueOpType
	if txn != nil {
		logicalOp = MVCCWriteIntentOpType
	}

	var meta *enginepb.MVCCMetadata
	buf.newMeta = enginepb.MVCCMetadata{
		IntentHistory: buf.meta.IntentHistory,
	}

	var maybeTooOldErr error
	var prevIsValue bool
	var prevValSize int64
	var exReplaced bool
	if ok {
		// There is existing metadata for this key; ensure our write is permitted.
		meta = &buf.meta
		metaTimestamp := meta.Timestamp.ToTimestamp()
		exReplaced = !meta.Deleted

		// Handle intents. MVCC range tombstones should not require any special
		// handling, since they cannot be transactional.
		if meta.Txn != nil {
			// There is an uncommitted write intent.
			if txn == nil || meta.Txn.ID != txn.ID {
				// The current Put operation does not come from the same
				// transaction.
				return false, &roachpb.WriteIntentError{Intents: []roachpb.Intent{
					roachpb.MakeIntent(meta.Txn, key),
				}}
			} else if txn.Epoch < meta.Txn.Epoch {
				return false, errors.Errorf("put with epoch %d came after put with epoch %d in txn %s",
					txn.Epoch, meta.Txn.Epoch, txn.ID)
			} else if txn.Epoch == meta.Txn.Epoch && txn.Sequence <= meta.Txn.Sequence {
				// The transaction has executed at this sequence before. This is merely a
				// replay of the transactional write. Assert that all is in order and return
				// early.
				return false, replayTransactionalWrite(ctx, iter, meta, key, readTimestamp, value, txn, valueFn)
			}

			// We're overwriting the intent that was present at this key, before we do
			// that though - we must record the older value in the IntentHistory.
			oldVersionKey := metaKey
			oldVersionKey.Timestamp = metaTimestamp

			// But where to find the older value? There are 4 cases:
			// - last write inside txn, same epoch, seqnum of last write is not
			//   ignored: value at key.
			//   => read the value associated with the intent with consistent
			//   mvccGetInternal(). (This is the common case.)
			// - last write inside txn, same epoch, seqnum of last write is ignored:
			//   cannot use value at key.
			//   => try reading from intent history.
			//   => if all intent history entries are rolled back, fall back to last
			//      case below.
			// - last write outside txn or at different epoch: use inconsistent
			//   mvccGetInternal, which will find it outside.
			//
			// (Note that _some_ value is guaranteed to be found, as indicated by
			// ok == true above. The one notable exception is when there are no past
			// committed values, and all past writes by this transaction have been
			// rolled back, either due to transaction retries or transaction savepoint
			// rollbacks.)
			var exVal optionalValue
			// Set when the current provisional value is not ignored due to a txn
			// restart or a savepoint rollback. Represents an encoded MVCCValue.
			var curProvValRaw []byte
			if txn.Epoch == meta.Txn.Epoch /* last write inside txn */ {
				if !enginepb.TxnSeqIsIgnored(meta.Txn.Sequence, txn.IgnoredSeqNums) {
					// Seqnum of last write is not ignored. Retrieve the value.
					iter.SeekGE(oldVersionKey)
					var hasPoint bool
					if valid, err := iter.Valid(); err != nil {
						return false, err
					} else if valid {
						hasPoint, _ = iter.HasPointAndRange()
					}
					if !hasPoint || !iter.UnsafeKey().Equal(oldVersionKey) {
						return false, errors.Errorf("existing intent value missing: %s", oldVersionKey)
					}

					// NOTE: we use Value instead of UnsafeValue so that we can move the
					// iterator below without invalidating this byte slice.
					curProvValRaw, err = iter.Value()
					if err != nil {
						return false, err
					}
					curIntentVal, err := DecodeMVCCValue(curProvValRaw)
					if err != nil {
						return false, err
					}
					exVal = makeOptionalValue(curIntentVal.Value)
				} else {
					// Seqnum of last write was ignored. Try retrieving the value from the history.
					prevIntent, prevIntentOk := meta.GetPrevIntentSeq(txn.Sequence, txn.IgnoredSeqNums)
					if prevIntentOk {
						prevIntentVal, err := DecodeMVCCValue(prevIntent.Value)
						if err != nil {
							return false, err
						}
						exVal = makeOptionalValue(prevIntentVal.Value)
					}
				}
			}
			if !exVal.exists {
				// "last write inside txn && seqnum of all writes are ignored"
				// OR
				// "last write outside txn"
				// => use inconsistent mvccGetInternal to retrieve the last committed value at key.
				//
				// Since we want the last committed value on the key, we must make
				// an inconsistent read so we ignore our previous intents here.
				exVal, _, err = mvccGet(ctx, iter, key, readTimestamp, MVCCGetOptions{
					Inconsistent: true,
					Tombstones:   true,
				})
				if err != nil {
					return false, err
				}
			}

			exReplaced = exVal.IsPresent()

			// Make sure we process valueFn before clearing any earlier
			// version. For example, a conditional put within same
			// transaction should read previous write.
			if valueFn != nil {
				value, err = valueFn(exVal)
				if err != nil {
					return false, err
				}
			}

			// We are replacing our own write intent. If we are writing at
			// the same timestamp (see comments in else block) we can
			// overwrite the existing intent; otherwise we must manually
			// delete the old intent, taking care with MVCC stats.
			logicalOp = MVCCUpdateIntentOpType
			if metaTimestamp.Less(writeTimestamp) {
				{
					// If the older write intent has a version underneath it, we need to
					// read its size because its GCBytesAge contribution may change as we
					// move the intent above it. A similar phenomenon occurs in
					// MVCCResolveWriteIntent.
					//
					// TODO(erikgrinaker): Consider using mvccGet() here instead, but
					// needs benchmarking.
					prevKey := oldVersionKey.Next()
					iter.SeekGE(prevKey)
					valid, err := iter.Valid()
					if err != nil {
						return false, err
					} else if valid {
						// If we land on a bare range key, step onto the next key. This may
						// be a point key at the same key position, or a different key.
						if hasPoint, hasRange := iter.HasPointAndRange(); hasRange && !hasPoint {
							iter.Next()
							if valid, err = iter.Valid(); err != nil {
								return false, err
							}
						}
					}
					if valid && iter.UnsafeKey().Key.Equal(prevKey.Key) {
						prevUnsafeKey := iter.UnsafeKey()
						if !prevUnsafeKey.IsValue() {
							return false, errors.Errorf("expected an MVCC value key: %s", prevUnsafeKey)
						}

						// We must now be on a point key, but it may be covered by an
						// existing MVCC range tombstone. If it isn't, account for it.
						_, hasRange := iter.HasPointAndRange()
						if !hasRange || iter.RangeKeys().Versions[0].Timestamp.Less(prevUnsafeKey.Timestamp) {
							prevValLen, prevValIsTombstone, err := iter.MVCCValueLenAndIsTombstone()
							if err != nil {
								return false, err
							}
							if !prevValIsTombstone {
								prevIsValue = !prevValIsTombstone
								prevValSize = int64(prevValLen)
							}
						}
					}
					iter = nil // prevent accidental use below
				}

				if err := writer.ClearMVCC(oldVersionKey); err != nil {
					return false, err
				}
			} else if writeTimestamp.Less(metaTimestamp) {
				// This case occurs when we're writing a key twice within a
				// txn, and our timestamp has been pushed forward because of
				// a write-too-old error on this key. For this case, we want
				// to continue writing at the higher timestamp or else the
				// MVCCMetadata could end up pointing *under* the newer write.
				writeTimestamp = metaTimestamp
			}
			// Since an intent with a smaller sequence number exists for the
			// same transaction, we must add the previous value and sequence to
			// the intent history, if that previous value does not belong to an
			// ignored sequence number.
			//
			// If the epoch of the transaction doesn't match the epoch of the
			// intent, or if the existing value is nil due to all past writes
			// being ignored and there are no other committed values, blow away
			// the intent history.
			//
			// Note that the only case where txn.Epoch == meta.Txn.Epoch &&
			// exVal == nil will be true is when all past writes by this
			// transaction are ignored, and there are no past committed values
			// at this key. In that case, we can also blow up the intent
			// history.
			if txn.Epoch == meta.Txn.Epoch && exVal.exists {
				// Only add the current provisional value to the intent
				// history if the current sequence number is not ignored. There's no
				// reason to add past committed values or a value already in the intent
				// history back into it.
				if curProvValRaw != nil {
					prevIntentValRaw := curProvValRaw
					prevIntentSequence := meta.Txn.Sequence
					buf.newMeta.AddToIntentHistory(prevIntentSequence, prevIntentValRaw)
				}
			} else {
				buf.newMeta.IntentHistory = nil
			}
		} else if readTimestamp.LessEq(metaTimestamp) {
			// This is the case where we're trying to write under a committed
			// value. Obviously we can't do that, but we can increment our
			// timestamp to one logical tick past the existing value and go on
			// to write, but then also return a write-too-old error indicating
			// what the timestamp ended up being. This timestamp can then be
			// used to increment the txn timestamp and be returned with the
			// response.
			//
			// For this to work, this function needs to complete its mutation to
			// the Writer even if it plans to return a write-too-old error. This
			// allows logic that lives in evaluateBatch to determine what to do
			// with the error and whether it should prevent the batch from being
			// proposed or not.
			//
			// NB: even if metaTimestamp is less than writeTimestamp, we can't
			// avoid the WriteTooOld error if metaTimestamp is equal to or
			// greater than readTimestamp. This is because certain operations
			// like ConditionalPuts and InitPuts avoid ever needing refreshes
			// by ensuring that they propagate WriteTooOld errors immediately
			// instead of allowing their transactions to continue and be retried
			// before committing.
			writeTimestamp.Forward(metaTimestamp.Next())
			maybeTooOldErr = roachpb.NewWriteTooOldError(readTimestamp, writeTimestamp, key)
			// If we're in a transaction, always get the value at the orig
			// timestamp. Outside of a transaction, the read timestamp advances
			// to the the latest value's timestamp + 1 as well. The new
			// timestamp is returned to the caller in maybeTooOldErr. Because
			// we're outside of a transaction, we'll never actually commit this
			// value, but that's a concern of evaluateBatch and not here.
			if txn == nil {
				readTimestamp = writeTimestamp
			}
			// Inject a function to inspect the existing value at readTimestamp and
			// populate exReplaced.
			exReplacedFn := func(exVal optionalValue) (roachpb.Value, error) {
				exReplaced = exVal.IsPresent()
				if valueFn != nil {
					return valueFn(exVal)
				}
				return value, nil // pass through original value
			}
			if value, err = maybeGetValue(ctx, iter, key, value, ok, readTimestamp, exReplacedFn); err != nil {
				return false, err
			}
		} else {
			if value, err = maybeGetValue(ctx, iter, key, value, ok, readTimestamp, valueFn); err != nil {
				return false, err
			}
		}
	} else {
		// There is no existing value for this key. Even if the new value is
		// nil write a deletion tombstone for the key.
		if valueFn != nil {
			value, err = valueFn(optionalValue{exists: false})
			if err != nil {
				return false, err
			}
		}
	}

	versionKey := metaKey
	versionKey.Timestamp = writeTimestamp

	versionValue := MVCCValue{}
	versionValue.Value = value
	versionValue.LocalTimestamp = localTimestamp

	if buildutil.CrdbTestBuild {
		if seq, seqOK := kvnemesisutil.FromContext(ctx); seqOK {
			versionValue.KVNemesisSeq.Set(seq)
		}
	}

	if !versionValue.LocalTimestampNeeded(versionKey.Timestamp) ||
		!writer.ShouldWriteLocalTimestamps(ctx) {
		versionValue.LocalTimestamp = hlc.ClockTimestamp{}
	}

	// Write the mvcc metadata now that we have sizes for the latest
	// versioned value. For values, the size of keys is always accounted
	// for as MVCCVersionTimestampSize. The size of the metadata key is
	// accounted for separately.
	newMeta := &buf.newMeta
	{
		var txnMeta *enginepb.TxnMeta
		if txn != nil {
			txnMeta = &txn.TxnMeta
			// If we bumped the WriteTimestamp, we update both the TxnMeta and the
			// MVCCMetadata.Timestamp.
			if txnMeta.WriteTimestamp != versionKey.Timestamp {
				txnMetaCpy := *txnMeta
				txnMetaCpy.WriteTimestamp.Forward(versionKey.Timestamp)
				txnMeta = &txnMetaCpy
			}
		}
		newMeta.Txn = txnMeta
	}
	newMeta.Timestamp = versionKey.Timestamp.ToLegacyTimestamp()
	newMeta.KeyBytes = MVCCVersionTimestampSize
	newMeta.ValBytes = int64(encodedMVCCValueSize(versionValue))
	newMeta.Deleted = versionValue.IsTombstone()

	var metaKeySize, metaValSize int64
	if newMeta.Txn != nil {
		// Determine whether the transaction had previously written an intent on
		// this key and we intend to update that intent, or whether this is the
		// first time an intent is being written. ok represents the presence of a
		// meta (an actual intent or a manufactured meta). buf.meta.Txn!=nil
		// represents a non-manufactured meta, i.e., there is an intent.
		alreadyExists := ok && buf.meta.Txn != nil
		// Write the intent metadata key.
		metaKeySize, metaValSize, err = buf.putIntentMeta(
			ctx, writer, metaKey, newMeta, alreadyExists)
		if err != nil {
			return false, err
		}
	} else {
		// Per-key stats count the full-key once and MVCCVersionTimestampSize for
		// each versioned value. We maintain that accounting even when the MVCC
		// metadata is implicit.
		metaKeySize = int64(metaKey.EncodedSize())
	}

	// Write the mvcc value.
	//
	// NB: this was previously performed before the mvcc metadata write, but
	// benchmarking has show that performing this write after results in a 6%
	// throughput improvement on write-heavy workloads. The reason for this is
	// that the meta key is always ordered before the value key and that
	// RocksDB's skiplist memtable implementation includes a fast-path for
	// sequential insertion patterns.
	if err := writer.PutMVCC(versionKey, versionValue); err != nil {
		return false, err
	}

	// Update MVCC stats.
	if ms != nil {
		// Adjust the stats metadata for MVCC range tombstones. The MVCC stats
		// update only cares about changes to real point keys, but the above logic
		// needs to care about MVCC range tombstones for conflict purposes.
		//
		// Specifically, if a real point key was covered by a range tombstone, we
		// must set meta.Timestamp to the timestamp where the real point key was
		// deleted (either by a point tombstone or the lowest range tombstone). If
		// there was no real point key, meta must be nil. In all other cases,
		// meta.Timestamp will already equal origRealKeyChanged.
		if origRealKeyChanged.IsEmpty() {
			meta = nil // no real point key was found
		}
		if meta != nil {
			meta.Timestamp = origRealKeyChanged.ToLegacyTimestamp()
		}
		ms.Add(updateStatsOnPut(key, prevIsValue, prevValSize, origMetaKeySize, origMetaValSize,
			metaKeySize, metaValSize, meta, newMeta))
	}

	// Log the logical MVCC operation.
	logicalOpDetails := MVCCLogicalOpDetails{
		Key:       key,
		Timestamp: writeTimestamp,
		Safe:      true,
	}
	if txn := buf.newMeta.Txn; txn != nil {
		logicalOpDetails.Txn = *txn
	}
	writer.LogLogicalOp(logicalOp, logicalOpDetails)

	return exReplaced, maybeTooOldErr
}

// MVCCIncrement fetches the value for key, and assuming the value is
// an "integer" type, increments it by inc and stores the new
// value. The newly incremented value is returned.
//
// An initial value is read from the key using the same operational
// timestamp as we use to write a value.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCIncrement(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	txn *roachpb.Transaction,
	inc int64,
) (int64, error) {
	iter := newMVCCIterator(
		rw, timestamp, false /* rangeKeyMasking */, false /* noInterleavedIntents */, IterOptions{
			KeyTypes: IterKeyTypePointsAndRanges,
			Prefix:   true,
		},
	)
	defer iter.Close()

	var int64Val int64
	var newInt64Val int64
	valueFn := func(value optionalValue) (roachpb.Value, error) {
		if value.IsPresent() {
			var err error
			if int64Val, err = value.GetInt(); err != nil {
				return roachpb.Value{}, errors.Errorf("key %q does not contain an integer value", key)
			}
		}

		// Check for overflow and underflow.
		if willOverflow(int64Val, inc) {
			// Return the old value, since we've failed to modify it.
			newInt64Val = int64Val
			return roachpb.Value{}, &roachpb.IntegerOverflowError{
				Key:            key,
				CurrentValue:   int64Val,
				IncrementValue: inc,
			}
		}
		newInt64Val = int64Val + inc

		newValue := roachpb.Value{}
		newValue.SetInt(newInt64Val)
		newValue.InitChecksum(key)
		return newValue, nil
	}
	err := mvccPutUsingIter(ctx, rw, iter, ms, key, timestamp, localTimestamp, noValue, txn, valueFn)

	return newInt64Val, err
}

// CPutMissingBehavior describes the handling a non-existing expected value.
type CPutMissingBehavior bool

const (
	// CPutAllowIfMissing is used to indicate a CPut can also succeed when the
	// expected entry does not exist.
	CPutAllowIfMissing CPutMissingBehavior = true
	// CPutFailIfMissing is used to indicate the existing value must match the
	// expected value exactly i.e. if a value is expected, it must exist.
	CPutFailIfMissing CPutMissingBehavior = false
)

// MVCCConditionalPut sets the value for a specified key only if the expected
// value matches. If not, the return a ConditionFailedError containing the
// actual value. An empty expVal signifies that the key is expected to not
// exist.
//
// The condition check reads a value from the key using the same operational
// timestamp as we use to write a value.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter is
// confusing and redundant. See the comment on mvccPutInternal for details.
//
// An empty expVal means that the key is expected to not exist. If not empty,
// expValue needs to correspond to a Value.TagAndDataBytes() - i.e. a key's
// value without the checksum (as the checksum includes the key too).
func MVCCConditionalPut(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	expVal []byte,
	allowIfDoesNotExist CPutMissingBehavior,
	txn *roachpb.Transaction,
) error {
	iter := newMVCCIterator(
		rw, timestamp, false /* rangeKeyMasking */, false /* noInterleavedIntents */, IterOptions{
			KeyTypes: IterKeyTypePointsAndRanges,
			Prefix:   true,
		},
	)
	defer iter.Close()

	return mvccConditionalPutUsingIter(
		ctx, rw, iter, ms, key, timestamp, localTimestamp, value, expVal, allowIfDoesNotExist, txn)
}

// MVCCBlindConditionalPut is a fast-path of MVCCConditionalPut. See the
// MVCCConditionalPut comments for details of the
// semantics. MVCCBlindConditionalPut skips retrieving the existing metadata
// for the key requiring the caller to guarantee no versions for the key
// currently exist.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCBlindConditionalPut(
	ctx context.Context,
	writer Writer,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	expVal []byte,
	allowIfDoesNotExist CPutMissingBehavior,
	txn *roachpb.Transaction,
) error {
	return mvccConditionalPutUsingIter(
		ctx, writer, nil, ms, key, timestamp, localTimestamp, value, expVal, allowIfDoesNotExist, txn)
}

func mvccConditionalPutUsingIter(
	ctx context.Context,
	writer Writer,
	iter MVCCIterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	expBytes []byte,
	allowNoExisting CPutMissingBehavior,
	txn *roachpb.Transaction,
) error {
	valueFn := func(existVal optionalValue) (roachpb.Value, error) {
		if expValPresent, existValPresent := len(expBytes) != 0, existVal.IsPresent(); expValPresent && existValPresent {
			if !bytes.Equal(expBytes, existVal.TagAndDataBytes()) {
				return roachpb.Value{}, &roachpb.ConditionFailedError{
					ActualValue: existVal.ToPointer(),
				}
			}
		} else if expValPresent != existValPresent && (existValPresent || !bool(allowNoExisting)) {
			return roachpb.Value{}, &roachpb.ConditionFailedError{
				ActualValue: existVal.ToPointer(),
			}
		}
		return value, nil
	}
	return mvccPutUsingIter(ctx, writer, iter, ms, key, timestamp, localTimestamp, noValue, txn, valueFn)
}

// MVCCInitPut sets the value for a specified key if the key doesn't exist. It
// returns a ConditionFailedError when the write fails or if the key exists with
// an existing value that is different from the supplied value. If
// failOnTombstones is set to true, tombstones count as mismatched values and
// will cause a ConditionFailedError.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCInitPut(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	failOnTombstones bool,
	txn *roachpb.Transaction,
) error {
	iter := newMVCCIterator(
		rw, timestamp, false /* rangeKeyMasking */, false /* noInterleavedIntents */, IterOptions{
			KeyTypes: IterKeyTypePointsAndRanges,
			Prefix:   true,
		},
	)
	defer iter.Close()
	return mvccInitPutUsingIter(ctx, rw, iter, ms, key, timestamp, localTimestamp, value, failOnTombstones, txn)
}

// MVCCBlindInitPut is a fast-path of MVCCInitPut. See the MVCCInitPut
// comments for details of the semantics. MVCCBlindInitPut skips
// retrieving the existing metadata for the key requiring the caller
// to guarantee no version for the key currently exist.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCBlindInitPut(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	failOnTombstones bool,
	txn *roachpb.Transaction,
) error {
	return mvccInitPutUsingIter(
		ctx, rw, nil, ms, key, timestamp, localTimestamp, value, failOnTombstones, txn)
}

func mvccInitPutUsingIter(
	ctx context.Context,
	rw ReadWriter,
	iter MVCCIterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	value roachpb.Value,
	failOnTombstones bool,
	txn *roachpb.Transaction,
) error {
	valueFn := func(existVal optionalValue) (roachpb.Value, error) {
		if failOnTombstones && existVal.IsTombstone() {
			// We found a tombstone and failOnTombstones is true: fail.
			return roachpb.Value{}, &roachpb.ConditionFailedError{
				ActualValue: existVal.ToPointer(),
			}
		}
		if existVal.IsPresent() && !existVal.EqualTagAndData(value) {
			// The existing value does not match the supplied value.
			return roachpb.Value{}, &roachpb.ConditionFailedError{
				ActualValue: existVal.ToPointer(),
			}
		}
		return value, nil
	}
	return mvccPutUsingIter(ctx, rw, iter, ms, key, timestamp, localTimestamp, noValue, txn, valueFn)
}

// mvccKeyFormatter is an fmt.Formatter for MVCC Keys.
type mvccKeyFormatter struct {
	key MVCCKey
	err error
}

var _ fmt.Formatter = mvccKeyFormatter{}

// Format implements the fmt.Formatter interface.
func (m mvccKeyFormatter) Format(f fmt.State, c rune) {
	if m.err != nil {
		errors.FormatError(m.err, f, c)
		return
	}
	m.key.Format(f, c)
}

// MVCCMerge implements a merge operation. Merge adds integer values,
// concatenates undifferentiated byte slice values, and efficiently
// combines time series observations if the roachpb.Value tag value
// indicates the value byte slice is of type TIMESERIES.
//
// Merges are not really MVCC operations: they operate on inline values with no
// version, and do not check for conflicts with other MVCC versions.
func MVCCMerge(
	_ context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	metaKey := MakeMVCCMetadataKey(key)

	buf := newPutBuffer()

	// Every type flows through here, so we can't use the typed getters.
	rawBytes := value.RawBytes

	// Encode and merge the MVCC metadata with inlined value.
	meta := &buf.meta
	*meta = enginepb.MVCCMetadata{RawBytes: rawBytes}
	// If non-zero, set the merge timestamp to provide some replay protection.
	if !timestamp.IsEmpty() {
		buf.ts = timestamp.ToLegacyTimestamp()
		meta.MergeTimestamp = &buf.ts
	}
	data, err := buf.marshalMeta(meta)
	if err == nil {
		if err = rw.Merge(metaKey, data); err == nil && ms != nil {
			ms.Add(updateStatsOnMerge(
				key, int64(len(rawBytes)), timestamp.WallTime))
		}
	}
	buf.release()
	return err
}

// MVCCClearTimeRange clears all MVCC versions (point keys and range keys)
// within the span [key, endKey) which have timestamps in the span
// (startTime, endTime]. This can have the apparent effect of "reverting" the
// range to startTime if all of the older revisions of cleared keys are still
// available (i.e. have not been GC'ed).
//
// Long runs of point keys that all qualify for clearing will be cleared via a
// single clear-range operation, as specified by clearRangeThreshold. Once
// maxBatchSize Clear and ClearRange operations are hit during iteration, the
// next matching key is instead returned in the resumeSpan. It is possible to
// exceed maxBatchSize by up to the size of the buffer of keys selected for
// deletion but not yet flushed (as done to detect long runs for cleaning in a
// single ClearRange).
//
// Limiting the number of keys or ranges of keys processed can still cause a
// batch that is too large -- in number of bytes -- for raft to replicate if the
// keys are very large. So if the total length of the keys or key spans cleared
// exceeds maxBatchByteSize it will also stop and return a resume span.
//
// leftPeekBound and rightPeekBound are bounds that will be used to peek for
// surrounding MVCC range keys that may be merged or fragmented by our range key
// clears. They should correspond to the latches held by the command, and not
// exceed the Raft range boundaries.
//
// This function handles the stats computations to determine the correct
// incremental deltas of clearing these keys (and correctly determining if it
// does or not not change the live and gc keys).
//
// If the underlying iterator encounters an intent with a timestamp in the span
// (startTime, endTime], or any inline meta, this method will return an error.
//
// TODO(erikgrinaker): endTime does not actually work here -- if there are keys
// above endTime then the stats do not properly account for them. We probably
// don't need those semantics, so consider renaming this to MVCCRevertRange and
// removing the endTime parameter.
func MVCCClearTimeRange(
	_ context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	leftPeekBound, rightPeekBound roachpb.Key,
	clearRangeThreshold int,
	maxBatchSize, maxBatchByteSize int64,
) (roachpb.Key, error) {
	var batchSize, batchByteSize int64
	var resumeKey roachpb.Key

	if clearRangeThreshold == 0 {
		clearRangeThreshold = 2
	}
	if maxBatchSize == 0 {
		maxBatchSize = math.MaxInt64
	}
	if maxBatchByteSize == 0 {
		maxBatchByteSize = math.MaxInt64
	}
	if rightPeekBound == nil {
		rightPeekBound = keys.MaxKey
	}

	// endTime must be set. Otherwise, MVCCIncrementalIterator defaults to
	// MaxTimestamp, unlike the code below which uses it literally.
	if endTime.IsEmpty() {
		return nil, errors.New("end time is required")
	}

	// endTime must also be above startTime.
	if endTime.LessEq(startTime) {
		return nil, errors.Errorf("end time %s must be above start time %s", endTime, startTime)
	}

	// Since we're setting up multiple iterators, we require consistent iterators.
	if !rw.ConsistentIterators() {
		return nil, errors.AssertionFailedf("requires consistent iterators")
	}

	// When iterating, instead of immediately clearing a matching key we can
	// accumulate it in buf until either a) clearRangeThreshold is reached and
	// we discard the buffer, instead just keeping track of where the span of keys
	// matching started or b) a non-matching key is seen and we flush the buffer
	// keys one by one as Clears. Once we switch to just tracking where the run
	// started, on seeing a non-matching key we flush the run via one ClearRange.
	// This can be a big win for reverting bulk-ingestion of clustered data as the
	// entire span may likely match and thus could be cleared in one ClearRange
	// instead of hundreds of thousands of individual Clears.
	buf := make([]MVCCKey, clearRangeThreshold)
	var bufSize int
	var clearRangeStart MVCCKey

	clearMatchingKey := func(k MVCCKey) {
		if len(clearRangeStart.Key) == 0 {
			// Currently buffering keys to clear one-by-one.
			if bufSize < clearRangeThreshold {
				buf[bufSize].Key = append(buf[bufSize].Key[:0], k.Key...)
				buf[bufSize].Timestamp = k.Timestamp
				bufSize++
			} else {
				// Buffer is now full -- switch to just tracking the start of the range
				// from which we will clear when we either see a non-matching key or if
				// we finish iterating.
				clearRangeStart = buf[0]
				bufSize = 0
			}
		}
	}

	flushClearedKeys := func(nonMatch MVCCKey) error {
		if len(clearRangeStart.Key) != 0 {
			if err := rw.ClearMVCCVersions(clearRangeStart, nonMatch); err != nil {
				return err
			}
			batchByteSize += int64(clearRangeStart.EncodedSize() + nonMatch.EncodedSize())
			batchSize++
			clearRangeStart = MVCCKey{}
		} else if bufSize > 0 {
			var encodedBufSize int64
			for i := 0; i < bufSize; i++ {
				encodedBufSize += int64(buf[i].EncodedSize())
			}
			// Even though we didn't get a large enough number of keys to switch to
			// clearrange, the byte size of the keys we did get is now too large to
			// encode them all within the byte size limit, so use clearrange anyway.
			if batchByteSize+encodedBufSize >= maxBatchByteSize {
				if err := rw.ClearMVCCVersions(buf[0], nonMatch); err != nil {
					return err
				}
				batchByteSize += int64(buf[0].EncodedSize() + nonMatch.EncodedSize())
				batchSize++
			} else {
				for i := 0; i < bufSize; i++ {
					if buf[i].Timestamp.IsEmpty() {
						// Inline metadata. Not an intent because iteration below fails
						// if it sees an intent.
						if err := rw.ClearUnversioned(buf[i].Key); err != nil {
							return err
						}
					} else {
						if err := rw.ClearMVCC(buf[i]); err != nil {
							return err
						}
					}
				}
				batchByteSize += encodedBufSize
				batchSize += int64(bufSize)
			}
			bufSize = 0
		}
		return nil
	}

	// We also buffer the range key stack to clear, and flush it when we hit a new
	// stack.
	//
	// TODO(erikgrinaker): For now, we remove individual range keys. We could do
	// something similar to point keys and keep track of long runs to remove, but
	// we expect them to be rare so this should be fine for now.
	var clearRangeKeys MVCCRangeKeyStack

	flushRangeKeys := func(resumeKey roachpb.Key) error {
		if clearRangeKeys.IsEmpty() {
			return nil
		}
		if len(resumeKey) > 0 {
			if resumeKey.Compare(clearRangeKeys.Bounds.Key) <= 0 {
				return nil
			} else if resumeKey.Compare(clearRangeKeys.Bounds.EndKey) <= 0 {
				clearRangeKeys.Bounds.EndKey = resumeKey
			}
		}

		// Fetch the existing range keys (if any), to adjust MVCC stats. We set up
		// a new iterator for every batch, which both sees our own writes as well as
		// any range keys outside of the time bounds.
		rkIter := rw.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
			KeyTypes:   IterKeyTypeRangesOnly,
			LowerBound: leftPeekBound,
			UpperBound: rightPeekBound,
		})
		defer rkIter.Close()

		cmp, remaining, err := PeekRangeKeysRight(rkIter, clearRangeKeys.Bounds.Key)
		if err != nil {
			return err
		} else if cmp > 0 || !remaining.Bounds.Contains(clearRangeKeys.Bounds) {
			return errors.AssertionFailedf("did not find expected range key at %s", clearRangeKeys.Bounds)
		} else if !remaining.IsEmpty() {
			// Truncate the bounds to the cleared span, so that stats operate on the
			// post-fragmented state (if relevant).
			remaining.Bounds = clearRangeKeys.Bounds
		}

		// Clear the range keys.
		for _, v := range clearRangeKeys.Versions {
			rangeKey := clearRangeKeys.AsRangeKey(v)
			if err := rw.ClearMVCCRangeKey(rangeKey); err != nil {
				return err
			}
			batchSize++
			batchByteSize += int64(rangeKey.EncodedSize())

			if ms != nil {
				ms.Add(updateStatsOnRangeKeyClearVersion(remaining, v))
			}
			if _, ok := remaining.Remove(v.Timestamp); !ok {
				return errors.AssertionFailedf("did not find expected range key %s", rangeKey)
			}
		}
		remaining = remaining.Clone()

		// Update stats for any fragmentation or merging caused by the clears around
		// the bounds.
		if ms != nil {
			if cmp, lhs, err := PeekRangeKeysLeft(rkIter, clearRangeKeys.Bounds.Key); err != nil {
				return err
			} else if cmp > 0 {
				ms.Add(UpdateStatsOnRangeKeySplit(clearRangeKeys.Bounds.Key, lhs.Versions))
			} else if cmp == 0 && lhs.CanMergeRight(remaining) {
				ms.Add(updateStatsOnRangeKeyMerge(clearRangeKeys.Bounds.Key, lhs.Versions))
			}

			if cmp, rhs, err := PeekRangeKeysRight(rkIter, clearRangeKeys.Bounds.EndKey); err != nil {
				return err
			} else if cmp < 0 {
				ms.Add(UpdateStatsOnRangeKeySplit(clearRangeKeys.Bounds.EndKey, rhs.Versions))
			} else if cmp == 0 && remaining.CanMergeRight(rhs) {
				ms.Add(updateStatsOnRangeKeyMerge(clearRangeKeys.Bounds.EndKey, rhs.Versions))
			}
		}

		clearRangeKeys.Clear()
		return nil
	}

	// Using the IncrementalIterator with the time-bound iter optimization could
	// potentially be a big win here -- the expected use-case for this is to run
	// over an entire table's span with a very recent timestamp, rolling back just
	// the writes of some failed IMPORT and that could very likely only have hit
	// some small subset of the table's keyspace. However to get the stats right
	// we need a non-time-bound iter e.g. we need to know if there is an older key
	// under the one we are clearing to know if we're changing the number of live
	// keys. The MVCCIncrementalIterator uses a non-time-bound iter as its source
	// of truth, and only uses the TBI iterator as an optimization when finding
	// the next KV to iterate over. This pattern allows us to quickly skip over
	// swaths of uninteresting keys, but then use a normal iteration to actually
	// do the delete including updating the live key stats correctly.
	//
	// The MVCCIncrementalIterator checks for and fails on any intents in our
	// time-range, as we do not want to clear any running transactions. We don't
	// _expect_ to hit this since the RevertRange is only intended for non-live
	// key spans, but there could be an intent leftover.
	iter := NewMVCCIncrementalIterator(rw, MVCCIncrementalIterOptions{
		KeyTypes:  IterKeyTypePointsAndRanges,
		StartKey:  key,
		EndKey:    endKey,
		StartTime: startTime,
		EndTime:   endTime,
	})
	defer iter.Close()

	// clearedMetaKey is the latest surfaced key that will get cleared.
	var clearedMetaKey MVCCKey

	// clearedMeta contains metadata on the clearedMetaKey.
	var clearedMeta enginepb.MVCCMetadata

	// restoredMeta contains metadata on the previous version the clearedMetaKey.
	// Once the key in clearedMetaKey is cleared, the key represented in
	// restoredMeta becomes the latest version of this MVCC key.
	var restoredMeta enginepb.MVCCMetadata

	iter.SeekGE(MVCCKey{Key: key})
	for {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}

		k := iter.UnsafeKey()

		// If we encounter a new range key stack, flush the previous range keys (if
		// any) and buffer these for clearing.
		//
		// NB: RangeKeyChangedIgnoringTime() may fire on a hidden range key outside
		// of the time bounds, because of NextIgnoringTime(), in which case
		// HasPointAndRange() will return false,false.
		if iter.RangeKeyChangedIgnoringTime() {
			if err := flushRangeKeys(nil); err != nil { // empties clearRangeKeys
				return nil, err
			}
			if batchSize >= maxBatchSize || batchByteSize >= maxBatchByteSize {
				resumeKey = k.Key.Clone()
				break
			}

			hasPoint, hasRange := iter.HasPointAndRange()
			if hasRange {
				iter.RangeKeys().CloneInto(&clearRangeKeys)
			}
			if !hasPoint {
				// If we landed on a bare range tombstone, we need to check if it revealed
				// anything below the time bounds as well.
				iter.NextIgnoringTime()
				continue
			}
		}

		// Process point keys.
		valueLen, valueIsTombstone, err := iter.MVCCValueLenAndIsTombstone()
		if err != nil {
			return nil, err
		}

		// First, account for the point key that we cleared previously.
		if len(clearedMetaKey.Key) > 0 {
			metaKeySize := int64(clearedMetaKey.EncodedSize())
			if clearedMetaKey.Key.Equal(k.Key) {
				// Since the key matches, our previous clear "restored" this revision of
				// the this key, so update the stats with this as the "restored" key.
				restoredMeta.KeyBytes = MVCCVersionTimestampSize
				restoredMeta.ValBytes = int64(valueLen)
				restoredMeta.Deleted = valueIsTombstone
				restoredMeta.Timestamp = k.Timestamp.ToLegacyTimestamp()

				// If there was an MVCC range tombstone between this version and the
				// cleared key, then we didn't restore it after all, but we must still
				// adjust the stats for the range tombstone. RangeKeysIgnoringTime()
				// is cheap, so we don't need any caching here.
				if !restoredMeta.Deleted {
					if rangeKeys := iter.RangeKeysIgnoringTime(); !rangeKeys.IsEmpty() {
						if v, ok := rangeKeys.FirstAtOrAbove(k.Timestamp); ok {
							if v.Timestamp.LessEq(clearedMeta.Timestamp.ToTimestamp()) {
								restoredMeta.Deleted = true
								restoredMeta.KeyBytes = 0
								restoredMeta.ValBytes = 0
								restoredMeta.Timestamp = v.Timestamp.ToLegacyTimestamp()
							}
						}
					}
				}

				if ms != nil {
					ms.Add(updateStatsOnClear(clearedMetaKey.Key, metaKeySize, 0, metaKeySize, 0,
						&clearedMeta, &restoredMeta, restoredMeta.Timestamp.WallTime))
				}
			} else {
				if ms != nil {
					ms.Add(updateStatsOnClear(clearedMetaKey.Key, metaKeySize, 0, 0, 0, &clearedMeta, nil, 0))
				}
			}
		}

		// Eagerly check whether we've exceeded the batch size. If we return a
		// resumeKey we may truncate the buffered MVCC range tombstone clears
		// at the current key, in which case we can't record the current point
		// key as restored by the range tombstone clear below.
		if batchSize >= maxBatchSize || batchByteSize >= maxBatchByteSize {
			resumeKey = k.Key.Clone()
			clearedMetaKey.Key = clearedMetaKey.Key[:0]
			break
		}

		// Check if the current key was restored by a range tombstone clear, and
		// adjust stats accordingly. We've already accounted for the clear of the
		// previous point key above. We must also check that the clear actually
		// revealed the key, since it may have been covered by the point key that
		// we cleared or a different range tombstone below the one we cleared.
		if !valueIsTombstone {
			if v, ok := clearRangeKeys.FirstAtOrAbove(k.Timestamp); ok {
				if !clearedMetaKey.Key.Equal(k.Key) ||
					!clearedMeta.Timestamp.ToTimestamp().LessEq(v.Timestamp) {
					rangeKeys := iter.RangeKeysIgnoringTime()
					if rangeKeys.IsEmpty() || !rangeKeys.HasBetween(k.Timestamp, v.Timestamp.Prev()) {
						ms.Add(enginepb.MVCCStats{
							LastUpdateNanos: v.Timestamp.WallTime,
							LiveCount:       1,
							LiveBytes:       int64(k.EncodedSize()) + int64(valueLen),
						})
					}
				}
			}
		}

		clearedMetaKey.Key = clearedMetaKey.Key[:0]

		if startTime.Less(k.Timestamp) && k.Timestamp.LessEq(endTime) {
			clearMatchingKey(k)
			clearedMetaKey.Key = append(clearedMetaKey.Key[:0], k.Key...)
			clearedMeta.KeyBytes = MVCCVersionTimestampSize
			clearedMeta.ValBytes = int64(valueLen)
			clearedMeta.Deleted = valueIsTombstone
			clearedMeta.Timestamp = k.Timestamp.ToLegacyTimestamp()

			// Move the iterator to the next key/value in linear iteration even if it
			// lies outside (startTime, endTime].
			//
			// If iter lands on an older version of the current key, we will update
			// the stats in the next iteration of the loop. This is necessary to
			// report accurate stats as we have "uncovered" an older version of the
			// key by clearing the current version.
			//
			// If iter lands on the next key, it will either add to the current run of
			// keys to be cleared, or trigger a flush depending on whether or not it
			// lies in our time bounds respectively.
			iter.NextIgnoringTime()
		} else {
			// This key does not match, so we need to flush our run of matching keys.
			if err := flushClearedKeys(k); err != nil {
				return nil, err
			}
			// Move the incremental iterator to the next valid key that can be rolled
			// back. If TBI was enabled when initializing the incremental iterator,
			// this step could jump over large swaths of keys that do not qualify for
			// clearing. However, if we've cleared any range keys, then we need to
			// skip to the next key ignoring time, because it may not have been
			// revealed.
			if !clearRangeKeys.IsEmpty() {
				iter.NextKeyIgnoringTime()
			} else {
				iter.Next()
			}
		}
	}

	if len(clearedMetaKey.Key) > 0 && ms != nil {
		// If we cleared on the last iteration, no older revision of that key was
		// "restored", since otherwise we would have iterated over it.
		origMetaKeySize := int64(clearedMetaKey.EncodedSize())
		ms.Add(updateStatsOnClear(clearedMetaKey.Key, origMetaKeySize, 0, 0, 0, &clearedMeta, nil, 0))
	}

	if err := flushRangeKeys(resumeKey); err != nil {
		return nil, err
	}

	flushKey := endKey
	if len(resumeKey) > 0 {
		flushKey = resumeKey
	}
	return resumeKey, flushClearedKeys(MVCCKey{Key: flushKey})
}

// MVCCDeleteRange deletes the range of key/value pairs specified by start and
// end keys. It returns the range of keys deleted when returnedKeys is set,
// the next span to resume from, and the number of keys deleted.
// The returned resume span is nil if max keys aren't processed.
// The choice max=0 disables the limit.
func MVCCDeleteRange(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key, endKey roachpb.Key,
	max int64,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	txn *roachpb.Transaction,
	returnKeys bool,
) ([]roachpb.Key, *roachpb.Span, int64, error) {
	// In order for this operation to be idempotent when run transactionally, we
	// need to perform the initial scan at the previous sequence number so that
	// we don't see the result from equal or later sequences.
	var scanTxn *roachpb.Transaction
	if txn != nil {
		prevSeqTxn := txn.Clone()
		prevSeqTxn.Sequence--
		scanTxn = prevSeqTxn
	}
	res, err := MVCCScan(ctx, rw, key, endKey, timestamp, MVCCScanOptions{
		FailOnMoreRecent: true, Txn: scanTxn, MaxKeys: max,
	})
	if err != nil {
		return nil, nil, 0, err
	}

	buf := newPutBuffer()
	defer buf.release()
	iter := newMVCCIterator(
		rw, timestamp, false /* rangeKeyMasking */, false /* noInterleavedIntents */, IterOptions{
			KeyTypes: IterKeyTypePointsAndRanges,
			Prefix:   true,
		},
	)
	defer iter.Close()

	var keys []roachpb.Key
	for i, kv := range res.KVs {
		if _, err := mvccPutInternal(
			ctx, rw, iter, ms, kv.Key, timestamp, localTimestamp, noValue, txn, buf, nil,
		); err != nil {
			return nil, nil, 0, err
		}
		if returnKeys {
			if i == 0 {
				keys = make([]roachpb.Key, len(res.KVs))
			}
			keys[i] = kv.Key
		}
	}
	return keys, res.ResumeSpan, res.NumKeys, nil
}

// MVCCPredicateDeleteRange issues MVCC tombstones at endTime to live keys
// within the span [startKey, endKey) that also have MVCC versions that match
// the predicate filters. Long runs of matched keys will get deleted with a
// range Tombstone, while smaller runs will get deleted with point tombstones.
// The keyspaces of each run do not overlap.
//
// This operation is non-transactional, but will check for existing intents in
// the target key span, regardless of timestamp, and return a WriteIntentError
// containing up to maxIntents intents.
//
// MVCCPredicateDeleteRange will return with a resumeSpan if the number of tombstones
// written exceeds maxBatchSize or the size of the written tombstones exceeds maxByteSize.
// These constraints prevent overwhelming raft.
//
// If an MVCC key surfaced has a timestamp at or above endTime,
// MVCCPredicateDeleteRange returns a WriteTooOldError without a resumeSpan,
// even if tombstones were already written to disk. To resolve, the caller
// should retry the call at a higher timestamp, assuming they have the
// appropriate level of isolation (e.g. the span covers an offline table, in the
// case of IMPORT rollbacks).
//
// An example of how this works: Issuing DeleteRange[a,e)@3 with
// Predicate{StartTime=1} on the following keys would issue tombstones at a@3,
// b@3, and d@3.
//
// t3
// t2 a2 b2    d2 e2
// t1    b1 c1
//
//	a  b  c  d  e
func MVCCPredicateDeleteRange(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	startKey, endKey roachpb.Key,
	endTime hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	leftPeekBound, rightPeekBound roachpb.Key,
	predicates roachpb.DeleteRangePredicates,
	maxBatchSize, maxBatchByteSize int64,
	rangeTombstoneThreshold int64,
	maxIntents int64,
) (*roachpb.Span, error) {

	if maxBatchSize == 0 {
		// Set maxBatchSize to a large number to ensure MVCCPredicateDeleteRange
		// doesn't return early due to batch size. Note that maxBatchSize is only
		// set to 0 during testing.
		maxBatchSize = math.MaxInt64
	}

	// batchSize is the number tombstones (point and range) that have been flushed.
	var batchSize int64
	var batchByteSize int64

	// runSize is the number tombstones (point and range) that will get flushed in
	// the current run.
	var runSize int64
	var runByteSize int64

	var runStart, runEnd roachpb.Key

	buf := make([]roachpb.Key, rangeTombstoneThreshold)

	if ms == nil {
		return nil, errors.AssertionFailedf(
			"MVCCStats passed in to MVCCPredicateDeleteRange must be non-nil to ensure proper stats" +
				" computation during Delete operations")
	}

	// Check for any overlapping intents, and return them to be resolved.
	if intents, err := ScanIntents(ctx, rw, startKey, endKey, maxIntents, 0); err != nil {
		return nil, err
	} else if len(intents) > 0 {
		return nil, &roachpb.WriteIntentError{Intents: intents}
	}

	// continueRun returns three bools: the first is true if the current run
	// should continue; the second is true if the latest key is a point tombstone;
	// the third is true if the latest key is a range tombstone. If a non-nil
	// error is returned, the booleans are invalid. The run should continue if:
	//
	//  1) The latest version of the key is a point or range tombstone, with a
	//  timestamp below the client provided EndTime. Since the goal is to create
	//  long runs, any tombstoned key should continue the run.
	//
	//  2) The latest key is live, matches the predicates, and has a
	//  timestamp below EndTime.
	continueRun := func(k MVCCKey, iter *MVCCIncrementalIterator,
	) (toContinue bool, isPointTombstone bool, isRangeTombstone bool, err error) {
		// We need to see the full, unfiltered set of range keys, ignoring time
		// bounds. The RangeKeysIgnoringTime() call is cheap.
		hasPointKey, _ := iter.HasPointAndRange()
		rangeKeys := iter.RangeKeysIgnoringTime()
		hasRangeKey := !rangeKeys.IsEmpty()

		if hasRangeKey {
			newestRangeKey := rangeKeys.Newest()
			if endTime.LessEq(rangeKeys.Newest()) {
				return false, false, false, roachpb.NewWriteTooOldError(
					endTime, newestRangeKey.Next(), k.Key.Clone())
			}
			if !hasPointKey {
				// landed on bare range key.
				return true, false, true, nil
			}
			if k.Timestamp.Less(newestRangeKey) {
				// The latest range tombstone shadows the point key; ok to continue run.
				return true, false, true, nil
			}
		}

		// At this point, there exists a point key that shadows all range keys,
		// if they exist.
		if endTime.LessEq(k.Timestamp) {
			return false, false, false, roachpb.NewWriteTooOldError(endTime, k.Timestamp.Next(),
				k.Key.Clone())
		}
		_, isTombstone, err := iter.MVCCValueLenAndIsTombstone()
		if err != nil {
			return false, false, false, err
		}
		if isTombstone {
			// The latest version of the key is a point tombstone.
			return true, true, false, nil
		}

		// The latest key is a live point key. Conduct predicate filtering.
		if k.Timestamp.LessEq(predicates.StartTime) {
			return false, false, false, nil
		}

		// TODO (msbutler): use MVCCValueHeader to match on job ID predicate
		return true, false, false, nil
	}

	// Create some reusable machinery for flushing a run with point tombstones
	// that is typically used in a single MVCCPut call.
	pointTombstoneIter := newMVCCIterator(
		rw, endTime, false /* rangeKeyMasking */, false /* noInterleavedIntents */, IterOptions{
			KeyTypes: IterKeyTypePointsAndRanges,
			Prefix:   true,
		},
	)
	defer pointTombstoneIter.Close()
	pointTombstoneBuf := newPutBuffer()
	defer pointTombstoneBuf.release()

	flushDeleteKeys := func() error {
		if runSize == 0 {
			return nil
		}
		if runSize >= rangeTombstoneThreshold ||
			// Even if we didn't get a large enough number of keys to switch to
			// using range tombstones, the byte size of the keys we did get is now too large to
			// encode them all within the byte size limit, so use a range tombstone anyway.
			batchByteSize+runByteSize >= maxBatchByteSize {
			if err := MVCCDeleteRangeUsingTombstone(ctx, rw, ms,
				runStart, runEnd.Next(), endTime, localTimestamp, leftPeekBound, rightPeekBound,
				false /* idempotent */, maxIntents, nil); err != nil {
				return err
			}
			batchByteSize += int64(MVCCRangeKey{StartKey: runStart, EndKey: runEnd, Timestamp: endTime}.EncodedSize())
			batchSize++
		} else {
			// Use Point tombstones
			for i := int64(0); i < runSize; i++ {
				if _, err := mvccPutInternal(ctx, rw, pointTombstoneIter, ms, buf[i], endTime, localTimestamp, noValue,
					nil, pointTombstoneBuf, nil); err != nil {
					return err
				}
			}
			batchByteSize += runByteSize
			batchSize += runSize
		}
		runSize = 0
		runStart = roachpb.Key{}
		return nil
	}

	// Using the IncrementalIterator with the time-bound iter optimization could
	// potentially be a big win here -- the expected use-case for this is to run
	// over an entire table's span with a very recent timestamp, issuing tombstones to
	// writes of some failed IMPORT and that could very likely only have hit
	// some small subset of the table's keyspace.
	//
	// The MVCCIncrementalIterator uses a non-time-bound iter as its source
	// of truth, and only uses the TBI iterator as an optimization when finding
	// the next KV to iterate over. This pattern allows us to quickly skip over
	// swaths of uninteresting keys, but then iterates over the latest key of each MVCC key.
	//
	// Notice that the iterator's EndTime is set to hlc.MaxTimestamp, in order to
	// detect and fail on any keys written at or after the client provided
	// endTime. We don't _expect_ to hit intents or newer keys in the client
	// provided span since the MVCCPredicateDeleteRange is only intended for
	// non-live key spans, but there could be an intent leftover.
	iter := NewMVCCIncrementalIterator(rw, MVCCIncrementalIterOptions{
		EndKey:               endKey,
		StartTime:            predicates.StartTime,
		EndTime:              hlc.MaxTimestamp,
		RangeKeyMaskingBelow: endTime,
		KeyTypes:             IterKeyTypePointsAndRanges,
	})
	defer iter.Close()

	iter.SeekGE(MVCCKey{Key: startKey})
	for {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}
		k := iter.UnsafeKey()
		toContinue, isPointTombstone, isRangeTombstone, err := continueRun(k, iter)
		if err != nil {
			return nil, err
		}

		// If the latest version of the key is a tombstone at a timestamp < endtime,
		// the timestamp could be less than predicates.startTime. In this case, the
		// run can continue and Since there's no need to issue another tombstone,
		// don't update runSize or buf.
		if isRangeTombstone {
			// Because range key information can be inferred at point keys,
			// skip over the surfaced range key, and reason about shadowed keys at
			// the surfaced point key.
			//
			// E.g. Scanning the keys below:
			//  2  a2
			//  1  o---o
			//     a   b
			//
			//  would result in two surfaced keys:
			//   {a-b}@1;
			//   a2, {a-b}@1
			//
			// Note that the range key gets surfaced before the point key,
			// even though the point key shadows it.
			iter.NextIgnoringTime()
		} else if isPointTombstone {
			// Since the latest version of this key is a point tombstone, skip over
			// older versions of this key, and move the iterator to the next key
			// even if it lies outside (startTime, endTime), to see if there's a
			// need to flush.
			iter.NextKeyIgnoringTime()
		} else if toContinue {
			// The latest version of the key is live, matches the predicate filters
			// -- e.g. has a timestamp between (predicates.startTime, Endtime);
			// therefore, plan to delete it.
			if batchSize+runSize >= maxBatchSize || batchByteSize+runByteSize >= maxBatchByteSize {
				// The matched key will be the start the resume span.
				if err := flushDeleteKeys(); err != nil {
					return nil, err
				}
				return &roachpb.Span{Key: k.Key.Clone(), EndKey: endKey}, nil
			}
			if runSize == 0 {
				runStart = append(runStart[:0], k.Key...)
			}
			runEnd = append(runEnd[:0], k.Key...)

			if runSize < rangeTombstoneThreshold {
				// Only buffer keys if there's a possibility of issuing point tombstones.
				//
				// To avoid unecessary memory allocation, overwrite the previous key at
				// buffer's current position. No data corruption occurs because the
				// buffer is flushed up to runSize.
				buf[runSize] = append(buf[runSize][:0], runEnd...)
			}

			runSize++
			runByteSize += int64(k.EncodedSize())

			// Move the iterator to the next key in linear iteration even if it lies
			// outside (startTime, endTime), to see if there's a need to flush. We can
			// skip to the next key, as we don't care about older versions of the
			// current key we're about to delete.
			iter.NextKeyIgnoringTime()
		} else {
			// This key does not match. Flush the run of matching keys,
			// to prevent issuing tombstones on keys that do not match the predicates.
			if err := flushDeleteKeys(); err != nil {
				return nil, err
			}
			// Move the incremental iterator to the next valid MVCC key that can be
			// deleted. If TBI was enabled when initializing the incremental iterator,
			// this step could jump over large swaths of keys that do not qualify for
			// clearing.
			iter.NextKey()
		}
	}
	return nil, flushDeleteKeys()
}

// MVCCDeleteRangeUsingTombstone deletes the given MVCC keyspan at the given
// timestamp using an MVCC range tombstone (rather than MVCC point tombstones).
// This operation is non-transactional, but will check for existing intents and
// return a WriteIntentError containing up to maxIntents intents. Can't be used
// across local keyspace.
//
// The leftPeekBound and rightPeekBound parameters are used when looking for
// range tombstones that we'll merge or overlap with. These are provided to
// prevent the command from reading outside of the CRDB range bounds and latch
// bounds. nil means no bounds.
//
// If idempotent is true, the MVCC range tombstone will only be written if there
// exists any point keys/tombstones in the span that aren't already covered by
// an MVCC range tombstone. Notably, it will not write a tombstone across an
// empty span either.
//
// If msCovered is given, it must contain the current stats of the data that
// will be covered by the MVCC range tombstone. This avoids scanning across all
// point keys in the span, but will still do a time-bound scan to check for
// newer point keys that we conflict with.
//
// When deleting an entire Raft range, passing the current MVCCStats as
// msCovered and setting left/rightPeekBound to start/endKey will make the
// deletion significantly faster.
func MVCCDeleteRangeUsingTombstone(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	startKey, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	localTimestamp hlc.ClockTimestamp,
	leftPeekBound, rightPeekBound roachpb.Key,
	idempotent bool,
	maxIntents int64,
	msCovered *enginepb.MVCCStats,
) error {
	// Validate the range key. We must do this first, to catch e.g. any bound violations.
	rangeKey := MVCCRangeKey{StartKey: startKey, EndKey: endKey, Timestamp: timestamp}
	if err := rangeKey.Validate(); err != nil {
		return err
	}

	// We currently don't allow MVCC range tombstones across the local keyspace,
	// to be safe. This wouldn't handle MVCC stats (SysBytes) correctly either.
	if startKey.Compare(keys.LocalMax) < 0 {
		return errors.AssertionFailedf("can't write MVCC range tombstone across local keyspan %s",
			rangeKey)
	}

	// Encode the value.
	var value MVCCValue
	value.LocalTimestamp = localTimestamp
	if !value.LocalTimestampNeeded(timestamp) || !rw.ShouldWriteLocalTimestamps(ctx) {
		value.LocalTimestamp = hlc.ClockTimestamp{}
	}
	if buildutil.CrdbTestBuild {
		if seq, ok := kvnemesisutil.FromContext(ctx); ok {
			value.KVNemesisSeq.Set(seq)
		}
	}
	valueRaw, err := EncodeMVCCValue(value)
	if err != nil {
		return err
	}

	// Check for any overlapping intents, and return them to be resolved.
	if intents, err := ScanIntents(ctx, rw, startKey, endKey, maxIntents, 0); err != nil {
		return err
	} else if len(intents) > 0 {
		return &roachpb.WriteIntentError{Intents: intents}
	}

	// If requested, check if there are any point keys/tombstones in the span that
	// aren't already covered by MVCC range tombstones. Also check for conflicts
	// with newer MVCC range tombstones.
	if idempotent {
		if noPointKeys, err := func() (bool, error) {
			iter := rw.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
				KeyTypes:             IterKeyTypePointsAndRanges,
				LowerBound:           startKey,
				UpperBound:           endKey,
				RangeKeyMaskingBelow: timestamp,
			})
			defer iter.Close()
			for iter.SeekGE(MVCCKey{Key: startKey}); ; iter.Next() {
				if ok, err := iter.Valid(); err != nil {
					return false, err
				} else if !ok {
					break
				}
				if hasPoint, _ := iter.HasPointAndRange(); hasPoint {
					return false, nil
				} else if newest := iter.RangeKeys().Newest(); timestamp.LessEq(newest) {
					return false, roachpb.NewWriteTooOldError(timestamp, newest.Next(), iter.RangeBounds().Key)
				}
			}
			return true, nil
		}(); err != nil || noPointKeys {
			return err
		}
	}

	// If we're omitting point keys in the stats/conflict scan below, we need to
	// do a separate time-bound scan for point key conflicts.
	if msCovered != nil {
		if err := func() error {
			iter := NewMVCCIncrementalIterator(rw, MVCCIncrementalIterOptions{
				KeyTypes:  IterKeyTypePointsOnly,
				StartKey:  startKey,
				EndKey:    endKey,
				StartTime: timestamp.Prev(), // make inclusive
			})
			defer iter.Close()
			iter.SeekGE(MVCCKey{Key: startKey})
			if ok, err := iter.Valid(); err != nil {
				return err
			} else if ok {
				key := iter.UnsafeKey()
				return roachpb.NewWriteTooOldError(timestamp, key.Timestamp.Next(), key.Key)
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	// Scan for conflicts and MVCC stats updates. We can omit point keys from the
	// scan if stats are already known for the live data.
	iterOpts := IterOptions{
		KeyTypes:             IterKeyTypePointsAndRanges,
		LowerBound:           startKey,
		UpperBound:           endKey,
		RangeKeyMaskingBelow: timestamp, // lower point keys have already been accounted for
	}
	if msCovered != nil {
		iterOpts.KeyTypes = IterKeyTypeRangesOnly
		iterOpts.RangeKeyMaskingBelow = hlc.Timestamp{}
	}
	iter := rw.NewMVCCIterator(MVCCKeyIterKind, iterOpts)
	defer iter.Close()

	iter.SeekGE(MVCCKey{Key: startKey})
	prevRangeEnd := startKey.Clone()
	for {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		// Process range keys.
		if iter.RangeKeyChanged() {
			hasPoint, hasRange := iter.HasPointAndRange()
			if hasRange {
				rangeKeys := iter.RangeKeys()
				if timestamp.LessEq(rangeKeys.Newest()) {
					return roachpb.NewWriteTooOldError(timestamp, rangeKeys.Newest().Next(),
						rangeKeys.Bounds.Key)
				}

				if ms != nil {
					// If the encountered range key does not abut the previous range key,
					// we'll write a new range key fragment in the gap between them.
					if !rangeKeys.Bounds.Key.Equal(prevRangeEnd) {
						ms.Add(updateStatsOnRangeKeyPut(MVCCRangeKeyStack{
							Bounds:   roachpb.Span{Key: prevRangeEnd, EndKey: rangeKeys.Bounds.Key},
							Versions: MVCCRangeKeyVersions{{Timestamp: timestamp, Value: valueRaw}},
						}))
					}
					ms.Add(updateStatsOnRangeKeyPutVersion(rangeKeys,
						MVCCRangeKeyVersion{Timestamp: timestamp, Value: valueRaw}))
				}

				prevRangeEnd = append(prevRangeEnd[:0], rangeKeys.Bounds.EndKey...)
			}

			// If we hit a bare range key, it's possible that there's a point key on the
			// same key as its start key, so take a normal step to look for it.
			if !hasPoint {
				iter.Next()
				continue
			}
		}

		// Process point key.
		key := iter.UnsafeKey()
		if timestamp.LessEq(key.Timestamp) {
			return roachpb.NewWriteTooOldError(timestamp, key.Timestamp.Next(), key.Key)
		}
		if key.Timestamp.IsEmpty() {
			return errors.Errorf("can't write range tombstone across inline key %s", key)
		}
		if ms != nil {
			valueLen, isTombstone, err := iter.MVCCValueLenAndIsTombstone()
			if err != nil {
				return err
			}
			ms.Add(updateStatsOnRangeKeyCover(timestamp, key, valueLen, isTombstone))
		}
		iter.NextKey()
	}

	// Once we've iterated across the range key span, fill in the final gap
	// between the previous existing range key fragment and the end of the range
	// key if any. If no existing fragments were found during iteration above,
	// this will be the entire new range key.
	if ms != nil && !prevRangeEnd.Equal(endKey) {
		ms.Add(updateStatsOnRangeKeyPut(MVCCRangeKeyStack{
			Bounds:   roachpb.Span{Key: prevRangeEnd, EndKey: endKey},
			Versions: MVCCRangeKeyVersions{{Timestamp: timestamp, Value: valueRaw}},
		}))
	}

	// Check if the range key will merge with or fragment any existing range keys
	// at the bounds, and adjust stats accordingly.
	if ms != nil && (!leftPeekBound.Equal(startKey) || !rightPeekBound.Equal(endKey)) {
		if rightPeekBound == nil {
			rightPeekBound = keys.MaxKey
		}
		rkIter := rw.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
			KeyTypes:   IterKeyTypeRangesOnly,
			LowerBound: leftPeekBound,
			UpperBound: rightPeekBound,
		})
		defer rkIter.Close()

		// Peek to the left.
		if cmp, lhs, err := PeekRangeKeysLeft(rkIter, startKey); err != nil {
			return err

		} else if cmp > 0 {
			// We're fragmenting an existing range key.
			ms.Add(UpdateStatsOnRangeKeySplit(startKey, lhs.Versions))

		} else if cmp == 0 {
			// We may be merging with an existing range key to the left, possibly
			// along with an existing stack below us.
			lhs = lhs.Clone()
			rhs := rangeKey.AsStack(valueRaw)
			if cmp, below, err := PeekRangeKeysRight(rkIter, startKey); err != nil {
				return err
			} else if cmp == 0 {
				rhs.Versions = append(rhs.Versions, below.Versions...)
			}
			if lhs.CanMergeRight(rhs) {
				ms.Add(updateStatsOnRangeKeyMerge(startKey, rhs.Versions))
			}
		}

		// Peek to the right.
		if cmp, rhs, err := PeekRangeKeysRight(rkIter, endKey); err != nil {
			return err

		} else if cmp < 0 {
			// We're fragmenting an existing range key.
			ms.Add(UpdateStatsOnRangeKeySplit(endKey, rhs.Versions))

		} else if cmp == 0 {
			// We may be merging with an existing range key to the right, possibly
			// along with an existing stack below us.
			lhs := rangeKey.AsStack(valueRaw)
			rhs = rhs.Clone()
			if cmp, below, err := PeekRangeKeysLeft(rkIter, endKey); err != nil {
				return err
			} else if cmp == 0 {
				lhs.Versions = append(lhs.Versions, below.Versions...)
			}
			if lhs.CanMergeRight(rhs) {
				ms.Add(updateStatsOnRangeKeyMerge(endKey, rhs.Versions))
			}
		}
	}

	// If we're given MVCC stats for the covered data, mark it as deleted at the
	// current timestamp.
	if ms != nil && msCovered != nil {
		ms.Add(updateStatsOnRangeKeyCoverStats(timestamp, *msCovered))
	}

	if err := rw.PutMVCCRangeKey(rangeKey, value); err != nil {
		return err
	}

	rw.LogLogicalOp(MVCCDeleteRangeOpType, MVCCLogicalOpDetails{
		Safe:      true,
		Key:       rangeKey.StartKey,
		EndKey:    rangeKey.EndKey,
		Timestamp: rangeKey.Timestamp,
	})

	return nil
}

func recordIteratorStats(ctx context.Context, iter MVCCIterator) {
	sp := tracing.SpanFromContext(ctx)
	if sp.RecordingType() == tracingpb.RecordingOff {
		// Short-circuit before doing any work.
		return
	}
	iteratorStats := iter.Stats()
	stats := &iteratorStats.Stats
	steps := stats.ReverseStepCount[pebble.InterfaceCall] + stats.ForwardStepCount[pebble.InterfaceCall]
	seeks := stats.ReverseSeekCount[pebble.InterfaceCall] + stats.ForwardSeekCount[pebble.InterfaceCall]
	internalSteps := stats.ReverseStepCount[pebble.InternalIterCall] + stats.ForwardStepCount[pebble.InternalIterCall]
	internalSeeks := stats.ReverseSeekCount[pebble.InternalIterCall] + stats.ForwardSeekCount[pebble.InternalIterCall]
	sp.RecordStructured(&roachpb.ScanStats{
		NumInterfaceSeeks:              uint64(seeks),
		NumInternalSeeks:               uint64(internalSeeks),
		NumInterfaceSteps:              uint64(steps),
		NumInternalSteps:               uint64(internalSteps),
		BlockBytes:                     stats.InternalStats.BlockBytes,
		BlockBytesInCache:              stats.InternalStats.BlockBytesInCache,
		KeyBytes:                       stats.InternalStats.KeyBytes,
		ValueBytes:                     stats.InternalStats.ValueBytes,
		PointCount:                     stats.InternalStats.PointCount,
		PointsCoveredByRangeTombstones: stats.InternalStats.PointsCoveredByRangeTombstones,
		RangeKeyCount:                  uint64(stats.RangeKeyStats.Count),
		RangeKeyContainedPoints:        uint64(stats.RangeKeyStats.ContainedPoints),
		RangeKeySkippedPoints:          uint64(stats.RangeKeyStats.SkippedPoints),
	})
}

// mvccScanInit performs some preliminary checks on the validity of options for
// a scan.
//
// If ok=true is returned, then the pebbleMVCCScanner must be release()'d when
// no longer needed. The scanner is initialized with the given results.
//
// If ok=false is returned, then the returned result and the error are the
// result of the scan.
func mvccScanInit(
	iter MVCCIterator,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
	results results,
) (ok bool, _ *pebbleMVCCScanner, _ MVCCScanResult, _ error) {
	if len(endKey) == 0 {
		return false, nil, MVCCScanResult{}, emptyKeyError()
	}
	if err := opts.validate(); err != nil {
		return false, nil, MVCCScanResult{}, err
	}
	if opts.MaxKeys < 0 {
		return false, nil, MVCCScanResult{
			ResumeSpan:   &roachpb.Span{Key: key, EndKey: endKey},
			ResumeReason: roachpb.RESUME_KEY_LIMIT,
		}, nil
	}
	if opts.TargetBytes < 0 {
		return false, nil, MVCCScanResult{
			ResumeSpan:   &roachpb.Span{Key: key, EndKey: endKey},
			ResumeReason: roachpb.RESUME_BYTE_LIMIT,
		}, nil
	}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)

	*mvccScanner = pebbleMVCCScanner{
		parent:           iter,
		memAccount:       opts.MemoryAccount,
		lockTable:        opts.LockTable,
		reverse:          opts.Reverse,
		start:            key,
		end:              endKey,
		ts:               timestamp,
		maxKeys:          opts.MaxKeys,
		targetBytes:      opts.TargetBytes,
		allowEmpty:       opts.AllowEmpty,
		wholeRows:        opts.WholeRowsOfSize > 1, // single-KV rows don't need processing
		maxIntents:       opts.MaxIntents,
		inconsistent:     opts.Inconsistent,
		skipLocked:       opts.SkipLocked,
		tombstones:       opts.Tombstones,
		failOnMoreRecent: opts.FailOnMoreRecent,
		keyBuf:           mvccScanner.keyBuf,
	}

	mvccScanner.init(opts.Txn, opts.Uncertainty, results)
	return true /* ok */, mvccScanner, MVCCScanResult{}, nil
}

func mvccScanToBytes(
	ctx context.Context,
	iter MVCCIterator,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	var results pebbleResults
	if opts.WholeRowsOfSize > 1 {
		results.lastOffsetsEnabled = true
		results.lastOffsets = make([]int, opts.WholeRowsOfSize)
	}
	ok, mvccScanner, res, err := mvccScanInit(iter, key, endKey, timestamp, opts, &results)
	if !ok {
		return res, err
	}
	defer mvccScanner.release()

	res.ResumeSpan, res.ResumeReason, res.ResumeNextBytes, err = mvccScanner.scan(ctx)

	if err != nil {
		return MVCCScanResult{}, err
	}

	res.KVData = results.finish()
	if err = finalizeScanResult(ctx, mvccScanner, &res, opts.errOnIntents()); err != nil {
		return MVCCScanResult{}, err
	}
	return res, nil
}

// finalizeScanResult updates the MVCCScanResult in-place after the scan was
// completed successfully. It also performs some additional auxiliary tasks
// (like recording iterators stats).
func finalizeScanResult(
	ctx context.Context, mvccScanner *pebbleMVCCScanner, res *MVCCScanResult, errOnIntents bool,
) error {
	res.NumKeys, res.NumBytes, _ = mvccScanner.results.sizeInfo(0 /* lenKey */, 0 /* lenValue */)

	// If we have a trace, emit the scan stats that we produced.
	recordIteratorStats(ctx, mvccScanner.parent)

	var err error
	res.Intents, err = buildScanIntents(mvccScanner.intentsRepr())
	if err != nil {
		return err
	}

	if errOnIntents && len(res.Intents) > 0 {
		return &roachpb.WriteIntentError{Intents: res.Intents}
	}
	return nil
}

// mvccScanToKvs converts the raw key/value pairs returned by MVCCIterator.MVCCScan
// into a slice of roachpb.KeyValues.
func mvccScanToKvs(
	ctx context.Context,
	iter MVCCIterator,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	res, err := mvccScanToBytes(ctx, iter, key, endKey, timestamp, opts)
	if err != nil {
		return MVCCScanResult{}, err
	}
	res.KVs = make([]roachpb.KeyValue, res.NumKeys)
	kvData := res.KVData
	res.KVData = nil

	var i int
	if err := MVCCScanDecodeKeyValues(kvData, func(key MVCCKey, rawBytes []byte) error {
		res.KVs[i].Key = key.Key
		res.KVs[i].Value.RawBytes = rawBytes
		res.KVs[i].Value.Timestamp = key.Timestamp
		i++
		return nil
	}); err != nil {
		return MVCCScanResult{}, err
	}
	return res, err
}

func buildScanIntents(data []byte) ([]roachpb.Intent, error) {
	if len(data) == 0 {
		return nil, nil
	}

	reader, err := NewPebbleBatchReader(data)
	if err != nil {
		return nil, err
	}

	intents := make([]roachpb.Intent, 0, reader.Count())
	var meta enginepb.MVCCMetadata
	for reader.Next() {
		key, err := reader.MVCCKey()
		if err != nil {
			return nil, err
		}
		if err := protoutil.Unmarshal(reader.Value(), &meta); err != nil {
			return nil, err
		}
		if meta.Txn == nil {
			return nil, errors.AssertionFailedf("unexpected nil MVCCMetadata.Txn: %v", meta)
		}
		intents = append(intents, roachpb.MakeIntent(meta.Txn, key.Key))
	}

	if err := reader.Error(); err != nil {
		return nil, err
	}
	return intents, nil
}

// MVCCScanOptions bundles options for the MVCCScan family of functions.
type MVCCScanOptions struct {
	// See the documentation for MVCCScan for information on these parameters.
	Inconsistent     bool
	SkipLocked       bool
	Tombstones       bool
	Reverse          bool
	FailOnMoreRecent bool
	Txn              *roachpb.Transaction
	Uncertainty      uncertainty.Interval
	// MaxKeys is the maximum number of kv pairs returned from this operation.
	// The zero value represents an unbounded scan. If the limit stops the scan,
	// a corresponding ResumeSpan is returned. As a special case, the value -1
	// returns no keys in the result (returning the first key via the
	// ResumeSpan).
	MaxKeys int64
	// TargetBytes is a byte threshold to limit the amount of data pulled into
	// memory during a Scan operation. Once the target is satisfied (i.e. met or
	// exceeded) by the emitted KV pairs, iteration stops (with a ResumeSpan as
	// appropriate). In particular, at least one kv pair is returned (when one
	// exists), unless AllowEmpty is set.
	//
	// The number of bytes a particular kv pair accrues depends on internal data
	// structures, but it is guaranteed to exceed that of the bytes stored in
	// the key and value itself.
	//
	// The zero value indicates no limit.
	TargetBytes int64
	// AllowEmpty will return an empty result if the first kv pair exceeds the
	// TargetBytes limit.
	AllowEmpty bool
	// WholeRowsOfSize will prevent returning partial rows when limits (MaxKeys or
	// TargetBytes) are set. The value indicates the max number of keys per row.
	// If the last KV pair(s) belong to a partial row, they will be removed from
	// the result -- except if the result only consists of a single partial row
	// and AllowEmpty is false, in which case the remaining KV pairs of the row
	// will be fetched and returned too.
	WholeRowsOfSize int32
	// MaxIntents is a maximum number of intents collected by scanner in
	// consistent mode before returning WriteIntentError.
	//
	// Not used in inconsistent scans.
	// The zero value indicates no limit.
	MaxIntents int64
	// MemoryAccount is used for tracking memory allocations.
	MemoryAccount *mon.BoundAccount
	// LockTable is used to determine whether keys are locked in the in-memory
	// lock table when scanning with the SkipLocked option.
	LockTable LockTableView
	// DontInterleaveIntents, when set, makes it such that intent metadata is not
	// interleaved with the results of the scan. Setting this option means that
	// the underlying pebble iterator will only scan over the MVCC keyspace and
	// will not use an `intentInterleavingIter`. It is only appropriate to use
	// this when the caller does not need to know whether a given key is an intent
	// or not. It is usually set by read-only requests that have resolved their
	// conflicts before they begin their MVCC scan.
	DontInterleaveIntents bool
}

func (opts *MVCCScanOptions) validate() error {
	if opts.Inconsistent && opts.Txn != nil {
		return errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if opts.Inconsistent && opts.SkipLocked {
		return errors.Errorf("cannot allow inconsistent reads with skip locked option")
	}
	if opts.Inconsistent && opts.FailOnMoreRecent {
		return errors.Errorf("cannot allow inconsistent reads with fail on more recent option")
	}
	if opts.DontInterleaveIntents && opts.SkipLocked {
		return errors.Errorf("cannot disable interleaved intents with skip locked option")
	}
	return nil
}

func (opts *MVCCScanOptions) errOnIntents() bool {
	return !opts.Inconsistent && !opts.SkipLocked
}

// MVCCScanResult groups the values returned from an MVCCScan operation. Depending
// on the operation invoked, KVData or KVs is populated, but never both.
type MVCCScanResult struct {
	KVData  [][]byte
	KVs     []roachpb.KeyValue
	NumKeys int64
	// NumBytes is the number of bytes this scan result accrued in terms of the
	// MVCCScanOptions.TargetBytes parameter. This roughly measures the bytes
	// used for encoding the uncompressed kv pairs contained in the result.
	NumBytes int64

	ResumeSpan      *roachpb.Span
	ResumeReason    roachpb.ResumeReason
	ResumeNextBytes int64 // populated if TargetBytes != 0, size of next resume kv
	Intents         []roachpb.Intent
}

// MVCCScan scans the key range [key, endKey) in the provided reader up to some
// maximum number of results in ascending order. If it hits max, it returns a
// "resume span" to be used in the next call to this function. If the limit is
// not hit, the resume span will be nil. Otherwise, it will be the sub-span of
// [key, endKey) that has not been scanned.
//
// For an unbounded scan, specify a max of zero.
//
// Only keys that with a timestamp less than or equal to the supplied timestamp
// will be included in the scan results. If a transaction is provided and the
// scan encounters a value with a timestamp between the supplied timestamp and
// the transaction's global uncertainty limit, an uncertainty error will be
// returned. This window of uncertainty is reduced down to the local uncertainty
// limit, if one is provided.
//
// In tombstones mode, if the most recent value for a key is a deletion
// tombstone, the scan result will contain a roachpb.KeyValue for that key whose
// RawBytes field is nil. Otherwise, the key-value pair will be omitted from the
// result entirely. MVCC range tombstones will be emitted as synthetic point
// tombstones above existing point keys, but not below them and not if they
// don't overlap any point keys at all. This is unlike MVCCGet, which will
// always synthesize point tombstones if the key overlaps a range tombstone,
// regardless of whether a point key exists below it.
//
// When scanning inconsistently, any encountered intents will be placed in the
// dedicated result parameter. By contrast, when scanning consistently, any
// encountered intents will cause the scan to return a WriteIntentError with the
// intents embedded within.
//
// Note that transactional scans must be consistent. Put another way, only
// non-transactional scans may be inconsistent.
//
// When scanning in "skip locked" mode, keys that are locked by transactions
// other than the reader are not included in the result set and do not result in
// a WriteIntentError. Instead, these keys are included in the encountered
// intents result parameter so that they can be resolved asynchronously. In this
// mode, the LockTableView provided in the options is consulted for each key to
// determine whether it is locked with an unreplicated lock.
//
// When scanning in "fail on more recent" mode, a WriteTooOldError will be
// returned if the scan observes a version with a timestamp at or above the read
// timestamp. If the scan observes multiple versions with timestamp at or above
// the read timestamp, the maximum will be returned in the WriteTooOldError.
// Similarly, a WriteIntentError will be returned if the scan observes another
// transaction's intent, even if it has a timestamp above the read timestamp.
func MVCCScan(
	ctx context.Context,
	reader Reader,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	iter := newMVCCIterator(
		reader, timestamp, !opts.Tombstones, opts.DontInterleaveIntents, IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: key,
			UpperBound: endKey,
		},
	)
	defer iter.Close()
	return mvccScanToKvs(ctx, iter, key, endKey, timestamp, opts)
}

// MVCCScanToBytes is like MVCCScan, but it returns the results in a byte array.
func MVCCScanToBytes(
	ctx context.Context,
	reader Reader,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	iter := newMVCCIterator(
		reader, timestamp, !opts.Tombstones, opts.DontInterleaveIntents, IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: key,
			UpperBound: endKey,
		},
	)
	defer iter.Close()
	return mvccScanToBytes(ctx, iter, key, endKey, timestamp, opts)
}

// MVCCScanAsTxn constructs a temporary transaction from the given transaction
// metadata and calls MVCCScan as that transaction. This method is required only
// for reading intents of a transaction when only its metadata is known and
// should rarely be used.
//
// The read is carried out without the chance of uncertainty restarts.
func MVCCScanAsTxn(
	ctx context.Context,
	reader Reader,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	txnMeta enginepb.TxnMeta,
) (MVCCScanResult, error) {
	return MVCCScan(ctx, reader, key, endKey, timestamp, MVCCScanOptions{
		Txn: &roachpb.Transaction{
			TxnMeta:                txnMeta,
			Status:                 roachpb.PENDING,
			ReadTimestamp:          txnMeta.WriteTimestamp,
			GlobalUncertaintyLimit: txnMeta.WriteTimestamp,
		}})
}

// MVCCIterate iterates over the key range [start,end). At each step of the
// iteration, f() is invoked with the current key/value pair. If f returns
// iterutil.StopIteration, the iteration stops with no error propagated. If f
// returns any other error, the iteration stops and the error is propagated. If
// the reverse flag is set, the iterator will be moved in reverse order. If the
// scan options specify an inconsistent scan, all "ignored" intents will be
// returned. In consistent mode, intents are only ever returned as part of a
// WriteIntentError. In Tombstones mode, MVCC range tombstones are emitted as
// synthetic point tombstones above existing point keys.
func MVCCIterate(
	ctx context.Context,
	reader Reader,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
	f func(roachpb.KeyValue) error,
) ([]roachpb.Intent, error) {
	iter := newMVCCIterator(
		reader, timestamp, !opts.Tombstones, opts.DontInterleaveIntents, IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: key,
			UpperBound: endKey,
		},
	)
	defer iter.Close()

	var intents []roachpb.Intent
	for {
		const maxKeysPerScan = 1000
		opts := opts
		opts.MaxKeys = maxKeysPerScan
		res, err := mvccScanToKvs(
			ctx, iter, key, endKey, timestamp, opts)
		if err != nil {
			return nil, err
		}

		if len(res.Intents) > 0 {
			if intents == nil {
				intents = res.Intents
			} else {
				intents = append(intents, res.Intents...)
			}
		}

		for i := range res.KVs {
			if err := f(res.KVs[i]); err != nil {
				err = iterutil.Map(err)
				return intents, err
			}
		}

		if res.ResumeSpan == nil {
			break
		}
		if opts.Reverse {
			endKey = res.ResumeSpan.EndKey
		} else {
			key = res.ResumeSpan.Key
		}
	}

	return intents, nil
}

// MVCCResolveWriteIntent either commits, aborts (rolls back), or moves forward
// in time an extant write intent for a given txn according to commit
// parameter. ResolveWriteIntent will skip write intents of other txns.
//
// An opts.TargetBytes of < 0 means resolve nothing and returns the intent as
// the resume span. If opts.TargetBytes >= 0, then resolve intent and resume
// span is nil.
//
// Returns whether or not an intent was found to resolve, number of bytes added
// to the write batch by intent resolution, and the resume span if the max
// bytes limit was exceeded.
//
// Transaction epochs deserve a bit of explanation. The epoch for a
// transaction is incremented on transaction retries. A transaction
// retry is different from an abort. Retries can occur in SSI
// transactions when the commit timestamp is not equal to the proposed
// transaction timestamp. On a retry, the epoch is incremented instead
// of creating an entirely new transaction. This allows the intents
// that were written on previous runs to serve as locks which prevent
// concurrent reads from further incrementing the timestamp cache,
// making further transaction retries less likely.
//
// Because successive retries of a transaction may end up writing to
// different keys, the epochs serve to classify which intents get
// committed in the event the transaction succeeds (all those with
// epoch matching the commit epoch), and which intents get aborted,
// even if the transaction succeeds.
func MVCCResolveWriteIntent(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	intent roachpb.LockUpdate,
	opts MVCCResolveWriteIntentOptions,
) (ok bool, numBytes int64, resumeSpan *roachpb.Span, err error) {
	if len(intent.Key) == 0 {
		return false, 0, nil, emptyKeyError()
	}
	if len(intent.EndKey) > 0 {
		return false, 0, nil, errors.Errorf("can't resolve range intent as point intent")
	}
	if opts.TargetBytes < 0 {
		return false, 0, &roachpb.Span{Key: intent.Key}, nil
	}

	iterAndBuf := GetBufUsingIter(rw.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
		KeyTypes: IterKeyTypePointsAndRanges,
		Prefix:   true,
	}))
	iterAndBuf.iter.SeekIntentGE(intent.Key, intent.Txn.ID)
	// Production code will use a buffered writer, which makes the numBytes
	// calculation accurate. Note that an inaccurate numBytes (e.g. 0 in the
	// case of an unbuffered writer) does not affect any safety properties of
	// the database.
	beforeBytes := rw.BufferedSize()
	ok, err = mvccResolveWriteIntent(ctx, rw, iterAndBuf.iter, ms, intent, iterAndBuf.buf)
	numBytes = int64(rw.BufferedSize() - beforeBytes)
	// Using defer would be more convenient, but it is measurably slower.
	iterAndBuf.Cleanup()
	return ok, numBytes, nil, err
}

// iterForKeyVersions provides a subset of the functionality of MVCCIterator.
// The expected use-case is when the iter is already positioned at the intent
// (if one exists) for a particular key, or some version, and positioning
// operators like SeekGE, Next are only being used to find other versions for
// that key, and never to find intents for other keys. A full-fledged
// MVCCIterator can be used here. The methods below have the same behavior as
// in MVCCIterator, but the caller should never call SeekGE with an empty
// MVCCKey.Timestamp. Additionally, Next must be preceded by at least one call
// to SeekGE.
type iterForKeyVersions interface {
	Valid() (bool, error)
	HasPointAndRange() (bool, bool)
	SeekGE(key MVCCKey)
	Next()
	UnsafeKey() MVCCKey
	UnsafeValue() ([]byte, error)
	MVCCValueLenAndIsTombstone() (int, bool, error)
	ValueProto(msg protoutil.Message) error
	RangeKeys() MVCCRangeKeyStack
}

// separatedIntentAndVersionIter is an implementation of iterForKeyVersions
// used for ranged intent resolution. The MVCCIterator used by it is of
// MVCCKeyIterKind. The caller attempting to do ranged intent resolution uses
// seekEngineKey, nextEngineKey to iterate over the lock table, and for each
// lock/intent that needs to be resolved passes this iterator to
// mvccResolveWriteIntent. The MVCCIterator is positioned lazily, only if
// needed -- the fast path for intent resolution when a transaction is
// committing and does not need to change the provisional value or timestamp,
// does not need to position the MVCCIterator. The other cases, which include
// transaction aborts and changing provisional value timestamps, or changing
// the provisional value due to savepoint rollback, will position the
// MVCCIterator, and are the slow path.
// Note that even this slow path is faster than when intents were interleaved,
// since it can avoid iterating over keys with no intents.
type separatedIntentAndVersionIter struct {
	engineIter EngineIterator
	mvccIter   MVCCIterator

	// Already parsed meta, when the starting position is at an intent.
	meta            *enginepb.MVCCMetadata
	atMVCCIter      bool
	engineIterValid bool
	engineIterErr   error
	intentKey       roachpb.Key
}

var _ iterForKeyVersions = &separatedIntentAndVersionIter{}

func (s *separatedIntentAndVersionIter) seekEngineKeyGE(key EngineKey) {
	s.atMVCCIter = false
	s.meta = nil
	s.engineIterValid, s.engineIterErr = s.engineIter.SeekEngineKeyGE(key)
	s.initIntentKey()
}

func (s *separatedIntentAndVersionIter) nextEngineKey() {
	s.atMVCCIter = false
	s.meta = nil
	s.engineIterValid, s.engineIterErr = s.engineIter.NextEngineKey()
	s.initIntentKey()
}

func (s *separatedIntentAndVersionIter) initIntentKey() {
	if s.engineIterValid {
		engineKey, err := s.engineIter.UnsafeEngineKey()
		if err != nil {
			s.engineIterErr = err
			s.engineIterValid = false
			return
		}
		if s.intentKey, err = keys.DecodeLockTableSingleKey(engineKey.Key); err != nil {
			s.engineIterErr = err
			s.engineIterValid = false
			return
		}
	}
}

func (s *separatedIntentAndVersionIter) Valid() (bool, error) {
	if s.atMVCCIter {
		return s.mvccIter.Valid()
	}
	return s.engineIterValid, s.engineIterErr
}

func (s *separatedIntentAndVersionIter) HasPointAndRange() (bool, bool) {
	hasPoint, hasRange := s.mvccIter.HasPointAndRange()
	if !s.atMVCCIter {
		hasPoint = s.engineIterValid
	}
	return hasPoint, hasRange
}

func (s *separatedIntentAndVersionIter) RangeKeys() MVCCRangeKeyStack {
	return s.mvccIter.RangeKeys()
}

func (s *separatedIntentAndVersionIter) SeekGE(key MVCCKey) {
	if !key.IsValue() {
		panic(errors.AssertionFailedf("SeekGE only permitted for values"))
	}
	s.mvccIter.SeekGE(key)
	s.atMVCCIter = true
}

func (s *separatedIntentAndVersionIter) Next() {
	if !s.atMVCCIter {
		panic(errors.AssertionFailedf("Next not preceded by SeekGE"))
	}
	s.mvccIter.Next()
}

func (s *separatedIntentAndVersionIter) UnsafeKey() MVCCKey {
	if s.atMVCCIter {
		return s.mvccIter.UnsafeKey()
	}
	return MVCCKey{Key: s.intentKey}
}

func (s *separatedIntentAndVersionIter) UnsafeValue() ([]byte, error) {
	if s.atMVCCIter {
		return s.mvccIter.UnsafeValue()
	}
	return s.engineIter.UnsafeValue()
}

func (s *separatedIntentAndVersionIter) MVCCValueLenAndIsTombstone() (int, bool, error) {
	if !s.atMVCCIter {
		return 0, false, errors.AssertionFailedf("not at MVCC value")
	}
	return s.mvccIter.MVCCValueLenAndIsTombstone()
}

func (s *separatedIntentAndVersionIter) ValueProto(msg protoutil.Message) error {
	if s.atMVCCIter {
		return s.mvccIter.ValueProto(msg)
	}
	meta, ok := msg.(*enginepb.MVCCMetadata)
	if ok && meta == s.meta {
		// Already parsed.
		return nil
	}
	v, err := s.engineIter.UnsafeValue()
	if err != nil {
		return err
	}
	return protoutil.Unmarshal(v, msg)
}

// mvccGetIntent uses an iterForKeyVersions that has been seeked to
// metaKey.Key for a potential intent, and tries to retrieve an intent if it
// is present. ok returns true iff an intent for that key is found. In that
// case, keyBytes and valBytes are set to non-zero and the deserialized intent
// is placed in meta.
func mvccGetIntent(
	iter iterForKeyVersions, metaKey MVCCKey, meta *enginepb.MVCCMetadata,
) (ok bool, keyBytes, valBytes int64, err error) {
	if ok, err := iter.Valid(); !ok {
		return false, 0, 0, err
	}
	if hasPoint, _ := iter.HasPointAndRange(); !hasPoint {
		return false, 0, 0, nil
	}
	unsafeKey := iter.UnsafeKey()
	if !unsafeKey.Key.Equal(metaKey.Key) {
		return false, 0, 0, nil
	}
	if unsafeKey.IsValue() {
		return false, 0, 0, nil
	}
	if err := iter.ValueProto(meta); err != nil {
		return false, 0, 0, err
	}
	v, err := iter.UnsafeValue()
	// iter.ValueProto returned without err, so we should not see an error now.
	if err != nil {
		return false, 0, 0, errors.HandleAsAssertionFailure(err)
	}
	valLen := int64(len(v))
	return true, int64(unsafeKey.EncodedSize()), valLen, nil
}

// With the separated lock table, we are employing a performance optimization:
// when an intent metadata is removed, we preferably want to do so using a
// SingleDel (as opposed to a Del). This is only safe if the previous operations
// on the metadata key allow it. Due to practical limitations, at the time of
// writing the condition we need is that the pebble history of the key consists
// of a single SET. (#69891 tracks an improvement, to also make SingleDel safe
// in the case of `<anything>; (DEL or legitimate SingleDel); SET; SingleDel`,
// which will open up further optimizations).
//
// It is difficult to track the history of engine writes to a key precisely, in
// particular when values are ever aborted. So we apply the optimization only to
// the main case in which it is useful, namely that of a transaction committing
// its intent that it never re-wrote in the initial epoch (i.e. no chance of it
// ever being removed before as part of being pushed). Note that when a txn
// refreshes, it stays in the original epoch, and the intents are moved, which
// does *not* cause a write to the MVCC metadata key (for which the history has
// to remain a single SET). So transactions that "only" refresh are covered by
// the optimization as well.
//
// Note that a transaction can "partially abort" and still commit due to nested
// SAVEPOINTs, such as in the below example:
//
//	BEGIN;
//	  SAVEPOINT foo;
//	    INSERT INTO kv VALUES(1, 1);
//	  ROLLBACK TO SAVEPOINT foo;
//	  INSERT INTO kv VALUES(1, 2);
//	COMMIT;
//
// This would first remove the intent (1,1) during the ROLLBACK using a Del (the
// anomaly below would occur the same if a SingleDel were used here), and thus
// without an additional condition the INSERT (1,2) would be eligible for
// committing via a SingleDel. This has to be avoided as well, since the
// metadata key for k=1 has the following history:
//
// - Set // when (1,1) is written
// - Del // during ROLLBACK
// - Set // when (1,2) is written
// - SingleDel // on COMMIT
//
// However, this sequence could compact as follows (at the time of writing, bound
// to change with #69891):
//
//   - Set (Del Set') SingleDel
//     ↓
//   - Set   Set'     SingleDel
//   - Set  (Set'     SingleDel)
//     ↓
//   - Set
//
// which means that a previously deleted intent metadata would erroneously
// become visible again. So on top of restricting SingleDel to the COMMIT case,
// we also restrict it to the case of having no ignored sequence number ranges
// (i.e. no nested txn was rolled back before the commit).
//
// For a deeper discussion of these correctness problems (avoided using the
// scoping down in this helper), see:
//
// https://github.com/cockroachdb/cockroach/issues/69891
type singleDelOptimizationHelper struct {
	// Internal state, don't access this, use the getters instead
	// (that's what the _ prefix is trying to communicate).
	_didNotUpdateMeta *bool
	_hasIgnoredSeqs   bool
	_epoch            enginepb.TxnEpoch
}

// v is the inferred value of the TxnDidNotUpdateMeta field.
func (h singleDelOptimizationHelper) v() bool {
	if h._didNotUpdateMeta == nil {
		return false
	}
	return *h._didNotUpdateMeta
}

// onCommitIntent returns true if the SingleDel optimization is available
// for committing an intent.
func (h singleDelOptimizationHelper) onCommitIntent() bool {
	// We're committing the intent at epoch zero, the meta tracking says we didn't
	// rewrite the intent, and we also didn't previously remove the metadata for
	// this key as part of a voluntary rollback of a nested txn. So we are safe to
	// use a SingleDel here.
	return h.v() && !h._hasIgnoredSeqs && h._epoch == 0
}

// onAbortIntent returns true if the SingleDel optimization is available
// for removing an intent. It is always false.
// Note that "removing an intent" can occur if we know that the epoch
// changed, or when a savepoint is rolled back. It does not imply that
// the transaction aborted.
func (h singleDelOptimizationHelper) onAbortIntent() bool {
	return false
}

// mvccResolveWriteIntent is the core logic for resolving an intent.
// REQUIRES: iter is already seeked to intent.Key.
// REQUIRES: iter surfaces range keys via IterKeyTypePointsAndRanges.
// Returns whether an intent was found and resolved, false otherwise.
func mvccResolveWriteIntent(
	ctx context.Context,
	rw ReadWriter,
	iter iterForKeyVersions,
	ms *enginepb.MVCCStats,
	intent roachpb.LockUpdate,
	buf *putBuffer,
) (bool, error) {
	metaKey := MakeMVCCMetadataKey(intent.Key)
	meta := &buf.meta
	ok, origMetaKeySize, origMetaValSize, err :=
		mvccGetIntent(iter, metaKey, meta)
	if err != nil {
		return false, err
	}
	if !ok || meta.Txn == nil || intent.Txn.ID != meta.Txn.ID {
		return false, nil
	}
	metaTimestamp := meta.Timestamp.ToTimestamp()
	canSingleDelHelper := singleDelOptimizationHelper{
		_didNotUpdateMeta: meta.TxnDidNotUpdateMeta,
		_hasIgnoredSeqs:   len(intent.IgnoredSeqNums) > 0,
		// NB: the value is only used if epochs match, so it doesn't
		// matter if we use the one from meta or incoming request here.
		_epoch: intent.Txn.Epoch,
	}

	// A commit with a newer epoch than the intent effectively means that we
	// wrote this intent before an earlier retry, but didn't write it again
	// after. We treat such intents as uncommitted.
	//
	// A commit with a newer timestamp than the intent means that our timestamp
	// was pushed during the course of an epoch. We treat such intents as
	// committed after moving their timestamp forward. This is possible if a
	// transaction writes an intent and then successfully refreshes its
	// timestamp to avoid a restart.
	//
	// A commit with an older epoch than the intent should never happen because
	// epoch increments require client action. This means that they can't be
	// caused by replays.
	//
	// A commit with an older timestamp than the intent should not happen under
	// normal circumstances because a client should never bump its timestamp
	// after issuing an EndTxn request. Replays of intent writes that are pushed
	// forward due to WriteTooOld errors without client action combined with
	// replays of intent resolution make this configuration a possibility. We
	// treat such intents as uncommitted.
	epochsMatch := meta.Txn.Epoch == intent.Txn.Epoch
	timestampsValid := metaTimestamp.LessEq(intent.Txn.WriteTimestamp)
	timestampChanged := metaTimestamp.Less(intent.Txn.WriteTimestamp)
	commit := intent.Status == roachpb.COMMITTED && epochsMatch && timestampsValid

	// Note the small difference to commit epoch handling here: We allow
	// a push from a previous epoch to move a newer intent. That's not
	// necessary, but useful for allowing pushers to make forward
	// progress. Consider the following, where B reads at a timestamp
	// that's higher than any write by A in the following diagram:
	//
	// | client A@epo | B (pusher) |
	// =============================
	// | write@1      |            |
	// |              | read       |
	// |              | push       |
	// | restart      |            |
	// | write@2      |            |
	// |              | resolve@1  |
	// =============================
	//
	// In this case, if we required the epochs to match, we would not push the
	// intent forward, and client B would upon retrying after its successful
	// push and apparent resolution run into the new version of an intent again
	// (which is at a higher timestamp due to the restart, but not out of the
	// way of A). It would then actually succeed on the second iteration (since
	// the new Epoch propagates to the Push and via that, to the Pushee txn
	// used for resolving), but that costs latency.
	// TODO(tschottdorf): various epoch-related scenarios here deserve more
	// testing.
	inProgress := !intent.Status.IsFinalized() && meta.Txn.Epoch >= intent.Txn.Epoch
	pushed := inProgress && timestampChanged
	latestKey := MVCCKey{Key: intent.Key, Timestamp: metaTimestamp}

	// Handle partial txn rollbacks. If the current txn sequence
	// is part of a rolled back (ignored) seqnum range, we're going
	// to erase that MVCC write and reveal the previous value.
	// If _all_ the writes get removed in this way, the intent
	// can be considered empty and marked for removal (removeIntent = true).
	// If only part of the intent history was rolled back, but the intent still
	// remains, the rolledBackVal is set to a non-nil value.
	var rolledBackVal *MVCCValue
	if len(intent.IgnoredSeqNums) > 0 {
		// NOTE: mvccMaybeRewriteIntentHistory mutates its meta argument.
		// TODO(nvanbenschoten): this is an awkward interface. We shouldn't
		// be mutating meta and we shouldn't be restoring the previous value
		// here. Instead, this should all be handled down below.
		var removeIntent bool
		removeIntent, rolledBackVal, err = mvccMaybeRewriteIntentHistory(ctx, rw, intent.IgnoredSeqNums, meta, latestKey)
		if err != nil {
			return false, err
		}

		if removeIntent {
			// This intent should be cleared. Set commit, pushed, and inProgress to
			// false so that this intent isn't updated, gets cleared, and committed
			// values are left untouched. Also ensure that rolledBackVal is set to nil
			// or we could end up trying to update the intent instead of removing it.
			commit = false
			pushed = false
			inProgress = false
			rolledBackVal = nil
		}

		if rolledBackVal != nil {
			// If we need to update the intent to roll back part of its intent
			// history, make sure that we don't regress its timestamp, even if the
			// caller provided an outdated timestamp.
			intent.Txn.WriteTimestamp.Forward(metaTimestamp)
		}
	}

	// There's nothing to do if meta's epoch is greater than or equal txn's
	// epoch and the state is still in progress but the intent was not pushed
	// to a larger timestamp, and if the rollback code did not modify or mark
	// the intent for removal.
	if inProgress && !pushed && rolledBackVal == nil {
		return false, nil
	}

	// If we're committing, or if the commit timestamp of the intent has been moved forward, and if
	// the proposed epoch matches the existing epoch: update the meta.Txn. For commit, it's set to
	// nil; otherwise, we update its value. We may have to update the actual version value (remove old
	// and create new with proper timestamp-encoded key) if timestamp changed.
	//
	// If the intent has disappeared in mvccMaybeRewriteIntentHistory, we skip
	// this block and fall down to the intent/value deletion code path. This
	// is because removeIntent implies rolledBackVal == nil, pushed == false, and
	// commit == false.
	if commit || pushed || rolledBackVal != nil {
		buf.newMeta = *meta
		newMeta := &buf.newMeta

		// The intent might be committing at a higher timestamp, or it might be
		// getting pushed.
		newTimestamp := intent.Txn.WriteTimestamp

		// Assert that the intent timestamp never regresses. The logic above should
		// not allow this, regardless of the input to this function.
		if newTimestamp.Less(metaTimestamp) {
			return false, errors.AssertionFailedf("timestamp regression (%s -> %s) "+
				"during intent resolution, commit=%t pushed=%t rolledBackVal=%t",
				metaTimestamp, newTimestamp, commit, pushed, rolledBackVal != nil)
		}

		// If we're moving the intent's timestamp, rewrite it and adjust stats.
		var prevIsValue bool
		var prevValSize int64
		if timestampChanged {
			oldKey := latestKey
			newKey := oldKey
			newKey.Timestamp = newTimestamp

			// Rewrite the versioned value at the new timestamp.
			iter.SeekGE(oldKey)
			valid, err := iter.Valid()
			if err != nil {
				return false, err
			} else if valid {
				if hasPoint, hasRange := iter.HasPointAndRange(); hasRange && !hasPoint {
					// If the seek lands on a bare range key, attempt to step to a point.
					iter.Next()
					if valid, err = iter.Valid(); err != nil {
						return false, err
					} else if valid {
						valid, _ = iter.HasPointAndRange()
					}
				}
			}
			if !valid || !iter.UnsafeKey().Equal(oldKey) {
				return false, errors.Errorf("existing intent value missing: %s", oldKey)
			}
			v, err := iter.UnsafeValue()
			if err != nil {
				return false, err
			}
			oldValue, err := DecodeMVCCValue(v)
			if err != nil {
				return false, err
			}
			// Special case: If mvccMaybeRewriteIntentHistory rolled back to a value
			// in the intent history and wrote that at oldKey, iter would not be able
			// to "see" the value since it was created before that value was written
			// to the engine. In this case, reuse the value returned by
			// mvccMaybeRewriteIntentHistory.
			if rolledBackVal != nil {
				oldValue = *rolledBackVal
			}

			// The local timestamp does not change during intent resolution unless the
			// resolver provides a clock observation from this node that was captured
			// while the transaction was still pending, in which case it can be advanced
			// to the observed timestamp.
			newValue := oldValue
			newValue.LocalTimestamp = oldValue.GetLocalTimestamp(oldKey.Timestamp)
			newValue.LocalTimestamp.Forward(intent.ClockWhilePending.Timestamp)
			if !newValue.LocalTimestampNeeded(newKey.Timestamp) || !rw.ShouldWriteLocalTimestamps(ctx) {
				newValue.LocalTimestamp = hlc.ClockTimestamp{}
			}

			// Update the MVCC metadata with the timestamp for the upcoming write (or
			// at least the stats update).
			newMeta.Txn.WriteTimestamp = newTimestamp
			newMeta.Timestamp = newTimestamp.ToLegacyTimestamp()
			newMeta.KeyBytes = MVCCVersionTimestampSize
			newMeta.ValBytes = int64(encodedMVCCValueSize(newValue))
			newMeta.Deleted = newValue.IsTombstone()

			if err = rw.PutMVCC(newKey, newValue); err != nil {
				return false, err
			}
			if err = rw.ClearMVCC(oldKey); err != nil {
				return false, err
			}

			// If there is a value under the intent as it moves timestamps, then
			// that value may need an adjustment of its GCBytesAge. This is
			// because it became non-live at orig.Timestamp originally, and now
			// only becomes non-live at newMeta.Timestamp. For that reason, we
			// have to read that version's size.
			//
			// Look for the first real versioned key, i.e. the key just below
			// the (old) meta's timestamp, and for any MVCC range tombstones.
			iter.Next()
			if valid, err := iter.Valid(); err != nil {
				return false, err
			} else if valid {
				if hasPoint, hasRange := iter.HasPointAndRange(); hasPoint {
					if unsafeKey := iter.UnsafeKey(); unsafeKey.Key.Equal(oldKey.Key) {
						if !hasRange || iter.RangeKeys().Versions[0].Timestamp.Less(unsafeKey.Timestamp) {
							prevValLen, prevValIsTombstone, err := iter.MVCCValueLenAndIsTombstone()
							if err != nil {
								return false, err
							}
							prevIsValue = !prevValIsTombstone
							prevValSize = int64(prevValLen)
						}
					}
				}
			}
		}

		// Update or remove the metadata key.
		var metaKeySize, metaValSize int64
		if !commit {
			// Keep existing intent if we're updating it. We update the existing
			// metadata's timestamp instead of using the supplied intent meta to avoid
			// overwriting a newer epoch (see comments above). The pusher's job isn't
			// to do anything to update the intent but to move the timestamp forward,
			// even if it can.
			metaKeySize, metaValSize, err = buf.putIntentMeta(
				ctx, rw, metaKey, newMeta, true /* alreadyExists */)
		} else {
			metaKeySize = int64(metaKey.EncodedSize())
			err = rw.ClearIntent(metaKey.Key, canSingleDelHelper.onCommitIntent(), meta.Txn.ID)
		}
		if err != nil {
			return false, err
		}

		// Update stat counters related to resolving the intent.
		if ms != nil {
			ms.Add(updateStatsOnResolve(intent.Key, prevIsValue, prevValSize, origMetaKeySize, origMetaValSize,
				metaKeySize, metaValSize, meta, newMeta, commit))
		}

		// Log the logical MVCC operation.
		logicalOp := MVCCCommitIntentOpType
		if pushed {
			logicalOp = MVCCUpdateIntentOpType
		}
		rw.LogLogicalOp(logicalOp, MVCCLogicalOpDetails{
			Txn:       intent.Txn,
			Key:       intent.Key,
			Timestamp: intent.Txn.WriteTimestamp,
		})

		return true, nil
	}

	// Otherwise, we're deleting the intent, which includes deleting the
	// MVCCMetadata.
	//
	// Note that we have to support a somewhat unintuitive case - an ABORT with
	// intent.Txn.Epoch < meta.Txn.Epoch:
	// - writer1 writes key0 at epoch 0
	// - writer2 with higher priority encounters intent at key0 (epoch 0)
	// - writer1 restarts, now at epoch one (txn record not updated)
	// - writer1 writes key0 at epoch 1
	// - writer2 dispatches ResolveIntent to key0 (with epoch 0)
	// - ResolveIntent with epoch 0 aborts intent from epoch 1.

	// First clear the provisional value.
	if err := rw.ClearMVCC(latestKey); err != nil {
		return false, err
	}

	// Log the logical MVCC operation.
	rw.LogLogicalOp(MVCCAbortIntentOpType, MVCCLogicalOpDetails{
		Txn: intent.Txn,
		Key: intent.Key,
	})

	ok = false

	// These variables containing the next key-value information are initialized
	// in the following if-block when ok is set to true. These are only read
	// after the if-block when ok is true (i.e., they were initialized).
	var unsafeNextKey MVCCKey
	var nextValueLen int
	var nextValueIsTombstone bool
	if nextKey := latestKey.Next(); nextKey.IsValue() {
		// The latestKey was not the smallest possible timestamp {WallTime: 0,
		// Logical: 1}. Practically, this is the only case that will occur in
		// production.
		var hasPoint, hasRange bool
		iter.SeekGE(nextKey)
		if ok, err = iter.Valid(); err != nil {
			return false, err
		} else if ok {
			// If the seek lands on a bare range key, attempt to step to a point.
			if hasPoint, hasRange = iter.HasPointAndRange(); hasRange && !hasPoint {
				iter.Next()
				if ok, err = iter.Valid(); err != nil {
					return false, err
				} else if ok {
					hasPoint, hasRange = iter.HasPointAndRange()
					ok = hasPoint
				}
			}
		}
		if ok = ok && iter.UnsafeKey().Key.Equal(latestKey.Key); ok {
			unsafeNextKey = iter.UnsafeKey()
			if !unsafeNextKey.IsValue() {
				// Should never see an intent for this key since we seeked to a
				// particular timestamp.
				return false, errors.Errorf("expected an MVCC value key: %s", unsafeNextKey)
			}
			nextValueLen, nextValueIsTombstone, err = iter.MVCCValueLenAndIsTombstone()
			if err != nil {
				return false, err
			}
			// If a non-tombstone point key is covered by a range tombstone, then
			// synthesize a point tombstone at the lowest range tombstone covering it.
			// This is where the point key ceases to exist, contributing to GCBytesAge.
			if !nextValueIsTombstone && hasRange {
				if v, found := iter.RangeKeys().FirstAtOrAbove(unsafeNextKey.Timestamp); found {
					unsafeNextKey.Timestamp = v.Timestamp
					nextValueIsTombstone = true
					nextValueLen = 0
				}
			}
		}
		iter = nil // prevent accidental use below
	}
	// Else stepped to next key, so !ok

	if !ok {
		// If there is no other version, we should just clean up the key entirely.
		if err = rw.ClearIntent(metaKey.Key, canSingleDelHelper.onAbortIntent(), meta.Txn.ID); err != nil {
			return false, err
		}
		// Clear stat counters attributable to the intent we're aborting.
		if ms != nil {
			ms.Add(updateStatsOnClear(
				intent.Key, origMetaKeySize, origMetaValSize, 0, 0, meta, nil, 0))
		}
		return true, nil
	}

	// Update the keyMetadata with the next version.
	buf.newMeta = enginepb.MVCCMetadata{
		Deleted:  nextValueIsTombstone,
		KeyBytes: MVCCVersionTimestampSize,
		ValBytes: int64(nextValueLen),
	}
	if err = rw.ClearIntent(metaKey.Key, canSingleDelHelper.onAbortIntent(), meta.Txn.ID); err != nil {
		return false, err
	}
	metaKeySize := int64(metaKey.EncodedSize())
	metaValSize := int64(0)

	// Update stat counters with older version.
	if ms != nil {
		ms.Add(updateStatsOnClear(intent.Key, origMetaKeySize, origMetaValSize, metaKeySize,
			metaValSize, meta, &buf.newMeta, unsafeNextKey.Timestamp.WallTime))
	}

	return true, nil
}

// mvccMaybeRewriteIntentHistory rewrites the intent to reveal the latest
// stored value, ignoring all values from the history that have an
// ignored seqnum.
// The remove return value, when true, indicates that
// all the writes in the intent are ignored and the intent should
// be marked for removal as it does not exist any more.
// The updatedVal, when non-nil, indicates that the intent was updated
// and should be overwritten in engine.
func mvccMaybeRewriteIntentHistory(
	ctx context.Context,
	engine ReadWriter,
	ignoredSeqNums []enginepb.IgnoredSeqNumRange,
	meta *enginepb.MVCCMetadata,
	latestKey MVCCKey,
) (remove bool, updatedVal *MVCCValue, err error) {
	if !enginepb.TxnSeqIsIgnored(meta.Txn.Sequence, ignoredSeqNums) {
		// The latest write was not ignored. Nothing to do here.  We'll
		// proceed with the intent as usual.
		return false, nil, nil
	}
	// Find the latest historical write before that that was not
	// ignored.
	var i int
	for i = len(meta.IntentHistory) - 1; i >= 0; i-- {
		e := &meta.IntentHistory[i]
		if !enginepb.TxnSeqIsIgnored(e.Sequence, ignoredSeqNums) {
			break
		}
	}

	// If i < 0, we don't have an intent any more: everything
	// has been rolled back.
	if i < 0 {
		return true, nil, nil
	}

	// Otherwise, we place back the write at that history entry
	// back into the intent.
	restoredValRaw := meta.IntentHistory[i].Value
	restoredVal, err := DecodeMVCCValue(restoredValRaw)
	if err != nil {
		return false, nil, err
	}
	meta.Txn.Sequence = meta.IntentHistory[i].Sequence
	meta.IntentHistory = meta.IntentHistory[:i]
	meta.Deleted = restoredVal.IsTombstone()
	meta.ValBytes = int64(len(restoredValRaw))
	// And also overwrite whatever was there in storage.
	err = engine.PutMVCC(latestKey, restoredVal)

	return false, &restoredVal, err
}

// IterAndBuf used to pass iterators and buffers between MVCC* calls, allowing
// reuse without the callers needing to know the particulars.
type IterAndBuf struct {
	buf  *putBuffer
	iter MVCCIterator
}

// GetBufUsingIter returns an IterAndBuf using the supplied iterator.
func GetBufUsingIter(iter MVCCIterator) IterAndBuf {
	return IterAndBuf{
		buf:  newPutBuffer(),
		iter: iter,
	}
}

// Cleanup must be called to release the resources when done.
func (b IterAndBuf) Cleanup() {
	b.buf.release()
	if b.iter != nil {
		b.iter.Close()
	}
}

// MVCCResolveWriteIntentRange commits or aborts (rolls back) the range of write
// intents specified by start and end keys for a given txn.
// ResolveWriteIntentRange will skip write intents of other txns.
//
// An opts.MaxKeys of zero means unbounded. An opts.MaxKeys of < 0 means
// resolve nothing and returns the entire intent span as the resume span. An
// opts.TargetBytes of 0 means no byte limit. An opts.TargetBytes of < 0 means
// resolve nothing and returns the entire intent span as the resume span. If
// opts.TargetBytes > 0, then resolve intents in the range until the number of
// bytes added to the write batch by intent resolution exceeds
// opts.TargetBytes.
//
// Returns the number of intents resolved, number of bytes added to the write
// batch by intent resolution, the resume span if the max keys or bytes limit
// was exceeded, and the resume reason.
func MVCCResolveWriteIntentRange(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	intent roachpb.LockUpdate,
	opts MVCCResolveWriteIntentRangeOptions,
) (
	numKeys, numBytes int64,
	resumeSpan *roachpb.Span,
	resumeReason roachpb.ResumeReason,
	err error,
) {
	keysExceeded := opts.MaxKeys < 0
	bytesExceeded := opts.TargetBytes < 0
	if keysExceeded || bytesExceeded {
		resumeSpan := intent.Span // don't inline or `intent` would escape to heap
		if keysExceeded {
			resumeReason = roachpb.RESUME_KEY_LIMIT
		} else if bytesExceeded {
			resumeReason = roachpb.RESUME_BYTE_LIMIT
		}
		return 0, 0, &resumeSpan, resumeReason, nil
	}
	ltStart, _ := keys.LockTableSingleKey(intent.Key, nil)
	ltEnd, _ := keys.LockTableSingleKey(intent.EndKey, nil)
	engineIter := rw.NewEngineIterator(IterOptions{LowerBound: ltStart, UpperBound: ltEnd})
	var mvccIter MVCCIterator
	iterOpts := IterOptions{
		KeyTypes:   IterKeyTypePointsAndRanges,
		LowerBound: intent.Key,
		UpperBound: intent.EndKey,
	}
	if rw.ConsistentIterators() {
		// Production code should always have consistent iterators.
		mvccIter = rw.NewMVCCIterator(MVCCKeyIterKind, iterOpts)
	} else {
		// For correctness, we need mvccIter to be consistent with engineIter.
		mvccIter = newPebbleIteratorByCloning(
			engineIter.GetRawIter(), iterOpts, StandardDurability, rw.SupportsRangeKeys())
	}
	iterAndBuf := GetBufUsingIter(mvccIter)
	defer func() {
		engineIter.Close()
		iterAndBuf.Cleanup()
	}()
	putBuf := iterAndBuf.buf
	sepIter := &separatedIntentAndVersionIter{
		engineIter: engineIter,
		mvccIter:   iterAndBuf.iter,
	}
	// Seek sepIter to position it for the loop below. The loop itself will
	// only step the iterator and not seek.
	sepIter.seekEngineKeyGE(EngineKey{Key: ltStart})

	intentEndKey := intent.EndKey
	intent.EndKey = nil

	var lastResolvedKey roachpb.Key
	for {
		if valid, err := sepIter.Valid(); err != nil {
			return 0, 0, nil, 0, err
		} else if !valid {
			// No more intents in the given range.
			break
		}
		keysExceeded = opts.MaxKeys > 0 && numKeys == opts.MaxKeys
		bytesExceeded = opts.TargetBytes > 0 && numBytes >= opts.TargetBytes
		if keysExceeded || bytesExceeded {
			if keysExceeded {
				resumeReason = roachpb.RESUME_KEY_LIMIT
			} else if bytesExceeded {
				resumeReason = roachpb.RESUME_BYTE_LIMIT
			}
			// We could also compute a tighter nextKey here if we wanted to.
			return numKeys, numBytes, &roachpb.Span{Key: lastResolvedKey.Next(), EndKey: intentEndKey}, resumeReason, nil
		}
		// Parse the MVCCMetadata to see if it is a relevant intent.
		meta := &putBuf.meta
		if err := sepIter.ValueProto(meta); err != nil {
			return 0, 0, nil, 0, err
		}
		if meta.Txn == nil {
			return 0, 0, nil, 0, errors.Errorf("intent with no txn")
		}
		if intent.Txn.ID != meta.Txn.ID {
			// Intent for a different txn, so ignore.
			sepIter.nextEngineKey()
			continue
		}
		// Stash the parsed meta so don't need to parse it again in
		// mvccResolveWriteIntent. This parsing can be ~10% of the resolution cost
		// in some benchmarks.
		sepIter.meta = meta
		// Copy the underlying bytes of the unsafe key. This is needed for
		// stability of the key passed to mvccResolveWriteIntent, and for the
		// subsequent iteration to construct a resume span.
		lastResolvedKey = append(lastResolvedKey[:0], sepIter.UnsafeKey().Key...)
		intent.Key = lastResolvedKey
		beforeBytes := rw.BufferedSize()
		ok, err := mvccResolveWriteIntent(ctx, rw, sepIter, ms, intent, putBuf)
		if err != nil {
			log.Warningf(ctx, "failed to resolve intent for key %q: %+v", lastResolvedKey, err)
		} else if ok {
			numKeys++
		}
		numBytes += int64(rw.BufferedSize() - beforeBytes)
		sepIter.nextEngineKey()
	}
	return numKeys, numBytes, nil, 0, nil
}

// MVCCGarbageCollect creates an iterator on the ReadWriter. In parallel
// it iterates through the keys listed for garbage collection by the
// keys slice. The iterator is seeked in turn to each listed
// key, clearing all values with timestamps <= to expiration. The
// timestamp parameter is used to compute the intent age on GC.
//
// Note that this method will be sorting the keys.
//
// REQUIRES: the keys are either all local keys, or all global keys, and
// not a mix of the two. This is to accommodate the implementation below
// that creates an iterator with bounds that span from the first to last
// key (in sorted order).
func MVCCGarbageCollect(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	keys []roachpb.GCRequest_GCKey,
	timestamp hlc.Timestamp,
) error {

	var count int64
	defer func(begin time.Time) {
		log.Eventf(ctx, "done with GC evaluation for %d keys at %.2f keys/sec. Deleted %d entries",
			len(keys), float64(len(keys))*1e9/float64(timeutil.Since(begin)), count)
	}(timeutil.Now())

	// If there are no keys then there is no work.
	if len(keys) == 0 {
		return nil
	}

	// Sort the slice to both determine the bounds and ensure that we're seeking
	// in increasing order.
	sort.Slice(keys, func(i, j int) bool {
		iKey := MVCCKey{Key: keys[i].Key, Timestamp: keys[i].Timestamp}
		jKey := MVCCKey{Key: keys[j].Key, Timestamp: keys[j].Timestamp}
		return iKey.Less(jKey)
	})

	// Bound the iterator appropriately for the set of keys we'll be garbage
	// collecting.
	iter := rw.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
		LowerBound: keys[0].Key,
		UpperBound: keys[len(keys)-1].Key.Next(),
		KeyTypes:   IterKeyTypePointsAndRanges,
	})
	defer iter.Close()

	// Cached stack of range tombstones covering current point. Used to determine
	// GCBytesAge of deleted value by searching first covering range tombstone
	// above.
	var rangeTombstones MVCCRangeKeyStack

	// Iterate through specified GC keys.
	meta := &enginepb.MVCCMetadata{}
	for _, gcKey := range keys {
		encKey := MakeMVCCMetadataKey(gcKey.Key)
		// TODO(oleg): Results of this call are not obvious and logic to handle
		// stats updates for different real and synthesized metadata becomes
		// unnecessary complicated. Revisit this to make it cleaner.
		ok, metaKeySize, metaValSize, realKeyChanged, err := mvccGetMetadata(iter, encKey, meta)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		// If mvccGetMetadata landed on a bare range tombstone for the key it will
		// synthesize deletion meta. We need to filter this case out to avoid
		// updating key stats as the key doesn't exist.
		// Empty realKeyChanged is an indication that there are no values for the
		// key present, but it may contain an inlined metadata which we filter out
		// by checking that it is not inlined.
		// In that case, we can skip to the next gc key as there's nothing to GC.
		// As a side effect of this change, we should be positioned on the point
		// key or inlined meta at this point and can do further checks for GC
		// eligibility.
		inlinedValue := meta.IsInline()
		if realKeyChanged.IsEmpty() && !inlinedValue {
			continue
		}

		// We are guaranteed now to be positioned at the meta or version key that
		// belongs to gcKey history.

		unsafeKey := iter.UnsafeKey()
		implicitMeta := unsafeKey.IsValue()
		// Note that we naively can't terminate GC'ing keys loop early if we
		// enter any of branches below, as it will update the stats under the
		// provision that the (implicit or explicit) meta key (and thus all
		// versions) are being removed. We had this faulty functionality at some
		// point; it should no longer be necessary since the higher levels already
		// make sure each individual GCRequest does bounded work.
		//
		// First check for the case of range tombstone covering keys when no
		// metadata is available.
		if implicitMeta && meta.Deleted && !meta.Timestamp.Equal(unsafeKey.Timestamp) {
			// If we have implicit deletion meta, and realKeyChanged is not the first
			// key in history, that means it is covered by a range tombstone (which
			// was used to synthesize meta).
			if unsafeKey.Timestamp.LessEq(gcKey.Timestamp) {
				// If first object in history is at or below gcKey timestamp then we
				// have no explicit meta and all objects are subject to deletion.
				if ms != nil {
					ms.Add(updateStatsOnGC(gcKey.Key, metaKeySize, metaValSize, true, /* metaKey */
						realKeyChanged.WallTime))
				}
			}
		} else if meta.Timestamp.ToTimestamp().LessEq(gcKey.Timestamp) {
			// Then, check whether all values of the key are being deleted in the
			// rest of the cases.
			//
			// For version keys, don't allow GC'ing the meta key if it's
			// not marked deleted. However, for inline values we allow it;
			// they are internal and GCing them directly saves the extra
			// deletion step.
			if !meta.Deleted && !inlinedValue {
				return errors.Errorf("request to GC non-deleted, latest value of %q", gcKey.Key)
			}
			if meta.Txn != nil {
				return errors.Errorf("request to GC intent at %q", gcKey.Key)
			}
			if ms != nil {
				if inlinedValue {
					updateStatsForInline(ms, gcKey.Key, metaKeySize, metaValSize, 0, 0)
					ms.AgeTo(timestamp.WallTime)
				} else {
					ms.Add(updateStatsOnGC(gcKey.Key, metaKeySize, metaValSize, true /* metaKey */, meta.Timestamp.WallTime))
				}
			}
			if !implicitMeta {
				// This must be an inline entry since we are not allowed to clear
				// intents, and we've confirmed that meta.Txn == nil earlier.
				if err := rw.ClearUnversioned(iter.UnsafeKey().Key); err != nil {
					return err
				}
				count++
			}
		}

		if !implicitMeta {
			// The iter is pointing at an MVCCMetadata, advance to the next entry.
			iter.Next()
		}

		// For GCBytesAge, this requires keeping track of the previous key's
		// timestamp (prevNanos). See ComputeStats for a more easily digested and
		// better commented version of this logic. The below block will set
		// prevNanos to the appropriate value and position the iterator at the first
		// garbage version.
		prevNanos := timestamp.WallTime
		{
			// If true - forward iteration positioned iterator on first garbage
			// (key.ts <= gc.ts).
			var foundPrevNanos bool
			{
				// We'll step the iterator a few time before attempting to seek.

				// True if we found next key while iterating. That means there's no
				// garbage for the key.
				var foundNextKey bool

				// If there are a large number of versions which are not garbage,
				// iterating through all of them is very inefficient. However, if there
				// are few, SeekLT is inefficient. MVCCGarbageCollect will try to step
				// the iterator a few times to find the predecessor of gcKey before
				// resorting to seeking.
				//
				// In a synthetic benchmark where there is one version of garbage and
				// one not, this optimization showed a 50% improvement. More
				// importantly, this optimization mitigated the overhead of the Seek
				// approach when almost all of the versions are garbage.
				const nextsBeforeSeekLT = 4
				for i := 0; i < nextsBeforeSeekLT; i++ {
					if i > 0 {
						iter.Next()
					}
					if ok, err := iter.Valid(); err != nil {
						return err
					} else if !ok {
						foundNextKey = true
						break
					}
					if hasPoint, _ := iter.HasPointAndRange(); !hasPoint {
						foundNextKey = true
						break
					}
					unsafeIterKey := iter.UnsafeKey()
					if !unsafeIterKey.Key.Equal(encKey.Key) {
						foundNextKey = true
						break
					}
					if unsafeIterKey.Timestamp.LessEq(gcKey.Timestamp) {
						foundPrevNanos = true
						break
					}
					prevNanos = unsafeIterKey.Timestamp.WallTime
				}

				// We have nothing to GC for this key if we found the next key.
				if foundNextKey {
					continue
				}
			}

			// Stepping with the iterator did not get us to our target garbage key or
			// its predecessor. Seek to the predecessor to find the right value for
			// prevNanos and position the iterator on the gcKey.
			if !foundPrevNanos {
				gcKeyMVCC := MVCCKey{Key: gcKey.Key, Timestamp: gcKey.Timestamp}
				iter.SeekLT(gcKeyMVCC)
				if ok, err := iter.Valid(); err != nil {
					return err
				} else if ok {
					if hasPoint, _ := iter.HasPointAndRange(); hasPoint {
						// Use the previous version's timestamp if it's for this key.
						if iter.UnsafeKey().Key.Equal(gcKey.Key) {
							prevNanos = iter.UnsafeKey().Timestamp.WallTime
						}
						// Seek to the first version for deletion.
						iter.Next()
					}
				}
			}
		}

		// At this point iterator is positioned on first garbage version and forward
		// iteration will give us all versions to delete up to the next key.

		if ms != nil {
			// We need to iterate ranges only to compute GCBytesAge if we are updating
			// stats.
			//
			// We can't rely on range key changed iterator functionality here as we do
			// seek on every loop iteration to find next key to GC.
			if _, hasRange := iter.HasPointAndRange(); hasRange {
				if !rangeTombstones.Bounds.Key.Equal(iter.RangeBounds().Key) {
					iter.RangeKeys().CloneInto(&rangeTombstones)
				}
			} else if !rangeTombstones.IsEmpty() {
				rangeTombstones.Clear()
			}
		}

		// Iterate through the garbage versions, accumulating their stats and
		// issuing clear operations.
		for ; ; iter.Next() {
			if ok, err := iter.Valid(); err != nil {
				return err
			} else if !ok {
				break
			}
			unsafeIterKey := iter.UnsafeKey()
			if !unsafeIterKey.Key.Equal(encKey.Key) {
				break
			}
			if !unsafeIterKey.IsValue() {
				break
			}
			if ms != nil {
				valLen, valIsTombstone, err := iter.MVCCValueLenAndIsTombstone()
				if err != nil {
					return err
				}
				keySize := MVCCVersionTimestampSize
				valSize := int64(valLen)

				// A non-deletion becomes non-live when its newer neighbor shows up.
				// A deletion tombstone becomes non-live right when it is created.
				fromNS := prevNanos
				if valIsTombstone {
					fromNS = unsafeIterKey.Timestamp.WallTime
				} else if !rangeTombstones.IsEmpty() {
					// For non deletions, we need to find if we had a range tombstone
					// between this and next value (prevNanos) to use its timestamp for
					// computing GCBytesAge.
					if kv, ok := rangeTombstones.FirstAtOrAbove(unsafeIterKey.Timestamp); ok {
						if kv.Timestamp.WallTime < fromNS {
							fromNS = kv.Timestamp.WallTime
						}
					}
				}

				ms.Add(updateStatsOnGC(gcKey.Key, keySize, valSize, false /* metaKey */, fromNS))
			}
			count++
			if err := rw.ClearMVCC(unsafeIterKey); err != nil {
				return err
			}
			prevNanos = unsafeIterKey.Timestamp.WallTime
		}
	}

	return nil
}

// CollectableGCRangeKey is a struct containing range key as well as span
// boundaries locked for particular range key.
// Range GC needs a latch span as it needs to expand iteration beyond the
// range key itself to find adjacent ranges and those ranges should be safe to
// read.
type CollectableGCRangeKey struct {
	MVCCRangeKey
	LatchSpan roachpb.Span
}

// MVCCGarbageCollectRangeKeys is similar in functionality to MVCCGarbageCollect but
// operates on range keys. It does sanity checks that no values exist below
// range tombstones so that no values are exposed in case point values GC was
// not performed correctly by the level above.
func MVCCGarbageCollectRangeKeys(
	ctx context.Context, rw ReadWriter, ms *enginepb.MVCCStats, rks []CollectableGCRangeKey,
) error {

	var count int64
	defer func(begin time.Time) {
		// TODO(oleg): this could be misleading if GC fails, but this function still
		// reports how many keys were GC'd. The approach is identical to what point
		// key GC does for consistency, but both places could be improved.
		log.Eventf(ctx,
			"done with GC evaluation for %d range keys at %.2f keys/sec. Deleted %d entries",
			len(rks), float64(len(rks))*1e9/float64(timeutil.Since(begin)), count)
	}(timeutil.Now())

	if len(rks) == 0 {
		return nil
	}

	// Validate range keys are well formed.
	for _, rk := range rks {
		if err := rk.Validate(); err != nil {
			return errors.Wrap(err, "failed to validate gc range keys in mvcc gc")
		}
	}

	sort.Slice(rks, func(i, j int) bool {
		return rks[i].Compare(rks[j].MVCCRangeKey) < 0
	})

	// Validate that keys are non-overlapping.
	for i := 1; i < len(rks); i++ {
		if rks[i].StartKey.Compare(rks[i-1].EndKey) < 0 {
			return errors.Errorf("range keys in gc request should be non-overlapping: %s vs %s",
				rks[i-1].String(), rks[i].String())
		}
	}

	// gcRangeKey garbage collects the given range key, using a closure to manage
	// iterator lifetimes via defer.
	gcRangeKey := func(gcKey CollectableGCRangeKey) error {

		// lhs keeps track of any range key to the left, in case they merge to the
		// right following GC.
		var lhs MVCCRangeKeyStack

		// Bound the iterator appropriately for the set of keys we'll be garbage
		// collecting. We are using latch bounds to collect info about adjacent
		// range fragments for correct MVCCStats updates.
		iter := rw.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
			LowerBound: gcKey.LatchSpan.Key,
			UpperBound: gcKey.LatchSpan.EndKey,
			KeyTypes:   IterKeyTypeRangesOnly,
		})
		defer iter.Close()

		for iter.SeekGE(MVCCKey{Key: gcKey.LatchSpan.Key}); ; iter.Next() {
			if ok, err := iter.Valid(); err != nil {
				return err
			} else if !ok {
				break
			}

			rangeKeys := iter.RangeKeys()

			// Check if preceding range tombstone is adjacent to GC'd one. If we
			// started iterating too early, just skip to next key.
			if rangeKeys.Bounds.EndKey.Compare(gcKey.StartKey) <= 0 {
				rangeKeys.CloneInto(&lhs)
				continue
			}

			// Terminate loop once we've reached a range tombstone past the right
			// GC range key boundary, but check if we merge with it.
			if rangeKeys.Bounds.Key.Compare(gcKey.EndKey) >= 0 {
				if ms != nil && lhs.CanMergeRight(rangeKeys) {
					ms.Add(updateStatsOnRangeKeyMerge(rangeKeys.Bounds.Key, rangeKeys.Versions))
				}
				break
			}

			// If there's nothing to GC, keep moving.
			if !rangeKeys.Oldest().LessEq(gcKey.Timestamp) {
				// Even if we don't GC anything for this range fragment, we might have
				// changed previous and it might become mergable as a result.
				if ms != nil && lhs.CanMergeRight(rangeKeys) {
					ms.Add(updateStatsOnRangeKeyMerge(rangeKeys.Bounds.Key, rangeKeys.Versions))
				}
				rangeKeys.CloneInto(&lhs)
				continue
			}

			// Account for any range key fragmentation due to the clears. The actual
			// clears of the inside fragments will be accounted for later. We also
			// truncate the bounds of the range key stack to the GC bounds, since
			// this is the part we'll be clearing.
			if rangeKeys.Bounds.Key.Compare(gcKey.StartKey) < 0 {
				rangeKeys.Bounds.Key = gcKey.StartKey
				if ms != nil {
					ms.Add(UpdateStatsOnRangeKeySplit(gcKey.StartKey, rangeKeys.Versions))
				}
			}
			if rangeKeys.Bounds.EndKey.Compare(gcKey.EndKey) > 0 {
				rangeKeys.Bounds.EndKey = gcKey.EndKey
				if ms != nil {
					ms.Add(UpdateStatsOnRangeKeySplit(gcKey.EndKey, rangeKeys.Versions))
				}
			}

			// Clear the range keys, and keep track of any remaining range keys. We
			// do this in reverse order, so that we can shorten the slice in place
			// while we're iterating.
			for i := rangeKeys.Len() - 1; i >= 0; i-- {
				v := rangeKeys.Versions[i]
				if !v.Timestamp.LessEq(gcKey.Timestamp) {
					break
				}
				if err := rw.ClearMVCCRangeKey(rangeKeys.AsRangeKey(v)); err != nil {
					return err
				}
				if ms != nil {
					ms.Add(updateStatsOnRangeKeyClearVersion(rangeKeys, v))
				}
				rangeKeys.Versions = rangeKeys.Versions[:i]
			}

			// Check whether we're merging with the stack to our left, and record
			// the current stack for the next iteration.
			if ms != nil && lhs.CanMergeRight(rangeKeys) {
				ms.Add(updateStatsOnRangeKeyMerge(rangeKeys.Bounds.Key, rangeKeys.Versions))
			}
			rangeKeys.CloneInto(&lhs)

			// Verify that there are no remaining data under the deleted range using
			// time bound iterator.
			ptIter := NewMVCCIncrementalIterator(rw, MVCCIncrementalIterOptions{
				KeyTypes:     IterKeyTypePointsOnly,
				StartKey:     rangeKeys.Bounds.Key,
				EndKey:       rangeKeys.Bounds.EndKey,
				EndTime:      gcKey.Timestamp,
				IntentPolicy: MVCCIncrementalIterIntentPolicyEmit,
			})
			defer ptIter.Close()

			for ptIter.SeekGE(MVCCKey{Key: rangeKeys.Bounds.Key}); ; ptIter.Next() {
				if ok, err := ptIter.Valid(); err != nil {
					return err
				} else if !ok {
					break
				}
				// Disallow any value under the range key. We only skip intents as they
				// must have a provisional value with appropriate timestamp.
				if pointKey := ptIter.UnsafeKey(); pointKey.IsValue() {
					return errors.Errorf("attempt to delete range tombstone %q hiding key at %q",
						gcKey, pointKey)
				}
			}
		}
		return nil
	}

	for _, gcKey := range rks {
		if err := gcRangeKey(gcKey); err != nil {
			return err
		}
	}

	return nil
}

// MVCCGarbageCollectWholeRange removes all the range data and resets counters.
// It only does so if data is completely covered by range keys
func MVCCGarbageCollectWholeRange(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	start, end roachpb.Key,
	gcThreshold hlc.Timestamp,
	rangeStats enginepb.MVCCStats,
) error {
	if rangeStats.ContainsEstimates == 0 && rangeStats.LiveCount > 0 {
		return errors.Errorf("range contains live data, can't use GC clear range")
	}
	if _, err := CanGCEntireRange(ctx, rw, start, end, gcThreshold); err != nil {
		return err
	}
	if err := rw.ClearRawRange(start, end, true, true); err != nil {
		return err
	}
	if ms != nil {
		// Reset point and range counters as we deleted the whole range.
		rangeStats.AgeTo(ms.LastUpdateNanos)
		ms.LiveCount -= rangeStats.LiveCount
		ms.LiveBytes -= rangeStats.LiveBytes
		ms.KeyCount -= rangeStats.KeyCount
		ms.KeyBytes -= rangeStats.KeyBytes
		ms.ValCount -= rangeStats.ValCount
		ms.ValBytes -= rangeStats.ValBytes
		ms.RangeKeyCount -= rangeStats.RangeKeyCount
		ms.RangeKeyBytes -= rangeStats.RangeKeyBytes
		ms.RangeValCount -= rangeStats.RangeValCount
		ms.RangeValBytes -= rangeStats.RangeValBytes
		ms.GCBytesAge -= rangeStats.GCBytesAge
		// We also zero out intents as range can't be cleared if intents are
		// present.
		// This should only be the case if stats are estimates and intent
		// information was not accurate.
		ms.IntentCount -= rangeStats.IntentCount
		ms.IntentBytes -= rangeStats.IntentBytes
		ms.IntentAge -= rangeStats.IntentAge
		ms.SeparatedIntentCount -= rangeStats.SeparatedIntentCount
	}
	return nil
}

// CanGCEntireRange checks if a span of keys doesn't contain any live data
// and all data is covered by range tombstones at or below provided threshold.
// This functions is meant for fast path deletion by GC where range can be
// removed by a range tombstone.
func CanGCEntireRange(
	ctx context.Context, rw Reader, start, end roachpb.Key, gcThreshold hlc.Timestamp,
) (coveredByRangeTombstones bool, err error) {
	// It makes no sense to check local ranges for fast path.
	if isLocal(start) || isLocal(end) {
		return coveredByRangeTombstones, errors.Errorf("range emptiness check can only be done on global ranges")
	}
	iter := rw.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
		KeyTypes:             IterKeyTypePointsAndRanges,
		LowerBound:           start,
		UpperBound:           end,
		RangeKeyMaskingBelow: gcThreshold,
	})
	defer iter.Close()
	iter.SeekGE(MVCCKey{Key: start})
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return coveredByRangeTombstones, err
		} else if !ok {
			break
		}
		hasPoint, hasRange := iter.HasPointAndRange()
		if hasPoint {
			return coveredByRangeTombstones, errors.Errorf("found key not covered by range tombstone %s",
				iter.UnsafeKey())
		}
		if hasRange {
			coveredByRangeTombstones = true
			newest := iter.RangeKeys().Newest()
			if gcThreshold.Less(newest) {
				return coveredByRangeTombstones, errors.Errorf("range tombstones above gc threshold. GC=%s, range=%s",
					gcThreshold.String(), newest.String())
			}
		}
	}
	return coveredByRangeTombstones, nil
}

// MVCCGarbageCollectPointsWithClearRange removes garbage collected points data
// within range [start@startTimestamp, endTimestamp). This function performs a
// check to ensure that no non-garbage data (most recent or history with
// timestamp greater that threshold) is being deleted. Range tombstones are kept
// intact and need to be removed separately.
func MVCCGarbageCollectPointsWithClearRange(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	start, end roachpb.Key,
	startTimestamp hlc.Timestamp,
	gcThreshold hlc.Timestamp,
) error {
	var countKeys int64
	var removedEntries int64
	defer func(begin time.Time) {
		// TODO(oleg): this could be misleading if GC fails, but this function still
		// reports how many keys were GC'd. The approach is identical to what point
		// key GC does for consistency, but both places could be improved.
		log.Eventf(ctx,
			"done with GC evaluation for clear range of %d keys at %.2f keys/sec. Deleted %d entries",
			countKeys, float64(countKeys)*1e9/float64(timeutil.Since(begin)), removedEntries)
	}(timeutil.Now())

	iter := rw.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
		LowerBound: start,
		UpperBound: end,
		KeyTypes:   IterKeyTypePointsAndRanges,
	})
	defer iter.Close()

	iter.SeekGE(MVCCKey{Key: start})

	var (
		// prevPointKey is a newer version (with higher timestamp) of current key.
		// Its key component is updated when key is first seen at the beginning of
		// loop, and timestamp is reset to empty.
		// Its timestamp component is updated at the end of loop (including
		// continue).
		// It is used to check that current key is covered and eligible for GC as
		// well as mvcc stats calculations.
		prevPointKey    MVCCKey
		rangeTombstones MVCCRangeKeyStack
		firstKey        = true
	)

	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		if iter.RangeKeyChanged() {
			iter.RangeKeys().CloneInto(&rangeTombstones)
			if hasPoint, _ := iter.HasPointAndRange(); !hasPoint {
				continue
			}
		}

		// Invariant: we're now positioned on a point key. The iterator can only
		// be positioned on a bare range key when `RangeKeyChanged()` returns `true`.
		countKeys++
		unsafeKey := iter.UnsafeKey()
		newKey := !prevPointKey.Key.Equal(unsafeKey.Key)
		if newKey {
			unsafeKey.CloneInto(&prevPointKey)
			prevPointKey.Timestamp = hlc.Timestamp{}
		}

		// Skip keys that fall outside of range (only until we reach the first
		// eligible key).
		if firstKey && unsafeKey.Compare(MVCCKey{Key: start, Timestamp: startTimestamp}) < 0 {
			prevPointKey.Timestamp = unsafeKey.Timestamp
			continue
		}
		firstKey = false

		if unsafeKey.Timestamp.IsEmpty() {
			// Found unresolved intent. We use .String() explicitly as it is not
			// including  timestamps if they are zero, but Format() does.
			return errors.Errorf("attempt to GC intent %s using clear range",
				unsafeKey.String())
		}
		if gcThreshold.Less(unsafeKey.Timestamp) {
			// Current version is above GC threshold so it is not safe to clear.
			return errors.Errorf("attempt to GC data %s above threshold %s with clear range",
				unsafeKey, gcThreshold)
		}

		valueLen, isTombstone, err := iter.MVCCValueLenAndIsTombstone()
		if err != nil {
			return err
		}

		// Find timestamp covering current key.
		coveredBy := prevPointKey.Timestamp
		if rangeKeyCover, ok := rangeTombstones.FirstAtOrAbove(unsafeKey.Timestamp); ok {
			// If there's a range between current value and value above
			// use that timestamp.
			if coveredBy.IsEmpty() || rangeKeyCover.Timestamp.Less(coveredBy) {
				coveredBy = rangeKeyCover.Timestamp
			}
		}

		if isGarbage := !coveredBy.IsEmpty() && coveredBy.LessEq(gcThreshold) || isTombstone; !isGarbage {
			// Current version is below threshold and is not a tombstone, but
			// preceding one is above so it is visible and can't be cleared.
			return errors.Errorf("attempt to GC data %s still visible at GC threshold %s with clear range",
				unsafeKey, gcThreshold)
		}

		validTill := coveredBy
		if isTombstone {
			validTill = unsafeKey.Timestamp
		}

		if ms != nil {
			if newKey {
				ms.Add(updateStatsOnGC(unsafeKey.Key, int64(EncodedMVCCKeyPrefixLength(unsafeKey.Key)), 0,
					true /* metaKey */, validTill.WallTime))
			}
			ms.Add(updateStatsOnGC(unsafeKey.Key, MVCCVersionTimestampSize, int64(valueLen), false, /* metaKey */
				validTill.WallTime))
		}
		prevPointKey.Timestamp = unsafeKey.Timestamp
		removedEntries++
	}

	// If timestamp is not empty we delete subset of versions (this may be first
	// key of requested range or full extent).
	if err := rw.ClearMVCCVersions(MVCCKey{Key: start, Timestamp: startTimestamp}, MVCCKey{Key: end}); err != nil {
		return err
	}
	return nil
}

// MVCCFindSplitKey finds a key from the given span such that the left side of
// the split is roughly targetSize bytes. It only considers MVCC point keys, not
// range keys. The returned key will never be chosen from the key ranges listed
// in keys.NoSplitSpans.
func MVCCFindSplitKey(
	_ context.Context, reader Reader, key, endKey roachpb.RKey, targetSize int64,
) (roachpb.Key, error) {
	if key.Less(roachpb.RKey(keys.LocalMax)) {
		key = roachpb.RKey(keys.LocalMax)
	}

	it := reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: endKey.AsRawKey()})
	defer it.Close()

	// We want to avoid splitting at the first key in the range because that
	// could result in an empty left-hand range. To prevent this, we scan for
	// the first key in the range and consider the key that sorts directly after
	// this as the minimum split key.
	//
	// In addition, we must never return a split key that falls within a table
	// row. (Rows in tables with multiple column families are comprised of
	// multiple keys, one key per column family.)
	//
	// Managing this is complicated: the logic for picking a split key that
	// creates ranges of the right size lives in C++, while the logic for
	// determining whether a key falls within a table row lives in Go.
	//
	// Most of the time, we can let C++ pick whatever key it wants. If it picks a
	// key in the middle of a row, we simply rewind the key to the start of the
	// row. This is handled by keys.EnsureSafeSplitKey.
	//
	// If, however, that first row in the range is so large that it exceeds the
	// range size threshold on its own, and that row is comprised of multiple
	// column families, we have a problem. C++ will hand us a key in the middle of
	// that row, keys.EnsureSafeSplitKey will rewind the key to the beginning of
	// the row, and... we'll end up with what's likely to be the start key of the
	// range. The higher layers of the stack will take this to mean that no splits
	// are required, when in fact the range is desperately in need of a split.
	//
	// Note that the first range of a table or a partition of a table does not
	// start on a row boundary and so we have a slightly different problem.
	// Instead of not splitting the range at all, we'll create a split at the
	// start of the first row, resulting in an unnecessary empty range from the
	// beginning of the table to the first row in the table (e.g., from /Table/51
	// to /Table/51/1/aardvark...). The right-hand side of the split will then be
	// susceptible to never being split as outlined above.
	//
	// To solve both of these problems, we find the end of the first row in Go,
	// then plumb that to C++ as a "minimum split key." We're then guaranteed that
	// the key C++ returns will rewind to the start key of the range.
	//
	// On a related note, we find the first row by actually looking at the first
	// key in the range. A previous version of this code attempted to derive
	// the first row only by looking at `key`, the start key of the range; this
	// was dangerous because partitioning can split off ranges that do not start
	// at valid row keys. The keys that are present in the range, by contrast, are
	// necessarily valid row keys.
	it.SeekGE(MakeMVCCMetadataKey(key.AsRawKey()))
	if ok, err := it.Valid(); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	var minSplitKey roachpb.Key
	if _, tenID, err := keys.DecodeTenantPrefix(it.UnsafeKey().Key); err == nil {
		if _, _, err := keys.MakeSQLCodec(tenID).DecodeTablePrefix(it.UnsafeKey().Key); err == nil {
			// The first key in this range represents a row in a SQL table. Advance the
			// minSplitKey past this row to avoid the problems described above.
			firstRowKey, err := keys.EnsureSafeSplitKey(it.Key().Key)
			if err != nil {
				return nil, err
			}
			// Allow a split key before other rows in the same table.
			minSplitKey = firstRowKey.PrefixEnd()
		}
	}
	if minSplitKey == nil {
		// The first key in the range does not represent a row in a SQL table.
		// Allow a split at any key that sorts after it.
		minSplitKey = it.Key().Key.Next()
	}

	splitKey, err := it.FindSplitKey(key.AsRawKey(), endKey.AsRawKey(), minSplitKey, targetSize)
	if err != nil {
		return nil, err
	}
	// Ensure the key is a valid split point that does not fall in the middle of a
	// SQL row by removing the column family ID, if any, from the end of the key.
	return keys.EnsureSafeSplitKey(splitKey.Key)
}

// willOverflow returns true iff adding both inputs would under- or overflow
// the 64 bit integer range.
func willOverflow(a, b int64) bool {
	// Morally MinInt64 < a+b < MaxInt64, but without overflows.
	// First make sure that a <= b. If not, swap them.
	if a > b {
		a, b = b, a
	}
	// Now b is the larger of the numbers, and we compare sizes
	// in a way that can never over- or underflow.
	if b > 0 {
		return a > math.MaxInt64-b
	}
	return math.MinInt64-b > a
}

// ComputeStats scans the given key span and computes MVCC stats. nowNanos
// specifies the wall time in nanoseconds since the epoch and is used to compute
// age-related stats quantities.
func ComputeStats(r Reader, start, end roachpb.Key, nowNanos int64) (enginepb.MVCCStats, error) {
	return ComputeStatsWithVisitors(r, start, end, nowNanos, nil, nil)
}

// ComputeStatsWithVisitors is like ComputeStats, but also takes a point and/or
// range key callback that is invoked for each key.
func ComputeStatsWithVisitors(
	r Reader,
	start, end roachpb.Key,
	nowNanos int64,
	pointKeyVisitor func(MVCCKey, []byte) error,
	rangeKeyVisitor func(MVCCRangeKeyValue) error,
) (enginepb.MVCCStats, error) {
	iter := r.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
		KeyTypes:   IterKeyTypePointsAndRanges,
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()
	iter.SeekGE(MVCCKey{Key: start})
	return computeStatsForIterWithVisitors(iter, nowNanos, pointKeyVisitor, rangeKeyVisitor)
}

// ComputeStatsForIter is like ComputeStats, but scans across the given iterator
// until exhausted. The iterator must have appropriate bounds, key types, and
// intent options set, and it must have been seeked to the appropriate starting
// point.
//
// We don't take start/end here, because that would require expensive key
// comparisons. We also don't seek to e.g. MinKey, because that might violate
// spanset assertions.
//
// Most callers should use ComputeStats() instead. This exists primarily for use
// with SST iterators.
func ComputeStatsForIter(iter SimpleMVCCIterator, nowNanos int64) (enginepb.MVCCStats, error) {
	return computeStatsForIterWithVisitors(iter, nowNanos, nil, nil)
}

// computeStatsForIterWithVisitors performs the actual stats computation for the
// other ComputeStats methods.
//
// The iterator must already have been seeked. This requirement is to comply
// with spanset assertions, such that ComputeStats can seek to the given start
// key (satisfying the spanset asserter), while ComputeStatsForIter can seek to
// MinKey (in effect the iterator's lower bound) as it's geared towards SST
// iterators which are not subject to spanset assertions.
//
// Notably, we do not want to take the start/end key here, and instead rely on
// the iterator's bounds, to avoid expensive key comparisons.
func computeStatsForIterWithVisitors(
	iter SimpleMVCCIterator,
	nowNanos int64,
	pointKeyVisitor func(MVCCKey, []byte) error,
	rangeKeyVisitor func(MVCCRangeKeyValue) error,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats
	var meta enginepb.MVCCMetadata
	var prevKey roachpb.Key
	var first bool

	// Values start accruing GCBytesAge at the timestamp at which they
	// are shadowed (i.e. overwritten) whereas deletion tombstones
	// use their own timestamp. We're iterating through versions in
	// reverse chronological order and use this variable to keep track
	// of the point in time at which the current key begins to age.
	var accrueGCAgeNanos int64
	var rangeTombstones MVCCRangeKeyVersions

	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return ms, err
		} else if !ok {
			break
		}

		// Process MVCC range tombstones, and buffer them in rangeTombstones
		// for all overlapping point keys.
		if iter.RangeKeyChanged() {
			if hasPoint, hasRange := iter.HasPointAndRange(); hasRange {
				rangeKeys := iter.RangeKeys()
				rangeKeys.Versions.CloneInto(&rangeTombstones)

				for i, v := range rangeTombstones {
					// Only the top-most fragment contributes the key and its bounds, but
					// all versions contribute timestamps and values.
					//
					// NB: Point keys always use 12 bytes for the key timestamp, even
					// though it is actually variable-length, likely for historical
					// reasons. But for range keys we may as well use the actual
					// variable-length encoded size.
					keyBytes := int64(EncodedMVCCTimestampSuffixLength(v.Timestamp))
					valBytes := int64(len(v.Value))
					if i == 0 {
						ms.RangeKeyCount++
						keyBytes += int64(EncodedMVCCKeyPrefixLength(rangeKeys.Bounds.Key) +
							EncodedMVCCKeyPrefixLength(rangeKeys.Bounds.EndKey))
					}
					ms.RangeKeyBytes += keyBytes
					ms.RangeValCount++
					ms.RangeValBytes += valBytes
					ms.GCBytesAge += (keyBytes + valBytes) * (nowNanos/1e9 - v.Timestamp.WallTime/1e9)

					if rangeKeyVisitor != nil {
						if err := rangeKeyVisitor(rangeKeys.AsRangeKeyValue(v)); err != nil {
							return enginepb.MVCCStats{}, err
						}
					}
				}

				if !hasPoint {
					continue
				}
			} else {
				rangeTombstones.Clear()
			}
		}

		unsafeKey := iter.UnsafeKey()

		if pointKeyVisitor != nil {
			// NB: pointKeyVisitor is typically nil, so we will typically not call
			// iter.UnsafeValue().
			v, err := iter.UnsafeValue()
			if err != nil {
				return enginepb.MVCCStats{}, err
			}
			if err := pointKeyVisitor(unsafeKey, v); err != nil {
				return enginepb.MVCCStats{}, err
			}
		}

		// Check for ignored keys.
		if bytes.HasPrefix(unsafeKey.Key, keys.LocalRangeIDPrefix) {
			// RangeID-local key.
			_ /* rangeID */, infix, suffix, _ /* detail */, err := keys.DecodeRangeIDKey(unsafeKey.Key)
			if err != nil {
				return enginepb.MVCCStats{}, errors.Wrap(err, "unable to decode rangeID key")
			}

			if infix.Equal(keys.LocalRangeIDReplicatedInfix) {
				// Replicated RangeID-local key.
				if suffix.Equal(keys.LocalRangeAppliedStateSuffix) {
					// RangeAppliedState key. Ignore.
					continue
				}
			}
		}

		isSys := isSysLocal(unsafeKey.Key)
		isValue := unsafeKey.IsValue()
		implicitMeta := isValue && !bytes.Equal(unsafeKey.Key, prevKey)
		prevKey = append(prevKey[:0], unsafeKey.Key...)

		// Find the closest range tombstone above the point key. Range tombstones
		// cannot exist above intents, and are undefined across inline values, so we
		// only take them into account for versioned values.
		//
		// TODO(erikgrinaker): Rather than doing a full binary search for each
		// point, we can keep track of the current index and move downwards in the
		// stack as we descend through older versions, resetting once we hit a new
		// key.
		var nextRangeTombstone hlc.Timestamp
		if isValue {
			if !rangeTombstones.IsEmpty() && unsafeKey.Timestamp.LessEq(rangeTombstones.Newest()) {
				if v, ok := rangeTombstones.FirstAtOrAbove(unsafeKey.Timestamp); ok {
					nextRangeTombstone = v.Timestamp
				}
			}
		}

		var valueLen int
		var mvccValueIsTombstone bool
		if isValue {
			// MVCC value
			var err error
			valueLen, mvccValueIsTombstone, err = iter.MVCCValueLenAndIsTombstone()
			if err != nil {
				return enginepb.MVCCStats{}, errors.Wrap(err, "unable to decode MVCCValue")
			}
		} else {
			valueLen = iter.ValueLen()
		}
		if implicitMeta {
			// INVARIANT: implicitMeta => isValue.
			// No MVCCMetadata entry for this series of keys.
			meta.Reset()
			meta.KeyBytes = MVCCVersionTimestampSize
			meta.ValBytes = int64(valueLen)
			meta.Deleted = mvccValueIsTombstone
			meta.Timestamp.WallTime = unsafeKey.Timestamp.WallTime
		}

		if !isValue || implicitMeta {
			metaKeySize := int64(len(unsafeKey.Key)) + 1
			var metaValSize int64
			if !implicitMeta {
				metaValSize = int64(valueLen)
			}
			totalBytes := metaKeySize + metaValSize
			first = true

			if !implicitMeta {
				v, err := iter.UnsafeValue()
				if err != nil {
					return enginepb.MVCCStats{}, err
				}
				if err := protoutil.Unmarshal(v, &meta); err != nil {
					return ms, errors.Wrap(err, "unable to decode MVCCMetadata")
				}
			}

			if isSys {
				ms.SysBytes += totalBytes
				ms.SysCount++
				if isAbortSpanKey(unsafeKey.Key) {
					ms.AbortSpanBytes += totalBytes
				}
			} else {
				if meta.Deleted {
					// First value is deleted, so it's GC'able; add meta key & value bytes to age stat.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - meta.Timestamp.WallTime/1e9)
				} else if nextRangeTombstone.IsSet() {
					// First value was deleted by a range tombstone, so it accumulates GC age from
					// the range tombstone's timestamp.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - nextRangeTombstone.WallTime/1e9)
				} else {
					ms.LiveBytes += totalBytes
					ms.LiveCount++
				}
				ms.KeyBytes += metaKeySize
				ms.ValBytes += metaValSize
				ms.KeyCount++
				if meta.IsInline() {
					ms.ValCount++
				}
			}
			if !implicitMeta {
				continue
			}
		}

		totalBytes := int64(valueLen) + MVCCVersionTimestampSize
		if isSys {
			ms.SysBytes += totalBytes
		} else {
			if first {
				first = false
				if meta.Deleted {
					// First value is deleted, so it's GC'able; add key & value bytes to age stat.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - meta.Timestamp.WallTime/1e9)
				} else if nextRangeTombstone.IsSet() {
					// First value was deleted by a range tombstone; add key & value bytes to
					// age stat from range tombstone onwards.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - nextRangeTombstone.WallTime/1e9)
				} else {
					ms.LiveBytes += totalBytes
				}
				if meta.Txn != nil {
					ms.IntentBytes += totalBytes
					ms.IntentCount++
					ms.SeparatedIntentCount++
					ms.IntentAge += nowNanos/1e9 - meta.Timestamp.WallTime/1e9
				}
				if meta.KeyBytes != MVCCVersionTimestampSize {
					return ms, errors.Errorf("expected mvcc metadata key bytes to equal %d; got %d "+
						"(meta: %s)", MVCCVersionTimestampSize, meta.KeyBytes, &meta)
				}
				if meta.ValBytes != int64(valueLen) {
					return ms, errors.Errorf("expected mvcc metadata val bytes to equal %d; got %d "+
						"(meta: %s)", valueLen, meta.ValBytes, &meta)
				}
				accrueGCAgeNanos = meta.Timestamp.WallTime
			} else {
				// Overwritten value. Is it a deletion tombstone?
				if mvccValueIsTombstone {
					// The contribution of the tombstone picks up GCByteAge from its own timestamp on.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - unsafeKey.Timestamp.WallTime/1e9)
				} else if nextRangeTombstone.IsSet() && nextRangeTombstone.WallTime < accrueGCAgeNanos {
					// The kv pair was deleted by a range tombstone below the next
					// version, so it accumulates garbage from the range tombstone.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - nextRangeTombstone.WallTime/1e9)
				} else {
					// The kv pair is an overwritten value, so it became non-live when the closest more
					// recent value was written.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - accrueGCAgeNanos/1e9)
				}
				// Update for the next version we may end up looking at.
				accrueGCAgeNanos = unsafeKey.Timestamp.WallTime
			}
			ms.KeyBytes += MVCCVersionTimestampSize
			ms.ValBytes += int64(valueLen)
			ms.ValCount++
		}
	}

	ms.LastUpdateNanos = nowNanos
	return ms, nil
}

// MVCCIsSpanEmpty returns true if there are no MVCC keys whatsoever in the key
// span in the requested time interval. If a time interval is given and any
// inline values are encountered, an error may be returned.
func MVCCIsSpanEmpty(
	ctx context.Context, reader Reader, opts MVCCIsSpanEmptyOptions,
) (isEmpty bool, _ error) {
	// Only use an MVCCIncrementalIterator if time bounds are given, since it will
	// error on any inline values, and the caller may want to respect them instead.
	var iter SimpleMVCCIterator
	if opts.StartTS.IsEmpty() && opts.EndTS.IsEmpty() {
		iter = reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: opts.StartKey,
			UpperBound: opts.EndKey,
		})
	} else {
		iter = NewMVCCIncrementalIterator(reader, MVCCIncrementalIterOptions{
			KeyTypes:     IterKeyTypePointsAndRanges,
			StartKey:     opts.StartKey,
			EndKey:       opts.EndKey,
			StartTime:    opts.StartTS,
			EndTime:      opts.EndTS,
			IntentPolicy: MVCCIncrementalIterIntentPolicyEmit,
		})
	}
	defer iter.Close()
	iter.SeekGE(MVCCKey{Key: opts.StartKey})
	valid, err := iter.Valid()
	if err != nil {
		return false, err
	}
	return !valid, nil
}

// MVCCExportFingerprint exports a fingerprint for point keys in the keyrange
// [StartKey, EndKey) over the interval (StartTS, EndTS]. Each key/timestamp and
// value is hashed using a fnv64 hasher, and combined into a running aggregate
// via a XOR. On completion of the export this aggregate is returned as the
// fingerprint.
//
// Range keys are not fingerprinted but instead written to a pebble SST that is
// returned to the caller. This is because range keys do not have a stable,
// discrete identity and so it is up to the caller to define a deterministic
// fingerprinting scheme across all returned range keys. The returned boolean
// indicates whether any rangekeys were encountered during the export, this bool
// is used by the caller to throw away the empty SST file and avoid unnecessary
// allocations.
func MVCCExportFingerprint(
	ctx context.Context, cs *cluster.Settings, reader Reader, opts MVCCExportOptions, dest io.Writer,
) (roachpb.BulkOpSummary, MVCCKey, uint64, bool, error) {
	ctx, span := tracing.ChildSpan(ctx, "storage.MVCCExportFingerprint")
	defer span.Finish()

	hasher := fnv.New64()
	fingerprintWriter := makeFingerprintWriter(ctx, hasher, cs, dest, opts.FingerprintOptions)
	defer fingerprintWriter.Close()

	summary, resumeKey, err := mvccExportToWriter(ctx, reader, opts, &fingerprintWriter)
	if err != nil {
		return roachpb.BulkOpSummary{}, MVCCKey{}, 0, false, err
	}

	fingerprint, err := fingerprintWriter.Finish()
	if err != nil {
		return roachpb.BulkOpSummary{}, MVCCKey{}, 0, false, err
	}

	hasRangeKeys := fingerprintWriter.sstWriter.DataSize != 0
	return summary, resumeKey, fingerprint, hasRangeKeys, err
}

// MVCCExportToSST exports changes to the keyrange [StartKey, EndKey) over the
// interval (StartTS, EndTS] as a Pebble SST. See mvccExportToWriter for more
// details.
func MVCCExportToSST(
	ctx context.Context, cs *cluster.Settings, reader Reader, opts MVCCExportOptions, dest io.Writer,
) (roachpb.BulkOpSummary, MVCCKey, error) {
	ctx, span := tracing.ChildSpan(ctx, "storage.MVCCExportToSST")
	defer span.Finish()
	sstWriter := MakeBackupSSTWriter(ctx, cs, dest)
	defer sstWriter.Close()

	summary, resumeKey, err := mvccExportToWriter(ctx, reader, opts, &sstWriter)
	if err != nil {
		return roachpb.BulkOpSummary{}, MVCCKey{}, err
	}

	if summary.DataSize == 0 {
		// If no records were added to the sstable, skip
		// completing it and return an empty summary.
		//
		// We still propagate the resumeKey because our
		// iteration may have been halted because of resource
		// limitations before any keys were added to the
		// returned SST.
		return roachpb.BulkOpSummary{}, resumeKey, nil
	}

	return summary, resumeKey, sstWriter.Finish()
}

// ExportWriter is a trimmed down version of the Writer interface. It contains
// only those methods used during ExportRequest command evaluation.
type ExportWriter interface {
	// PutRawMVCCRangeKey writes an MVCC range key with the provided encoded
	// MVCCValue. It will replace any overlapping range keys at the given
	// timestamp (even partial overlap). Only MVCC range tombstones, i.e. an empty
	// value, are currently allowed (other kinds will need additional handling in
	// MVCC APIs and elsewhere, e.g. stats and GC). It can be used to avoid
	// decoding and immediately re-encoding an MVCCValue, but should generally be
	// avoided due to the lack of type safety.
	//
	// It is safe to modify the contents of the arguments after PutRawMVCCRangeKey
	// returns.
	PutRawMVCCRangeKey(MVCCRangeKey, []byte) error
	// PutRawMVCC sets the given key to the encoded MVCCValue. It requires that
	// the timestamp is non-empty (see {PutUnversioned,PutIntent} if the timestamp
	// is empty). It can be used to avoid decoding and immediately re-encoding an
	// MVCCValue, but should generally be avoided due to the lack of type safety.
	//
	// It is safe to modify the contents of the arguments after PutRawMVCC
	// returns.
	PutRawMVCC(key MVCCKey, value []byte) error
	// PutUnversioned sets the given key to the value provided. It is for use
	// with inline metadata (not intents) and other unversioned keys (like
	// Range-ID local keys).
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutUnversioned(key roachpb.Key, value []byte) error
}

// mvccExportToWriter exports changes to the keyrange [StartKey, EndKey) over
// the interval (StartTS, EndTS] to the passed in writer. See MVCCExportOptions
// for options. StartTS may be zero.
//
// This comes in two principal flavors: all revisions or latest revision only.
// In all-revisions mode, exports everything matching the span and time bounds,
// i.e. extracts contiguous blocks of MVCC history. In latest-revision mode,
// extracts just the changes necessary to transform an MVCC snapshot at StartTS
// into one equivalent to the data at EndTS, but without including all
// intermediate revisions not visible at EndTS. The latter mode is used for
// incremental backups that can only be restored to EndTS, the former allows
// restoring to any intermediate timestamp.
//
// Tombstones (both point and MVCC range tombstones) are treated like revisions.
// That is, if all revisions are requested, all tombstones in (StartTS, EndTS]
// and overlapping [StartKey, EndKey) are returned. If only the latest revision
// is requested, only the most recent matching tombstone is returned.
//
// Intents within the time and span bounds will return a WriteIntentError, while
// intents outside are ignored.
//
// Returns an export summary and a resume key that allows resuming the export if
// it reached a limit. Data is written to the writer as it is collected. If an
// error is returned then the writer's contents are undefined. It is the
// responsibility of the caller to Finish() / Close() the passed in writer.
func mvccExportToWriter(
	ctx context.Context, reader Reader, opts MVCCExportOptions, writer ExportWriter,
) (roachpb.BulkOpSummary, MVCCKey, error) {
	// If we're not exporting all revisions then we can mask point keys below any
	// MVCC range tombstones, since we don't care about them.
	var rangeKeyMasking hlc.Timestamp
	if !opts.ExportAllRevisions {
		rangeKeyMasking = opts.EndTS
	}

	elasticCPUHandle := admission.ElasticCPUWorkHandleFromContext(ctx)
	iter := NewMVCCIncrementalIterator(reader, MVCCIncrementalIterOptions{
		KeyTypes:             IterKeyTypePointsAndRanges,
		StartKey:             opts.StartKey.Key,
		EndKey:               opts.EndKey,
		StartTime:            opts.StartTS,
		EndTime:              opts.EndTS,
		RangeKeyMaskingBelow: rangeKeyMasking,
		IntentPolicy:         MVCCIncrementalIterIntentPolicyAggregate,
	})
	defer iter.Close()

	paginated := opts.TargetSize > 0
	hasElasticCPULimiter := elasticCPUHandle != nil
	// trackKeyBoundary is true if we need to know whether the
	// iteration has proceeded to a new key.
	//
	// If opts.ExportAllRevisions is false, then our iteration loop
	// will use NextKey() and thus will always be on a new key.
	//
	// If opts.ExportAllRevisions is true, we only need to track
	// key boundaries if we may return from our iteration before
	// the EndKey. This can happen if the user has requested
	// paginated results, or if we hit a resource limit.
	trackKeyBoundary := opts.ExportAllRevisions && (paginated || hasElasticCPULimiter)
	firstIteration := true
	// skipTombstones controls whether we include tombstones.
	//
	// We want tombstones if we are exporting all reivions or if
	// we have a StartTS. A non-empty StartTS is used by
	// incremental backups and thus needs to see tombstones if
	// that happens to be the latest value.
	skipTombstones := !opts.ExportAllRevisions && opts.StartTS.IsEmpty()

	var rows RowCounter
	// Only used if trackKeyBoundary is true.
	var curKey roachpb.Key
	var resumeKey MVCCKey
	var rangeKeys MVCCRangeKeyStack
	var rangeKeysSize int64

	// maxRangeKeysSizeIfTruncated calculates the worst-case size of the currently
	// buffered rangeKeys if we were to stop iteration after the current resumeKey
	// and flush the pending range keys with the new truncation bound given by
	// resumeKey.
	//
	// For example, if we've buffered the range keys [a-c)@2 and [a-c)@1 with
	// total size 4 bytes, and then hit a byte limit between the point keys bbb@3
	// and bbb@1, we have to truncate the two range keys to [a-bbb\0)@1 and
	// [a-bbb\0)@2. The size of the flushed range keys is now 10 bytes, not 4
	// bytes. Since we're never allowed to exceed MaxSize, we have to check before
	// adding bbb@3 that we have room for both bbb@3 and the pending range keys if
	// they were to be truncated and flushed after it.
	//
	// This could either truncate the range keys at resumeKey, at resumeKey.Next()
	// if StopMidKey is enabled, or at the range keys' actual end key if there
	// doesn't end up being any further point keys covered by it and we go on to
	// flush them as-is at their normal end key. We need to make sure we have
	// enough MaxSize budget to flush them in all of these cases.
	maxRangeKeysSizeIfTruncated := func(resumeKey roachpb.Key) int64 {
		if rangeKeysSize == 0 {
			return 0
		}
		// We could be truncated in the middle of a point key version series, which
		// would require adding on a \0 byte via Key.Next(), so let's assume that.
		maxSize := rangeKeysSize
		endKeySize := len(rangeKeys.Bounds.EndKey)
		if s := maxSize + int64(rangeKeys.Len()*(len(resumeKey)-endKeySize+1)); s > maxSize {
			maxSize = s
		}
		return maxSize
	}

	iter.SeekGE(opts.StartKey)
	for {
		if ok, err := iter.Valid(); err != nil {
			return roachpb.BulkOpSummary{}, MVCCKey{}, err
		} else if !ok {
			break
		} else if iter.NumCollectedIntents() > 0 {
			break
		}

		unsafeKey := iter.UnsafeKey()

		// isNewKey is true when we aren't tracking key
		// boundaries because either we are not exporting all
		// revisions or because we know we won't stop before a
		// key boundary anyway.
		isNewKey := !trackKeyBoundary || !unsafeKey.Key.Equal(curKey)
		if trackKeyBoundary && isNewKey {
			curKey = append(curKey[:0], unsafeKey.Key...)
		}

		if firstIteration {
			// Don't check resources on first iteration to ensure we can make some progress regardless
			// of starvation. Otherwise operations could spin indefinitely.
			firstIteration = false
		} else {
			// Check if we're over our allotted CPU time + on a key boundary (we
			// prefer callers being able to use SSTs directly). Going over limit is
			// accounted for in admission control by penalizing the subsequent
			// request, so doing it slightly is fine.
			stopAllowed := isNewKey || opts.StopMidKey
			if overLimit, _ := elasticCPUHandle.OverLimit(); overLimit && stopAllowed {
				resumeKey = unsafeKey.Clone()
				if isNewKey {
					resumeKey.Timestamp = hlc.Timestamp{}
				}
				log.VInfof(ctx, 2, "paginating ExportRequest: CPU over-limit")
				break
			}
		}

		// When we encounter an MVCC range tombstone stack, we buffer it in
		// rangeKeys until we've moved past it or iteration ends (e.g. due to a
		// limit). If we return a resume key then we need to truncate the final
		// range key stack (and thus the SST) to the resume key, so we can't flush
		// them until we've moved past.
		if iter.RangeKeyChanged() {
			// Flush any pending range tombstones.
			for _, v := range rangeKeys.Versions {
				mvccValue, ok, err := tryDecodeSimpleMVCCValue(v.Value)
				if !ok && err == nil {
					mvccValue, err = decodeExtendedMVCCValue(v.Value)
				}
				if err != nil {
					return roachpb.BulkOpSummary{}, MVCCKey{}, errors.Wrapf(err,
						"decoding mvcc value %s", v.Value)
				}
				// Export only the inner roachpb.Value, not the MVCCValue header.
				rawValue := mvccValue.Value.RawBytes
				if err := writer.PutRawMVCCRangeKey(rangeKeys.AsRangeKey(v), rawValue); err != nil {
					return roachpb.BulkOpSummary{}, MVCCKey{}, err
				}
			}
			rows.BulkOpSummary.DataSize += rangeKeysSize
			rangeKeys.Clear()
			rangeKeysSize = 0

			// Buffer any new range keys.
			hasPoint, hasRange := iter.HasPointAndRange()
			if hasRange && !skipTombstones {
				if opts.ExportAllRevisions {
					iter.RangeKeys().CloneInto(&rangeKeys)
				} else {
					rks := iter.RangeKeys()
					rks.Versions = rks.Versions[:1]
					rks.CloneInto(&rangeKeys)
				}

				for _, v := range rangeKeys.Versions {
					rangeKeysSize += int64(
						len(rangeKeys.Bounds.Key) + len(rangeKeys.Bounds.EndKey) + len(v.Value))
				}

				// Check if the range keys exceed a limit, using similar logic as point
				// keys. We have to check both the size of the range keys as they are (in
				// case we emit them as-is), and the size of the range keys if they were
				// to be truncated at the start key due to a resume span (which could
				// happen if the next point key exceeds the max size).
				//
				// TODO(erikgrinaker): The limit logic here is a bit of a mess, but we're
				// complying with the existing point key logic for now. We should get rid
				// of some of the options and clean this up.
				curSize := rows.BulkOpSummary.DataSize
				reachedTargetSize := opts.TargetSize > 0 && uint64(curSize) >= opts.TargetSize
				newSize := curSize + maxRangeKeysSizeIfTruncated(rangeKeys.Bounds.Key)
				reachedMaxSize := opts.MaxSize > 0 && newSize > int64(opts.MaxSize)
				if curSize > 0 && paginated && (reachedTargetSize || reachedMaxSize) {
					rangeKeys.Clear()
					rangeKeysSize = 0
					resumeKey = unsafeKey.Clone()
					log.VInfof(ctx, 2, "paginating ExportRequest: rangekeys hit size limit: "+
						"reachedTargetSize: %t, reachedMaxSize: %t", reachedTargetSize, reachedMaxSize)
					break
				}
				if reachedMaxSize {
					return roachpb.BulkOpSummary{}, MVCCKey{}, &ExceedMaxSizeError{
						reached: newSize, maxSize: opts.MaxSize}
				}
			}

			// If we're on a bare range key, step forward. We can't use NextKey()
			// because there may be a point key at the range key's start bound.
			if !hasPoint {
				iter.Next()
				continue
			}
		}

		// Process point keys.
		unsafeValue, err := iter.UnsafeValue()
		if err != nil {
			return roachpb.BulkOpSummary{}, MVCCKey{}, err
		}
		skip := false
		if unsafeKey.IsValue() {
			mvccValue, ok, err := tryDecodeSimpleMVCCValue(unsafeValue)
			if !ok && err == nil {
				mvccValue, err = decodeExtendedMVCCValue(unsafeValue)
			}
			if err != nil {
				return roachpb.BulkOpSummary{}, MVCCKey{}, errors.Wrapf(err, "decoding mvcc value %s", unsafeKey)
			}

			// Export only the inner roachpb.Value, not the MVCCValue header.
			unsafeValue = mvccValue.Value.RawBytes

			// Skip tombstone records when start time is zero (non-incremental)
			// and we are not exporting all versions.
			skip = skipTombstones && mvccValue.IsTombstone()
		}

		if !skip {
			if err := rows.Count(unsafeKey.Key); err != nil {
				return roachpb.BulkOpSummary{}, MVCCKey{}, errors.Wrapf(err, "decoding %s", unsafeKey)
			}
			curSize := rows.BulkOpSummary.DataSize
			curSizeWithRangeKeys := curSize + maxRangeKeysSizeIfTruncated(unsafeKey.Key)
			reachedTargetSize := curSizeWithRangeKeys > 0 &&
				uint64(curSizeWithRangeKeys) >= opts.TargetSize
			kvSize := int64(len(unsafeKey.Key) + len(unsafeValue))
			if curSize == 0 && opts.MaxSize > 0 && kvSize > int64(opts.MaxSize) {
				// This single key exceeds the MaxSize. Even if we paginate below, this will still fail.
				return roachpb.BulkOpSummary{}, MVCCKey{}, &ExceedMaxSizeError{reached: kvSize, maxSize: opts.MaxSize}
			}
			newSize := curSize + kvSize
			newSizeWithRangeKeys := curSizeWithRangeKeys + kvSize
			reachedMaxSize := opts.MaxSize > 0 && newSizeWithRangeKeys > int64(opts.MaxSize)

			// When paginating we stop writing in two cases:
			// - target size is reached and we wrote all versions of a key
			// - maximum size reached and we are allowed to stop mid key
			if paginated && (isNewKey && reachedTargetSize || opts.StopMidKey && reachedMaxSize) {
				resumeKey = unsafeKey.Clone()
				if isNewKey || !opts.StopMidKey {
					resumeKey.Timestamp = hlc.Timestamp{}
				}
				log.VInfof(ctx, 2, "paginating ExportRequest: point keys hit size limit: "+
					"reachedTargetSize: %t, reachedMaxSize: %t", reachedTargetSize, reachedMaxSize)
				break
			}
			if reachedMaxSize {
				return roachpb.BulkOpSummary{}, MVCCKey{}, &ExceedMaxSizeError{
					reached: newSizeWithRangeKeys, maxSize: opts.MaxSize}
			}
			if unsafeKey.Timestamp.IsEmpty() {
				// This should never be an intent since the incremental iterator returns
				// an error when encountering intents.
				if err := writer.PutUnversioned(unsafeKey.Key, unsafeValue); err != nil {
					return roachpb.BulkOpSummary{}, MVCCKey{}, errors.Wrapf(err, "adding key %s", unsafeKey)
				}
			} else {
				if err := writer.PutRawMVCC(unsafeKey, unsafeValue); err != nil {
					return roachpb.BulkOpSummary{}, MVCCKey{}, errors.Wrapf(err, "adding key %s", unsafeKey)
				}
			}
			rows.BulkOpSummary.DataSize = newSize
		}

		if opts.ExportAllRevisions {
			iter.Next()
		} else {
			iter.NextKey()
		}
	}

	// First check if we encountered an intent while iterating the data.
	// If we do it means this export can't complete and is aborted. We need to loop over remaining data
	// to collect all matching intents before returning them in an error to the caller.
	if iter.NumCollectedIntents() > 0 {
		for uint64(iter.NumCollectedIntents()) < opts.MaxIntents {
			iter.NextKey()
			// If we encounter other errors during intent collection, we return our original write intent failure.
			// We would find this new error again upon retry.
			ok, _ := iter.Valid()
			if !ok {
				break
			}
		}
		err := iter.TryGetIntentError()
		return roachpb.BulkOpSummary{}, MVCCKey{}, err
	}

	// Flush any pending buffered range keys, truncated to the resume key (if
	// any). If there is a resume timestamp, i.e. when resuming in between two
	// versions, the range keys must cover the resume key too. This will cause the
	// next export's range keys to overlap with this one, e.g.: [a-f) with resume
	// key c@7 will export range keys [a-c\0) first, and then [c-f) when resuming,
	// which overlaps at [c-c\0).
	if !rangeKeys.IsEmpty() {
		// Calculate the new rangeKeysSize due to the new resume bounds.
		if len(resumeKey.Key) > 0 && rangeKeys.Bounds.EndKey.Compare(resumeKey.Key) > 0 {
			oldEndLen := len(rangeKeys.Bounds.EndKey)
			rangeKeys.Bounds.EndKey = resumeKey.Key
			if resumeKey.Timestamp.IsSet() {
				rangeKeys.Bounds.EndKey = rangeKeys.Bounds.EndKey.Next()
			}
			rangeKeysSize += int64(rangeKeys.Len() * (len(rangeKeys.Bounds.EndKey) - oldEndLen))
		}
		for _, v := range rangeKeys.Versions {
			mvccValue, ok, err := tryDecodeSimpleMVCCValue(v.Value)
			if !ok && err == nil {
				mvccValue, err = decodeExtendedMVCCValue(v.Value)
			}
			if err != nil {
				return roachpb.BulkOpSummary{}, MVCCKey{}, errors.Wrapf(err,
					"decoding mvcc value %s", v.Value)
			}
			// Export only the inner roachpb.Value, not the MVCCValue header.
			rawValue := mvccValue.Value.RawBytes
			if err := writer.PutRawMVCCRangeKey(rangeKeys.AsRangeKey(v), rawValue); err != nil {
				return roachpb.BulkOpSummary{}, MVCCKey{}, err
			}
		}
		rows.BulkOpSummary.DataSize += rangeKeysSize
	}

	return rows.BulkOpSummary, resumeKey, nil
}

// MVCCExportOptions contains options for MVCCExportToSST.
type MVCCExportOptions struct {
	// StartKey determines start of the exported interval (inclusive).
	// StartKey.Timestamp is either empty which represent starting from a potential
	// intent and continuing to versions or non-empty, which represents starting
	// from a particular version.
	StartKey MVCCKey
	// EndKey determines the end of exported interval (exclusive).
	EndKey roachpb.Key
	// StartTS and EndTS determine exported time range as (startTS, endTS].
	StartTS, EndTS hlc.Timestamp
	// If ExportAllRevisions is true export every revision of a key for the interval,
	// otherwise only the latest value within the interval is exported.
	ExportAllRevisions bool
	// If TargetSize is positive, it indicates that the export should produce SSTs
	// which are roughly target size. Specifically, it will return an SST such that
	// the last key is responsible for meeting or exceeding the targetSize, unless the
	// iteration has been stopped because of resource limitations.
	TargetSize uint64
	// If MaxSize is positive, it is an absolute maximum on byte size for the
	// returned sst. If it is the case that the versions of the last key will lead
	// to an SST that exceeds maxSize, an error will be returned. This parameter
	// exists to prevent creating SSTs which are too large to be used.
	MaxSize uint64
	// MaxIntents specifies the number of intents to collect and return in a
	// WriteIntentError. The caller will likely resolve the returned intents and
	// retry the call, which would be quadratic, so this significantly reduces the
	// overall number of scans. 0 disables batching and returns the first intent,
	// pass math.MaxUint64 to collect all.
	MaxIntents uint64
	// If StopMidKey is false, once function reaches targetSize it would continue
	// adding all versions until it reaches next key or end of range. If true, it
	// would stop immediately when targetSize is reached and return the next versions
	// timestamp in resumeTs so that subsequent operation can pass it to firstKeyTs.
	//
	// NB: If the result contains MVCC range tombstones, this can cause MVCC range
	// tombstones in two subsequent SSTs to overlap. For example, given the range
	// tombstone [a-f)@5, if we stop between c@4 and c@2 and return a resume key c@2,
	// then the response will contain a truncated MVCC range tombstone [a-c\0)@5
	// which covers the point key at c, but resuming from c@2 will contain the
	// MVCC range tombstone [c-f)@5 which overlaps with the MVCC range tombstone
	// in the previous response in the interval [c-c\0)@5. This overlap will not
	// cause problems with multiplexed iteration using NewSSTIterator(), nor when
	// ingesting the SSTs via `AddSSTable`.
	StopMidKey bool
	// FingerprintOptions controls how fingerprints are generated
	// when using MVCCExportFingerprint.
	FingerprintOptions MVCCExportFingerprintOptions
}

type MVCCExportFingerprintOptions struct {
	// If StripTenantPrefix is true, keys that appear to be
	// tenant-prefixed have the tenant-prefix removed before
	// hashing.
	StripTenantPrefix bool
	// If StripValueChecksum is true, checksums are removed from
	// the value before hashing.
	StripValueChecksum bool
}

// MVCCIsSpanEmptyOptions configures the MVCCIsSpanEmpty function.
type MVCCIsSpanEmptyOptions struct {
	// StartKey determines start of the checked span.
	StartKey roachpb.Key
	// EndKey determines the end of exported interval (exclusive).
	EndKey roachpb.Key
	// StartTS and EndTS determine the scanned time range as (startTS, endTS].
	StartTS, EndTS hlc.Timestamp
}

// PeekRangeKeysLeft peeks for any range keys to the left of the given key.
// It returns the relative position of any range keys to the peek key, along
// with the (unsafe) range key stack:
//
// -1: range key to the left not touching the peek key, or no range key found.
//
//	0: range key to the left ends at the peek key.
//
// +1: range key to the left overlaps with the peek key, extending to the right.
func PeekRangeKeysLeft(iter MVCCIterator, peekKey roachpb.Key) (int, MVCCRangeKeyStack, error) {
	iter.SeekLT(MVCCKey{Key: peekKey})
	if ok, err := iter.Valid(); err != nil {
		return 0, MVCCRangeKeyStack{}, err
	} else if !ok {
		return -1, MVCCRangeKeyStack{}, nil
	} else if _, hasRange := iter.HasPointAndRange(); !hasRange {
		return -1, MVCCRangeKeyStack{}, nil
	}
	rangeKeys := iter.RangeKeys()
	return rangeKeys.Bounds.EndKey.Compare(peekKey), rangeKeys, nil
}

// PeekRangeKeysRight peeks for any range keys to the right of the given key.
// It returns the relative position of any range keys to the peek key, along
// with the (unsafe) range key stack:
//
// -1: range key to the right overlaps with the peek key, existing to the left.
//
//	0: range key to the right starts at the peek key.
//
// +1: range key to the right not touching the peek key, or no range key found.
func PeekRangeKeysRight(iter MVCCIterator, peekKey roachpb.Key) (int, MVCCRangeKeyStack, error) {
	iter.SeekGE(MVCCKey{Key: peekKey})
	if ok, err := iter.Valid(); err != nil {
		return 0, MVCCRangeKeyStack{}, err
	} else if !ok {
		return 1, MVCCRangeKeyStack{}, nil
	} else if _, hasRange := iter.HasPointAndRange(); !hasRange {
		return 1, MVCCRangeKeyStack{}, nil
	}
	rangeKeys := iter.RangeKeys()
	return rangeKeys.Bounds.Key.Compare(peekKey), rangeKeys, nil
}

// ReplacePointTombstonesWithRangeTombstones will replace existing point
// tombstones with equivalent point-sized range tombstones in the given span,
// updating stats as needed. Only the most recent version is considered.
// If end is nil, start.Next() is assumed.
//
// NB: The caller must disable spanset assertions for the reader, since we'll
// peek beyond the given bounds to adjust range key stats. We're not terribly
// concerned about any stats mismatches caused by these missing latches.
func ReplacePointTombstonesWithRangeTombstones(
	ctx context.Context, rw ReadWriter, ms *enginepb.MVCCStats, start, end roachpb.Key,
) error {
	// We don't want to emit DeleteRange rangefeed events, since these may be
	// below the resolved timestamp by now.
	rw = DisableOpLogger(rw)

	if keys.IsLocal(start) {
		return nil
	}
	if len(end) == 0 {
		end = start.Next()
	}

	iter := rw.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
		KeyTypes:   IterKeyTypePointsAndRanges,
		Prefix:     end.Equal(start.Next()),
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()

	var clearedKey MVCCKey
	var rangeKeys MVCCRangeKeyStack
	iter.SeekGE(MVCCKey{Key: start})
	for {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		if iter.RangeKeyChanged() {
			iter.RangeKeys().CloneInto(&rangeKeys)
		}

		// Skip bare range keys.
		hasPoint, hasRange := iter.HasPointAndRange()
		if !hasPoint {
			iter.Next()
			continue
		}

		key := iter.UnsafeKey()

		// Skip intents and inline values, and system table keys which
		// might be watched by rangefeeds.
		if key.Timestamp.IsEmpty() || isWatchedSystemTable(key.Key) {
			iter.NextKey()
			continue
		}

		// Skip non-tombstone values.
		valueLen, isTombstone, err := iter.MVCCValueLenAndIsTombstone()
		if err != nil {
			return err
		}
		if !isTombstone {
			iter.NextKey()
			continue
		}

		// Skip keys below range tombstones. We can't use range key masking because
		// we may need to see older versions of point keys that are below a range
		// tombstone.
		if hasRange && key.Timestamp.LessEq(rangeKeys.Newest()) {
			iter.NextKey()
			continue
		}

		// Clear the point key, and construct a meta record for stats.
		clearedMeta := &enginepb.MVCCMetadata{
			KeyBytes:  MVCCVersionTimestampSize,
			ValBytes:  int64(valueLen),
			Deleted:   true,
			Timestamp: key.Timestamp.ToLegacyTimestamp(),
		}
		clearedKey.Key = append(clearedKey.Key[:0], key.Key...)
		clearedKey.Timestamp = key.Timestamp
		clearedKeySize := int64(EncodedMVCCKeyPrefixLength(clearedKey.Key))
		if err := rw.ClearMVCC(key); err != nil {
			return err
		}

		// Step to the next key to look for an older version, and construct a meta
		// record for stats.
		var restoredMeta *enginepb.MVCCMetadata
		iter.Next()
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if ok {
			if key = iter.UnsafeKey(); key.Key.Equal(clearedKey.Key) {
				valueLen, isTombstone, err = iter.MVCCValueLenAndIsTombstone()
				if err != nil {
					return err
				}
				restoredMeta = &enginepb.MVCCMetadata{
					KeyBytes:  MVCCVersionTimestampSize,
					ValBytes:  int64(valueLen),
					Deleted:   isTombstone,
					Timestamp: key.Timestamp.ToLegacyTimestamp(),
				}
				if _, hasRange := iter.HasPointAndRange(); hasRange {
					if v, ok := iter.RangeKeys().FirstAtOrAbove(key.Timestamp); ok {
						restoredMeta.Deleted = true
						restoredMeta.KeyBytes = 0
						restoredMeta.ValBytes = 0
						restoredMeta.Timestamp = v.Timestamp.ToLegacyTimestamp()
					}
				}
			}
		}

		if ms != nil {
			var restoredKeySize, restoredNanos int64
			if restoredMeta != nil {
				restoredKeySize = clearedKeySize
				restoredNanos = restoredMeta.Timestamp.WallTime
			}
			ms.Add(updateStatsOnClear(clearedKey.Key,
				clearedKeySize, 0, restoredKeySize, 0, clearedMeta, restoredMeta, restoredNanos))
		}

		// Write the range tombstone, with proper stats.
		if err := MVCCDeleteRangeUsingTombstone(ctx, rw, ms,
			clearedKey.Key, clearedKey.Key.Next(), clearedKey.Timestamp, hlc.ClockTimestamp{},
			start.Prevish(roachpb.PrevishKeyLength), end.Next(), false, 0, nil); err != nil {
			return err
		}

		// If we restored a version at this key, step to the next key. Otherwise,
		// we're already on the next key.
		if restoredMeta != nil {
			iter.NextKey()
		}
	}

	return nil
}

// In order to test the correctness of range deletion tombstones, we added a
// testing knob to replace point deletions with range deletion tombstones in
// some tests. Unfortuantely, doing so affects the correctness of rangefeeds.
// The tests in question do not use rangefeeds, but some system functionality
// does use rangefeeds internally. The primary impact is that catch-up scans
// will miss deletes. That makes these issues rare and hard to detect. In order
// to deflake these tests, we avoid rewriting deletes on relevant system
// tables.
func isWatchedSystemTable(key roachpb.Key) bool {
	rem, _, err := keys.DecodeTenantPrefix(key)
	if err != nil { // allow unprefixed keys to pass through
		return false
	}
	_, tableID, _, err := keys.DecodeTableIDIndexID(rem)
	if err != nil { // allow keys which do not correspond to sql tables
		return false
	}
	switch tableID {
	case keys.SettingsTableID, keys.SpanConfigurationsTableID,
		keys.SQLInstancesTableID, keys.DescriptorTableID, keys.ZonesTableID:
		return true
	default:
		return false
	}
}

// MVCCLookupRangeKeyValue reads the value header for a range deletion on
// [key,endKey) at the specified timestamp. The range deletion is allowed to be
// fragmented (with identical value) and is allowed to extend out of
// [key,endKey). An error is returned if a matching range deletion cannot be
// found.
func MVCCLookupRangeKeyValue(
	reader Reader, key, endKey roachpb.Key, ts hlc.Timestamp,
) ([]byte, error) {
	it := reader.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
		LowerBound: key,
		UpperBound: endKey,
		KeyTypes:   IterKeyTypeRangesOnly,
	})
	defer it.Close()

	it.SeekGE(MVCCKey{Key: key})

	// Start by assuming that we've already seen [min, key) and now we're iterating
	// to fill this up to [min, endKey).
	span := roachpb.Span{
		Key:    roachpb.KeyMin,
		EndKey: append([]byte(nil), key...), // copy since we'll mutate this memory
	}
	first := true
	var val []byte
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		rkv, ok := it.RangeKeys().FirstAtOrAbove(ts)
		if !ok || rkv.Timestamp != ts {
			return nil, errors.Errorf(
				"gap [%s,...) in expected range deletion [%s,%s)", span.EndKey, key, endKey)
		}

		unsafeBounds := it.RangeBounds() // only valid until next call to iterator
		if !span.EndKey.Equal(unsafeBounds.Key) {
			return nil, errors.Errorf(
				"gap [%s,%s) in expected range deletion [%s,%s)", span.EndKey, unsafeBounds.Key, key, endKey,
			)
		}

		if first {
			val = append(val, rkv.Value...)
			first = false
		} else if !bytes.Equal(val, rkv.Value) {
			return nil, errors.Errorf(
				"value change at %s in expected range deletion [%s,%s)", unsafeBounds.Key, key, endKey)
		}

		span.EndKey = append(span.EndKey[:0], unsafeBounds.EndKey...)
	}
	if !span.EndKey.Equal(endKey) {
		return nil, errors.Errorf(
			"gap [%s,...) in expected range deletion [%s,%s)", span.EndKey, key, endKey)
	}
	// Made it!
	return val, nil
}
