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
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
)

const (
	// MVCCVersionTimestampSize is the size of the timestamp portion of MVCC
	// version keys (used to update stats).
	MVCCVersionTimestampSize int64 = 12
)

var (
	// MVCCKeyMax is a maximum mvcc-encoded key value which sorts after
	// all other keys.
	MVCCKeyMax = MakeMVCCMetadataKey(roachpb.KeyMax)
	// NilKey is the nil MVCCKey.
	NilKey = MVCCKey{}
)

// MakeValue returns the inline value.
func MakeValue(meta enginepb.MVCCMetadata) roachpb.Value {
	return roachpb.Value{RawBytes: meta.RawBytes}
}

// IsIntentOf returns true if the meta record is an intent of the supplied
// transaction.
func IsIntentOf(meta *enginepb.MVCCMetadata, txn *roachpb.Transaction) bool {
	return meta.Txn != nil && txn != nil && meta.Txn.ID == txn.ID
}

// MVCCKey is a versioned key, distinguished from roachpb.Key with the addition
// of a timestamp.
type MVCCKey struct {
	Key       roachpb.Key
	Timestamp hlc.Timestamp
}

// MakeMVCCMetadataKey creates an MVCCKey from a roachpb.Key.
func MakeMVCCMetadataKey(key roachpb.Key) MVCCKey {
	return MVCCKey{Key: key}
}

// Next returns the next key.
func (k MVCCKey) Next() MVCCKey {
	ts := k.Timestamp.Prev()
	if ts == (hlc.Timestamp{}) {
		return MVCCKey{
			Key: k.Key.Next(),
		}
	}
	return MVCCKey{
		Key:       k.Key,
		Timestamp: ts,
	}
}

// Less compares two keys.
func (k MVCCKey) Less(l MVCCKey) bool {
	if c := k.Key.Compare(l.Key); c != 0 {
		return c < 0
	}
	if !k.IsValue() {
		return l.IsValue()
	} else if !l.IsValue() {
		return false
	}
	return l.Timestamp.Less(k.Timestamp)
}

// Equal returns whether two keys are identical.
func (k MVCCKey) Equal(l MVCCKey) bool {
	return k.Key.Compare(l.Key) == 0 && k.Timestamp == l.Timestamp
}

// IsValue returns true iff the timestamp is non-zero.
func (k MVCCKey) IsValue() bool {
	return k.Timestamp != (hlc.Timestamp{})
}

// EncodedSize returns the size of the MVCCKey when encoded.
func (k MVCCKey) EncodedSize() int {
	n := len(k.Key) + 1
	if k.IsValue() {
		// Note that this isn't quite accurate: timestamps consume between 8-13
		// bytes. Fixing this only adjusts the accounting for timestamps, not the
		// actual on disk storage.
		n += int(MVCCVersionTimestampSize)
	}
	return n
}

// String returns a string-formatted version of the key.
func (k MVCCKey) String() string {
	if !k.IsValue() {
		return k.Key.String()
	}
	return fmt.Sprintf("%s/%s", k.Key, k.Timestamp)
}

// Format implements the fmt.Formatter interface.
func (k MVCCKey) Format(f fmt.State, c rune) {
	fmt.Fprintf(f, "%s/%s", k.Key, k.Timestamp)
}

// Len returns the size of the MVCCKey when encoded. Implements the
// pebble.Encodeable interface.
//
// TODO(itsbilal): Reconcile this with EncodedSize. Would require updating MVCC
// stats tests to reflect the more accurate lengths provided by this function.
func (k MVCCKey) Len() int {
	const (
		timestampSentinelLen      = 1
		walltimeEncodedLen        = 8
		logicalEncodedLen         = 4
		timestampEncodedLengthLen = 1
	)

	n := len(k.Key) + timestampEncodedLengthLen
	if k.Timestamp != (hlc.Timestamp{}) {
		n += timestampSentinelLen + walltimeEncodedLen
		if k.Timestamp.Logical != 0 {
			n += logicalEncodedLen
		}
	}
	return n
}

// MVCCKeyValue contains the raw bytes of the value for a key.
type MVCCKeyValue struct {
	Key   MVCCKey
	Value []byte
}

// isSysLocal returns whether the whether the key is system-local.
func isSysLocal(key roachpb.Key) bool {
	return key.Compare(keys.LocalMax) < 0
}

// updateStatsForInline updates stat counters for an inline value.
// These are simpler as they don't involve intents or multiple
// versions.
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
// values. Unfortunately, we're unable to keep accurate stats on merge
// as the actual details of the merge play out asynchronously during
// compaction. Instead, we undercount by adding only the size of the
// value.Bytes byte slice (an estimated 12 bytes for timestamp,
// included in valSize by caller). These errors are corrected during
// splits and merges.
func updateStatsOnMerge(key roachpb.Key, valSize, nowNanos int64) enginepb.MVCCStats {
	var ms enginepb.MVCCStats
	sys := isSysLocal(key)
	ms.AgeTo(nowNanos)

	_ = clusterversion.VersionContainsEstimatesCounter // see for info on ContainsEstimates migration
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
		prevIsValue := prevValSize > 0

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
	}

	return ms
}

// updateStatsOnResolve updates stat counters with the difference
// between the original and new metadata sizes. The size of the
// resolved value (key & bytes) are subtracted from the intents
// counters if commit=true.
func updateStatsOnResolve(
	key roachpb.Key,
	prevValSize int64,
	origMetaKeySize, origMetaValSize, metaKeySize, metaValSize int64,
	orig, meta *enginepb.MVCCMetadata,
	commit bool,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats

	if isSysLocal(key) {
		// Straightforward: old contribution goes, new contribution comes, and we're done.
		ms.SysBytes += (metaKeySize + metaValSize) - (origMetaValSize + origMetaKeySize)
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
		ms.LiveBytes -= orig.KeyBytes + meta.ValBytes
	}

	// IntentAge is always accrued from the intent's own timestamp on.
	ms.IntentBytes -= orig.KeyBytes + orig.ValBytes
	ms.IntentCount--

	// If there was a previous value (before orig.Timestamp), and it was not a
	// deletion tombstone, then we have to adjust its GCBytesAge contribution
	// which was previously anchored at orig.Timestamp and now has to move to
	// meta.Timestamp. Paralleling very similar code in the method below, this
	// is achieved by making the previous key live between orig.Timestamp and
	// meta.Timestamp. When the two are equal, this will be a zero adjustment,
	// and so in that case the caller may simply pass prevValSize=0 and can
	// skip computing that quantity in the first place.
	_ = updateStatsOnPut
	prevIsValue := prevValSize > 0

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
	}

	return ms
}

// updateStatsOnGC updates stat counters after garbage collection
// by subtracting key and value byte counts, updating key and
// value counts, and updating the GC'able bytes age. If meta is
// not nil, then the value being GC'd is the mvcc metadata and we
// decrement the key count.
//
// nonLiveMS is the timestamp at which the value became non-live.
// For a deletion tombstone this will be its own timestamp (rule two
// in updateStatsOnPut) and for a regular version it will be the closest
// newer version's (rule one).
func updateStatsOnGC(
	key roachpb.Key, keySize, valSize int64, meta *enginepb.MVCCMetadata, nonLiveMS int64,
) enginepb.MVCCStats {
	var ms enginepb.MVCCStats

	if isSysLocal(key) {
		ms.SysBytes -= (keySize + valSize)
		if meta != nil {
			ms.SysCount--
		}
		return ms
	}

	ms.AgeTo(nonLiveMS)
	ms.KeyBytes -= keySize
	ms.ValBytes -= valSize
	if meta != nil {
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
	value, _, mvccGetErr := MVCCGet(ctx, reader, key, timestamp, opts)
	found := value != nil
	// If we found a result, parse it regardless of the error returned by MVCCGet.
	if found && msg != nil {
		// If the unmarshal failed, return its result. Otherwise, pass
		// through the underlying error (which may be a WriteIntentError
		// to be handled specially alongside the returned value).
		if err := value.GetProto(msg); err != nil {
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
	txn *roachpb.Transaction,
	msg protoutil.Message,
) error {
	value := roachpb.Value{}
	if err := value.SetProto(msg); err != nil {
		return err
	}
	value.InitChecksum(key)
	return MVCCPut(ctx, rw, ms, key, timestamp, value, txn)
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
	msg protoutil.Message,
	txn *roachpb.Transaction,
) error {
	value := roachpb.Value{}
	if err := value.SetProto(msg); err != nil {
		return err
	}
	value.InitChecksum(key)
	return MVCCBlindPut(ctx, writer, ms, key, timestamp, value, txn)
}

type getBuffer struct {
	meta             enginepb.MVCCMetadata
	value            roachpb.Value
	allowUnsafeValue bool
	isUnsafeValue    bool
}

var getBufferPool = sync.Pool{
	New: func() interface{} {
		return &getBuffer{}
	},
}

func newGetBuffer() *getBuffer {
	buf := getBufferPool.Get().(*getBuffer)
	buf.allowUnsafeValue = false
	buf.isUnsafeValue = false
	return buf
}

func (b *getBuffer) release() {
	*b = getBuffer{}
	getBufferPool.Put(b)
}

// MVCCGetOptions bundles options for the MVCCGet family of functions.
type MVCCGetOptions struct {
	// See the documentation for MVCCGet for information on these parameters.
	Inconsistent     bool
	Tombstones       bool
	FailOnMoreRecent bool
	Txn              *roachpb.Transaction
}

func (opts *MVCCGetOptions) validate() error {
	if opts.Inconsistent && opts.Txn != nil {
		return errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if opts.Inconsistent && opts.FailOnMoreRecent {
		return errors.Errorf("cannot allow inconsistent reads with fail on more recent option")
	}
	return nil
}

// MVCCGet returns the most recent value for the specified key whose timestamp
// is less than or equal to the supplied timestamp. If no such value exists, nil
// is returned instead.
//
// In tombstones mode, if the most recent value is a deletion tombstone, the
// result will be a non-nil roachpb.Value whose RawBytes field is nil.
// Otherwise, a deletion tombstone results in a nil roachpb.Value.
//
// In inconsistent mode, if an intent is encountered, it will be placed in the
// dedicated return parameter. By contrast, in consistent mode, an intent will
// generate a WriteIntentError with the intent embedded within, and the intent
// result parameter will be nil.
//
// Note that transactional gets must be consistent. Put another way, only
// non-transactional gets may be inconsistent.
//
// If the timestamp is specified as hlc.Timestamp{}, the value is expected to be
// "inlined". See MVCCPut().
//
// When reading in "fail on more recent" mode, a WriteTooOldError will be
// returned if the read observes a version with a timestamp above the read
// timestamp. Similarly, a WriteIntentError will be returned if the read
// observes another transaction's intent, even if it has a timestamp above
// the read timestamp.
func MVCCGet(
	ctx context.Context, reader Reader, key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	iter := reader.NewIterator(IterOptions{Prefix: true})
	defer iter.Close()
	return mvccGet(ctx, iter, key, timestamp, opts)
}

func mvccGet(
	ctx context.Context, iter Iterator, key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (value *roachpb.Value, intent *roachpb.Intent, err error) {
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}
	if timestamp.WallTime < 0 {
		return nil, nil, errors.Errorf("cannot write to %q at timestamp %s", key, timestamp)
	}
	if err := opts.validate(); err != nil {
		return nil, nil, err
	}

	// If the iterator has a specialized implementation, defer to that.
	if mvccIter, ok := iter.(MVCCIterator); ok && mvccIter.MVCCOpsSpecialized() {
		return mvccIter.MVCCGet(key, timestamp, opts)
	}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer pebbleMVCCScannerPool.Put(mvccScanner)

	// MVCCGet is implemented as an MVCCScan where we retrieve a single key. We
	// specify an empty key for the end key which will ensure we don't retrieve a
	// key different than the start key. This is a bit of a hack.
	*mvccScanner = pebbleMVCCScanner{
		parent:           iter,
		start:            key,
		ts:               timestamp,
		maxKeys:          1,
		inconsistent:     opts.Inconsistent,
		tombstones:       opts.Tombstones,
		failOnMoreRecent: opts.FailOnMoreRecent,
		keyBuf:           mvccScanner.keyBuf,
	}

	mvccScanner.init(opts.Txn)
	mvccScanner.get()

	if mvccScanner.err != nil {
		return nil, nil, mvccScanner.err
	}
	intents, err := buildScanIntents(mvccScanner.intentsRepr())
	if err != nil {
		return nil, nil, err
	}
	if !opts.Inconsistent && len(intents) > 0 {
		return nil, nil, &roachpb.WriteIntentError{Intents: intents}
	}

	if len(intents) > 1 {
		return nil, nil, errors.Errorf("expected 0 or 1 intents, got %d", len(intents))
	} else if len(intents) == 1 {
		intent = &intents[0]
	}

	if len(mvccScanner.results.repr) == 0 {
		return nil, intent, nil
	}

	mvccKey, rawValue, _, err := MVCCScanDecodeKeyValue(mvccScanner.results.repr)
	if err != nil {
		return nil, nil, err
	}

	value = &roachpb.Value{
		RawBytes:  rawValue,
		Timestamp: mvccKey.Timestamp,
	}
	return
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
) (*roachpb.Value, *roachpb.Intent, error) {
	return MVCCGet(ctx, reader, key, timestamp, MVCCGetOptions{
		Txn: &roachpb.Transaction{
			TxnMeta:       txnMeta,
			Status:        roachpb.PENDING,
			ReadTimestamp: txnMeta.WriteTimestamp,
			MaxTimestamp:  txnMeta.WriteTimestamp,
		}})
}

// mvccGetMetadata returns or reconstructs the meta key for the given key.
// A prefix scan using the iterator is performed, resulting in one of the
// following successful outcomes:
// 1) iterator finds nothing; returns (false, 0, 0, nil).
// 2) iterator finds an explicit meta key; unmarshals and returns its size.
// 3) iterator finds a value, i.e. the meta key is implicit.
//    In this case, it accounts for the size of the key with the portion
//    of the user key found which is not the MVCC timestamp suffix (since
//    that is the usual contribution of the meta key). The value size returned
//    will be zero.
// The passed in MVCCMetadata must not be nil.
//
// If the supplied iterator is nil, no seek operation is performed. This is
// used by the Blind{Put,ConditionalPut} operations to avoid seeking when the
// metadata is known not to exist.
func mvccGetMetadata(
	iter Iterator, metaKey MVCCKey, meta *enginepb.MVCCMetadata,
) (ok bool, keyBytes, valBytes int64, err error) {
	if iter == nil {
		return false, 0, 0, nil
	}
	iter.SeekGE(metaKey)
	if ok, err := iter.Valid(); !ok {
		return false, 0, 0, err
	}

	unsafeKey := iter.UnsafeKey()
	if !unsafeKey.Key.Equal(metaKey.Key) {
		return false, 0, 0, nil
	}

	if !unsafeKey.IsValue() {
		if err := iter.ValueProto(meta); err != nil {
			return false, 0, 0, err
		}
		return true, int64(unsafeKey.EncodedSize()), int64(len(iter.UnsafeValue())), nil
	}

	meta.Reset()
	// For values, the size of keys is always accounted for as
	// MVCCVersionTimestampSize. The size of the metadata key is
	// accounted for separately.
	meta.KeyBytes = MVCCVersionTimestampSize
	meta.ValBytes = int64(len(iter.UnsafeValue()))
	meta.Deleted = meta.ValBytes == 0
	meta.Timestamp = hlc.LegacyTimestamp(unsafeKey.Timestamp)
	return true, int64(unsafeKey.EncodedSize()) - meta.KeyBytes, 0, nil
}

type valueSafety int

const (
	unsafeValue valueSafety = iota
	safeValue
)

// mvccGetInternal parses the MVCCMetadata from the specified raw key
// value, and reads the versioned value indicated by timestamp, taking
// the transaction txn into account. getValue is a helper function to
// get an earlier version of the value when doing historical reads.
//
// The consistent parameter specifies whether reads should ignore any write
// intents (regardless of the actual status of their transaction) and read the
// most recent non-intent value instead. In the event that an inconsistent read
// does encounter an intent (currently there can only be one), it is returned
// via the roachpb.Intent slice, in addition to the result.
//
// TODO(peter): mvccGetInternal is used by maybeGetValue and
// mvccResolveWriteIntent. Removing those uses is a bit tricky to do in a
// performant way as they are touching optimizations which result in the calls
// usually not hitting RocksDB. This shows up on benchmarks such as
// BenchmarkMVCCConditionalPut_RocksDB/Replace.
func mvccGetInternal(
	_ context.Context,
	iter Iterator,
	metaKey MVCCKey,
	timestamp hlc.Timestamp,
	consistent bool,
	allowedSafety valueSafety,
	txn *roachpb.Transaction,
	buf *getBuffer,
) (*roachpb.Value, *roachpb.Intent, valueSafety, error) {
	if !consistent && txn != nil {
		return nil, nil, safeValue, errors.Errorf(
			"cannot allow inconsistent reads within a transaction")
	}

	if timestamp.WallTime < 0 {
		return nil, nil, safeValue, errors.Errorf("cannot write to %q at timestamp %s", metaKey.Key, timestamp)
	}

	meta := &buf.meta

	// If value is inline, return immediately; txn & timestamp are irrelevant.
	if meta.IsInline() {
		value := &buf.value
		*value = roachpb.Value{RawBytes: meta.RawBytes}
		if err := value.Verify(metaKey.Key); err != nil {
			return nil, nil, safeValue, err
		}
		return value, nil, safeValue, nil
	}
	var ignoredIntent *roachpb.Intent
	metaTimestamp := hlc.Timestamp(meta.Timestamp)
	if !consistent && meta.Txn != nil && metaTimestamp.LessEq(timestamp) {
		// If we're doing inconsistent reads and there's an intent, we
		// ignore the intent by insisting that the timestamp we're reading
		// at is a historical timestamp < the intent timestamp. However, we
		// return the intent separately; the caller may want to resolve it.
		intent := roachpb.MakeIntent(meta.Txn, metaKey.Key)
		ignoredIntent = &intent
		timestamp = metaTimestamp.Prev()
	}

	checkUncertainty := txn != nil && timestamp.Less(txn.MaxTimestamp)
	isIntent := meta.Txn != nil
	ownIntent := IsIntentOf(meta, txn) // false if !isIntent
	if isIntent && !ownIntent {
		// Trying to read the last value, but it's another transaction's intent.
		// The reader will have to act on this if the intent has a low enough
		// timestamp. Intents for other transactions are visible at or below:
		//   max(txn.MaxTimestamp, timestamp)
		maxVisibleTimestamp := timestamp
		if checkUncertainty {
			maxVisibleTimestamp = txn.MaxTimestamp
		}
		if metaTimestamp.LessEq(maxVisibleTimestamp) {
			return nil, nil, safeValue, &roachpb.WriteIntentError{
				Intents: []roachpb.Intent{
					roachpb.MakeIntent(meta.Txn, metaKey.Key),
				},
			}
		}
	}

	seekKey := metaKey
	checkValueTimestamp := false
	if metaTimestamp.LessEq(timestamp) || ownIntent {
		// We are reading the latest value, which is either an intent written
		// by this transaction or not an intent at all (so there's no
		// conflict). Note that when reading the own intent, the timestamp
		// specified is irrelevant; we always want to see the intent (see
		// TestMVCCGetWithPushedTimestamp).
		seekKey.Timestamp = metaTimestamp

		// Check for case where we're reading our own txn's intent
		// but it's got a different epoch. This can happen if the
		// txn was restarted and an earlier iteration wrote the value
		// we're now reading. In this case, we skip the intent.
		if ownIntent && txn.Epoch != meta.Txn.Epoch {
			if txn.Epoch < meta.Txn.Epoch {
				return nil, nil, safeValue, errors.Errorf(
					"failed to read with epoch %d due to a write intent with epoch %d",
					txn.Epoch, meta.Txn.Epoch)
			}
			// Seek past the intent's timestamp and at least as far
			// back as the read timestamp. This is necessary if the
			// intent is at a higher timestamp than we're trying to
			// read at.
			if timestamp.Less(metaTimestamp) {
				seekKey.Timestamp = timestamp
			} else {
				seekKey.Timestamp = metaTimestamp.Prev()
			}
		}
	} else if checkUncertainty {
		// In this branch, the latest timestamp is ahead, and so the read of an
		// "old" value in a transactional context at time (timestamp, MaxTimestamp]
		// occurs, leading to a clock uncertainty error if a version exists in
		// that time interval.
		if metaTimestamp.LessEq(txn.MaxTimestamp) {
			// Second case: Our read timestamp is behind the latest write, but the
			// latest write could possibly have happened before our read in
			// absolute time if the writer had a fast clock.
			// The reader should try again with a later timestamp than the
			// one given below.
			return nil, nil, safeValue, roachpb.NewReadWithinUncertaintyIntervalError(
				timestamp, metaTimestamp, txn)
		}

		// We want to know if anything has been written ahead of timestamp, but
		// before MaxTimestamp.
		seekKey.Timestamp = txn.MaxTimestamp
		checkValueTimestamp = true
	} else {
		// Third case: We're reading a historical value either outside of a
		// transaction, or in the absence of future versions that clock uncertainty
		// would apply to.
		seekKey.Timestamp = timestamp
	}
	if seekKey.Timestamp == (hlc.Timestamp{}) {
		return nil, ignoredIntent, safeValue, nil
	}

	iter.SeekGE(seekKey)
	if ok, err := iter.Valid(); err != nil {
		return nil, nil, safeValue, err
	} else if !ok {
		return nil, ignoredIntent, safeValue, nil
	}

	unsafeKey := iter.UnsafeKey()
	if !unsafeKey.Key.Equal(metaKey.Key) {
		return nil, ignoredIntent, safeValue, nil
	}
	if !unsafeKey.IsValue() {
		return nil, nil, safeValue, errors.Errorf(
			"expected scan to versioned value reading key %s; got %s %s",
			metaKey.Key, unsafeKey, unsafeKey.Timestamp)
	}

	if checkValueTimestamp {
		if timestamp.Less(unsafeKey.Timestamp) {
			// Fourth case: Our read timestamp is sufficiently behind the newest
			// value, but there is another previous write with the same issues as in
			// the second case, so the reader will have to come again with a higher
			// read timestamp.
			return nil, nil, safeValue, roachpb.NewReadWithinUncertaintyIntervalError(
				timestamp, unsafeKey.Timestamp, txn)
		}
		// Fifth case: There's no value in our future up to MaxTimestamp, and those
		// are the only ones that we're not certain about. The correct key has
		// already been read above, so there's nothing left to do.
	}

	value := &buf.value
	if allowedSafety == unsafeValue {
		value.RawBytes = iter.UnsafeValue()
	} else {
		value.RawBytes = iter.Value()
	}
	value.Timestamp = unsafeKey.Timestamp
	if err := value.Verify(metaKey.Key); err != nil {
		return nil, nil, safeValue, err
	}
	return value, ignoredIntent, allowedSafety, nil
}

// putBuffer holds pointer data needed by mvccPutInternal. Bundling
// this data into a single structure reduces memory
// allocations. Managing this temporary buffer using a sync.Pool
// completely eliminates allocation from the put common path.
type putBuffer struct {
	meta    enginepb.MVCCMetadata
	newMeta enginepb.MVCCMetadata
	ts      hlc.LegacyTimestamp
	tmpbuf  []byte
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

func (b *putBuffer) putMeta(
	writer Writer, key MVCCKey, meta *enginepb.MVCCMetadata,
) (keyBytes, valBytes int64, err error) {
	if meta.Txn != nil && !meta.Timestamp.Equal(hlc.LegacyTimestamp(meta.Txn.WriteTimestamp)) {
		// The timestamps are supposed to be in sync. If they weren't, it wouldn't
		// be clear for readers which one to use for what.
		return 0, 0, errors.AssertionFailedf(
			"meta.Timestamp != meta.Txn.WriteTimestamp: %s != %s", meta.Timestamp, meta.Txn.WriteTimestamp)
	}
	bytes, err := b.marshalMeta(meta)
	if err != nil {
		return 0, 0, err
	}
	if err := writer.Put(key, bytes); err != nil {
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
	value roachpb.Value,
	txn *roachpb.Transaction,
) error {
	// If we're not tracking stats for the key and we're writing a non-versioned
	// key we can utilize a blind put to avoid reading any existing value.
	var iter Iterator
	blind := ms == nil && timestamp == (hlc.Timestamp{})
	if !blind {
		iter = rw.NewIterator(IterOptions{Prefix: true})
		defer iter.Close()
	}
	return mvccPutUsingIter(ctx, rw, iter, ms, key, timestamp, value, txn, nil /* valueFn */)
}

// MVCCBlindPut is a fast-path of MVCCPut. See the MVCCPut comments for details
// of the semantics. MVCCBlindPut skips retrieving the existing metadata for
// the key requiring the caller to guarantee no versions for the key currently
// exist in order for stats to be updated properly. If a previous version of
// the key does exist it is up to the caller to properly account for their
// existence in updating the stats.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp paramater is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCBlindPut(
	ctx context.Context,
	writer Writer,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
) error {
	return mvccPutUsingIter(ctx, writer, nil, ms, key, timestamp, value, txn, nil /* valueFn */)
}

// MVCCDelete marks the key deleted so that it will not be returned in
// future get responses.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp paramater is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCDelete(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	txn *roachpb.Transaction,
) error {
	iter := rw.NewIterator(IterOptions{Prefix: true})
	defer iter.Close()

	return mvccPutUsingIter(ctx, rw, iter, ms, key, timestamp, noValue, txn, nil /* valueFn */)
}

var noValue = roachpb.Value{}

// mvccPutUsingIter sets the value for a specified key using the provided
// Iterator. The function takes a value and a valueFn, only one of which
// should be provided. If the valueFn is nil, value's raw bytes will be set
// for the key, else the bytes provided by the valueFn will be used.
func mvccPutUsingIter(
	ctx context.Context,
	writer Writer,
	iter Iterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
	valueFn func(*roachpb.Value) ([]byte, error),
) error {
	var rawBytes []byte
	if valueFn == nil {
		if value.Timestamp != (hlc.Timestamp{}) {
			return errors.Errorf("cannot have timestamp set in value on Put")
		}
		rawBytes = value.RawBytes
	}

	buf := newPutBuffer()

	err := mvccPutInternal(ctx, writer, iter, ms, key, timestamp, rawBytes,
		txn, buf, valueFn)

	// Using defer would be more convenient, but it is measurably slower.
	buf.release()
	return err
}

// maybeGetValue returns either value (if valueFn is nil) or else
// the result of calling valueFn on the data read at readTS.
func maybeGetValue(
	ctx context.Context,
	iter Iterator,
	metaKey MVCCKey,
	value []byte,
	exists bool,
	readTS hlc.Timestamp,
	txn *roachpb.Transaction,
	buf *putBuffer,
	valueFn func(*roachpb.Value) ([]byte, error),
) ([]byte, error) {
	// If a valueFn is specified, read existing value using the iter.
	if valueFn == nil {
		return value, nil
	}
	var exVal *roachpb.Value
	if exists {
		getBuf := newGetBuffer()
		defer getBuf.release()
		getBuf.meta = buf.meta // initialize get metadata from what we've already read
		var err error
		if exVal, _, _, err = mvccGetInternal(
			ctx, iter, metaKey, readTS, true /* consistent */, safeValue, txn, getBuf); err != nil {
			return nil, err
		}
	}
	return valueFn(exVal)
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
	writer Writer,
	iter Iterator,
	meta *enginepb.MVCCMetadata,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value []byte,
	txn *roachpb.Transaction,
	buf *putBuffer,
	valueFn func(*roachpb.Value) ([]byte, error),
) error {
	var found bool
	var writtenValue []byte
	var err error
	metaKey := MakeMVCCMetadataKey(key)
	if txn.Sequence == meta.Txn.Sequence {
		// This is a special case. This is when the intent hasn't made it
		// to the intent history yet. We must now assert the value written
		// in the intent to the value we're trying to write.
		getBuf := newGetBuffer()
		defer getBuf.release()
		getBuf.meta = buf.meta
		var exVal *roachpb.Value
		if exVal, _, _, err = mvccGetInternal(
			ctx, iter, metaKey, timestamp, true /* consistent */, unsafeValue, txn, getBuf); err != nil {
			return err
		}
		writtenValue = exVal.RawBytes
		found = true
	} else {
		// Get the value from the intent history.
		writtenValue, found = meta.GetIntentValue(txn.Sequence)
	}
	if !found {
		return errors.Errorf("transaction %s with sequence %d missing an intent with lower sequence %d",
			txn.ID, meta.Txn.Sequence, txn.Sequence)
	}

	// If the valueFn is specified, we must apply it to the would-be value at the key.
	if valueFn != nil {
		var exVal *roachpb.Value

		// If there's an intent history, use that.
		prevIntent, prevValueWritten := meta.GetPrevIntentSeq(txn.Sequence, txn.IgnoredSeqNums)
		if prevValueWritten {
			// If the previous value was found in the IntentHistory,
			// simply apply the value function to the historic value
			// to get the would-be value.
			prevVal := prevIntent.Value

			exVal = &roachpb.Value{RawBytes: prevVal}
		}
		if exVal == nil {
			// If the previous value at the key wasn't written by this
			// transaction, or it was hidden by a rolled back seqnum, we
			// look at last committed value on the key.
			getBuf := newGetBuffer()
			defer getBuf.release()
			getBuf.meta = buf.meta

			// Since we want the last committed value on the key, we must make
			// an inconsistent read so we ignore our previous intents here.
			exVal, _, _, err = mvccGetInternal(
				ctx, iter, metaKey, timestamp, false /* consistent */, unsafeValue, nil /* txn */, getBuf)
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
	if !bytes.Equal(value, writtenValue) {
		return errors.Errorf("transaction %s with sequence %d has a different value %+v after recomputing from what was written: %+v",
			txn.ID, txn.Sequence, value, writtenValue)
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
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp parameter
// is redundant. Specifically, the intent is written at the txn's
// provisional commit timestamp, txn.Timestamp, unless it is
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
func mvccPutInternal(
	ctx context.Context,
	writer Writer,
	iter Iterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value []byte,
	txn *roachpb.Transaction,
	buf *putBuffer,
	valueFn func(*roachpb.Value) ([]byte, error),
) error {
	if len(key) == 0 {
		return emptyKeyError()
	}

	if timestamp.WallTime < 0 {
		return errors.Errorf("cannot write to %q at timestamp %s", key, timestamp)
	}

	metaKey := MakeMVCCMetadataKey(key)
	ok, origMetaKeySize, origMetaValSize, err := mvccGetMetadata(iter, metaKey, &buf.meta)
	if err != nil {
		return err
	}

	// Verify we're not mixing inline and non-inline values.
	putIsInline := timestamp == (hlc.Timestamp{})
	if ok && putIsInline != buf.meta.IsInline() {
		return errors.Errorf("%q: put is inline=%t, but existing value is inline=%t",
			metaKey, putIsInline, buf.meta.IsInline())
	}
	// Handle inline put. No IntentHistory is required for inline writes
	// as they aren't allowed within transactions.
	if putIsInline {
		if txn != nil {
			return errors.Errorf("%q: inline writes not allowed within transactions", metaKey)
		}
		var metaKeySize, metaValSize int64
		if value, err = maybeGetValue(
			ctx, iter, metaKey, value, ok, timestamp, txn, buf, valueFn); err != nil {
			return err
		}
		if value == nil {
			metaKeySize, metaValSize, err = 0, 0, writer.Clear(metaKey)
		} else {
			buf.meta = enginepb.MVCCMetadata{RawBytes: value}
			metaKeySize, metaValSize, err = buf.putMeta(writer, metaKey, &buf.meta)
		}
		if ms != nil {
			updateStatsForInline(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize)
		}
		if err == nil {
			writer.LogLogicalOp(MVCCWriteValueOpType, MVCCLogicalOpDetails{
				Key:  key,
				Safe: true,
			})
		}
		return err
	}

	// Determine the read and write timestamps for the write. For a
	// non-transactional write, these will be identical. For a transactional
	// write, we read at the transaction's original timestamp (forwarded by any
	// refresh timestamp) but write intents at its provisional commit timestamp.
	// See the comment on the txn.Timestamp field definition for rationale.
	readTimestamp := timestamp
	writeTimestamp := timestamp
	if txn != nil {
		readTimestamp = txn.ReadTimestamp
		if readTimestamp != timestamp {
			return errors.AssertionFailedf(
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
	var prevValSize int64
	if ok {
		// There is existing metadata for this key; ensure our write is permitted.
		meta = &buf.meta
		metaTimestamp := hlc.Timestamp(meta.Timestamp)

		if meta.Txn != nil {
			// There is an uncommitted write intent.
			if txn == nil || meta.Txn.ID != txn.ID {
				// The current Put operation does not come from the same
				// transaction.
				return &roachpb.WriteIntentError{Intents: []roachpb.Intent{
					roachpb.MakeIntent(meta.Txn, key),
				}}
			} else if txn.Epoch < meta.Txn.Epoch {
				return errors.Errorf("put with epoch %d came after put with epoch %d in txn %s",
					txn.Epoch, meta.Txn.Epoch, txn.ID)
			} else if txn.Epoch == meta.Txn.Epoch && txn.Sequence <= meta.Txn.Sequence {
				// The transaction has executed at this sequence before. This is merely a
				// replay of the transactional write. Assert that all is in order and return
				// early.
				return replayTransactionalWrite(ctx, writer, iter, meta, ms, key, readTimestamp, value, txn, buf, valueFn)
			}

			// We're overwriting the intent that was present at this key, before we do
			// that though - we must record the older value in the IntentHistory.

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
			var existingVal *roachpb.Value
			// Set to true when the current provisional value is not ignored due to
			// a txn restart or a savepoint rollback.
			var curProvNotIgnored bool
			if txn.Epoch == meta.Txn.Epoch /* last write inside txn */ {
				if !enginepb.TxnSeqIsIgnored(meta.Txn.Sequence, txn.IgnoredSeqNums) {
					// Seqnum of last write is not ignored. Retrieve the value
					// using a consistent read.
					getBuf := newGetBuffer()
					// Release the buffer after using the existing value.
					defer getBuf.release()
					getBuf.meta = buf.meta // initialize get metadata from what we've already read

					existingVal, _, _, err = mvccGetInternal(
						ctx, iter, metaKey, readTimestamp, true /* consistent */, safeValue, txn, getBuf)
					if err != nil {
						return err
					}
					curProvNotIgnored = true
				} else {
					// Seqnum of last write was ignored. Try retrieving the value from the history.
					prevIntent, prevValueWritten := meta.GetPrevIntentSeq(txn.Sequence, txn.IgnoredSeqNums)
					if prevValueWritten {
						existingVal = &roachpb.Value{RawBytes: prevIntent.Value}
					}
				}
			}
			if existingVal == nil {
				// "last write inside txn && seqnum of all writes are ignored"
				// OR
				// "last write outside txn"
				// => use inconsistent mvccGetInternal to retrieve the last committed value at key.
				getBuf := newGetBuffer()
				defer getBuf.release()
				getBuf.meta = buf.meta

				// Since we want the last committed value on the key, we must make
				// an inconsistent read so we ignore our previous intents here.
				existingVal, _, _, err = mvccGetInternal(
					ctx, iter, metaKey, readTimestamp, false /* consistent */, unsafeValue, nil /* txn */, getBuf)
				if err != nil {
					return err
				}
			}

			// Make sure we process valueFn before clearing any earlier
			// version. For example, a conditional put within same
			// transaction should read previous write.
			if valueFn != nil {
				value, err = valueFn(existingVal)
				if err != nil {
					return err
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
					latestKey := MVCCKey{Key: key, Timestamp: metaTimestamp}
					_, prevUnsafeVal, haveNextVersion, err := unsafeNextVersion(iter, latestKey)
					if err != nil {
						return err
					}
					if haveNextVersion {
						prevValSize = int64(len(prevUnsafeVal))
					}
					iter = nil // prevent accidental use below
				}

				versionKey := metaKey
				versionKey.Timestamp = metaTimestamp
				if err := writer.Clear(versionKey); err != nil {
					return err
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
			// same transaction, we must add the previous value and sequence
			// to the intent history, if that previous value does not belong to an
			// ignored sequence number.
			//
			// If the epoch of the transaction doesn't match the epoch of the
			// intent, or if the existing value is nil due to all past writes being
			// ignored and there are no other committed values, blow away the intent
			// history.
			//
			// Note that the only case where txn.Epoch == meta.Txn.Epoch &&
			// existingVal == nil will be true is when all past writes by this
			// transaction are ignored, and there are no past committed values at this
			// key. In that case, we can also blow up the intent history.
			if txn.Epoch == meta.Txn.Epoch && existingVal != nil {
				// Only add the current provisional value to the intent
				// history if the current sequence number is not ignored. There's no
				// reason to add past committed values or a value already in the intent
				// history back into it.
				if curProvNotIgnored {
					prevIntentValBytes := existingVal.RawBytes
					prevIntentSequence := meta.Txn.Sequence
					buf.newMeta.AddToIntentHistory(prevIntentSequence, prevIntentValBytes)
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
			maybeTooOldErr = roachpb.NewWriteTooOldError(readTimestamp, writeTimestamp)
			// If we're in a transaction, always get the value at the orig
			// timestamp.
			if txn != nil {
				if value, err = maybeGetValue(
					ctx, iter, metaKey, value, ok, readTimestamp, txn, buf, valueFn); err != nil {
					return err
				}
			} else {
				// Outside of a transaction, the read timestamp advances to the
				// the latest value's timestamp + 1 as well. The new timestamp
				// is returned to the caller in maybeTooOldErr. Because we're
				// outside of a transaction, we'll never actually commit this
				// value, but that's a concern of evaluateBatch and not here.
				readTimestamp = writeTimestamp
				if value, err = maybeGetValue(
					ctx, iter, metaKey, value, ok, readTimestamp, txn, buf, valueFn); err != nil {
					return err
				}
			}
		} else {
			if value, err = maybeGetValue(
				ctx, iter, metaKey, value, ok, readTimestamp, txn, buf, valueFn); err != nil {
				return err
			}
		}
	} else {
		// There is no existing value for this key. Even if the new value is
		// nil write a deletion tombstone for the key.
		if valueFn != nil {
			value, err = valueFn(nil)
			if err != nil {
				return err
			}
		}
	}

	{
		var txnMeta *enginepb.TxnMeta
		if txn != nil {
			txnMeta = &txn.TxnMeta
			// If we bumped the WriteTimestamp, we update both the TxnMeta and the
			// MVCCMetadata.Timestamp.
			if txnMeta.WriteTimestamp.Less(writeTimestamp) {
				txnMetaCpy := *txnMeta
				txnMetaCpy.WriteTimestamp.Forward(writeTimestamp)
				txnMeta = &txnMetaCpy
			}
		}
		buf.newMeta.Txn = txnMeta
		buf.newMeta.Timestamp = hlc.LegacyTimestamp(writeTimestamp)
	}
	newMeta := &buf.newMeta

	// Write the mvcc metadata now that we have sizes for the latest
	// versioned value. For values, the size of keys is always accounted
	// for as MVCCVersionTimestampSize. The size of the metadata key is
	// accounted for separately.
	newMeta.KeyBytes = MVCCVersionTimestampSize
	newMeta.ValBytes = int64(len(value))
	newMeta.Deleted = value == nil

	var metaKeySize, metaValSize int64
	if newMeta.Txn != nil {
		metaKeySize, metaValSize, err = buf.putMeta(writer, metaKey, newMeta)
		if err != nil {
			return err
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
	versionKey := metaKey
	versionKey.Timestamp = writeTimestamp
	if err := writer.Put(versionKey, value); err != nil {
		return err
	}

	// Update MVCC stats.
	if ms != nil {
		ms.Add(updateStatsOnPut(key, prevValSize, origMetaKeySize, origMetaValSize,
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

	return maybeTooOldErr
}

// MVCCIncrement fetches the value for key, and assuming the value is
// an "integer" type, increments it by inc and stores the new
// value. The newly incremented value is returned.
//
// An initial value is read from the key using the same operational
// timestamp as we use to write a value.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp paramater is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCIncrement(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	txn *roachpb.Transaction,
	inc int64,
) (int64, error) {
	iter := rw.NewIterator(IterOptions{Prefix: true})
	defer iter.Close()

	var int64Val int64
	var newInt64Val int64
	err := mvccPutUsingIter(ctx, rw, iter, ms, key, timestamp, noValue, txn, func(value *roachpb.Value) ([]byte, error) {
		if value.IsPresent() {
			var err error
			if int64Val, err = value.GetInt(); err != nil {
				return nil, errors.Errorf("key %q does not contain an integer value", key)
			}
		}

		// Check for overflow and underflow.
		if willOverflow(int64Val, inc) {
			// Return the old value, since we've failed to modify it.
			newInt64Val = int64Val
			return nil, &roachpb.IntegerOverflowError{
				Key:            key,
				CurrentValue:   int64Val,
				IncrementValue: inc,
			}
		}
		newInt64Val = int64Val + inc

		newValue := roachpb.Value{}
		newValue.SetInt(newInt64Val)
		newValue.InitChecksum(key)
		return newValue.RawBytes, nil
	})

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
// dictate the timestamp of the operation, and the timestamp paramater is
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
	value roachpb.Value,
	expVal []byte,
	allowIfDoesNotExist CPutMissingBehavior,
	txn *roachpb.Transaction,
) error {
	iter := rw.NewIterator(IterOptions{Prefix: true})
	defer iter.Close()

	return mvccConditionalPutUsingIter(ctx, rw, iter, ms, key, timestamp, value, expVal, allowIfDoesNotExist, txn)
}

// MVCCBlindConditionalPut is a fast-path of MVCCConditionalPut. See the
// MVCCConditionalPut comments for details of the
// semantics. MVCCBlindConditionalPut skips retrieving the existing metadata
// for the key requiring the caller to guarantee no versions for the key
// currently exist.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp paramater is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCBlindConditionalPut(
	ctx context.Context,
	writer Writer,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	expVal []byte,
	allowIfDoesNotExist CPutMissingBehavior,
	txn *roachpb.Transaction,
) error {
	return mvccConditionalPutUsingIter(ctx, writer, nil, ms, key, timestamp, value, expVal, allowIfDoesNotExist, txn)
}

func mvccConditionalPutUsingIter(
	ctx context.Context,
	writer Writer,
	iter Iterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	expBytes []byte,
	allowNoExisting CPutMissingBehavior,
	txn *roachpb.Transaction,
) error {
	return mvccPutUsingIter(
		ctx, writer, iter, ms, key, timestamp, noValue, txn,
		func(existVal *roachpb.Value) ([]byte, error) {
			if expValPresent, existValPresent := len(expBytes) != 0, existVal.IsPresent(); expValPresent && existValPresent {
				if !bytes.Equal(expBytes, existVal.TagAndDataBytes()) {
					return nil, &roachpb.ConditionFailedError{
						ActualValue: existVal.ShallowClone(),
					}
				}
			} else if expValPresent != existValPresent && (existValPresent || !bool(allowNoExisting)) {
				return nil, &roachpb.ConditionFailedError{
					ActualValue: existVal.ShallowClone(),
				}
			}
			return value.RawBytes, nil
		})
}

// MVCCInitPut sets the value for a specified key if the key doesn't exist. It
// returns a ConditionFailedError when the write fails or if the key exists with
// an existing value that is different from the supplied value. If
// failOnTombstones is set to true, tombstones count as mismatched values and
// will cause a ConditionFailedError.
//
// Note that, when writing transactionally, the txn's timestamps
// dictate the timestamp of the operation, and the timestamp paramater is
// confusing and redundant. See the comment on mvccPutInternal for details.
func MVCCInitPut(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	failOnTombstones bool,
	txn *roachpb.Transaction,
) error {
	iter := rw.NewIterator(IterOptions{Prefix: true})
	defer iter.Close()
	return mvccInitPutUsingIter(ctx, rw, iter, ms, key, timestamp, value, failOnTombstones, txn)
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
	value roachpb.Value,
	failOnTombstones bool,
	txn *roachpb.Transaction,
) error {
	return mvccInitPutUsingIter(ctx, rw, nil, ms, key, timestamp, value, failOnTombstones, txn)
}

func mvccInitPutUsingIter(
	ctx context.Context,
	rw ReadWriter,
	iter Iterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	failOnTombstones bool,
	txn *roachpb.Transaction,
) error {
	return mvccPutUsingIter(
		ctx, rw, iter, ms, key, timestamp, noValue, txn,
		func(existVal *roachpb.Value) ([]byte, error) {
			if failOnTombstones && existVal != nil && len(existVal.RawBytes) == 0 {
				// We found a tombstone and failOnTombstones is true: fail.
				return nil, &roachpb.ConditionFailedError{ActualValue: existVal.ShallowClone()}
			}
			if existVal.IsPresent() && !existVal.EqualTagAndData(value) {
				// The existing value does not match the supplied value.
				return nil, &roachpb.ConditionFailedError{
					ActualValue: existVal.ShallowClone(),
				}
			}
			return value.RawBytes, nil
		})
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
	if timestamp != (hlc.Timestamp{}) {
		buf.ts = hlc.LegacyTimestamp(timestamp)
		meta.MergeTimestamp = &buf.ts
	}
	data, err := buf.marshalMeta(meta)
	if err == nil {
		if err = rw.Merge(metaKey, data); err == nil && ms != nil {
			ms.Add(updateStatsOnMerge(
				key, int64(len(rawBytes))+MVCCVersionTimestampSize, timestamp.WallTime))
		}
	}
	buf.release()
	return err
}

// MVCCClearTimeRange clears all MVCC versions within the span [key, endKey)
// which have timestamps in the span (startTime, endTime]. This can have the
// apparent effect of "reverting" the range to startTime if all of the older
// revisions of cleared keys are still available (i.e. have not been GC'ed).
//
// Long runs of keys that all qualify for clearing will be cleared via a single
// clear-range operation. Once maxBatchSize Clear and ClearRange operations are
// hit during iteration, the next matching key is instead returned in the
// resumeSpan. It is possible to exceed maxBatchSize by up to the size of the
// buffer of keys selected for deletion but not yet flushed (as done to detect
// long runs for cleaning in a single ClearRange).
//
// This function does not handle the stats computations to determine the correct
// incremental deltas of clearing these keys (and correctly determining if it
// does or not not change the live and gc keys) so the caller is responsible for
// recomputing stats over the resulting span if needed.
func MVCCClearTimeRange(
	_ context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	key, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	maxBatchSize int64,
) (*roachpb.Span, error) {
	var batchSize int64
	var resume *roachpb.Span

	// When iterating, instead of immediately clearing a matching key we can
	// accumulate it in buf until either a) useRangeClearThreshold is reached and
	// we discard the buffer, instead just keeping track of where the span of keys
	// matching started or b) a non-matching key is seen and we flush the buffer
	// keys one by one as Clears. Once we switch to just tracking where the run
	// started, on seeing a non-matching key we flush the run via one ClearRange.
	// This can be a big win for reverting bulk-ingestion of clustered data as the
	// entire span may likely match and thus could be cleared in one ClearRange
	// instead of hundreds of thousands of individual Clears. This constant hasn't
	// been tuned here at all, but was just borrowed from `clearRangeData` where
	// where this strategy originated.
	const useClearRangeThreshold = 64
	var buf [useClearRangeThreshold]MVCCKey
	var bufSize int
	var clearRangeStart MVCCKey

	clearMatchingKey := func(k MVCCKey) {
		if len(clearRangeStart.Key) == 0 {
			// Currently buffering keys to clear one-by-one.
			if bufSize < useClearRangeThreshold {
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
			if err := rw.ClearRange(clearRangeStart, nonMatch); err != nil {
				return err
			}
			batchSize++
			clearRangeStart = MVCCKey{}
		} else if bufSize > 0 {
			for i := 0; i < bufSize; i++ {
				if err := rw.Clear(buf[i]); err != nil {
					return err
				}
			}
			batchSize += int64(bufSize)
			bufSize = 0
		}
		return nil
	}

	// TODO(dt): time-bound iter could potentially be a big win here -- the
	// expected use-case for this is to run over an entire table's span with a
	// very recent timestamp, rolling back just the writes of some failed IMPORT
	// and that could very likely only have hit some small subset of the table's
	// keyspace. However to get the stats right we need a non-time-bound iter
	// e.g. we need to know if there is an older key under the one we are
	// clearing to know if we're changing the number of live keys. An approach
	// that seems promising might be to use time-bound iter to find candidates
	// for deletion, allowing us to quickly skip over swaths of uninteresting
	// keys, but then use a normal iteration to actually do the delete including
	// updating the live key stats correctly.
	it := rw.NewIterator(IterOptions{LowerBound: key, UpperBound: endKey})
	defer it.Close()

	var clearedMetaKey MVCCKey
	var clearedMeta enginepb.MVCCMetadata
	var restoredMeta enginepb.MVCCMetadata
	for it.SeekGE(MVCCKey{Key: key}); ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return nil, err
		} else if !ok {
			break
		}

		k := it.UnsafeKey()

		if ms != nil && len(clearedMetaKey.Key) > 0 {
			metaKeySize := int64(clearedMetaKey.EncodedSize())
			if bytes.Equal(clearedMetaKey.Key, k.Key) {
				// Since the key matches, our previous clear "restored" this revision of
				// the this key, so update the stats with this as the "restored" key.
				valueSize := int64(len(it.Value()))
				restoredMeta.KeyBytes = MVCCVersionTimestampSize
				restoredMeta.Deleted = valueSize == 0
				restoredMeta.ValBytes = valueSize
				restoredMeta.Timestamp = hlc.LegacyTimestamp(k.Timestamp)

				ms.Add(updateStatsOnClear(
					clearedMetaKey.Key, metaKeySize, 0, metaKeySize, 0, &clearedMeta, &restoredMeta, k.Timestamp.WallTime,
				))
			} else {
				// We cleared a revision of a different key, so nothing was "restored".
				ms.Add(updateStatsOnClear(clearedMetaKey.Key, metaKeySize, 0, 0, 0, &clearedMeta, nil, 0))
			}
			clearedMetaKey.Key = clearedMetaKey.Key[:0]
		}

		if c := k.Key.Compare(endKey); c >= 0 {
			break
		}

		// We need to check for and fail on any intents in our time-range, as we do
		// not want to clear any running transactions. We don't _expect_ to hit this
		// since the RevertRange is only intended for non-live key spans, but there
		// could be an intent leftover.
		var meta enginepb.MVCCMetadata
		if !k.IsValue() {
			if err := it.ValueProto(&meta); err != nil {
				return nil, err
			}
			ts := hlc.Timestamp(meta.Timestamp)
			if meta.Txn != nil && startTime.Less(ts) && ts.LessEq(endTime) {
				err := &roachpb.WriteIntentError{
					Intents: []roachpb.Intent{
						roachpb.MakeIntent(meta.Txn, append([]byte{}, k.Key...)),
					}}
				return nil, err
			}
		}

		if startTime.Less(k.Timestamp) && k.Timestamp.LessEq(endTime) {
			if batchSize >= maxBatchSize {
				resume = &roachpb.Span{Key: append([]byte{}, k.Key...), EndKey: endKey}
				break
			}
			clearMatchingKey(k)
			if ms != nil {
				clearedMetaKey.Key = append(clearedMetaKey.Key[:0], k.Key...)
				clearedMeta.KeyBytes = MVCCVersionTimestampSize
				clearedMeta.ValBytes = int64(len(it.UnsafeValue()))
				clearedMeta.Deleted = clearedMeta.ValBytes == 0
				clearedMeta.Timestamp = hlc.LegacyTimestamp(k.Timestamp)
			}
		} else {
			// This key does not match, so we need to flush our run of matching keys.
			if err := flushClearedKeys(k); err != nil {
				return nil, err
			}
		}
	}

	if ms != nil && len(clearedMetaKey.Key) > 0 {
		// If we cleared on the last iteration, no older revision of that key was
		// "restored", since otherwise we would have iterated over it.
		origMetaKeySize := int64(clearedMetaKey.EncodedSize())
		ms.Add(updateStatsOnClear(clearedMetaKey.Key, origMetaKeySize, 0, 0, 0, &clearedMeta, nil, 0))
	}

	return resume, flushClearedKeys(MVCCKey{Key: endKey})
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
	txn *roachpb.Transaction,
	returnKeys bool,
) ([]roachpb.Key, *roachpb.Span, int64, error) {
	// In order to detect the potential write intent by another concurrent
	// transaction with a newer timestamp, we need to use the max timestamp for
	// scan.
	scanTs := hlc.MaxTimestamp
	// In order for this operation to be idempotent when run transactionally, we
	// need to perform the initial scan at the previous sequence number so that
	// we don't see the result from equal or later sequences.
	var scanTxn *roachpb.Transaction
	if txn != nil {
		prevSeqTxn := txn.Clone()
		prevSeqTxn.Sequence--
		scanTxn = prevSeqTxn
	}
	res, err := MVCCScan(
		ctx, rw, key, endKey, scanTs, MVCCScanOptions{Txn: scanTxn, MaxKeys: max})
	if err != nil {
		return nil, nil, 0, err
	}

	buf := newPutBuffer()
	iter := rw.NewIterator(IterOptions{Prefix: true})

	for i := range res.KVs {
		err = mvccPutInternal(
			ctx, rw, iter, ms, res.KVs[i].Key, timestamp, nil, txn, buf, nil)
		if err != nil {
			break
		}
	}

	iter.Close()
	buf.release()

	var keys []roachpb.Key
	if returnKeys && err == nil && len(res.KVs) > 0 {
		keys = make([]roachpb.Key, len(res.KVs))
		for i := range res.KVs {
			keys[i] = res.KVs[i].Key
		}
	}

	return keys, res.ResumeSpan, res.NumKeys, err
}

func mvccScanToBytes(
	ctx context.Context,
	iter Iterator,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	if len(endKey) == 0 {
		return MVCCScanResult{}, emptyKeyError()
	}
	if err := opts.validate(); err != nil {
		return MVCCScanResult{}, err
	}
	if opts.MaxKeys < 0 || opts.TargetBytes < 0 {
		resumeSpan := &roachpb.Span{Key: key, EndKey: endKey}
		return MVCCScanResult{ResumeSpan: resumeSpan}, nil
	}

	// If the iterator has a specialized implementation, defer to that.
	if mvccIter, ok := iter.(MVCCIterator); ok && mvccIter.MVCCOpsSpecialized() {
		return mvccIter.MVCCScan(key, endKey, timestamp, opts)
	}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer pebbleMVCCScannerPool.Put(mvccScanner)

	*mvccScanner = pebbleMVCCScanner{
		parent:           iter,
		reverse:          opts.Reverse,
		start:            key,
		end:              endKey,
		ts:               timestamp,
		maxKeys:          opts.MaxKeys,
		targetBytes:      opts.TargetBytes,
		inconsistent:     opts.Inconsistent,
		tombstones:       opts.Tombstones,
		failOnMoreRecent: opts.FailOnMoreRecent,
		keyBuf:           mvccScanner.keyBuf,
	}

	mvccScanner.init(opts.Txn)

	var res MVCCScanResult
	var err error
	res.ResumeSpan, err = mvccScanner.scan()

	if err != nil {
		return MVCCScanResult{}, err
	}

	res.KVData = mvccScanner.results.finish()
	res.NumKeys = mvccScanner.results.count
	res.NumBytes = mvccScanner.results.bytes

	res.Intents, err = buildScanIntents(mvccScanner.intentsRepr())
	if err != nil {
		return MVCCScanResult{}, err
	}

	if !opts.Inconsistent && len(res.Intents) > 0 {
		return MVCCScanResult{}, &roachpb.WriteIntentError{Intents: res.Intents}
	}
	return res, nil
}

// mvccScanToKvs converts the raw key/value pairs returned by Iterator.MVCCScan
// into a slice of roachpb.KeyValues.
func mvccScanToKvs(
	ctx context.Context,
	iter Iterator,
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

	reader, err := NewRocksDBBatchReader(data)
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
	Tombstones       bool
	Reverse          bool
	FailOnMoreRecent bool
	Txn              *roachpb.Transaction
	// MaxKeys is the maximum number of kv pairs returned from this operation.
	// The zero value represents an unbounded scan. If the limit stops the scan,
	// a corresponding ResumeSpan is returned. As a special case, the value -1
	// returns no keys in the result (returning the first key via the
	// ResumeSpan).
	MaxKeys int64
	// TargetBytes is a byte threshold to limit the amount of data pulled into
	// memory during a Scan operation. Once the target is satisfied (i.e. met or
	// exceeded) by the emitted emitted KV pairs, iteration stops (with a
	// ResumeSpan as appropriate). In particular, at least one kv pair is
	// returned (when one exists).
	//
	// The number of bytes a particular kv pair accrues depends on internal data
	// structures, but it is guaranteed to exceed that of the bytes stored in
	// the key and value itself.
	//
	// The zero value indicates no limit.
	TargetBytes int64
}

func (opts *MVCCScanOptions) validate() error {
	if opts.Inconsistent && opts.Txn != nil {
		return errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if opts.Inconsistent && opts.FailOnMoreRecent {
		return errors.Errorf("cannot allow inconsistent reads with fail on more recent option")
	}
	return nil
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

	ResumeSpan *roachpb.Span
	Intents    []roachpb.Intent
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
// the transaction's max timestamp, an uncertainty error will be returned.
//
// In tombstones mode, if the most recent value for a key is a deletion
// tombstone, the scan result will contain a roachpb.KeyValue for that key whose
// RawBytes field is nil. Otherwise, the key-value pair will be omitted from the
// result entirely.
//
// When scanning inconsistently, any encountered intents will be placed in the
// dedicated result parameter. By contrast, when scanning consistently, any
// encountered intents will cause the scan to return a WriteIntentError with the
// intents embedded within.
//
// Note that transactional scans must be consistent. Put another way, only
// non-transactional scans may be inconsistent.
//
// When scanning in "fail on more recent" mode, a WriteTooOldError will be
// returned if the scan observes a version with a timestamp above the read
// timestamp. If the scan observes multiple versions with timestamp above
// the read timestamp, the maximum will be returned in the WriteTooOldError.
// Similarly, a WriteIntentError will be returned if the scan observes
// another transaction's intent, even if it has a timestamp above the read
// timestamp.
func MVCCScan(
	ctx context.Context,
	reader Reader,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
) (MVCCScanResult, error) {
	iter := reader.NewIterator(IterOptions{LowerBound: key, UpperBound: endKey})
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
	iter := reader.NewIterator(IterOptions{LowerBound: key, UpperBound: endKey})
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
			TxnMeta:       txnMeta,
			Status:        roachpb.PENDING,
			ReadTimestamp: txnMeta.WriteTimestamp,
			MaxTimestamp:  txnMeta.WriteTimestamp,
		}})
}

// MVCCIterate iterates over the key range [start,end). At each step of the
// iteration, f() is invoked with the current key/value pair. If f returns
// true (done) or an error, the iteration stops and the error is propagated.
// If the reverse is flag set the iterator will be moved in reverse order.
// If the scan options specify an inconsistent scan, all "ignored" intents
// will be returned. In consistent mode, intents are only ever returned as
// part of a WriteIntentError.
func MVCCIterate(
	ctx context.Context,
	reader Reader,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
	f func(roachpb.KeyValue) (bool, error),
) ([]roachpb.Intent, error) {
	iter := reader.NewIterator(IterOptions{LowerBound: key, UpperBound: endKey})
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
			done, err := f(res.KVs[i])
			if err != nil {
				return nil, err
			}
			if done {
				return intents, nil
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

// MVCCResolveWriteIntent either commits or aborts (rolls back) an
// extant write intent for a given txn according to commit parameter.
// ResolveWriteIntent will skip write intents of other txns. It returns
// whether or not an intent was found to resolve.
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
//
// TODO(tschottdorf): encountered a bug in which a Txn committed with
// its original timestamp after laying down intents at higher timestamps.
// Doesn't look like this code here caught that. Shouldn't resolve intents
// when they're not at the timestamp the Txn mandates them to be.
func MVCCResolveWriteIntent(
	ctx context.Context, rw ReadWriter, ms *enginepb.MVCCStats, intent roachpb.LockUpdate,
) (bool, error) {
	iterAndBuf := GetBufUsingIter(rw.NewIterator(IterOptions{Prefix: true}))
	ok, err := MVCCResolveWriteIntentUsingIter(ctx, rw, iterAndBuf, ms, intent)
	// Using defer would be more convenient, but it is measurably slower.
	iterAndBuf.Cleanup()
	return ok, err
}

// MVCCResolveWriteIntentUsingIter is a variant of MVCCResolveWriteIntent that
// uses iterator and buffer passed as parameters (e.g. when used in a loop).
func MVCCResolveWriteIntentUsingIter(
	ctx context.Context,
	rw ReadWriter,
	iterAndBuf IterAndBuf,
	ms *enginepb.MVCCStats,
	intent roachpb.LockUpdate,
) (bool, error) {
	if len(intent.Key) == 0 {
		return false, emptyKeyError()
	}
	if len(intent.EndKey) > 0 {
		return false, errors.Errorf("can't resolve range intent as point intent")
	}
	return mvccResolveWriteIntent(ctx, rw, iterAndBuf.iter, ms, intent, iterAndBuf.buf)
}

// unsafeNextVersion positions the iterator at the successor to latestKey. If this value
// exists and is a version of the same key, returns the UnsafeKey() and UnsafeValue() of that
// key-value pair along with `true`.
func unsafeNextVersion(iter Iterator, latestKey MVCCKey) (MVCCKey, []byte, bool, error) {
	// Compute the next possible mvcc value for this key.
	nextKey := latestKey.Next()
	iter.SeekGE(nextKey)

	if ok, err := iter.Valid(); err != nil || !ok || !iter.UnsafeKey().Key.Equal(latestKey.Key) {
		return MVCCKey{}, nil, false /* never ok */, err
	}
	unsafeKey := iter.UnsafeKey()
	if !unsafeKey.IsValue() {
		return MVCCKey{}, nil, false, errors.Errorf("expected an MVCC value key: %s", unsafeKey)
	}
	return unsafeKey, iter.UnsafeValue(), true, nil
}

// mvccResolveWriteIntent is the core logic for resolving an intent.
// Returns whether an intent was found and resolved, false otherwise.
func mvccResolveWriteIntent(
	ctx context.Context,
	rw ReadWriter,
	iter Iterator,
	ms *enginepb.MVCCStats,
	intent roachpb.LockUpdate,
	buf *putBuffer,
) (bool, error) {
	metaKey := MakeMVCCMetadataKey(intent.Key)
	meta := &buf.meta
	ok, origMetaKeySize, origMetaValSize, err := mvccGetMetadata(iter, metaKey, meta)
	if err != nil {
		return false, err
	}
	if !ok || meta.Txn == nil || intent.Txn.ID != meta.Txn.ID {
		return false, nil
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
	timestampsValid := hlc.Timestamp(meta.Timestamp).LessEq(intent.Txn.WriteTimestamp)
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
	pushed := inProgress && hlc.Timestamp(meta.Timestamp).Less(intent.Txn.WriteTimestamp)
	latestKey := MVCCKey{Key: intent.Key, Timestamp: hlc.Timestamp(meta.Timestamp)}

	// Handle partial txn rollbacks. If the current txn sequence
	// is part of a rolled back (ignored) seqnum range, we're going
	// to erase that MVCC write and reveal the previous value.
	// If _all_ the writes get removed in this way, the intent
	// can be considered empty and marked for removal (removeIntent = true).
	// If only part of the intent history was rolled back, but the intent still
	// remains, the rolledBackVal is set to a non-nil value.
	var rolledBackVal []byte
	if len(intent.IgnoredSeqNums) > 0 {
		var removeIntent bool
		removeIntent, rolledBackVal, err = mvccMaybeRewriteIntentHistory(ctx, rw, intent.IgnoredSeqNums, meta, latestKey)
		if err != nil {
			return false, err
		}

		if removeIntent {
			// This intent should be cleared. Set commit, pushed, and inProgress to
			// false so that this intent isn't updated, gets cleared, and committed
			// values are left untouched.
			commit = false
			pushed = false
			inProgress = false
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
		// The intent might be committing at a higher timestamp, or it might be
		// getting pushed.
		newTimestamp := intent.Txn.WriteTimestamp

		buf.newMeta = *meta
		// Set the timestamp for upcoming write (or at least the stats update).
		buf.newMeta.Timestamp = hlc.LegacyTimestamp(newTimestamp)
		buf.newMeta.Txn.WriteTimestamp = newTimestamp

		// Update or remove the metadata key.
		var metaKeySize, metaValSize int64
		if !commit {
			// Keep existing intent if we're updating it. We update the existing
			// metadata's timestamp instead of using the supplied intent meta to avoid
			// overwriting a newer epoch (see comments above). The pusher's job isn't
			// to do anything to update the intent but to move the timestamp forward,
			// even if it can.
			metaKeySize, metaValSize, err = buf.putMeta(rw, metaKey, &buf.newMeta)
		} else {
			metaKeySize = int64(metaKey.EncodedSize())
			err = rw.Clear(metaKey)
		}
		if err != nil {
			return false, err
		}

		// If we're moving the intent's timestamp, adjust stats and
		// rewrite it.
		var prevValSize int64
		if buf.newMeta.Timestamp != meta.Timestamp {
			oldKey := MVCCKey{Key: intent.Key, Timestamp: hlc.Timestamp(meta.Timestamp)}
			newKey := MVCCKey{Key: intent.Key, Timestamp: newTimestamp}

			// Rewrite the versioned value at the new timestamp.
			iter.SeekGE(oldKey)
			if valid, err := iter.Valid(); err != nil {
				return false, err
			} else if !valid || !iter.UnsafeKey().Equal(oldKey) {
				return false, errors.Errorf("existing intent value missing: %s", oldKey)
			}
			value := iter.UnsafeValue()
			// Special case: If mvccMaybeRewriteIntentHistory rolled back to a value
			// in the intent history and wrote that at oldKey, iter would not be able
			// to "see" the value since it was created before that value was written
			// to the engine. In this case, reuse the value returned by
			// mvccMaybeRewriteIntentHistory.
			if rolledBackVal != nil {
				value = rolledBackVal
			}
			if err = rw.Put(newKey, value); err != nil {
				return false, err
			}
			if err = rw.Clear(oldKey); err != nil {
				return false, err
			}

			// If there is a value under the intent as it moves timestamps, then
			// that value may need an adjustment of its GCBytesAge. This is
			// because it became non-live at orig.Timestamp originally, and now
			// only becomes non-live at newMeta.Timestamp. For that reason, we
			// have to read that version's size.
			//
			// Look for the first real versioned key, i.e. the key just below
			// the (old) meta's timestamp.
			iter.Next()
			if valid, err := iter.Valid(); err != nil {
				return false, err
			} else if valid && iter.UnsafeKey().Key.Equal(oldKey.Key) {
				prevValSize = int64(len(iter.UnsafeValue()))
			}
		}

		// Update stat counters related to resolving the intent.
		if ms != nil {
			ms.Add(updateStatsOnResolve(intent.Key, prevValSize, origMetaKeySize, origMetaValSize,
				metaKeySize, metaValSize, meta, &buf.newMeta, commit))
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

	// First clear the intent value.
	if err := rw.Clear(latestKey); err != nil {
		return false, err
	}

	// Log the logical MVCC operation.
	rw.LogLogicalOp(MVCCAbortIntentOpType, MVCCLogicalOpDetails{
		Txn: intent.Txn,
		Key: intent.Key,
	})

	unsafeNextKey, unsafeNextValue, ok, err := unsafeNextVersion(iter, latestKey)
	if err != nil {
		return false, err
	}
	iter = nil // prevent accidental use below

	if !ok {
		// If there is no other version, we should just clean up the key entirely.
		if err = rw.Clear(metaKey); err != nil {
			return false, err
		}
		// Clear stat counters attributable to the intent we're aborting.
		if ms != nil {
			ms.Add(updateStatsOnClear(intent.Key, origMetaKeySize, origMetaValSize, 0, 0, meta, nil, 0))
		}
		return true, nil
	}

	// Get the bytes for the next version so we have size for stat counts.
	valueSize := int64(len(unsafeNextValue))
	// Update the keyMetadata with the next version.
	buf.newMeta = enginepb.MVCCMetadata{
		Deleted:  valueSize == 0,
		KeyBytes: MVCCVersionTimestampSize,
		ValBytes: valueSize,
	}
	if err := rw.Clear(metaKey); err != nil {
		return false, err
	}
	metaKeySize := int64(metaKey.EncodedSize())
	metaValSize := int64(0)

	// Update stat counters with older version.
	if ms != nil {
		ms.Add(updateStatsOnClear(intent.Key, origMetaKeySize, origMetaValSize,
			metaKeySize, metaValSize, meta, &buf.newMeta, unsafeNextKey.Timestamp.WallTime))
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
) (remove bool, updatedVal []byte, err error) {
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
	restoredVal := meta.IntentHistory[i].Value
	meta.Txn.Sequence = meta.IntentHistory[i].Sequence
	meta.IntentHistory = meta.IntentHistory[:i]
	meta.Deleted = len(restoredVal) == 0
	meta.ValBytes = int64(len(restoredVal))
	// And also overwrite whatever was there in storage.
	err = engine.Put(latestKey, restoredVal)

	return false, restoredVal, err
}

// IterAndBuf used to pass iterators and buffers between MVCC* calls, allowing
// reuse without the callers needing to know the particulars.
type IterAndBuf struct {
	buf  *putBuffer
	iter Iterator
}

// GetIterAndBuf returns an IterAndBuf for passing into various MVCC* methods.
func GetIterAndBuf(reader Reader, opts IterOptions) IterAndBuf {
	return GetBufUsingIter(reader.NewIterator(opts))
}

// GetBufUsingIter returns an IterAndBuf using the supplied iterator.
func GetBufUsingIter(iter Iterator) IterAndBuf {
	return IterAndBuf{
		buf:  newPutBuffer(),
		iter: iter,
	}
}

// Cleanup must be called to release the resources when done.
func (b IterAndBuf) Cleanup() {
	b.buf.release()
	b.iter.Close()
}

// MVCCResolveWriteIntentRange commits or aborts (rolls back) the
// range of write intents specified by start and end keys for a given
// txn. ResolveWriteIntentRange will skip write intents of other
// txns. Returns the number of intents resolved and a resume span if
// the max keys limit was exceeded.
func MVCCResolveWriteIntentRange(
	ctx context.Context, rw ReadWriter, ms *enginepb.MVCCStats, intent roachpb.LockUpdate, max int64,
) (int64, *roachpb.Span, error) {
	iterAndBuf := GetIterAndBuf(rw, IterOptions{UpperBound: intent.EndKey})
	defer iterAndBuf.Cleanup()
	return MVCCResolveWriteIntentRangeUsingIter(ctx, rw, iterAndBuf, ms, intent, max)
}

// MVCCResolveWriteIntentRangeUsingIter commits or aborts (rolls back)
// the range of write intents specified by start and end keys for a
// given txn. ResolveWriteIntentRange will skip write intents of other
// txns.
//
// Returns the number of intents resolved and a resume span if the max
// keys limit was exceeded. A max of zero means unbounded. A max of -1
// means resolve nothing and return the entire intent span as the resume
// span.
func MVCCResolveWriteIntentRangeUsingIter(
	ctx context.Context,
	rw ReadWriter,
	iterAndBuf IterAndBuf,
	ms *enginepb.MVCCStats,
	intent roachpb.LockUpdate,
	max int64,
) (int64, *roachpb.Span, error) {
	if max < 0 {
		resumeSpan := intent.Span // don't inline or `intent` would escape to heap
		return 0, &resumeSpan, nil
	}

	encKey := MakeMVCCMetadataKey(intent.Key)
	encEndKey := MakeMVCCMetadataKey(intent.EndKey)
	nextKey := encKey

	var keyBuf []byte
	num := int64(0)
	intent.EndKey = nil

	for {
		if max > 0 && num == max {
			return num, &roachpb.Span{Key: nextKey.Key, EndKey: encEndKey.Key}, nil
		}

		iterAndBuf.iter.SeekGE(nextKey)
		if ok, err := iterAndBuf.iter.Valid(); err != nil {
			return 0, nil, err
		} else if !ok || !iterAndBuf.iter.UnsafeKey().Less(encEndKey) {
			// No more keys exists in the given range.
			break
		}

		// Manually copy the underlying bytes of the unsafe key. This construction
		// reuses keyBuf across iterations.
		key := iterAndBuf.iter.UnsafeKey()
		keyBuf = append(keyBuf[:0], key.Key...)
		key.Key = keyBuf

		var err error
		var ok bool
		if !key.IsValue() {
			intent.Key = key.Key
			ok, err = mvccResolveWriteIntent(ctx, rw, iterAndBuf.iter, ms, intent, iterAndBuf.buf)
		}
		if err != nil {
			log.Warningf(ctx, "failed to resolve intent for key %q: %+v", key.Key, err)
		} else if ok {
			num++
		}

		// nextKey is already a metadata key...
		nextKey.Key = key.Key.Next()
		if !nextKey.Less(encEndKey) {
			// ... but we don't want to Seek to a key outside of the range as we validate
			// those span accesses (see TestSpanSetMVCCResolveWriteIntentRangeUsingIter).
			break
		}
	}

	return num, nil, nil
}

// MVCCGarbageCollect creates an iterator on the ReadWriter. In parallel
// it iterates through the keys listed for garbage collection by the
// keys slice. The iterator is seeked in turn to each listed
// key, clearing all values with timestamps <= to expiration. The
// timestamp parameter is used to compute the intent age on GC.
func MVCCGarbageCollect(
	ctx context.Context,
	rw ReadWriter,
	ms *enginepb.MVCCStats,
	keys []roachpb.GCRequest_GCKey,
	timestamp hlc.Timestamp,
) error {
	// We're allowed to use a prefix iterator because we always Seek() the
	// iterator when handling a new user key.
	iter := rw.NewIterator(IterOptions{Prefix: true})
	defer iter.Close()

	var count int64
	defer func(begin time.Time) {
		log.Eventf(ctx, "done with GC evaluation for %d keys at %.2f keys/sec. Deleted %d entries",
			len(keys), float64(len(keys))*1e9/float64(timeutil.Since(begin)), count)
	}(timeutil.Now())

	// Iterate through specified GC keys.
	meta := &enginepb.MVCCMetadata{}
	for _, gcKey := range keys {
		encKey := MakeMVCCMetadataKey(gcKey.Key)
		ok, metaKeySize, metaValSize, err := mvccGetMetadata(iter, encKey, meta)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		inlinedValue := meta.IsInline()
		implicitMeta := iter.UnsafeKey().IsValue()
		// First, check whether all values of the key are being deleted.
		//
		// Note that we naively can't terminate GC'ing keys loop early if we
		// enter this branch, as it will update the stats under the provision
		// that the (implicit or explicit) meta key (and thus all versions) are
		// being removed. We had this faulty functionality at some point; it
		// should no longer be necessary since the higher levels already make
		// sure each individual GCRequest does bounded work.
		if hlc.Timestamp(meta.Timestamp).LessEq(gcKey.Timestamp) {
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
					ms.Add(updateStatsOnGC(gcKey.Key, metaKeySize, metaValSize, meta, meta.Timestamp.WallTime))
				}
			}
			if !implicitMeta {
				if err := rw.Clear(iter.UnsafeKey()); err != nil {
					return err
				}
				count++
			}
		}

		if !implicitMeta {
			// The iter is pointing at an MVCCMetadata, advance to the next entry.
			iter.Next()
		}

		// TODO(tschottdorf): Can't we just Seek() to a key with timestamp
		// gcKey.Timestamp to avoid potentially cycling through a large prefix
		// of versions we can't GC? The batching mechanism in the GC queue sends
		// requests susceptible to that happening when there are lots of versions.
		// A minor complication there will be that we need to know the first non-
		// deletable value's timestamp (for prevNanos).

		// Now, iterate through all values, GC'ing ones which have expired.
		// For GCBytesAge, this requires keeping track of the previous key's
		// timestamp (prevNanos). See ComputeStatsGo for a more easily digested
		// and better commented version of this logic.

		prevNanos := timestamp.WallTime
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
			if unsafeIterKey.Timestamp.LessEq(gcKey.Timestamp) {
				if ms != nil {
					// FIXME: use prevNanos instead of unsafeIterKey.Timestamp, except
					// when it's a deletion.
					valSize := int64(len(iter.UnsafeValue()))

					// A non-deletion becomes non-live when its newer neighbor shows up.
					// A deletion tombstone becomes non-live right when it is created.
					fromNS := prevNanos
					if valSize == 0 {
						fromNS = unsafeIterKey.Timestamp.WallTime
					}

					ms.Add(updateStatsOnGC(gcKey.Key, MVCCVersionTimestampSize,
						valSize, nil, fromNS))
				}
				count++
				if err := rw.Clear(unsafeIterKey); err != nil {
					return err
				}
			}
			prevNanos = unsafeIterKey.Timestamp.WallTime
		}
	}

	return nil
}

// MVCCFindSplitKey finds a key from the given span such that the left side of
// the split is roughly targetSize bytes. The returned key will never be chosen
// from the key ranges listed in keys.NoSplitSpans.
func MVCCFindSplitKey(
	_ context.Context, reader Reader, key, endKey roachpb.RKey, targetSize int64,
) (roachpb.Key, error) {
	if key.Less(roachpb.RKey(keys.LocalMax)) {
		key = roachpb.RKey(keys.LocalMax)
	}

	it := reader.NewIterator(IterOptions{UpperBound: endKey.AsRawKey()})
	defer it.Close()

	// We want to avoid splitting at the first key in the range because that
	// could result in an empty left-hand range. To prevent this, we scan for
	// the first key in the range and consider the key that sorts directly after
	// this as the minimum split key.
	//
	// In addition, we must never return a split key that falls within a table
	// row. (Rows in tables with multiple column families are comprised of
	// multiple keys, one key per column family.) However, we do allow a split
	// key that falls between a row and its interleaved rows.
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
	// key in the the range. A previous version of this code attempted to derive
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
			// Allow a split key before other rows in the same table or before any
			// rows in interleaved tables.
			minSplitKey = encoding.EncodeInterleavedSentinel(firstRowKey)
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

// ComputeStatsGo scans the underlying engine from start to end keys and
// computes stats counters based on the values. This method is used after a
// range is split to recompute stats for each subrange. The start key is always
// adjusted to avoid counting local keys in the event stats are being recomputed
// for the first range (i.e. the one with start key == KeyMin). The nowNanos arg
// specifies the wall time in nanoseconds since the epoch and is used to compute
// the total age of all intents.
//
// Most codepaths will be computing stats on a RocksDB iterator, which is
// implemented in c++, so iter.ComputeStats will save several cgo calls per kv
// processed. (Plus, on equal footing, the c++ implementation is slightly
// faster.) ComputeStatsGo is here for codepaths that have a pure-go
// implementation of SimpleIterator.
//
// When optional callbacks are specified, they are invoked for each physical
// key-value pair (i.e. not for implicit meta records), and iteration is aborted
// on the first error returned from any of them.
//
// Callbacks must copy any data they intend to hold on to.
//
// This implementation must match engine/db.cc:MVCCComputeStatsInternal.
func ComputeStatsGo(
	iter SimpleIterator,
	start, end roachpb.Key,
	nowNanos int64,
	callbacks ...func(MVCCKey, []byte) error,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats

	var meta enginepb.MVCCMetadata
	var prevKey []byte
	first := false

	// Values start accruing GCBytesAge at the timestamp at which they
	// are shadowed (i.e. overwritten) whereas deletion tombstones
	// use their own timestamp. We're iterating through versions in
	// reverse chronological order and use this variable to keep track
	// of the point in time at which the current key begins to age.
	var accrueGCAgeNanos int64
	mvccEndKey := MakeMVCCMetadataKey(end)

	iter.SeekGE(MakeMVCCMetadataKey(start))
	for ; ; iter.Next() {
		ok, err := iter.Valid()
		if err != nil {
			return ms, err
		}
		if !ok || !iter.UnsafeKey().Less(mvccEndKey) {
			break
		}

		unsafeKey := iter.UnsafeKey()
		unsafeValue := iter.UnsafeValue()

		for _, f := range callbacks {
			if err := f(unsafeKey, unsafeValue); err != nil {
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

		if implicitMeta {
			// No MVCCMetadata entry for this series of keys.
			meta.Reset()
			meta.KeyBytes = MVCCVersionTimestampSize
			meta.ValBytes = int64(len(unsafeValue))
			meta.Deleted = len(unsafeValue) == 0
			meta.Timestamp.WallTime = unsafeKey.Timestamp.WallTime
		}

		if !isValue || implicitMeta {
			metaKeySize := int64(len(unsafeKey.Key)) + 1
			var metaValSize int64
			if !implicitMeta {
				metaValSize = int64(len(unsafeValue))
			}
			totalBytes := metaKeySize + metaValSize
			first = true

			if !implicitMeta {
				if err := protoutil.Unmarshal(unsafeValue, &meta); err != nil {
					return ms, errors.Wrap(err, "unable to decode MVCCMetadata")
				}
			}

			if isSys {
				ms.SysBytes += totalBytes
				ms.SysCount++
			} else {
				if !meta.Deleted {
					ms.LiveBytes += totalBytes
					ms.LiveCount++
				} else {
					// First value is deleted, so it's GC'able; add meta key & value bytes to age stat.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - meta.Timestamp.WallTime/1e9)
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

		totalBytes := int64(len(unsafeValue)) + MVCCVersionTimestampSize
		if isSys {
			ms.SysBytes += totalBytes
		} else {
			if first {
				first = false
				if !meta.Deleted {
					ms.LiveBytes += totalBytes
				} else {
					// First value is deleted, so it's GC'able; add key & value bytes to age stat.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - meta.Timestamp.WallTime/1e9)
				}
				if meta.Txn != nil {
					ms.IntentBytes += totalBytes
					ms.IntentCount++
					ms.IntentAge += nowNanos/1e9 - meta.Timestamp.WallTime/1e9
				}
				if meta.KeyBytes != MVCCVersionTimestampSize {
					return ms, errors.Errorf("expected mvcc metadata key bytes to equal %d; got %d "+
						"(meta: %s)", MVCCVersionTimestampSize, meta.KeyBytes, &meta)
				}
				if meta.ValBytes != int64(len(unsafeValue)) {
					return ms, errors.Errorf("expected mvcc metadata val bytes to equal %d; got %d "+
						"(meta: %s)", len(unsafeValue), meta.ValBytes, &meta)
				}
				accrueGCAgeNanos = meta.Timestamp.WallTime
			} else {
				// Overwritten value. Is it a deletion tombstone?
				isTombstone := len(unsafeValue) == 0
				if isTombstone {
					// The contribution of the tombstone picks up GCByteAge from its own timestamp on.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - unsafeKey.Timestamp.WallTime/1e9)
				} else {
					// The kv pair is an overwritten value, so it became non-live when the closest more
					// recent value was written.
					ms.GCBytesAge += totalBytes * (nowNanos/1e9 - accrueGCAgeNanos/1e9)
				}
				// Update for the next version we may end up looking at.
				accrueGCAgeNanos = unsafeKey.Timestamp.WallTime
			}
			ms.KeyBytes += MVCCVersionTimestampSize
			ms.ValBytes += int64(len(unsafeValue))
			ms.ValCount++
		}
	}

	ms.LastUpdateNanos = nowNanos
	return ms, nil
}

// computeCapacity returns capacity details for the engine's available storage,
// by querying the underlying file system.
func computeCapacity(path string, maxSizeBytes int64) (roachpb.StoreCapacity, error) {
	fileSystemUsage := gosigar.FileSystemUsage{}
	dir := path
	if dir == "" {
		// This is an in-memory instance. Pretend we're empty since we
		// don't know better and only use this for testing. Using any
		// part of the actual file system here can throw off allocator
		// rebalancing in a hard-to-trace manner. See #7050.
		return roachpb.StoreCapacity{
			Capacity:  maxSizeBytes,
			Available: maxSizeBytes,
		}, nil
	}
	if err := fileSystemUsage.Get(dir); err != nil {
		return roachpb.StoreCapacity{}, err
	}

	if fileSystemUsage.Total > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Total), humanizeutil.IBytes(math.MaxInt64))
	}
	if fileSystemUsage.Avail > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Avail), humanizeutil.IBytes(math.MaxInt64))
	}
	fsuTotal := int64(fileSystemUsage.Total)
	fsuAvail := int64(fileSystemUsage.Avail)

	// Find the total size of all the files in the r.dir and all its
	// subdirectories.
	var totalUsedBytes int64
	if errOuter := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// This can happen if rocksdb removes files out from under us - just keep
			// going to get the best estimate we can.
			if os.IsNotExist(err) {
				return nil
			}
			// Special-case: if the store-dir is configured using the root of some fs,
			// e.g. "/mnt/db", we might have special fs-created files like lost+found
			// that we can't read, so just ignore them rather than crashing.
			if os.IsPermission(err) && filepath.Base(path) == "lost+found" {
				return nil
			}
			return err
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
	if maxSizeBytes == 0 || maxSizeBytes >= fsuTotal || path == "" {
		return roachpb.StoreCapacity{
			Capacity:  fsuTotal,
			Available: fsuAvail,
			Used:      totalUsedBytes,
		}, nil
	}

	available := maxSizeBytes - totalUsedBytes
	if available > fsuAvail {
		available = fsuAvail
	}
	if available < 0 {
		available = 0
	}

	return roachpb.StoreCapacity{
		Capacity:  maxSizeBytes,
		Available: available,
		Used:      totalUsedBytes,
	}, nil
}

// checkForKeyCollisionsGo iterates through both existingIter and an SST
// iterator on the provided data in lockstep and errors out at the first key
// collision, where a collision refers to any two MVCC keys with the
// same user key, and with a different timestamp or value.
//
// An exception is made when the latest version of the colliding key is a
// tombstone from an MVCC delete in the existing data. If the timestamp of the
// SST key is greater than or equal to the timestamp of the tombstone, then it
// is not considered a collision and we continue iteration from the next key in
// the existing data.
func checkForKeyCollisionsGo(
	existingIter Iterator, sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	var skippedKVStats enginepb.MVCCStats
	sstIter, err := NewMemSSTIterator(sstData, false)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}

	defer sstIter.Close()
	sstIter.SeekGE(MakeMVCCMetadataKey(start))
	if ok, err := sstIter.Valid(); err != nil || !ok {
		return enginepb.MVCCStats{}, errors.Wrap(err, "checking for key collisions")
	}

	ok, extErr := existingIter.Valid()
	ok2, sstErr := sstIter.Valid()
	for extErr == nil && sstErr == nil && ok && ok2 {
		existingKey := existingIter.UnsafeKey()
		existingValue := existingIter.UnsafeValue()
		sstKey := sstIter.UnsafeKey()
		sstValue := sstIter.UnsafeValue()

		if !existingKey.IsValue() {
			var mvccMeta enginepb.MVCCMetadata
			err := existingIter.ValueProto(&mvccMeta)
			if err != nil {
				return enginepb.MVCCStats{}, err
			}

			// Check for an inline value, as these are only used in non-user data.
			// This method is currently used by AddSSTable when performing an IMPORT
			// INTO. We do not expect to encounter any inline values, and thus we
			// report an error.
			if len(mvccMeta.RawBytes) > 0 {
				return enginepb.MVCCStats{}, errors.Errorf("inline values are unsupported when checking for key collisions")
			} else if mvccMeta.Txn != nil {
				// Check for a write intent.
				//
				// TODO(adityamaru): Currently, we raise a WriteIntentError on
				// encountering all intents. This is because, we do not expect to
				// encounter many intents during IMPORT INTO as we lock the key space we
				// are importing into. Older write intents could however be found in the
				// target key space, which will require appropriate resolution logic.
				writeIntentErr := roachpb.WriteIntentError{
					Intents: []roachpb.Intent{
						roachpb.MakeIntent(mvccMeta.Txn, existingIter.Key().Key),
					},
				}

				return enginepb.MVCCStats{}, &writeIntentErr
			} else {
				return enginepb.MVCCStats{}, errors.Errorf("intent without transaction")
			}
		}

		bytesCompare := bytes.Compare(existingKey.Key, sstKey.Key)
		if bytesCompare == 0 {
			// If the colliding key is a tombstone in the existing data, and the
			// timestamp of the sst key is greater than or equal to the timestamp of
			// the tombstone, then this is not considered a collision. We move the
			// iterator over the existing data to the next potentially colliding key
			// (skipping all versions of the deleted key), and resume iteration.
			//
			// If the ts of the sst key is less than that of the tombstone it is
			// changing existing data, and we treat this as a collision.
			if len(existingValue) == 0 && existingKey.Timestamp.LessEq(sstKey.Timestamp) {
				existingIter.NextKey()
				ok, extErr = existingIter.Valid()
				ok2, sstErr = sstIter.Valid()

				continue
			}

			// If the ingested KV has an identical timestamp and value as the existing
			// data then we do not consider it to be a collision. We move the iterator
			// over the existing data to the next potentially colliding key (skipping
			// all versions of the current key), and resume iteration.
			if sstKey.Timestamp.Equal(existingKey.Timestamp) && bytes.Equal(existingValue, sstValue) {
				// Even though we skip over the KVs described above, their stats have
				// already been accounted for resulting in a problem of double-counting.
				// To solve this we send back the stats of these skipped KVs so that we
				// can subtract them later. This enables us to construct accurate
				// MVCCStats and prevents expensive recomputation in the future.
				metaKeySize := int64(len(sstKey.Key) + 1)
				metaValSize := int64(0)
				totalBytes := metaKeySize + metaValSize

				// Update the skipped stats to account fot the skipped meta key.
				skippedKVStats.LiveBytes += totalBytes
				skippedKVStats.LiveCount++
				skippedKVStats.KeyBytes += metaKeySize
				skippedKVStats.ValBytes += metaValSize
				skippedKVStats.KeyCount++

				// Update the stats to account for the skipped versioned key/value.
				totalBytes = int64(len(sstValue)) + MVCCVersionTimestampSize
				skippedKVStats.LiveBytes += totalBytes
				skippedKVStats.KeyBytes += MVCCVersionTimestampSize
				skippedKVStats.ValBytes += int64(len(sstValue))
				skippedKVStats.ValCount++

				existingIter.NextKey()
				ok, extErr = existingIter.Valid()
				ok2, sstErr = sstIter.Valid()

				continue
			}

			err := &Error{msg: existingIter.Key().Key.String()}
			return enginepb.MVCCStats{}, errors.Wrap(err, "ingested key collides with an existing one")
		} else if bytesCompare < 0 {
			existingIter.SeekGE(MVCCKey{Key: sstKey.Key})
		} else {
			sstIter.SeekGE(MVCCKey{Key: existingKey.Key})
		}

		ok, extErr = existingIter.Valid()
		ok2, sstErr = sstIter.Valid()
	}

	if extErr != nil {
		return enginepb.MVCCStats{}, extErr
	}
	if sstErr != nil {
		return enginepb.MVCCStats{}, sstErr
	}

	return skippedKVStats, nil
}
