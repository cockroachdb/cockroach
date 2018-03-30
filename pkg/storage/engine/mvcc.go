// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License.

package engine

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// The size of the timestamp portion of MVCC version keys (used to update stats).
	mvccVersionTimestampSize int64 = 12
)

var (
	// MVCCKeyMax is a maximum mvcc-encoded key value which sorts after
	// all other keys.`
	MVCCKeyMax = MakeMVCCMetadataKey(roachpb.KeyMax)
	// NilKey is the nil MVCCKey.
	NilKey = MVCCKey{}
)

// AccountForSelf adjusts ms to account for the predicted impact it will have on
// the values that it records when the structure is initially stored. Specifically,
// MVCCStats is stored on the RangeStats key, which means that its creation will
// have an impact on system-local data size and key count.
func AccountForSelf(ms *enginepb.MVCCStats, rangeID roachpb.RangeID) error {
	key := keys.RangeStatsKey(rangeID)
	metaKey := MakeMVCCMetadataKey(key)

	// MVCCStats is stored inline, so compute MVCCMetadata accordingly.
	value := roachpb.Value{}
	if err := value.SetProto(ms); err != nil {
		return err
	}
	meta := enginepb.MVCCMetadata{RawBytes: value.RawBytes}

	updateStatsForInline(ms, key, 0, 0, int64(metaKey.EncodedSize()), int64(meta.Size()))
	return nil
}

// MakeValue returns the inline value.
func MakeValue(meta enginepb.MVCCMetadata) roachpb.Value {
	return roachpb.Value{RawBytes: meta.RawBytes}
}

// IsIntentOf returns true if the meta record is an intent of the supplied
// transaction.
func IsIntentOf(meta enginepb.MVCCMetadata, txn *roachpb.Transaction) bool {
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
		n += int(mvccVersionTimestampSize)
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
	ms.ContainsEstimates = true
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
	origMetaKeySize, origMetaValSize,
	metaKeySize, metaValSize int64,
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
			ms.LiveBytes += mvccVersionTimestampSize + prevValSize
		}

		// Note that there is an interesting special case here: it's possible that
		// meta.Timestamp.WallTime < orig.Timestamp.WallTime. This wouldn't happen
		// outside of tests (due to our semantics of txn.OrigTimestamp, which never
		// decreases) but it sure does happen in randomized testing. An earlier
		// version of the code used `Forward` here, which is incorrect as it would be
		// a no-op and fail to subtract out the intent bytes/GC age incurred due to
		// removing the meta entry at `orig.Timestamp` (when `orig != nil`).
		ms.AgeTo(meta.Timestamp.WallTime)

		if prevIsValue {
			// Make the previous non-deletion value non-live again, as explained in the
			// sibling block above.
			ms.LiveBytes -= mvccVersionTimestampSize + prevValSize
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
	origMetaKeySize, origMetaValSize,
	metaKeySize, metaValSize int64,
	orig, meta enginepb.MVCCMetadata,
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
		ms.LiveBytes += mvccVersionTimestampSize + prevValSize
	}

	ms.AgeTo(meta.Timestamp.WallTime)

	if prevIsValue {
		// The previous non-deletion value becomes non-live at meta.Timestamp.
		// See the sibling code above.
		ms.LiveBytes -= mvccVersionTimestampSize + prevValSize
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

// updateStatsOnAbort updates stat counters by subtracting an
// aborted value's key and value byte sizes. If an earlier version
// was restored, the restored values are added to live bytes and
// count if the restored value isn't a deletion tombstone.
func updateStatsOnAbort(
	key roachpb.Key,
	origMetaKeySize, origMetaValSize,
	restoredMetaKeySize, restoredMetaValSize int64,
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
	// 2. it is not, and it has been shadowed by the intent we're now aborting,
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
	ms.IntentBytes -= (orig.KeyBytes + orig.ValBytes)
	ms.IntentCount--

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
// found. The semantics of consistent are the same as in MVCCGet.
func MVCCGetProto(
	ctx context.Context,
	engine Reader,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
	msg protoutil.Message,
) (bool, error) {
	// TODO(tschottdorf): Consider returning skipped intents to the caller.
	value, _, mvccGetErr := MVCCGet(ctx, engine, key, timestamp, consistent, txn)
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
	engine ReadWriter,
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
	return MVCCPut(ctx, engine, ms, key, timestamp, value, txn)
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

// MVCCGet returns the value for the key specified in the request,
// while satisfying the given timestamp condition. The key may contain
// arbitrary bytes. If no value for the key exists, or it has been
// deleted returns nil for value.
//
// The values of multiple versions for the given key should
// be organized as follows:
// ...
// keyA : MVCCMetadata of keyA
// keyA_Timestamp_n : value of version_n
// keyA_Timestamp_n-1 : value of version_n-1
// ...
// keyA_Timestamp_0 : value of version_0
// keyB : MVCCMetadata of keyB
// ...
//
// The consistent parameter indicates that intents should cause
// WriteIntentErrors. If set to false, a possible intent on the key will be
// ignored for reading the value (but returned via the roachpb.Intent slice);
// the previous value (if any) is read instead.
func MVCCGet(
	ctx context.Context,
	engine Reader,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
) (*roachpb.Value, []roachpb.Intent, error) {
	iter := engine.NewIterator(true)
	value, intents, err := iter.MVCCGet(key, timestamp, txn, consistent, false /* tombstones */)
	iter.Close()
	return value, intents, err
}

// MVCCGetWithTombstone is like MVCCGet (see comments above), but if
// the value has been deleted, returns a non-nil value with RawBytes
// set to nil for tombstones.
func MVCCGetWithTombstone(
	ctx context.Context,
	engine Reader,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
) (*roachpb.Value, []roachpb.Intent, error) {
	iter := engine.NewIterator(true)
	value, intents, err := iter.MVCCGet(key, timestamp, txn, consistent, true /* tombstones */)
	iter.Close()
	return value, intents, err
}

// MVCCGetAsTxn constructs a temporary Transaction from the given txn
// metadata and calls MVCCGet as that transaction. This method is required
// only for reading intents of a transaction when only its metadata is known
// and should rarely be used.
// The read is carried out without the chance of uncertainty restarts.
func MVCCGetAsTxn(
	ctx context.Context,
	engine Reader,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	txnMeta enginepb.TxnMeta,
) (*roachpb.Value, []roachpb.Intent, error) {
	txn := &roachpb.Transaction{
		TxnMeta:       txnMeta,
		Status:        roachpb.PENDING,
		Writing:       true,
		OrigTimestamp: txnMeta.Timestamp,
		MaxTimestamp:  txnMeta.Timestamp,
	}
	return MVCCGet(ctx, engine, key, timestamp, true /* consistent */, txn)
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
	iter.Seek(metaKey)
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
	// mvccVersionTimestampSize. The size of the metadata key is
	// accounted for separately.
	meta.KeyBytes = mvccVersionTimestampSize
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
) (*roachpb.Value, []roachpb.Intent, valueSafety, error) {
	if !consistent && txn != nil {
		return nil, nil, safeValue, errors.Errorf(
			"cannot allow inconsistent reads within a transaction")
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
	var ignoredIntents []roachpb.Intent
	metaTimestamp := hlc.Timestamp(meta.Timestamp)
	if !consistent && meta.Txn != nil && !timestamp.Less(metaTimestamp) {
		// If we're doing inconsistent reads and there's an intent, we
		// ignore the intent by insisting that the timestamp we're reading
		// at is a historical timestamp < the intent timestamp. However, we
		// return the intent separately; the caller may want to resolve it.
		ignoredIntents = append(ignoredIntents,
			roachpb.Intent{Span: roachpb.Span{Key: metaKey.Key}, Status: roachpb.PENDING, Txn: *meta.Txn})
		timestamp = metaTimestamp.Prev()
	}

	ownIntent := IsIntentOf(*meta, txn) // false if txn == nil
	if !timestamp.Less(metaTimestamp) && meta.Txn != nil && !ownIntent {
		// Trying to read the last value, but it's another transaction's intent;
		// the reader will have to act on this.
		return nil, nil, safeValue, &roachpb.WriteIntentError{
			Intents: []roachpb.Intent{{Span: roachpb.Span{Key: metaKey.Key}, Status: roachpb.PENDING, Txn: *meta.Txn}},
		}
	}

	var checkValueTimestamp bool
	seekKey := metaKey

	if !timestamp.Less(metaTimestamp) || ownIntent {
		// We are reading the latest value, which is either an intent written
		// by this transaction or not an intent at all (so there's no
		// conflict). Note that when reading the own intent, the timestamp
		// specified is irrelevant; we always want to see the intent (see
		// TestMVCCReadWithPushedTimestamp).
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
			seekKey = seekKey.Next()
		}
	} else if txn != nil && timestamp.Less(txn.MaxTimestamp) {
		// In this branch, the latest timestamp is ahead, and so the read of an
		// "old" value in a transactional context at time (timestamp, MaxTimestamp]
		// occurs, leading to a clock uncertainty error if a version exists in
		// that time interval.
		if !txn.MaxTimestamp.Less(metaTimestamp) {
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
		if seekKey.Timestamp == (hlc.Timestamp{}) {
			return nil, ignoredIntents, safeValue, nil
		}
	}

	iter.Seek(seekKey)
	if ok, err := iter.Valid(); err != nil {
		return nil, nil, safeValue, err
	} else if !ok {
		return nil, ignoredIntents, safeValue, nil
	}

	unsafeKey := iter.UnsafeKey()
	if !unsafeKey.Key.Equal(metaKey.Key) {
		return nil, ignoredIntents, safeValue, nil
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
	return value, ignoredIntents, allowedSafety, nil
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
	n, err := protoutil.MarshalToWithoutFuzzing(meta, data)
	if err != nil {
		return nil, err
	}
	b.tmpbuf = data
	return data[:n], nil
}

func (b *putBuffer) putMeta(
	engine Writer, key MVCCKey, meta *enginepb.MVCCMetadata,
) (keyBytes, valBytes int64, err error) {
	bytes, err := b.marshalMeta(meta)
	if err != nil {
		return 0, 0, err
	}
	if err := engine.Put(key, bytes); err != nil {
		return 0, 0, err
	}
	return int64(key.EncodedSize()), int64(len(bytes)), nil
}

// MVCCPut sets the value for a specified key. It will save the value
// with different versions according to its timestamp and update the
// key metadata. The timestamp must be passed as a parameter; using
// the Timestamp field on the value results in an error.
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
	engine ReadWriter,
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
		iter = engine.NewIterator(true)
		defer iter.Close()
	}
	return mvccPutUsingIter(ctx, engine, iter, ms, key, timestamp, value, txn, nil /* valueFn */)
}

// MVCCBlindPut is a fast-path of MVCCPut. See the MVCCPut comments for details
// of the semantics. MVCCBlindPut skips retrieving the existing metadata for
// the key requiring the caller to guarantee no versions for the key currently
// exist in order for stats to be updated properly. If a previous version of
// the key does exist it is up to the caller to properly account for their
// existence in updating the stats.
func MVCCBlindPut(
	ctx context.Context,
	engine Writer,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
) error {
	return mvccPutUsingIter(ctx, engine, nil, ms, key, timestamp, value, txn, nil /* valueFn */)
}

// MVCCDelete marks the key deleted so that it will not be returned in
// future get responses.
func MVCCDelete(
	ctx context.Context,
	engine ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	txn *roachpb.Transaction,
) error {
	iter := engine.NewIterator(true)
	defer iter.Close()

	return mvccPutUsingIter(ctx, engine, iter, ms, key, timestamp, noValue, txn, nil /* valueFn */)
}

var noValue = roachpb.Value{}

// mvccPutUsingIter sets the value for a specified key using the provided
// Iterator. The function takes a value and a valueFn, only one of which
// should be provided. If the valueFn is nil, value's raw bytes will be set
// for the key, else the bytes provided by the valueFn will be used.
func mvccPutUsingIter(
	ctx context.Context,
	engine Writer,
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

	err := mvccPutInternal(ctx, engine, iter, ms, key, timestamp, rawBytes,
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

// mvccPutInternal adds a new timestamped value to the specified key.
// If value is nil, creates a deletion tombstone value. valueFn is
// an optional alternative to supplying value directly. It is passed
// the existing value (or nil if none exists) and returns the value
// to write or an error. If valueFn is supplied, value should be nil
// and vice versa. valueFn can delete by returning nil. Returning
// []byte{} will write an empty value, not delete.
func mvccPutInternal(
	ctx context.Context,
	engine Writer,
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
	// Handle inline put.
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
			metaKeySize, metaValSize, err = 0, 0, engine.Clear(metaKey)
		} else {
			buf.meta = enginepb.MVCCMetadata{RawBytes: value}
			metaKeySize, metaValSize, err = buf.putMeta(engine, metaKey, &buf.meta)
		}
		if ms != nil {
			updateStatsForInline(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize)
		}
		return err
	}

	var meta *enginepb.MVCCMetadata
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
				return &roachpb.WriteIntentError{Intents: []roachpb.Intent{{Span: roachpb.Span{Key: key}, Status: roachpb.PENDING, Txn: *meta.Txn}}}
			} else if txn.Epoch < meta.Txn.Epoch {
				return errors.Errorf("put with epoch %d came after put with epoch %d in txn %s",
					txn.Epoch, meta.Txn.Epoch, txn.ID)
			} else if txn.Epoch == meta.Txn.Epoch &&
				(txn.Sequence < meta.Txn.Sequence ||
					(txn.Sequence == meta.Txn.Sequence && txn.BatchIndex <= meta.Txn.BatchIndex)) {
				// Replay error if we encounter an older sequence number or
				// the same (or earlier) batch index for the same sequence.
				return roachpb.NewTransactionRetryError(roachpb.RETRY_POSSIBLE_REPLAY)
			}
			// Make sure we process valueFn before clearing any earlier
			// version.  For example, a conditional put within same
			// transaction should read previous write.
			if value, err = maybeGetValue(
				ctx, iter, metaKey, value, ok, timestamp, txn, buf, valueFn); err != nil {
				return err
			}
			// We are replacing our own write intent. If we are writing at
			// the same timestamp (see comments in else block) we can
			// overwrite the existing intent; otherwise we must manually
			// delete the old intent, taking care with MVCC stats.
			if metaTimestamp.Less(timestamp) {
				{
					// If the older write intent has a version underneath it, we need to
					// read its size because its GCBytesAge contribution may change as we
					// move the intent above it. A similar phenomenon occurs in
					// MVCCResolveWriteIntent.
					latestKey := MVCCKey{Key: key, Timestamp: metaTimestamp}
					_, prevVal, haveNextVersion, err := unsafeNextVersion(iter, latestKey)
					if err != nil {
						return err
					}
					if haveNextVersion {
						prevValSize = int64(len(prevVal))
					}
				}

				versionKey := metaKey
				versionKey.Timestamp = metaTimestamp
				if err := engine.Clear(versionKey); err != nil {
					return err
				}
			} else if timestamp.Less(metaTimestamp) {
				// This case occurs when we're writing a key twice within a
				// txn, and our timestamp has been pushed forward because of
				// a write-too-old error on this key. For this case, we want
				// to continue writing at the higher timestamp or else the
				// MVCCMetadata could end up pointing *under* the newer write.
				timestamp = metaTimestamp
			}
		} else if !metaTimestamp.Less(timestamp) {
			// This is the case where we're trying to write under a
			// committed value. Obviously we can't do that, but we can
			// increment our timestamp to one logical tick past the existing
			// value and go on to write, but then return a write-too-old
			// error indicating what the timestamp ended up being. This
			// timestamp can then be used to increment the txn timestamp and
			// be returned with the response.
			actualTimestamp := metaTimestamp.Next()
			maybeTooOldErr = &roachpb.WriteTooOldError{Timestamp: timestamp, ActualTimestamp: actualTimestamp}
			// If we're in a transaction, always get the value at the orig
			// timestamp.
			if txn != nil {
				if value, err = maybeGetValue(
					ctx, iter, metaKey, value, ok, timestamp, txn, buf, valueFn); err != nil {
					return err
				}
			} else {
				// Outside of a transaction, read the latest value and advance
				// the write timestamp to the latest value's timestamp + 1. The
				// new timestamp is returned to the caller in maybeTooOldErr.
				if value, err = maybeGetValue(
					ctx, iter, metaKey, value, ok, actualTimestamp, txn, buf, valueFn); err != nil {
					return err
				}
			}
			timestamp = actualTimestamp
		} else {
			if value, err = maybeGetValue(
				ctx, iter, metaKey, value, ok, timestamp, txn, buf, valueFn); err != nil {
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
		}
		buf.newMeta = enginepb.MVCCMetadata{
			Txn:       txnMeta,
			Timestamp: hlc.LegacyTimestamp(timestamp),
		}
	}
	newMeta := &buf.newMeta

	versionKey := metaKey
	versionKey.Timestamp = timestamp
	if err := engine.Put(versionKey, value); err != nil {
		return err
	}

	// Write the mvcc metadata now that we have sizes for the latest
	// versioned value. For values, the size of keys is always accounted
	// for as mvccVersionTimestampSize. The size of the metadata key is
	// accounted for separately.
	newMeta.KeyBytes = mvccVersionTimestampSize
	newMeta.ValBytes = int64(len(value))
	newMeta.Deleted = value == nil

	var metaKeySize, metaValSize int64
	if newMeta.Txn != nil {
		metaKeySize, metaValSize, err = buf.putMeta(engine, metaKey, newMeta)
		if err != nil {
			return err
		}
	} else {
		// Per-key stats count the full-key once and mvccVersionTimestampSize for
		// each versioned value. We maintain that accounting even when the MVCC
		// metadata is implicit.
		metaKeySize = int64(metaKey.EncodedSize())
	}

	// Update MVCC stats.
	if ms != nil {
		ms.Add(updateStatsOnPut(key, prevValSize, origMetaKeySize, origMetaValSize,
			metaKeySize, metaValSize, meta, newMeta))
	}

	return maybeTooOldErr
}

// MVCCIncrement fetches the value for key, and assuming the value is
// an "integer" type, increments it by inc and stores the new
// value. The newly incremented value is returned.
//
// An initial value is read from the key using the same operational
// timestamp as we use to write a value.
func MVCCIncrement(
	ctx context.Context,
	engine ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	txn *roachpb.Transaction,
	inc int64,
) (int64, error) {
	iter := engine.NewIterator(true)
	defer iter.Close()

	var int64Val int64
	var newInt64Val int64
	err := mvccPutUsingIter(ctx, engine, iter, ms, key, timestamp, noValue, txn, func(value *roachpb.Value) ([]byte, error) {
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

// MVCCConditionalPut sets the value for a specified key only if the
// expected value matches. If not, the return a ConditionFailedError
// containing the actual value.
//
// The condition check reads a value from the key using the same operational
// timestamp as we use to write a value.
func MVCCConditionalPut(
	ctx context.Context,
	engine ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	expVal *roachpb.Value,
	txn *roachpb.Transaction,
) error {
	iter := engine.NewIterator(true)
	defer iter.Close()

	return mvccConditionalPutUsingIter(ctx, engine, iter, ms, key, timestamp, value, expVal, txn)
}

// MVCCBlindConditionalPut is a fast-path of MVCCConditionalPut. See the
// MVCCConditionalPut comments for details of the
// semantics. MVCCBlindConditionalPut skips retrieving the existing metadata
// for the key requiring the caller to guarantee no versions for the key
// currently exist.
func MVCCBlindConditionalPut(
	ctx context.Context,
	engine Writer,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	expVal *roachpb.Value,
	txn *roachpb.Transaction,
) error {
	return mvccConditionalPutUsingIter(ctx, engine, nil, ms, key, timestamp, value, expVal, txn)
}

func mvccConditionalPutUsingIter(
	ctx context.Context,
	engine Writer,
	iter Iterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	expVal *roachpb.Value,
	txn *roachpb.Transaction,
) error {
	return mvccPutUsingIter(
		ctx, engine, iter, ms, key, timestamp, noValue, txn,
		func(existVal *roachpb.Value) ([]byte, error) {
			if expValPresent, existValPresent := expVal != nil, existVal.IsPresent(); expValPresent && existValPresent {
				// Every type flows through here, so we can't use the typed getters.
				if !expVal.EqualData(*existVal) {
					return nil, &roachpb.ConditionFailedError{
						ActualValue: existVal.ShallowClone(),
					}
				}
			} else if expValPresent != existValPresent {
				return nil, &roachpb.ConditionFailedError{
					ActualValue: existVal.ShallowClone(),
				}
			}
			return value.RawBytes, nil
		})
}

var errInitPutValueMatchesExisting = errors.New("the value matched the existing value")

// MVCCInitPut sets the value for a specified key if the key doesn't exist. It
// returns a ConditionFailedError when the write fails or if the key exists with
// an existing value that is different from the supplied value. If
// failOnTombstones is set to true, tombstones count as mismatched values and
// will cause a ConditionFailedError.
func MVCCInitPut(
	ctx context.Context,
	engine ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	failOnTombstones bool,
	txn *roachpb.Transaction,
) error {
	iter := engine.NewIterator(true)
	defer iter.Close()
	return mvccInitPutUsingIter(ctx, engine, iter, ms, key, timestamp, value, failOnTombstones, txn)
}

// MVCCBlindInitPut is a fast-path of MVCCInitPut. See the MVCCInitPut
// comments for details of the semantics. MVCCBlindInitPut skips
// retrieving the existing metadata for the key requiring the caller
// to guarantee no version for the key currently exist.
func MVCCBlindInitPut(
	ctx context.Context,
	engine ReadWriter,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	failOnTombstones bool,
	txn *roachpb.Transaction,
) error {
	return mvccInitPutUsingIter(ctx, engine, nil, ms, key, timestamp, value, failOnTombstones, txn)
}

func mvccInitPutUsingIter(
	ctx context.Context,
	engine ReadWriter,
	iter Iterator,
	ms *enginepb.MVCCStats,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value roachpb.Value,
	failOnTombstones bool,
	txn *roachpb.Transaction,
) error {
	err := mvccPutUsingIter(ctx, engine, iter, ms, key, timestamp, noValue, txn,
		func(existVal *roachpb.Value) ([]byte, error) {
			if failOnTombstones && existVal != nil && len(existVal.RawBytes) == 0 {
				// We found a tombstone and failOnTombstones is true: fail.
				return nil, &roachpb.ConditionFailedError{ActualValue: existVal.ShallowClone()}
			}
			if existVal.IsPresent() {
				if !value.EqualData(*existVal) {
					return nil, &roachpb.ConditionFailedError{
						ActualValue: existVal.ShallowClone(),
					}
				}
				// The existing value matches the supplied value; return an error
				// to prevent rewriting the value.
				return nil, errInitPutValueMatchesExisting
			}
			return value.RawBytes, nil
		},
	)
	// Dummy error to prevent an unnecessary write.
	if err == errInitPutValueMatchesExisting {
		err = nil
	}
	return err
}

// MVCCMerge implements a merge operation. Merge adds integer values,
// concatenates undifferentiated byte slice values, and efficiently
// combines time series observations if the roachpb.Value tag value
// indicates the value byte slice is of type TIMESERIES.
func MVCCMerge(
	ctx context.Context,
	engine ReadWriter,
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
		if err = engine.Merge(metaKey, data); err == nil && ms != nil {
			ms.Add(updateStatsOnMerge(
				key, int64(len(rawBytes))+mvccVersionTimestampSize, timestamp.WallTime))
		}
	}
	buf.release()
	return err
}

// MVCCDeleteRange deletes the range of key/value pairs specified by start and
// end keys. It returns the range of keys deleted when returnedKeys is set,
// the next span to resume from, and the number of keys deleted.
// The returned resume span is nil if max keys aren't processed.
func MVCCDeleteRange(
	ctx context.Context,
	engine ReadWriter,
	ms *enginepb.MVCCStats,
	key,
	endKey roachpb.Key,
	max int64,
	timestamp hlc.Timestamp,
	txn *roachpb.Transaction,
	returnKeys bool,
) ([]roachpb.Key, *roachpb.Span, int64, error) {
	// In order to detect the potential write intent by another concurrent
	// transaction with a newer timestamp, we need to use the max timestamp for
	// scan.
	kvs, resumeSpan, _, err := MVCCScan(
		ctx, engine, key, endKey, max, hlc.MaxTimestamp, true /* consistent */, txn)
	if err != nil {
		return nil, nil, 0, err
	}

	buf := newPutBuffer()
	iter := engine.NewIterator(true)

	for i := range kvs {
		err = mvccPutInternal(
			ctx, engine, iter, ms, kvs[i].Key, timestamp, nil, txn, buf, nil)
		if err != nil {
			break
		}
	}

	iter.Close()
	buf.release()

	var keys []roachpb.Key
	if returnKeys && err == nil && len(kvs) > 0 {
		keys = make([]roachpb.Key, len(kvs))
		for i := range kvs {
			keys[i] = kvs[i].Key
		}
	}

	return keys, resumeSpan, int64(len(kvs)), err
}

// mvccScanInternal scans the key range [key,endKey) up to some maximum number
// of results. Specify reverse=true to scan in descending instead of ascending
// order. If iter is not specified, a new iterator is created from engine.
func mvccScanInternal(
	ctx context.Context,
	engine Reader,
	iter Iterator,
	key,
	endKey roachpb.Key,
	max int64,
	timestamp hlc.Timestamp,
	consistent bool,
	tombstones bool,
	txn *roachpb.Transaction,
	reverse bool,
) ([]roachpb.KeyValue, *roachpb.Span, []roachpb.Intent, error) {
	if max == 0 {
		return nil, &roachpb.Span{Key: key, EndKey: endKey}, nil, nil
	}

	var ownIter bool
	if iter == nil {
		iter = engine.NewIterator(false)
		ownIter = true
	}
	kvData, numKvs, intentData, err := iter.MVCCScan(
		key, endKey, max, timestamp, txn, consistent, reverse, tombstones)
	if ownIter {
		iter.Close()
	}

	if err != nil {
		return nil, nil, nil, err
	}

	kvs, resumeKey, intents, err := buildScanResults(kvData, numKvs, intentData, max, consistent)
	var resumeSpan *roachpb.Span
	if resumeKey != nil {
		// NB: we copy the resume key here to ensure that it doesn't point to the
		// same shared buffer as the main results. Higher levels of the code may
		// cache the resume key and we don't want them pinning excessive amounts of
		// memory.
		if reverse {
			resumeKey = resumeKey[:len(resumeKey):len(resumeKey)]
			resumeSpan = &roachpb.Span{Key: key, EndKey: resumeKey.Next()}
		} else {
			resumeKey = append(roachpb.Key(nil), resumeKey...)
			resumeSpan = &roachpb.Span{Key: resumeKey, EndKey: endKey}
		}
	}
	return kvs, resumeSpan, intents, err
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
		intents = append(intents, roachpb.Intent{
			Span:   roachpb.Span{Key: key.Key},
			Status: roachpb.PENDING,
			Txn:    *meta.Txn,
		})
	}

	if err := reader.Error(); err != nil {
		return nil, err
	}
	return intents, nil
}

func buildScanResumeKey(kvData []byte, numKvs int64, max int64) ([]byte, error) {
	if len(kvData) == 0 {
		return nil, nil
	}
	if numKvs <= max {
		return nil, nil
	}
	var err error
	for i := int64(0); i < max; i++ {
		_, _, kvData, err = mvccScanDecodeKeyValue(kvData)
		if err != nil {
			return nil, err
		}
	}
	key, _, _, err := mvccScanDecodeKeyValue(kvData)
	if err != nil {
		return nil, err
	}
	return key.Key, nil
}

func buildScanResults(
	kvData []byte, numKvs int64, intentData []byte, max int64, consistent bool,
) ([]roachpb.KeyValue, roachpb.Key, []roachpb.Intent, error) {
	intents, err := buildScanIntents(intentData)
	if err != nil {
		return nil, nil, nil, err
	}
	if consistent && len(intents) > 0 {
		// When encountering intents during a consistent scan we still need to
		// return the resume key.
		resumeKey, err := buildScanResumeKey(kvData, numKvs, max)
		if err != nil {
			return nil, nil, nil, err
		}
		return nil, resumeKey, nil, &roachpb.WriteIntentError{Intents: intents}
	}
	if len(kvData) == 0 {
		return nil, nil, intents, nil
	}

	if numKvs == 0 {
		return nil, nil, intents, nil
	}

	// Iterator.MVCCScan will return up to max+1 results. The extra result is
	// returned so that we can generate the resumeKey in the same manner as the
	// old Go version of MVCCScan.
	//
	// TODO(peter): We should change the resumeKey to be Key.Next() of the last
	// key returned.
	n := numKvs
	if n > max {
		n = max
	}
	kvs := make([]roachpb.KeyValue, n)

	var key MVCCKey
	var rawBytes []byte
	for i := range kvs {
		key, rawBytes, kvData, err = mvccScanDecodeKeyValue(kvData)
		if err != nil {
			return nil, nil, nil, err
		}
		kvs[i].Key = key.Key
		kvs[i].Value.RawBytes = rawBytes
		kvs[i].Value.Timestamp = key.Timestamp
	}

	var resumeKey roachpb.Key
	if numKvs > max {
		key, _, _, err = mvccScanDecodeKeyValue(kvData)
		if err != nil {
			return nil, nil, nil, err
		}
		resumeKey = key.Key
	}

	return kvs, resumeKey, intents, nil
}

// MVCCScan scans the key range [key,endKey) key up to some maximum number of
// results in ascending order. If it hits max, it returns a span to be used in
// the next call to this function. Use MaxInt64 for not limit. If the limit is
// not hit, the returned span will be nil. Otherwise, it will be the sub-span of
// [key,endKey) that has not been scanned.
func MVCCScan(
	ctx context.Context,
	engine Reader,
	key,
	endKey roachpb.Key,
	max int64,
	timestamp hlc.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
) ([]roachpb.KeyValue, *roachpb.Span, []roachpb.Intent, error) {
	return mvccScanInternal(ctx, engine, nil, key, endKey, max, timestamp,
		consistent, false /* tombstones */, txn, false /* reverse */)
}

// MVCCReverseScan scans the key range [start,end) key up to some maximum
// number of results in descending order. If it hits max, it returns a span to
// be used in the next call to this function.
func MVCCReverseScan(
	ctx context.Context,
	engine Reader,
	key,
	endKey roachpb.Key,
	max int64,
	timestamp hlc.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
) ([]roachpb.KeyValue, *roachpb.Span, []roachpb.Intent, error) {
	return mvccScanInternal(ctx, engine, nil, key, endKey, max, timestamp,
		consistent, false /* tombstones */, txn, true /* reverse */)
}

// MVCCIterate iterates over the key range [start,end). At each step of the
// iteration, f() is invoked with the current key/value pair. If f returns
// true (done) or an error, the iteration stops and the error is propagated.
// If the reverse is flag set the iterator will be moved in reverse order.
func MVCCIterate(
	ctx context.Context,
	engine Reader,
	startKey, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	consistent bool,
	tombstones bool,
	txn *roachpb.Transaction,
	reverse bool,
	f func(roachpb.KeyValue) (bool, error),
) ([]roachpb.Intent, error) {
	// Get a new iterator.
	iter := engine.NewIterator(false)
	defer iter.Close()

	return MVCCIterateUsingIter(
		ctx, engine, startKey, endKey, timestamp, consistent, tombstones, txn, reverse, iter, f,
	)
}

// MVCCIterateUsingIter iterates over the key range [start,end) using the
// supplied iterator. See comments for MVCCIterate.
func MVCCIterateUsingIter(
	ctx context.Context,
	engine Reader,
	startKey, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	consistent bool,
	tombstones bool,
	txn *roachpb.Transaction,
	reverse bool,
	iter Iterator,
	f func(roachpb.KeyValue) (bool, error),
) ([]roachpb.Intent, error) {
	var intents []roachpb.Intent
	var wiErr error

	for {
		const maxKeysPerScan = 1000
		kvs, resume, newIntents, err := mvccScanInternal(
			ctx, engine, iter, startKey, endKey, maxKeysPerScan, timestamp, consistent, tombstones, txn, reverse)
		if err != nil {
			switch tErr := err.(type) {
			case *roachpb.WriteIntentError:
				// In the case of WriteIntentErrors, accumulate affected keys but continue scan.
				if wiErr == nil {
					wiErr = tErr
				} else {
					wiErr.(*roachpb.WriteIntentError).Intents = append(
						wiErr.(*roachpb.WriteIntentError).Intents, tErr.Intents...)
				}
			default:
				return nil, err
			}
		}

		if len(newIntents) > 0 {
			if intents == nil {
				intents = newIntents
			} else {
				intents = append(intents, newIntents...)
			}
		}

		for i := range kvs {
			done, err := f(kvs[i])
			if err != nil {
				return nil, err
			}
			if done {
				// TODO(peter): This isn't quite the same semantics as mvccIterateOld
				// as we can return intents for keys that are "past" what we've invoked
				// the callback on. That's fine as none of the callers use the returned
				// intents.
				return intents, wiErr
			}
		}

		if resume == nil {
			break
		}
		if reverse {
			endKey = resume.EndKey
		} else {
			startKey = resume.Key
		}
	}

	return intents, wiErr
}

// MVCCResolveWriteIntent either commits or aborts (rolls back) an
// extant write intent for a given txn according to commit parameter.
// ResolveWriteIntent will skip write intents of other txns.
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
	ctx context.Context, engine ReadWriter, ms *enginepb.MVCCStats, intent roachpb.Intent,
) error {
	iterAndBuf := GetBufUsingIter(engine.NewIterator(true))
	err := MVCCResolveWriteIntentUsingIter(ctx, engine, iterAndBuf, ms, intent)
	// Using defer would be more convenient, but it is measurably slower.
	iterAndBuf.Cleanup()
	return err
}

// MVCCResolveWriteIntentUsingIter is a variant of MVCCResolveWriteIntent that
// uses iterator and buffer passed as parameters (e.g. when used in a loop).
func MVCCResolveWriteIntentUsingIter(
	ctx context.Context,
	engine ReadWriter,
	iterAndBuf IterAndBuf,
	ms *enginepb.MVCCStats,
	intent roachpb.Intent,
) error {
	if len(intent.Key) == 0 {
		return emptyKeyError()
	}
	if len(intent.EndKey) > 0 {
		return errors.Errorf("can't resolve range intent as point intent")
	}
	_, err := mvccResolveWriteIntent(
		ctx, engine, iterAndBuf.iter, ms, intent, iterAndBuf.buf, false, /* forRange */
	)
	return err
}

// unsafeNextVersion positions the iterator at the successor to latestKey. If this value
// exists and is a version of the same key, returns the UnsafeKey() and UnsafeValue() of that
// key-value pair along with `true`.
func unsafeNextVersion(iter Iterator, latestKey MVCCKey) (MVCCKey, []byte, bool, error) {
	// Compute the next possible mvcc value for this key.
	nextKey := latestKey.Next()
	iter.Seek(nextKey)

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
// The forRange parameter specifies whether the intent is part of a
// range of write intents. In this case, checks which make sense only
// in the context of resolving a specific point intent are skipped.
// Returns whether an intent was found and resolved, false otherwise.
func mvccResolveWriteIntent(
	ctx context.Context,
	engine ReadWriter,
	iter Iterator,
	ms *enginepb.MVCCStats,
	intent roachpb.Intent,
	buf *putBuffer,
	forRange bool,
) (bool, error) {
	metaKey := MakeMVCCMetadataKey(intent.Key)
	meta := &buf.meta
	ok, origMetaKeySize, origMetaValSize, err := mvccGetMetadata(iter, metaKey, meta)
	if err != nil {
		return false, err
	}
	// For cases where there's no value corresponding to the key we're
	// resolving, and this is a committed transaction, log a warning if
	// the intent txn is epoch=0. For non-zero epoch transactions, this
	// is a common occurrence for intents which were resolved by
	// concurrent actors, and does not benefit from a warning.
	if !ok {
		if intent.Status == roachpb.COMMITTED && intent.Txn.Epoch == 0 {
			log.Warningf(ctx, "unable to find value for %s (%+v)",
				intent.Key, intent.Txn)
		}
		return false, nil
	}
	if meta.Txn == nil || intent.Txn.ID != meta.Txn.ID {
		// For the ranged case, this key isn't within our remit. Bail early.
		if forRange {
			return false, nil
		}
		if intent.Status == roachpb.COMMITTED {
			// The intent is being committed. Verify that it was already committed by
			// looking for a value at the transaction timestamp. Note that this check
			// has false positives, but such false positives should be very rare. See
			// #9399 for details.
			//
			// Note that we hit this code path relatively frequently when doing end
			// transaction processing for locally resolved intents. In those cases,
			// meta.Txn == nil but the subsequent call to mvccGetInternal will avoid
			// any additional seeks because the iterator is already positioned
			// correctly.
			gbuf := newGetBuffer()
			defer gbuf.release()
			gbuf.meta = buf.meta

			v, _, _, err := mvccGetInternal(ctx, iter, metaKey,
				intent.Txn.Timestamp, false, unsafeValue, nil, gbuf)
			if err != nil {
				log.Warningf(ctx, "unable to find value for %s @ %s: %v ",
					intent.Key, intent.Txn.Timestamp, err)
			} else if v == nil {
				// This should never happen as ok is true above.
				log.Warningf(ctx, "unable to find value for %s @ %s (%+v vs %+v)",
					intent.Key, intent.Txn.Timestamp, meta, intent.Txn)
			} else if v.Timestamp != intent.Txn.Timestamp && intent.Txn.Epoch == 0 {
				// We only log here if the txn epoch is zero, as it's a
				// common case for an intent written during an earlier
				// epoch to have been resolved already by a concurrent
				// reader or writer. Note that this warning can *still*
				// be a false positive.
				log.Warningf(ctx, "unable to find value for %s @ %s: %s (txn=%+v)",
					intent.Key, intent.Txn.Timestamp, v.Timestamp, intent.Txn)
			}
		}
		return false, nil
	}

	// A commit in an older epoch or timestamp is prevented by the
	// abort span under normal operation. Replays of EndTransaction
	// commands which occur after the transaction record has been erased
	// make this a possibility; we treat such intents as uncommitted.
	//
	// A commit with a newer epoch effectively means that we wrote this
	// intent before an earlier retry, but didn't write it again
	// after. A commit with an older timestamp than the intent should
	// not happen even on replays because BeginTransaction has replay
	// protection. The BeginTransaction replay protection guarantees a
	// restart in EndTransaction, so the replay won't resolve intents.
	epochsMatch := meta.Txn.Epoch == intent.Txn.Epoch
	timestampsValid := !intent.Txn.Timestamp.Less(hlc.Timestamp(meta.Timestamp))
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
	// ============================
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
	pushed := intent.Status == roachpb.PENDING &&
		hlc.Timestamp(meta.Timestamp).Less(intent.Txn.Timestamp) &&
		meta.Txn.Epoch >= intent.Txn.Epoch

	// If we're committing, or if the commit timestamp of the intent has been moved forward, and if
	// the proposed epoch matches the existing epoch: update the meta.Txn. For commit, it's set to
	// nil; otherwise, we update its value. We may have to update the actual version value (remove old
	// and create new with proper timestamp-encoded key) if timestamp changed.
	if commit || pushed {
		buf.newMeta = *meta
		// Set the timestamp for upcoming write (or at least the stats update).
		buf.newMeta.Timestamp = hlc.LegacyTimestamp(intent.Txn.Timestamp)

		// If the new intent/value is at a different timestamp than the old intent,
		// and there is a value under both, then that value may need an adjustment
		// of its GCBytesAge. This is because it became non-live at orig.Timestamp
		// originally, and now only becomes non-live at newMeta.Timestamp. For that
		// reason, we have to read that version's size.
		//
		// However, if we're not actually moving the intent, no stats adjustment is
		// necessary and we avoid reading the old version in that case.
		var prevValSize int64
		if buf.newMeta.Timestamp != meta.Timestamp {
			// Look for the first real versioned key, i.e. the key just below the (old) meta's
			// timestamp.
			latestKey := MVCCKey{Key: intent.Key, Timestamp: hlc.Timestamp(meta.Timestamp)}
			_, unsafeNextValue, haveNextVersion, err := unsafeNextVersion(iter, latestKey)
			if err != nil {
				return false, err
			}
			if haveNextVersion {
				prevValSize = int64(len(unsafeNextValue))
			}
		}

		var metaKeySize, metaValSize int64
		if pushed {
			// Keep existing intent if we're pushing timestamp. We keep the
			// existing metadata instead of using the supplied intent meta
			// to avoid overwriting a newer epoch (see comments above). The
			// pusher's job isn't to do anything to update the intent but
			// to move the timestamp forward, even if it can.
			metaKeySize, metaValSize, err = buf.putMeta(engine, metaKey, &buf.newMeta)
		} else {
			metaKeySize = int64(metaKey.EncodedSize())
			err = engine.Clear(metaKey)
		}
		if err != nil {
			return false, err
		}

		// Update stat counters related to resolving the intent.
		if ms != nil {
			ms.Add(updateStatsOnResolve(intent.Key, prevValSize, origMetaKeySize, origMetaValSize,
				metaKeySize, metaValSize, *meta, buf.newMeta, commit))
		}

		// If timestamp of value changed, need to rewrite versioned value.
		if hlc.Timestamp(meta.Timestamp) != intent.Txn.Timestamp {
			origKey := MVCCKey{Key: intent.Key, Timestamp: hlc.Timestamp(meta.Timestamp)}
			newKey := MVCCKey{Key: intent.Key, Timestamp: intent.Txn.Timestamp}
			valBytes, err := engine.Get(origKey)
			if err != nil {
				return false, err
			}
			if err = engine.Clear(origKey); err != nil {
				return false, err
			}
			if err = engine.Put(newKey, valBytes); err != nil {
				return false, err
			}
		}
		return true, nil
	}

	// This method shouldn't be called in this instance, but there's
	// nothing to do if meta's epoch is greater than or equal txn's
	// epoch and the state is still PENDING.
	if intent.Status == roachpb.PENDING && meta.Txn.Epoch >= intent.Txn.Epoch {
		return false, nil
	}

	// Otherwise, we're deleting the intent. We must find the next
	// versioned value and reset the metadata's latest timestamp. If
	// there are no other versioned values, we delete the metadata
	// key.
	//
	// Note that the somewhat unintuitive case of an ABORT with
	// intent.Txn.Epoch < meta.Txn.Epoch is possible:
	// - writer1 writes key0 at epoch 0
	// - writer2 with higher priority encounters intent at key0 (epoch 0)
	// - writer1 restarts, now at epoch one (txn record not updated)
	// - writer1 writes key0 at epoch 1
	// - writer2 dispatches ResolveIntent to key0 (with epoch 0)
	// - ResolveIntent with epoch 0 aborts intent from epoch 1.

	// First clear the intent value.
	latestKey := MVCCKey{Key: intent.Key, Timestamp: hlc.Timestamp(meta.Timestamp)}
	if err := engine.Clear(latestKey); err != nil {
		return false, err
	}

	unsafeNextKey, unsafeNextValue, ok, err := unsafeNextVersion(iter, latestKey)
	if err != nil {
		return false, err
	}
	iter = nil // prevent accidental use below

	if !ok {
		// If there is no other version, we should just clean up the key entirely.
		if err = engine.Clear(metaKey); err != nil {
			return false, err
		}
		// Clear stat counters attributable to the intent we're aborting.
		if ms != nil {
			ms.Add(updateStatsOnAbort(intent.Key, origMetaKeySize, origMetaValSize, 0, 0, meta, nil, 0))
		}
		return true, nil
	}

	// Get the bytes for the next version so we have size for stat counts.
	valueSize := int64(len(unsafeNextValue))
	// Update the keyMetadata with the next version.
	buf.newMeta = enginepb.MVCCMetadata{
		Deleted:  valueSize == 0,
		KeyBytes: mvccVersionTimestampSize,
		ValBytes: valueSize,
	}
	if err := engine.Clear(metaKey); err != nil {
		return false, err
	}
	metaKeySize := int64(metaKey.EncodedSize())
	metaValSize := int64(0)

	// Update stat counters with older version.
	if ms != nil {
		ms.Add(updateStatsOnAbort(intent.Key, origMetaKeySize, origMetaValSize,
			metaKeySize, metaValSize, meta, &buf.newMeta, unsafeNextKey.Timestamp.WallTime))
	}

	return true, nil
}

// IterAndBuf used to pass iterators and buffers between MVCC* calls, allowing
// reuse without the callers needing to know the particulars.
type IterAndBuf struct {
	buf  *putBuffer
	iter Iterator
}

// GetIterAndBuf returns an IterAndBuf for passing into various MVCC* methods.
func GetIterAndBuf(engine Reader) IterAndBuf {
	return GetBufUsingIter(engine.NewIterator(false))
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
	ctx context.Context, engine ReadWriter, ms *enginepb.MVCCStats, intent roachpb.Intent, max int64,
) (int64, *roachpb.Span, error) {
	iterAndBuf := GetIterAndBuf(engine)
	defer iterAndBuf.Cleanup()
	return MVCCResolveWriteIntentRangeUsingIter(ctx, engine, iterAndBuf, ms, intent, max)
}

// MVCCResolveWriteIntentRangeUsingIter commits or aborts (rolls back)
// the range of write intents specified by start and end keys for a
// given txn. ResolveWriteIntentRange will skip write intents of other
// txns. Returns the number of intents resolved and a resume span if
// the max keys limit was exceeded.
func MVCCResolveWriteIntentRangeUsingIter(
	ctx context.Context,
	engine ReadWriter,
	iterAndBuf IterAndBuf,
	ms *enginepb.MVCCStats,
	intent roachpb.Intent,
	max int64,
) (int64, *roachpb.Span, error) {
	encKey := MakeMVCCMetadataKey(intent.Key)
	encEndKey := MakeMVCCMetadataKey(intent.EndKey)
	nextKey := encKey

	var keyBuf []byte
	num := int64(0)
	intent.EndKey = nil

	for {
		if num == max {
			return num, &roachpb.Span{Key: nextKey.Key, EndKey: encEndKey.Key}, nil
		}

		iterAndBuf.iter.Seek(nextKey)
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
			ok, err = mvccResolveWriteIntent(
				ctx, engine, iterAndBuf.iter, ms, intent, iterAndBuf.buf, true, /* forRange */
			)
		}
		if err != nil {
			log.Warningf(ctx, "failed to resolve intent for key %q: %v", key.Key, err)
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

// MVCCGarbageCollect creates an iterator on the engine. In parallel
// it iterates through the keys listed for garbage collection by the
// keys slice. The engine iterator is seeked in turn to each listed
// key, clearing all values with timestamps <= to expiration. The
// timestamp parameter is used to compute the intent age on GC.
func MVCCGarbageCollect(
	ctx context.Context,
	engine ReadWriter,
	ms *enginepb.MVCCStats,
	keys []roachpb.GCRequest_GCKey,
	timestamp hlc.Timestamp,
) error {
	// We're allowed to use a prefix iterator because we always Seek() the
	// iterator when handling a new user key.
	iter := engine.NewIterator(true)
	defer iter.Close()

	var count int64
	defer func(begin time.Time) {
		log.Eventf(ctx, "done with GC evaluation for %d keys at %.2f keys/sec. Deleted %d entries",
			len(keys), float64(len(keys))*1E9/float64(timeutil.Since(begin)), count)
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
		if !gcKey.Timestamp.Less(hlc.Timestamp(meta.Timestamp)) {
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
				if err := engine.Clear(iter.UnsafeKey()); err != nil {
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
			if !gcKey.Timestamp.Less(unsafeIterKey.Timestamp) {
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

					ms.Add(updateStatsOnGC(gcKey.Key, mvccVersionTimestampSize,
						valSize, nil, fromNS))
				}
				count++
				if err := engine.Clear(unsafeIterKey); err != nil {
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
// from the key ranges listed in keys.NoSplitSpans (or listed in
// keys.NoSplitSpansWithoutMeta2Splits if allowMeta2Splits is false).
func MVCCFindSplitKey(
	ctx context.Context,
	engine Reader,
	key, endKey roachpb.RKey,
	targetSize int64,
	allowMeta2Splits bool,
) (roachpb.Key, error) {
	if key.Less(roachpb.RKey(keys.LocalMax)) {
		key = roachpb.RKey(keys.LocalMax)
	}

	it := engine.NewIterator(false /* prefix */)
	defer it.Close()

	minSplitKey := key
	// If this is a table, we can only split at row boundaries.
	if remainder, _, err := keys.DecodeTablePrefix(roachpb.Key(key)); err == nil {
		// If this is the first range containing a table, its start key won't
		// actually contain any row information, just the table ID. We don't want
		// to restrict splits on such tables, since key.PrefixEnd will just be the
		// end of the table span.
		if len(remainder) > 0 {
			minSplitKey = roachpb.RKey(roachpb.Key(key).PrefixEnd())
		}
	}
	splitKey, err := it.FindSplitKey(
		MakeMVCCMetadataKey(key.AsRawKey()),
		MakeMVCCMetadataKey(endKey.AsRawKey()),
		MakeMVCCMetadataKey(minSplitKey.AsRawKey()),
		targetSize,
		allowMeta2Splits)
	if err != nil {
		return nil, err
	}
	// The family ID has been removed from this key, making it a valid split point.
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
	iter SimpleIterator, start, end MVCCKey, nowNanos int64, callbacks ...func(MVCCKey, []byte) error,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats

	meta := &enginepb.MVCCMetadata{}
	var prevKey []byte
	first := false

	// Values start accruing GCBytesAge at the timestamp at which they
	// are shadowed (i.e. overwritten) whereas deletion tombstones
	// use their own timestamp. We're iterating through versions in
	// reverse chronological order and use this variable to keep track
	// of the point in time at which the current key begins to age.
	var accrueGCAgeNanos int64

	iter.Seek(start)
	for ; ; iter.Next() {
		ok, err := iter.Valid()
		if err != nil {
			return ms, err
		}
		if !ok || !iter.UnsafeKey().Less(end) {
			break
		}

		unsafeKey := iter.UnsafeKey()
		unsafeValue := iter.UnsafeValue()

		for _, f := range callbacks {
			if err := f(unsafeKey, unsafeValue); err != nil {
				return enginepb.MVCCStats{}, err
			}
		}

		isSys := bytes.Compare(unsafeKey.Key, keys.LocalMax) < 0
		isValue := unsafeKey.IsValue()
		implicitMeta := isValue && !bytes.Equal(unsafeKey.Key, prevKey)
		prevKey = append(prevKey[:0], unsafeKey.Key...)

		if implicitMeta {
			// No MVCCMetadata entry for this series of keys.
			meta.Reset()
			meta.KeyBytes = mvccVersionTimestampSize
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
				if err := protoutil.Unmarshal(unsafeValue, meta); err != nil {
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
					ms.GCBytesAge += totalBytes * (nowNanos/1E9 - meta.Timestamp.WallTime/1E9)
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

		totalBytes := int64(len(unsafeValue)) + mvccVersionTimestampSize
		if isSys {
			ms.SysBytes += totalBytes
		} else {
			if first {
				first = false
				if !meta.Deleted {
					ms.LiveBytes += totalBytes
				} else {
					// First value is deleted, so it's GC'able; add key & value bytes to age stat.
					ms.GCBytesAge += totalBytes * (nowNanos/1E9 - meta.Timestamp.WallTime/1E9)
				}
				if meta.Txn != nil {
					ms.IntentBytes += totalBytes
					ms.IntentCount++
					ms.IntentAge += nowNanos/1E9 - meta.Timestamp.WallTime/1E9
				}
				if meta.KeyBytes != mvccVersionTimestampSize {
					return ms, errors.Errorf("expected mvcc metadata key bytes to equal %d; got %d", mvccVersionTimestampSize, meta.KeyBytes)
				}
				if meta.ValBytes != int64(len(unsafeValue)) {
					return ms, errors.Errorf("expected mvcc metadata val bytes to equal %d; got %d", len(unsafeValue), meta.ValBytes)
				}
				accrueGCAgeNanos = meta.Timestamp.WallTime
			} else {
				// Overwritten value. Is it a deletion tombstone?
				isTombstone := len(unsafeValue) == 0
				if isTombstone {
					// The contribution of the tombstone picks up GCByteAge from its own timestamp on.
					ms.GCBytesAge += totalBytes * (nowNanos/1E9 - unsafeKey.Timestamp.WallTime/1E9)
				} else {
					// The kv pair is an overwritten value, so it became non-live when the closest more
					// recent value was written.
					ms.GCBytesAge += totalBytes * (nowNanos/1E9 - accrueGCAgeNanos/1E9)
				}
				// Update for the next version we may end up looking at.
				accrueGCAgeNanos = unsafeKey.Timestamp.WallTime
			}
			ms.KeyBytes += mvccVersionTimestampSize
			ms.ValBytes += int64(len(unsafeValue))
			ms.ValCount++
		}
	}

	ms.LastUpdateNanos = nowNanos
	return ms, nil
}
