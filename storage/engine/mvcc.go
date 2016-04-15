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
//
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"bytes"
	"fmt"
	"math"
	"sync"

	"golang.org/x/net/context"

	"github.com/dustin/go-humanize"
	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

const (
	// The size of the timestamp portion of MVCC version keys (used to update stats).
	mvccVersionTimestampSize int64 = 12
)

var (
	// MVCCKeyMax is a maximum mvcc-encoded key value which sorts after
	// all other keys.
	MVCCKeyMax = MakeMVCCMetadataKey(roachpb.KeyMax)
	// NilKey is the nil MVCCKey.
	NilKey = MVCCKey{}
)

// MVCCKey is a versioned key, distinguished from roachpb.Key with the addition
// of a timestamp.
type MVCCKey struct {
	Key       roachpb.Key
	Timestamp roachpb.Timestamp
}

// MakeMVCCMetadataKey creates an MVCCKey from a roachpb.Key.
func MakeMVCCMetadataKey(key roachpb.Key) MVCCKey {
	return MVCCKey{Key: key}
}

// Next returns the next key.
func (k MVCCKey) Next() MVCCKey {
	ts := k.Timestamp.Prev()
	if ts == roachpb.ZeroTimestamp {
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
	if !l.IsValue() {
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
	return k.Timestamp != roachpb.ZeroTimestamp
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

// Value returns the inline value.
func (meta MVCCMetadata) Value() roachpb.Value {
	return roachpb.Value{RawBytes: meta.RawBytes}
}

// IsInline returns true if the value is inlined in the metadata.
func (meta MVCCMetadata) IsInline() bool {
	return meta.RawBytes != nil
}

// IsIntentOf returns true if the meta record is an intent of the supplied
// transaction.
func (meta MVCCMetadata) IsIntentOf(txn *roachpb.Transaction) bool {
	return meta.Txn != nil && txn != nil && roachpb.TxnIDEqual(meta.Txn.ID, txn.ID)
}

// GCBytes is a convenience function which returns the number of gc bytes,
// that is the key and value bytes excluding the live bytes.
func (ms MVCCStats) GCBytes() int64 {
	return ms.KeyBytes + ms.ValBytes - ms.LiveBytes
}

// AgeTo encapsulates the complexity of computing the increment in age
// quantities contained in MVCCStats. Two MVCCStats structs only add and
// subtract meaningfully if their LastUpdateNanos matches, so aging them to
// the max of their LastUpdateNanos is a prerequisite.
// If nowNanos is behind ms.LastUpdateNanos, this method is a noop.
func (ms *MVCCStats) AgeTo(nowNanos int64) {
	if ms.LastUpdateNanos >= nowNanos {
		return
	}
	diffSeconds := nowNanos/1E9 - ms.LastUpdateNanos/1E9 // not (...)/1E9!

	ms.GCBytesAge += ms.GCBytes() * diffSeconds
	ms.IntentAge += ms.IntentCount * diffSeconds
	ms.LastUpdateNanos = nowNanos
}

// Add adds values from oms to ms. The ages will be moved forward to the
// larger of the LastUpdateNano timestamps involved.
func (ms *MVCCStats) Add(oms MVCCStats) {
	// Enforce the max LastUpdateNanos for both ages based on their
	// pre-addition state.
	ms.AgeTo(oms.LastUpdateNanos)
	oms.AgeTo(ms.LastUpdateNanos)
	// Now that we've done that, we may just add them.
	ms.IntentAge += oms.IntentAge
	ms.GCBytesAge += oms.GCBytesAge

	ms.LiveBytes += oms.LiveBytes
	ms.KeyBytes += oms.KeyBytes
	ms.ValBytes += oms.ValBytes
	ms.IntentBytes += oms.IntentBytes
	ms.LiveCount += oms.LiveCount
	ms.KeyCount += oms.KeyCount
	ms.ValCount += oms.ValCount
	ms.IntentCount += oms.IntentCount
	ms.SysBytes += oms.SysBytes
	ms.SysCount += oms.SysCount
}

// Subtract removes oms from ms. The ages will be moved forward to the larger of
// the LastUpdateNano timestamps involved.
func (ms *MVCCStats) Subtract(oms MVCCStats) {
	// Enforce the max LastUpdateNanos for both ages based on their
	// pre-subtraction state.
	ms.AgeTo(oms.LastUpdateNanos)
	oms.AgeTo(ms.LastUpdateNanos)
	// Now that we've done that, we may subtract.
	ms.IntentAge -= oms.IntentAge
	ms.GCBytesAge -= oms.GCBytesAge
	ms.LiveBytes -= oms.LiveBytes
	ms.KeyBytes -= oms.KeyBytes
	ms.ValBytes -= oms.ValBytes
	ms.IntentBytes -= oms.IntentBytes
	ms.LiveCount -= oms.LiveCount
	ms.KeyCount -= oms.KeyCount
	ms.ValCount -= oms.ValCount
	ms.IntentCount -= oms.IntentCount
	ms.SysBytes -= oms.SysBytes
	ms.SysCount -= oms.SysCount
}

// AccountForSelf adjusts ms to account for the predicted impact it will have on
// the values that it records when the structure is initially stored. Specifically,
// MVCCStats is stored on the RangeStats key, which means that its creation will
// have an impact on system-local data size and key count.
func (ms *MVCCStats) AccountForSelf(rangeID roachpb.RangeID) error {
	key := keys.RangeStatsKey(rangeID)
	metaKey := MakeMVCCMetadataKey(key)

	// MVCCStats is stored inline, so compute MVCCMetadata accordingly.
	value := roachpb.Value{}
	if err := value.SetProto(ms); err != nil {
		return err
	}
	meta := MVCCMetadata{RawBytes: value.RawBytes}

	updateStatsForInline(ms, key, 0, 0, int64(metaKey.EncodedSize()), int64(meta.Size()))
	return nil
}

// isSysLocal returns whether the whether the key is system-local.
func isSysLocal(key roachpb.Key) bool {
	return key.Compare(keys.LocalMax) < 0
}

// updateStatsForInline updates stat counters for an inline value.
// These are simpler as they don't involve intents or multiple
// versions.
func updateStatsForInline(
	ms *MVCCStats, key roachpb.Key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize int64,
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
func updateStatsOnMerge(key roachpb.Key, valSize, nowNanos int64) MVCCStats {
	var ms MVCCStats
	sys := isSysLocal(key)
	ms.AgeTo(nowNanos)
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
func updateStatsOnPut(key roachpb.Key, origMetaKeySize, origMetaValSize,
	metaKeySize, metaValSize int64, orig, meta *MVCCMetadata) MVCCStats {
	var ms MVCCStats
	sys := isSysLocal(key)

	// Remove current live counts for this key.
	if orig != nil {
		if sys {
			ms.SysBytes -= (origMetaKeySize + origMetaValSize)
			ms.SysCount--
		} else {
			// Move the (so far empty) stats to the timestamp at which the
			// previous entry was created, which is where we wish to reclassify
			// its contributions.
			ms.AgeTo(orig.Timestamp.WallTime)
			// If original version value for this key wasn't deleted, subtract
			// its contribution from live bytes in anticipation of adding in
			// contribution from new version below.
			if !orig.Deleted {
				ms.LiveBytes -= orig.KeyBytes + orig.ValBytes + origMetaKeySize + origMetaValSize
				ms.LiveCount--
				// Also, add the bytes from overwritten value to the GC'able bytes age stat.
			}

			ms.KeyBytes -= origMetaKeySize
			ms.ValBytes -= origMetaValSize
			ms.KeyCount--
			// If the original metadata for this key was an intent, subtract
			// its contribution from stat counters as it's being replaced.
			if orig.Txn != nil {
				// Subtract counts attributable to intent we're replacing.
				ms.KeyBytes -= orig.KeyBytes
				ms.ValBytes -= orig.ValBytes
				ms.ValCount--
				ms.IntentBytes -= (orig.KeyBytes + orig.ValBytes)
				ms.IntentCount--
			}
		}
	}

	// Move the stats to the new meta's timestamp. If we had an orig meta, this
	// ages those original stats by the time which the previous version was live.
	ms.AgeTo(meta.Timestamp.WallTime)
	if sys {
		ms.SysBytes += meta.KeyBytes + meta.ValBytes + metaKeySize + metaValSize
		ms.SysCount++
	} else {
		// If new version isn't a deletion tombstone, add it to live counters.
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
	}
	return ms
}

// updateStatsOnResolve updates stat counters with the difference
// between the original and new metadata sizes. The size of the
// resolved value (key & bytes) are subtracted from the intents
// counters if commit=true.
func updateStatsOnResolve(key roachpb.Key, origMetaKeySize, origMetaValSize,
	metaKeySize, metaValSize int64, orig, meta MVCCMetadata, commit bool) MVCCStats {
	var ms MVCCStats
	// In this case, we're only removing the contribution from having the
	// meta key around from orig.Timestamp to meta.Timestamp.
	ms.AgeTo(orig.Timestamp.WallTime)
	sys := isSysLocal(key)

	// Always zero.
	keyDiff := metaKeySize - origMetaKeySize
	// This is going to be nonpositive: the old meta key was
	// real, the new one is implicit.
	valDiff := metaValSize - origMetaValSize

	if sys {
		ms.SysBytes += keyDiff + valDiff
	} else {
		if !meta.Deleted {
			ms.LiveBytes += keyDiff + valDiff
		}
		ms.KeyBytes += keyDiff
		ms.ValBytes += valDiff
		// If committing, subtract out intent counts.
		if commit {
			ms.IntentBytes -= (meta.KeyBytes + meta.ValBytes)
			ms.IntentCount--
		}
	}
	ms.AgeTo(meta.Timestamp.WallTime)
	return ms
}

// updateStatsOnAbort updates stat counters by subtracting an
// aborted value's key and value byte sizes. If an earlier version
// was restored, the restored values are added to live bytes and
// count if the restored value isn't a deletion tombstone.
func updateStatsOnAbort(key roachpb.Key, origMetaKeySize, origMetaValSize,
	restoredMetaKeySize, restoredMetaValSize int64, orig, restored *MVCCMetadata,
	restoredNanos, txnNanos int64) MVCCStats {
	sys := isSysLocal(key)

	var ms MVCCStats

	// Three epochs of time here:
	// 1) creation of previous value (or 0) to creation of intent:
	//		[restoredNanos, orig.Timestamp.WallTime)
	// 2) creation of the intent (which we're now aborting) to the timestamp
	//    at which we're aborting:
	//		[orig.Timestamp.WallTime, txnNanos)
	if restored != nil {
		ms.AgeTo(restoredNanos)
		if sys {
			ms.SysBytes += restoredMetaKeySize + restoredMetaValSize
			ms.SysCount++
		} else {
			if !restored.Deleted {
				ms.LiveBytes += restored.KeyBytes + restored.ValBytes + restoredMetaKeySize + restoredMetaValSize
				ms.LiveCount++
			}
			ms.KeyBytes += restoredMetaKeySize
			ms.ValBytes += restoredMetaValSize
			ms.KeyCount++
			if restored.Txn != nil {
				panic("restored version should never be an intent")
			}
		}
	}

	ms.AgeTo(orig.Timestamp.WallTime)

	origTotalBytes := orig.KeyBytes + orig.ValBytes + origMetaKeySize + origMetaValSize
	if sys {
		ms.SysBytes -= origTotalBytes
		ms.SysCount--
	} else {
		if !orig.Deleted {
			ms.LiveBytes -= origTotalBytes
			ms.LiveCount--
		}
		ms.KeyBytes -= (orig.KeyBytes + origMetaKeySize)
		ms.ValBytes -= (orig.ValBytes + origMetaValSize)
		ms.KeyCount--
		ms.ValCount--
		ms.IntentBytes -= (orig.KeyBytes + orig.ValBytes)
		ms.IntentCount--
	}
	ms.AgeTo(txnNanos)

	return ms
}

// updateStatsOnGC updates stat counters after garbage collection
// by subtracting key and value byte counts, updating key and
// value counts, and updating the GC'able bytes age. If meta is
// not nil, then the value being GC'd is the mvcc metadata and we
// decrement the key count.
func updateStatsOnGC(
	key roachpb.Key, keySize, valSize int64, meta *MVCCMetadata, fromNS, toNS int64,
) MVCCStats {
	var ms MVCCStats
	ms.AgeTo(fromNS)
	sys := isSysLocal(key)
	if sys {
		ms.SysBytes -= (keySize + valSize)
		if meta != nil {
			ms.SysCount--
		}
	} else {
		ms.KeyBytes -= keySize
		ms.ValBytes -= valSize
		if meta != nil {
			ms.KeyCount--
		} else {
			ms.ValCount--
		}
	}
	ms.AgeTo(toNS)
	return ms
}

// MVCCGetRangeStats reads stat counters for the specified range and
// sets the values in the supplied MVCCStats struct.
func MVCCGetRangeStats(
	ctx context.Context,
	engine Engine,
	rangeID roachpb.RangeID,
	ms *MVCCStats,
) error {
	_, err := MVCCGetProto(ctx, engine, keys.RangeStatsKey(rangeID), roachpb.ZeroTimestamp, true, nil, ms)
	return err
}

// MVCCSetRangeStats sets stat counters for specified range.
func MVCCSetRangeStats(ctx context.Context,
	engine Engine,
	rangeID roachpb.RangeID,
	ms *MVCCStats,
) error {
	return MVCCPutProto(ctx, engine, nil, keys.RangeStatsKey(rangeID), roachpb.ZeroTimestamp, nil, ms)
}

// MVCCGetProto fetches the value at the specified key and unmarshals
// it using a protobuf decoder. Returns true on success or false if
// the key was not found. In the event of a WriteIntentError when
// consistent=false, we return the error and the decoded result; for
// all other errors (or when consistent=true) the decoded value is
// invalid.
func MVCCGetProto(
	ctx context.Context,
	engine Engine,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
	msg proto.Message,
) (bool, error) {
	// TODO(tschottdorf) Consider returning skipped intents to the caller.
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
	engine Engine,
	ms *MVCCStats,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	txn *roachpb.Transaction,
	msg proto.Message,
) error {
	value := roachpb.Value{}
	if err := value.SetProto(msg); err != nil {
		return err
	}
	value.InitChecksum(key)
	return MVCCPut(ctx, engine, ms, key, timestamp, value, txn)
}

type getBuffer struct {
	meta  MVCCMetadata
	value roachpb.Value
}

var getBufferPool = sync.Pool{
	New: func() interface{} {
		return &getBuffer{}
	},
}

func newGetBuffer() *getBuffer {
	return getBufferPool.Get().(*getBuffer)
}

func (b *getBuffer) release() {
	*b = getBuffer{}
	getBufferPool.Put(b)
}

// MVCCGet returns the value for the key specified in the request,
// while satisfying the given timestamp condition. The key may contain
// arbitrary bytes. If no value for the key exists, or it has been
// deleted, returns nil for value.
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
	engine Engine,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
) (*roachpb.Value, []roachpb.Intent, error) {
	iter := engine.NewIterator(key)
	defer iter.Close()

	return mvccGetUsingIter(ctx, iter, key, timestamp, consistent, txn)
}

func mvccGetUsingIter(
	ctx context.Context,
	iter Iterator,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
) (*roachpb.Value, []roachpb.Intent, error) {
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}

	buf := newGetBuffer()
	defer buf.release()

	metaKey := MakeMVCCMetadataKey(key)
	ok, _, _, err := mvccGetMetadata(iter, metaKey, &buf.meta)
	if !ok || err != nil {
		return nil, nil, err
	}

	value, intents, err := mvccGetInternal(ctx, iter, metaKey,
		timestamp, consistent, txn, buf)
	if value == &buf.value {
		value = &roachpb.Value{}
		*value = buf.value
		buf.value.Reset()
	}
	return value, intents, err
}

// MVCCGetAsTxn constructs a temporary Transaction from the given txn
// metadata and calls MVCCGet as that transaction. This method is required
// only for reading intents of a transaction when only its metadata is known
// and should rarely be used.
// The read is carried out without the chance of uncertainty restarts.
func MVCCGetAsTxn(
	ctx context.Context,
	engine Engine,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	consistent bool,
	txnMeta roachpb.TxnMeta,
) (*roachpb.Value, []roachpb.Intent, error) {
	txn := &roachpb.Transaction{
		TxnMeta:       txnMeta,
		Status:        roachpb.PENDING,
		Writing:       true,
		OrigTimestamp: txnMeta.Timestamp,
		MaxTimestamp:  txnMeta.Timestamp,
	}
	return MVCCGet(ctx, engine, key, timestamp, consistent, txn)
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
func mvccGetMetadata(iter Iterator, metaKey MVCCKey,
	meta *MVCCMetadata) (ok bool, keyBytes, valBytes int64, err error) {
	iter.Seek(metaKey)
	if !iter.Valid() {
		return false, 0, 0, nil
	}

	unsafeKey := iter.unsafeKey()
	if !unsafeKey.Key.Equal(metaKey.Key) {
		return false, 0, 0, nil
	}

	if !unsafeKey.IsValue() {
		if err := iter.ValueProto(meta); err != nil {
			return false, 0, 0, err
		}
		return true, int64(unsafeKey.EncodedSize()), int64(len(iter.unsafeValue())), nil
	}

	meta.Reset()
	// For values, the size of keys is always accounted for as
	// mvccVersionTimestampSize. The size of the metadata key is
	// accounted for separately.
	meta.KeyBytes = mvccVersionTimestampSize
	meta.ValBytes = int64(len(iter.unsafeValue()))
	meta.Deleted = len(iter.unsafeValue()) == 0
	meta.Timestamp = unsafeKey.Timestamp
	return err == nil, int64(unsafeKey.EncodedSize()) - meta.KeyBytes, 0, err
}

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
func mvccGetInternal(
	_ context.Context,
	iter Iterator,
	metaKey MVCCKey,
	timestamp roachpb.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
	buf *getBuffer,
) (*roachpb.Value, []roachpb.Intent, error) {
	if !consistent && txn != nil {
		return nil, nil, util.Errorf("cannot allow inconsistent reads within a transaction")
	}

	meta := &buf.meta

	// If value is inline, return immediately; txn & timestamp are irrelevant.
	if meta.IsInline() {
		value := &buf.value
		*value = roachpb.Value{RawBytes: meta.RawBytes}
		if err := value.Verify(metaKey.Key); err != nil {
			return nil, nil, err
		}
		return value, nil, nil
	}
	var ignoredIntents []roachpb.Intent
	if !consistent && meta.Txn != nil && !timestamp.Less(meta.Timestamp) {
		// If we're doing inconsistent reads and there's an intent, we
		// ignore the intent by insisting that the timestamp we're reading
		// at is a historical timestamp < the intent timestamp. However, we
		// return the intent separately; the caller may want to resolve it.
		ignoredIntents = append(ignoredIntents,
			roachpb.Intent{Span: roachpb.Span{Key: metaKey.Key}, Status: roachpb.PENDING, Txn: *meta.Txn})
		timestamp = meta.Timestamp.Prev()
	}

	ownIntent := meta.IsIntentOf(txn) // false if txn == nil
	if !timestamp.Less(meta.Timestamp) && meta.Txn != nil && !ownIntent {
		// Trying to read the last value, but it's another transaction's intent;
		// the reader will have to act on this.
		return nil, nil, &roachpb.WriteIntentError{
			Intents: []roachpb.Intent{{Span: roachpb.Span{Key: metaKey.Key}, Status: roachpb.PENDING, Txn: *meta.Txn}},
		}
	}

	var checkValueTimestamp bool
	seekKey := metaKey

	if !timestamp.Less(meta.Timestamp) || ownIntent {
		// We are reading the latest value, which is either an intent written
		// by this transaction or not an intent at all (so there's no
		// conflict). Note that when reading the own intent, the timestamp
		// specified is irrelevant; we always want to see the intent (see
		// TestMVCCReadWithPushedTimestamp).
		seekKey.Timestamp = meta.Timestamp

		// Check for case where we're reading our own txn's intent
		// but it's got a different epoch. This can happen if the
		// txn was restarted and an earlier iteration wrote the value
		// we're now reading. In this case, we skip the intent.
		if ownIntent && txn.Epoch != meta.Txn.Epoch {
			if txn.Epoch < meta.Txn.Epoch {
				return nil, nil, util.Errorf("failed to read with epoch %d due to a write intent with epoch %d",
					txn.Epoch, meta.Txn.Epoch)
			}
			seekKey = seekKey.Next()
		}
	} else if txn != nil && timestamp.Less(txn.MaxTimestamp) {
		// In this branch, the latest timestamp is ahead, and so the read of an
		// "old" value in a transactional context at time (timestamp, MaxTimestamp]
		// occurs, leading to a clock uncertainty error if a version exists in
		// that time interval.
		if !txn.MaxTimestamp.Less(meta.Timestamp) {
			// Second case: Our read timestamp is behind the latest write, but the
			// latest write could possibly have happened before our read in
			// absolute time if the writer had a fast clock.
			// The reader should try again with a later timestamp than the
			// one given below.
			return nil, nil, roachpb.NewReadWithinUncertaintyIntervalError(
				timestamp, meta.Timestamp)
		}

		// We want to know if anything has been written ahead of timestamp, but
		// before MaxTimestamp.
		seekKey.Timestamp = txn.MaxTimestamp
		checkValueTimestamp = true
	} else {
		// Third case: We're reading a historic value either outside of a
		// transaction, or in the absence of future versions that clock uncertainty
		// would apply to.
		seekKey.Timestamp = timestamp
		if seekKey.Timestamp == roachpb.ZeroTimestamp {
			return nil, ignoredIntents, nil
		}
	}

	if !iter.unsafeKey().Equal(seekKey) {
		iter.Seek(seekKey)
	}
	if !iter.Valid() {
		if err := iter.Error(); err != nil {
			return nil, nil, err
		}
		return nil, ignoredIntents, nil
	}

	unsafeKey := iter.unsafeKey()
	if !unsafeKey.Key.Equal(metaKey.Key) {
		return nil, ignoredIntents, nil
	}
	if !unsafeKey.IsValue() {
		return nil, nil, util.Errorf("expected scan to versioned value reading key %s; got %s %s",
			metaKey.Key, unsafeKey, unsafeKey.Timestamp)
	}

	if checkValueTimestamp {
		if timestamp.Less(unsafeKey.Timestamp) {
			// Fourth case: Our read timestamp is sufficiently behind the newest
			// value, but there is another previous write with the same issues as in
			// the second case, so the reader will have to come again with a higher
			// read timestamp.
			return nil, nil, roachpb.NewReadWithinUncertaintyIntervalError(
				timestamp, unsafeKey.Timestamp)
		}
		// Fifth case: There's no value in our future up to MaxTimestamp, and those
		// are the only ones that we're not certain about. The correct key has
		// already been read above, so there's nothing left to do.
	}

	if len(iter.unsafeValue()) == 0 {
		// Value is deleted.
		return nil, ignoredIntents, nil
	}

	value := &buf.value
	value.RawBytes = iter.Value()
	value.Timestamp = unsafeKey.Timestamp
	if err := value.Verify(metaKey.Key); err != nil {
		return nil, nil, err
	}
	return value, ignoredIntents, nil
}

// putBuffer holds pointer data needed by mvccPutInternal. Bundling
// this data into a single structure reduces memory
// allocations. Managing this temporary buffer using a sync.Pool
// completely eliminates allocation from the put common path.
type putBuffer struct {
	meta    MVCCMetadata
	newMeta MVCCMetadata
	newTxn  roachpb.TxnMeta
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
	*b = putBuffer{}
	putBufferPool.Put(b)
}

// MVCCPut sets the value for a specified key. It will save the value
// with different versions according to its timestamp and update the
// key metadata. The timestamp must be passed as a parameter; using
// the Timestamp field on the value results in an error.
//
// If the timestamp is specified as roachpb.ZeroTimestamp, the value is
// inlined instead of being written as a timestamp-versioned value. A
// zero timestamp write to a key precludes a subsequent write using a
// non-zero timestamp and vice versa. Inlined values require only a
// single row and never accumulate more than a single value. Successive
// zero timestamp writes to a key replace the value and deletes clear
// the value. In addition, zero timestamp values may be merged.
func MVCCPut(
	ctx context.Context,
	engine Engine,
	ms *MVCCStats,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
) error {
	iter := engine.NewIterator(key)
	defer iter.Close()

	return mvccPutUsingIter(ctx, engine, iter, ms, key, timestamp, value, txn, nil /* valueFn */)
}

// MVCCDelete marks the key deleted so that it will not be returned in
// future get responses.
func MVCCDelete(
	ctx context.Context,
	engine Engine,
	ms *MVCCStats,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	txn *roachpb.Transaction,
) error {
	iter := engine.NewIterator(key)
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
	engine Engine,
	iter Iterator,
	ms *MVCCStats,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	value roachpb.Value,
	txn *roachpb.Transaction,
	valueFn func(*roachpb.Value) ([]byte, error),
) error {
	var rawBytes []byte
	if valueFn == nil {
		if value.Timestamp != roachpb.ZeroTimestamp {
			return util.Errorf("cannot have timestamp set in value on Put")
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

// mvccPutInternal adds a new timestamped value to the specified key.
// If value is nil, creates a deletion tombstone value. valueFn is
// an optional alternative to supplying value directly. It is passed
// the existing value (or nil if none exists) and returns the value
// to write or an error. If valueFn is supplied, value should be nil
// and vice versa. valueFn can delete by returning nil. Returning
// []byte{} will write an empty value, not delete.
func mvccPutInternal(
	ctx context.Context,
	engine Engine,
	iter Iterator,
	ms *MVCCStats,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
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

	// maybeGetValue returns either value (if valueFn is nil) or else
	// the result of calling valueFn on the data read at readTS.
	maybeGetValue := func(exists bool, readTS roachpb.Timestamp) ([]byte, error) {
		// If a valueFn is specified, read existing value using the iter.
		if valueFn == nil {
			return value, nil
		}
		var exVal *roachpb.Value
		if exists {
			getBuf := newGetBuffer()
			defer getBuf.release()
			getBuf.meta = buf.meta // initialize get metadata from what we've already read
			if exVal, _, err = mvccGetInternal(ctx, iter, metaKey, readTS, true /* consistent */, txn, getBuf); err != nil {
				return nil, err
			}
		}
		return valueFn(exVal)
	}

	// Verify we're not mixing inline and non-inline values.
	putIsInline := timestamp.Equal(roachpb.ZeroTimestamp)
	if ok && putIsInline != buf.meta.IsInline() {
		return util.Errorf("%q: put is inline=%t, but existing value is inline=%t",
			metaKey, putIsInline, buf.meta.IsInline())
	}
	// Handle inline put.
	if putIsInline {
		if txn != nil {
			return util.Errorf("%q: inline writes not allowed within transactions", metaKey)
		}
		var metaKeySize, metaValSize int64
		if value, err = maybeGetValue(ok, timestamp); err != nil {
			return err
		}
		if value == nil {
			metaKeySize, metaValSize, err = 0, 0, engine.Clear(metaKey)
		} else {
			buf.meta = MVCCMetadata{RawBytes: value}
			metaKeySize, metaValSize, err = PutProto(engine, metaKey, &buf.meta)
		}
		if ms != nil {
			updateStatsForInline(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize)
		}
		return err
	}

	var meta *MVCCMetadata
	var maybeTooOldErr error
	if ok {
		// There is existing metadata for this key; ensure our write is permitted.
		meta = &buf.meta

		if meta.Txn != nil {
			// There is an uncommitted write intent.
			if txn == nil || !roachpb.TxnIDEqual(meta.Txn.ID, txn.ID) {
				// The current Put operation does not come from the same
				// transaction.
				return &roachpb.WriteIntentError{Intents: []roachpb.Intent{{Span: roachpb.Span{Key: key}, Status: roachpb.PENDING, Txn: *meta.Txn}}}
			} else if txn.Epoch < meta.Txn.Epoch {
				return util.Errorf("put with epoch %d came after put with epoch %d in txn %s",
					txn.Epoch, meta.Txn.Epoch, txn.ID)
			} else if txn.Sequence < meta.Txn.Sequence ||
				(txn.Sequence == meta.Txn.Sequence && txn.BatchIndex <= meta.Txn.BatchIndex) {
				// Replay error if we encounter an older sequence number or
				// the same (or earlier) batch index for the same sequence.
				return roachpb.NewTransactionRetryError()
			}
			// Make sure we process valueFn before clearing any earlier
			// version.  For example, a conditional put within same
			// transaction should read previous write.
			if value, err = maybeGetValue(ok, timestamp); err != nil {
				return err
			}
			// We are replacing our own older write intent. If we are
			// writing at the same timestamp we can simply overwrite it;
			// otherwise we must explicitly delete the obsolete intent.
			if !timestamp.Equal(meta.Timestamp) {
				versionKey := metaKey
				versionKey.Timestamp = meta.Timestamp
				if err = engine.Clear(versionKey); err != nil {
					return err
				}
			}
		} else if !meta.Timestamp.Less(timestamp) {
			// This is the case where we're trying to write under a
			// committed value. Obviously we can't do that, but we can
			// increment our timestamp to one logical tick past the existing
			// value and go on to write, but then return a write-too-old
			// error indicating what the timestamp ended up being. This
			// timestamp can then be used to increment the txn timestamp and
			// be returned with the response.
			actualTimestamp := meta.Timestamp.Next()
			maybeTooOldErr = &roachpb.WriteTooOldError{Timestamp: timestamp, ActualTimestamp: actualTimestamp}
			// If we're in a transaction, always get the value at the orig
			// timestamp.
			if txn != nil {
				if value, err = maybeGetValue(ok, timestamp); err != nil {
					return err
				}
			} else {
				// Outside of a transaction, read the latest value and advance
				// the write timestamp to the latest value's timestamp + 1. The
				// new timestamp is returned to the caller in maybeTooOldErr.
				if value, err = maybeGetValue(ok, actualTimestamp); err != nil {
					return err
				}
			}
			timestamp = actualTimestamp
		} else {
			if value, err = maybeGetValue(ok, timestamp); err != nil {
				return err
			}
		}
	} else {
		if value, err = maybeGetValue(ok, timestamp); err != nil {
			return err
		}
		// No existing metadata record. If this is a delete, do nothing;
		// otherwise we can perform the write.
		if value == nil {
			return nil
		}
	}
	{
		var txnMeta *roachpb.TxnMeta
		if txn != nil {
			txnMeta = &txn.TxnMeta
		}
		buf.newMeta = MVCCMetadata{Txn: txnMeta, Timestamp: timestamp}
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
		metaKeySize, metaValSize, err = PutProto(engine, metaKey, newMeta)
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
		ms.Add(updateStatsOnPut(key, origMetaKeySize, origMetaValSize,
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
	engine Engine,
	ms *MVCCStats,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	txn *roachpb.Transaction,
	inc int64,
) (int64, error) {
	iter := engine.NewIterator(key)
	defer iter.Close()

	var int64Val int64
	err := mvccPutUsingIter(ctx, engine, iter, ms, key, timestamp, noValue, txn, func(value *roachpb.Value) ([]byte, error) {
		if value != nil {
			var err error
			if int64Val, err = value.GetInt(); err != nil {
				return nil, util.Errorf("key %q does not contain an integer value", key)
			}
		}

		// Check for overflow and underflow.
		if willOverflow(int64Val, inc) {
			return nil, util.Errorf("key %s with value %d incremented by %d results in overflow", key, int64Val, inc)
		}

		int64Val = int64Val + inc
		newValue := roachpb.Value{}
		newValue.SetInt(int64Val)
		newValue.InitChecksum(key)
		return newValue.RawBytes, nil
	})

	return int64Val, err
}

// MVCCConditionalPut sets the value for a specified key only if the
// expected value matches. If not, the return a ConditionFailedError
// containing the actual value.
//
// The condition check reads a value from the key using the same operational
// timestamp as we use to write a value.
func MVCCConditionalPut(
	ctx context.Context,
	engine Engine,
	ms *MVCCStats,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	value roachpb.Value,
	expVal *roachpb.Value,
	txn *roachpb.Transaction,
) error {
	iter := engine.NewIterator(key)
	defer iter.Close()

	return mvccPutUsingIter(ctx, engine, iter, ms, key, timestamp, noValue, txn, func(existVal *roachpb.Value) ([]byte, error) {
		if expValPresent, existValPresent := expVal != nil, existVal != nil; expValPresent && existValPresent {
			// Every type flows through here, so we can't use the typed getters.
			if !bytes.Equal(expVal.RawBytes, existVal.RawBytes) {
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

// MVCCMerge implements a merge operation. Merge adds integer values,
// concatenates undifferentiated byte slice values, and efficiently
// combines time series observations if the roachpb.Value tag value
// indicates the value byte slice is of type TIMESERIES.
func MVCCMerge(
	ctx context.Context,
	engine Engine,
	ms *MVCCStats,
	key roachpb.Key,
	timestamp roachpb.Timestamp,
	value roachpb.Value,
) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	metaKey := MakeMVCCMetadataKey(key)

	// Every type flows through here, so we can't use the typed getters.
	rawBytes := value.RawBytes

	// Encode and merge the MVCC metadata with inlined value.
	meta := &MVCCMetadata{RawBytes: rawBytes}
	// If non-zero, set the merge timestamp to provide some replay protection.
	if !timestamp.Equal(roachpb.ZeroTimestamp) {
		meta.MergeTimestamp = &timestamp
	}
	data, err := protoutil.Marshal(meta)
	if err != nil {
		return err
	}
	if err := engine.Merge(metaKey, data); err != nil {
		return err
	}
	if ms != nil {
		ms.Add(updateStatsOnMerge(key, int64(len(rawBytes))+mvccVersionTimestampSize, timestamp.WallTime))
	}
	return nil
}

// MVCCDeleteRange deletes the range of key/value pairs specified by
// start and end keys. Specify max=0 for unbounded deletes.
func MVCCDeleteRange(
	ctx context.Context,
	engine Engine,
	ms *MVCCStats,
	key,
	endKey roachpb.Key,
	max int64,
	timestamp roachpb.Timestamp,
	txn *roachpb.Transaction,
	returnKeys bool,
) ([]roachpb.Key, error) {
	var keys []roachpb.Key
	num := int64(0)
	buf := newPutBuffer()
	iter := engine.NewIterator(endKey)
	f := func(kv roachpb.KeyValue) (bool, error) {
		if err := mvccPutInternal(ctx, engine, iter, ms, kv.Key, timestamp, nil, txn, buf, nil); err != nil {
			return true, err
		}
		if returnKeys {
			keys = append(keys, kv.Key)
		}
		num++
		// We check num rather than len(keys) since returnKeys could be false.
		if max != 0 && max >= num {
			return true, nil
		}
		return false, nil
	}

	// In order to detect the potential write intent by another
	// concurrent transaction with a newer timestamp, we need
	// to use the max timestamp for scan.
	_, err := MVCCIterate(ctx, engine, key, endKey, roachpb.MaxTimestamp, true, txn, false, f)

	iter.Close()
	buf.release()
	return keys, err
}

func getScanMeta(iter Iterator, encEndKey MVCCKey, meta *MVCCMetadata) (MVCCKey, error) {
	metaKey := iter.Key()
	if !metaKey.Less(encEndKey) {
		return NilKey, iter.Error()
	}
	if metaKey.IsValue() {
		meta.Reset()
		meta.Timestamp = metaKey.Timestamp
		// For values, the size of keys is always account for as
		// mvccVersionTimestampSize. The size of the metadata key is accounted for
		// separately.
		meta.KeyBytes = mvccVersionTimestampSize
		meta.ValBytes = int64(len(iter.unsafeValue()))
		meta.Deleted = len(iter.unsafeValue()) == 0
		return metaKey, nil
	}
	if err := iter.ValueProto(meta); err != nil {
		return NilKey, err
	}
	return metaKey, nil
}

func getReverseScanMeta(iter Iterator, encEndKey MVCCKey, meta *MVCCMetadata) (MVCCKey, error) {
	metaKey := iter.Key()
	// The metaKey < encEndKey is exceeding the boundary.
	if metaKey.Less(encEndKey) {
		return NilKey, iter.Error()
	}

	// If this isn't the meta key yet, scan again to get the meta key.
	// TODO(tschottdorf): can we save any work here or leverage
	// getScanMetaKey() above after doing the Seek() below?
	if metaKey.IsValue() {
		// The row with oldest version will be got by seeking reversely. We use the
		// key of this row to get the MVCC metadata key.
		iter.Seek(MakeMVCCMetadataKey(metaKey.Key))
		if !iter.Valid() {
			return NilKey, iter.Error()
		}

		meta.Reset()
		metaKey = iter.Key()
		meta.Timestamp = metaKey.Timestamp
		if metaKey.IsValue() {
			// For values, the size of keys is always account for as
			// mvccVersionTimestampSize. The size of the metadata key is accounted
			// for separately.
			meta.KeyBytes = mvccVersionTimestampSize
			meta.ValBytes = int64(len(iter.unsafeValue()))
			meta.Deleted = len(iter.unsafeValue()) == 0
			return metaKey, nil
		}
	}
	if err := iter.ValueProto(meta); err != nil {
		return NilKey, err
	}
	return metaKey, nil
}

// mvccScanInternal scans the key range [start,end) up to some maximum number
// of results. Specify max=0 for unbounded scans. Specify reverse=true to scan
// in descending instead of ascending order.
func mvccScanInternal(
	ctx context.Context,
	engine Engine,
	key,
	endKey roachpb.Key,
	max int64,
	timestamp roachpb.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
	reverse bool,
) ([]roachpb.KeyValue, []roachpb.Intent, error) {
	var res []roachpb.KeyValue
	intents, err := MVCCIterate(ctx, engine, key, endKey, timestamp, consistent, txn, reverse,
		func(kv roachpb.KeyValue) (bool, error) {
			res = append(res, kv)
			if max != 0 && max == int64(len(res)) {
				return true, nil
			}
			return false, nil
		})

	if err != nil {
		return nil, nil, err
	}
	return res, intents, nil
}

// MVCCScan scans the key range [start,end) key up to some maximum number of
// results in ascending order. Specify max=0 for unbounded scans.
func MVCCScan(
	ctx context.Context,
	engine Engine,
	key,
	endKey roachpb.Key,
	max int64,
	timestamp roachpb.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
) ([]roachpb.KeyValue, []roachpb.Intent, error) {
	return mvccScanInternal(ctx, engine, key, endKey, max, timestamp,
		consistent, txn, false /* !reverse */)
}

// MVCCReverseScan scans the key range [start,end) key up to some maximum number of
// results in descending order. Specify max=0 for unbounded scans.
func MVCCReverseScan(
	ctx context.Context,
	engine Engine,
	key,
	endKey roachpb.Key,
	max int64,
	timestamp roachpb.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
) ([]roachpb.KeyValue, []roachpb.Intent, error) {
	return mvccScanInternal(ctx, engine, key, endKey, max, timestamp,
		consistent, txn, true /* reverse */)
}

// MVCCIterate iterates over the key range [start,end). At each step of the
// iteration, f() is invoked with the current key/value pair. If f returns true
// (done) or an error, the iteration stops and the error is propagated. If the
// reverse is flag set the iterator will be moved in reverse order.
func MVCCIterate(ctx context.Context,
	engine Engine,
	startKey,
	endKey roachpb.Key,
	timestamp roachpb.Timestamp,
	consistent bool,
	txn *roachpb.Transaction,
	reverse bool,
	f func(roachpb.KeyValue) (bool, error),
) ([]roachpb.Intent, error) {
	if !consistent && txn != nil {
		return nil, util.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(endKey) == 0 {
		return nil, emptyKeyError()
	}

	buf := newGetBuffer()
	defer buf.release()

	// getMetaFunc is used to get the meta and the meta key of the current
	// row. encEndKey is used to judge whether iterator exceeds the boundary or
	// not.
	type getMetaFunc func(iter Iterator, encEndKey MVCCKey, meta *MVCCMetadata) (MVCCKey, error)
	var getMeta getMetaFunc

	// We store encEndKey and encKey in the same buffer to avoid memory
	// allocations.
	var encKey, encEndKey MVCCKey
	if reverse {
		encEndKey = MakeMVCCMetadataKey(startKey)
		encKey = MakeMVCCMetadataKey(endKey)
		getMeta = getReverseScanMeta
	} else {
		encEndKey = MakeMVCCMetadataKey(endKey)
		encKey = MakeMVCCMetadataKey(startKey)
		getMeta = getScanMeta
	}

	// Get a new iterator.
	iter := engine.NewIterator(nil)
	defer iter.Close()

	// Seeking for the first defined position.
	if reverse {
		iter.SeekReverse(encKey)
		if !iter.Valid() {
			return nil, iter.Error()
		}

		// If the key doesn't exist, the iterator is at the next key that does
		// exist in the database.
		metaKey := iter.Key()
		if !metaKey.Less(encKey) {
			iter.Prev()
		}
	} else {
		iter.Seek(encKey)
	}

	if !iter.Valid() {
		return nil, iter.Error()
	}

	// A slice to gather all encountered intents we skipped, in case of
	// inconsistent iteration.
	var intents []roachpb.Intent
	// Gathers up all the intents from WriteIntentErrors. We only get those if
	// the scan is consistent.
	var wiErr error

	for {
		metaKey, err := getMeta(iter, encEndKey, &buf.meta)
		if err != nil {
			return nil, err
		}
		// Exceeding the boundary.
		if metaKey.Key == nil {
			break
		}

		value, newIntents, err := mvccGetInternal(ctx, iter, metaKey, timestamp, consistent, txn, buf)
		intents = append(intents, newIntents...)
		if value != nil {
			done, err := f(roachpb.KeyValue{Key: metaKey.Key, Value: *value})
			if err != nil {
				return nil, err
			}
			if done {
				break
			}
		}

		if err != nil {
			switch tErr := err.(type) {
			case *roachpb.WriteIntentError:
				// In the case of WriteIntentErrors, accumulate affected keys but continue scan.
				if wiErr == nil {
					wiErr = tErr
				} else {
					wiErr.(*roachpb.WriteIntentError).Intents = append(wiErr.(*roachpb.WriteIntentError).Intents, tErr.Intents...)
				}
			default:
				return nil, err
			}
		}
		if reverse {
			// Seeking for the position of the given meta key.
			iter.Seek(metaKey)
			if !iter.Valid() {
				if err := iter.Error(); err != nil {
					return nil, err
				}
				break
			}
			// Move the iterator back, which gets us into the previous row (getMeta
			// moves us further back to the meta key).
			iter.Prev()
			if !iter.Valid() {
				if err := iter.Error(); err != nil {
					return nil, err
				}
				break
			}

		} else {
			iter.Seek(MakeMVCCMetadataKey(metaKey.Key.Next()))
			if !iter.Valid() {
				if err := iter.Error(); err != nil {
					return nil, err
				}
				break
			}
		}

	}
	return intents, wiErr
}

// MVCCResolveWriteIntent either commits or aborts (rolls back) an
// extant write intent for a given txn according to commit parameter.
// ResolveWriteIntent will skip write intents of other txns.
//
// Transaction epochs deserve a bit of explanation. The epoch for a
// transaction is incremented on transaction retry. Transaction retry
// is different from abort. Retries occur in SSI transactions when the
// commit timestamp is not equal to the proposed transaction
// timestamp. This might be because writes to different keys had to
// use higher timestamps than expected because of existing, committed
// value, or because reads pushed the transaction's commit timestamp
// forward. Retries also occur in the event that the txn tries to push
// another txn in order to write an intent but fails (i.e. it has
// lower priority).
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
func MVCCResolveWriteIntent(ctx context.Context,
	engine Engine,
	ms *MVCCStats,
	intent roachpb.Intent,
) error {
	buf := newPutBuffer()
	iter := engine.NewIterator(intent.Key)
	err := mvccResolveWriteIntent(ctx, engine, iter, ms, intent, buf)
	// Using defer would be more convenient, but it is measurably slower.
	buf.release()
	iter.Close()
	return err
}

// MVCCResolveWriteIntentUsingIter is a variant of MVCCResolveWriteIntent that
// uses iterator and buffer passed as parameters (e.g. when used in a loop).
func MVCCResolveWriteIntentUsingIter(
	ctx context.Context,
	engine Engine,
	iterAndBuf IterAndBuf,
	ms *MVCCStats,
	intent roachpb.Intent,
) error {
	return mvccResolveWriteIntent(ctx, engine, iterAndBuf.iter, ms,
		intent, iterAndBuf.buf)
}

func mvccResolveWriteIntent(
	ctx context.Context,
	engine Engine,
	iter Iterator,
	ms *MVCCStats,
	intent roachpb.Intent,
	buf *putBuffer,
) error {
	if len(intent.Key) == 0 {
		return emptyKeyError()
	}
	if len(intent.EndKey) > 0 {
		return util.Errorf("can't resolve range intent as point intent")
	}
	metaKey := MakeMVCCMetadataKey(intent.Key)
	meta := &buf.meta
	ok, origMetaKeySize, origMetaValSize, err := mvccGetMetadata(iter, metaKey, meta)
	if err != nil {
		return err
	}
	// For cases where there's no write intent to resolve, or one exists
	// which we can't resolve, this is a noop.
	if !ok || meta.Txn == nil || !roachpb.TxnIDEqual(intent.Txn.ID, meta.Txn.ID) {
		return nil
	}

	// A commit in an older epoch or timestamp is prevented by the
	// sequence cache under normal operation. Replays of EndTransaction
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
	timestampsValid := !intent.Txn.Timestamp.Less(meta.Timestamp)
	commit := intent.Status == roachpb.COMMITTED && epochsMatch && timestampsValid

	// Note the small difference to commit epoch handling here: We allow a push
	// from a previous epoch to move a newer intent. That's not necessary, but
	// useful. Consider the following, where B reads at a timestamp that's
	// higher than any write by A in the following diagram:
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
		meta.Txn.Timestamp.Less(intent.Txn.Timestamp) &&
		meta.Txn.Epoch >= intent.Txn.Epoch

	// If we're committing, or if the commit timestamp of the intent has
	// been moved forward, and if the proposed epoch matches the existing
	// epoch: update the meta.Txn. For commit, it's set to nil;
	// otherwise, we update its value. We may have to update the actual
	// version value (remove old and create new with proper
	// timestamp-encoded key) if timestamp changed.
	if commit || pushed {
		buf.newMeta = *meta
		// Set the timestamp for upcoming write (or at least the stats update).
		buf.newMeta.Timestamp = intent.Txn.Timestamp

		var metaKeySize, metaValSize int64
		var err error
		if pushed {
			// Keep intent if we're pushing timestamp.
			buf.newTxn = intent.Txn
			buf.newMeta.Txn = &buf.newTxn
			metaKeySize, metaValSize, err = PutProto(engine, metaKey, &buf.newMeta)
		} else {
			metaKeySize = int64(metaKey.EncodedSize())
			err = engine.Clear(metaKey)
		}
		if err != nil {
			return err
		}

		// Update stat counters related to resolving the intent.
		if ms != nil {
			ms.Add(updateStatsOnResolve(intent.Key, origMetaKeySize, origMetaValSize,
				metaKeySize, metaValSize, *meta, buf.newMeta, commit))
		}

		// If timestamp of value changed, need to rewrite versioned value.
		if !meta.Timestamp.Equal(intent.Txn.Timestamp) {
			origKey := MVCCKey{Key: intent.Key, Timestamp: meta.Timestamp}
			newKey := MVCCKey{Key: intent.Key, Timestamp: intent.Txn.Timestamp}
			valBytes, err := engine.Get(origKey)
			if err != nil {
				return err
			}
			if err = engine.Clear(origKey); err != nil {
				return err
			}
			if err = engine.Put(newKey, valBytes); err != nil {
				return err
			}
		}
		return nil
	}

	// This method shouldn't be called in this instance, but there's
	// nothing to do if meta's epoch is greater than or equal txn's
	// epoch and the state is still PENDING.
	if intent.Status == roachpb.PENDING && meta.Txn.Epoch >= intent.Txn.Epoch {
		return nil
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
	latestKey := MVCCKey{Key: intent.Key, Timestamp: meta.Timestamp}
	if err := engine.Clear(latestKey); err != nil {
		return err
	}

	// Compute the next possible mvcc value for this key.
	nextKey := latestKey.Next()
	iter.Seek(nextKey)

	// If there is no other version, we should just clean up the key entirely.
	if !iter.Valid() || !iter.unsafeKey().Key.Equal(intent.Key) {
		if err = engine.Clear(metaKey); err != nil {
			return err
		}
		// Clear stat counters attributable to the intent we're aborting.
		if ms != nil {
			ms.Add(updateStatsOnAbort(intent.Key, origMetaKeySize, origMetaValSize, 0, 0, meta, nil, 0, intent.Txn.Timestamp.WallTime))
		}
		return nil
	}

	iterKey := iter.Key()
	if !iterKey.IsValue() {
		return util.Errorf("expected an MVCC value key: %s", iterKey)
	}
	// Get the bytes for the next version so we have size for stat counts.
	valueSize := int64(len(iter.unsafeValue()))
	// Update the keyMetadata with the next version.
	buf.newMeta = MVCCMetadata{
		Deleted:  valueSize == 0,
		KeyBytes: mvccVersionTimestampSize,
		ValBytes: valueSize,
	}
	if err := engine.Clear(metaKey); err != nil {
		return err
	}
	metaKeySize := int64(metaKey.EncodedSize())
	metaValSize := int64(0)

	// Update stat counters with older version.
	if ms != nil {
		ms.Add(updateStatsOnAbort(intent.Key, origMetaKeySize, origMetaValSize,
			metaKeySize, metaValSize, meta, &buf.newMeta, iterKey.Timestamp.WallTime,
			intent.Txn.Timestamp.WallTime))
	}

	return nil
}

// IterAndBuf used to pass iterators and buffers between MVCC* calls, allowing
// reuse without the callers needing to know the particulars.
type IterAndBuf struct {
	buf  *putBuffer
	iter Iterator
}

// GetIterAndBuf returns a IterAndBuf for passing into various MVCC* methods.
func GetIterAndBuf(engine Engine) IterAndBuf {
	return IterAndBuf{
		buf:  newPutBuffer(),
		iter: engine.NewIterator(nil),
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
// txns. Specify max=0 for unbounded resolves.
func MVCCResolveWriteIntentRange(
	ctx context.Context, engine Engine, ms *MVCCStats, intent roachpb.Intent, max int64,
) (int64, error) {
	iterAndBuf := GetIterAndBuf(engine)
	defer iterAndBuf.Cleanup()

	return MVCCResolveWriteIntentRangeUsingIter(ctx, engine, iterAndBuf, ms, intent, max)
}

// MVCCResolveWriteIntentRangeUsingIter commits or aborts (rolls back) the
// range of write intents specified by start and end keys for a given
// txn. ResolveWriteIntentRange will skip write intents of other
// txns. Specify max=0 for unbounded resolves.
func MVCCResolveWriteIntentRangeUsingIter(
	ctx context.Context,
	engine Engine,
	iterAndBuf IterAndBuf,
	ms *MVCCStats,
	intent roachpb.Intent,
	max int64,
) (int64, error) {
	encKey := MakeMVCCMetadataKey(intent.Key)
	encEndKey := MakeMVCCMetadataKey(intent.EndKey)
	nextKey := encKey

	var keyBuf []byte
	num := int64(0)
	intent.EndKey = nil

	for {
		iterAndBuf.iter.Seek(nextKey)
		if !iterAndBuf.iter.Valid() || !iterAndBuf.iter.unsafeKey().Less(encEndKey) {
			// No more keys exists in the given range.
			break
		}

		// Manually copy the underlying bytes of the unsafe key. This construction
		// reuses keyBuf across iterations.
		key := iterAndBuf.iter.unsafeKey()
		keyBuf = append(keyBuf[:0], key.Key...)
		key.Key = keyBuf

		var err error
		if !key.IsValue() {
			intent.Key = key.Key
			err = mvccResolveWriteIntent(ctx, engine, iterAndBuf.iter, ms, intent, iterAndBuf.buf)
		}
		if err != nil {
			log.Warningf("failed to resolve intent for key %q: %v", key.Key, err)
		} else {
			num++
			if max != 0 && max == num {
				break
			}
		}

		// nextKey is already a metadata key.
		nextKey.Key = key.Key.Next()
	}

	return num, nil
}

// MVCCGarbageCollect creates an iterator on the engine. In parallel
// it iterates through the keys listed for garbage collection by the
// keys slice. The engine iterator is seeked in turn to each listed
// key, clearing all values with timestamps <= to expiration.
// The timestamp parameter is used to compute the intent age on GC.
func MVCCGarbageCollect(
	ctx context.Context,
	engine Engine,
	ms *MVCCStats,
	keys []roachpb.GCRequest_GCKey,
	timestamp roachpb.Timestamp,
) error {
	iter := engine.NewIterator(nil)
	defer iter.Close()
	// Iterate through specified GC keys.
	meta := &MVCCMetadata{}
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
		implicitMeta := iter.Key().IsValue()
		// First, check whether all values of the key are being deleted.
		if !gcKey.Timestamp.Less(meta.Timestamp) {
			// For version keys, don't allow GC'ing the meta key if it's
			// not marked deleted. However, for inline values we allow it;
			// they are internal and GCing them directly saves the extra
			// deletion step.
			if !meta.Deleted && !inlinedValue {
				return util.Errorf("request to GC non-deleted, latest value of %q", gcKey.Key)
			}
			if meta.Txn != nil {
				return util.Errorf("request to GC intent at %q", gcKey.Key)
			}
			if ms != nil {
				if inlinedValue {
					updateStatsForInline(ms, gcKey.Key, metaKeySize, metaValSize, 0, 0)
					ms.AgeTo(timestamp.WallTime)
				} else {
					ms.Add(updateStatsOnGC(gcKey.Key, metaKeySize, metaValSize, meta, meta.Timestamp.WallTime, timestamp.WallTime))
				}
			}
			if !implicitMeta {
				if err := engine.Clear(iter.Key()); err != nil {
					return err
				}
			}
		}

		if !implicitMeta {
			// The iter is pointing at an MVCCMetadata, advance to the next entry.
			iter.Next()
		}

		// Now, iterate through all values, GC'ing ones which have expired.
		for ; iter.Valid(); iter.Next() {
			iterKey := iter.Key()
			if !iterKey.Key.Equal(encKey.Key) {
				break
			}
			if !iterKey.IsValue() {
				break
			}
			if !gcKey.Timestamp.Less(iterKey.Timestamp) {
				if ms != nil {
					ms.Add(updateStatsOnGC(gcKey.Key, mvccVersionTimestampSize, int64(len(iter.Value())), nil, iterKey.Timestamp.WallTime, timestamp.WallTime))
				}
				if err := engine.Clear(iterKey); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// IsValidSplitKey returns whether the key is a valid split key. Certain key
// ranges cannot be split (the meta1 span and the system DB span); split keys
// chosen within any of these ranges are considered invalid. And a split key
// equal to Meta2KeyMax (\x03\xff\xff) is considered invalid.
func IsValidSplitKey(key roachpb.Key) bool {
	// TODO(peter): What is this restriction about? Document.
	if keys.Meta2KeyMax.Equal(key) {
		return false
	}
	for _, span := range keys.NoSplitSpans {
		if bytes.Compare(key, span.Key) >= 0 && bytes.Compare(key, span.EndKey) < 0 {
			return false
		}
	}
	return true
}

// MVCCFindSplitKey suggests a split key from the given user-space key
// range that aims to roughly cut into half the total number of bytes
// used (in raw key and value byte strings) in both subranges. Specify
// a snapshot engine to safely invoke this method in a goroutine.
//
// The split key will never be chosen from the key ranges listed in
// illegalSplitKeySpans.
//
// debugFn, if not nil, is used to print informational log messages about
// the key finding process.
func MVCCFindSplitKey(
	ctx context.Context,
	engine Engine,
	rangeID roachpb.RangeID,
	key,
	endKey roachpb.RKey,
	debugFn func(msg string, args ...interface{}),
) (roachpb.Key, error) {
	if key.Less(roachpb.RKey(keys.LocalMax)) {
		key = roachpb.RKey(keys.LocalMax)
	}

	logf := func(msg string, args ...interface{}) {
		if debugFn != nil {
			debugFn(msg, args...)
		} else if log.V(2) {
			log.Infof("FindSplitKey["+rangeID.String()+"] "+msg, args...)
		}
	}

	encStartKey := MakeMVCCMetadataKey(key.AsRawKey())
	encEndKey := MakeMVCCMetadataKey(endKey.AsRawKey())

	logf("searching split key for %d [%s, %s)", rangeID, key, endKey)

	// Get range size from stats.
	var ms MVCCStats
	if err := MVCCGetRangeStats(ctx, engine, rangeID, &ms); err != nil {
		return nil, err
	}

	rangeSize := ms.KeyBytes + ms.ValBytes
	targetSize := rangeSize / 2

	logf("range size: %s, targetSize %s", humanize.IBytes(uint64(rangeSize)), humanize.IBytes(uint64(targetSize)))

	sizeSoFar := int64(0)
	bestSplitKey := encStartKey
	bestSplitDiff := int64(math.MaxInt64)
	var lastKey roachpb.Key

	if err := engine.Iterate(encStartKey, encEndKey, func(kv MVCCKeyValue) (bool, error) {
		// Is key within a legal key range?
		valid := IsValidSplitKey(kv.Key.Key)

		// Determine if this key would make a better split than last "best" key.
		diff := targetSize - sizeSoFar
		if diff < 0 {
			diff = -diff
		}
		if valid && diff < bestSplitDiff {
			logf("better split: diff %d at %s", diff, kv.Key)
			bestSplitKey = kv.Key
			bestSplitDiff = diff
		}

		// Determine whether we've found best key and can exit iteration.
		done := !bestSplitKey.Key.Equal(encStartKey.Key) && diff > bestSplitDiff
		if done {
			logf("target size reached")
		}

		// Add this key/value to the size scanned so far.
		if kv.Key.IsValue() && bytes.Equal(kv.Key.Key, lastKey) {
			sizeSoFar += mvccVersionTimestampSize + int64(len(kv.Value))
		} else {
			sizeSoFar += int64(kv.Key.EncodedSize() + len(kv.Value))
		}
		lastKey = kv.Key.Key

		return done, nil
	}); err != nil {
		return nil, err
	}

	if bestSplitKey.Key.Equal(encStartKey.Key) {
		return nil, util.Errorf("the range cannot be split; considered range %q-%q has no valid splits", key, endKey)
	}

	// The key is an MVCC (versioned) key, so to avoid corrupting MVCC we only
	// return the base portion, which is fine to split in front of.
	return bestSplitKey.Key, nil
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
