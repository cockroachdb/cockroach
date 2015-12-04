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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"bytes"
	"fmt"
	"math"
	"sync"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
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

type encodedSpan struct {
	start, end MVCCKey
}

// MVCCKeyValue contains the raw bytes of the value for a key.
type MVCCKeyValue struct {
	Key   MVCCKey
	Value []byte
}

// illegalSplitKeySpans lists spans of keys that should not be split.
var illegalSplitKeySpans []encodedSpan

func init() {
	for _, r := range keys.NoSplitSpans {
		illegalSplitKeySpans = append(illegalSplitKeySpans,
			encodedSpan{
				start: MakeMVCCMetadataKey(r.Key),
				end:   MakeMVCCMetadataKey(r.EndKey),
			})
	}
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
	return meta.Txn != nil && txn != nil && bytes.Equal(meta.Txn.ID, txn.ID)
}

// Delta returns the difference between two MVCCStats structures.
func (ms *MVCCStats) Delta(oms *MVCCStats) MVCCStats {
	result := *ms
	result.Subtract(oms)
	return result
}

// Add adds values from oms to ms.
func (ms *MVCCStats) Add(oms *MVCCStats) {
	ms.LiveBytes += oms.LiveBytes
	ms.KeyBytes += oms.KeyBytes
	ms.ValBytes += oms.ValBytes
	ms.IntentBytes += oms.IntentBytes
	ms.LiveCount += oms.LiveCount
	ms.KeyCount += oms.KeyCount
	ms.ValCount += oms.ValCount
	ms.IntentCount += oms.IntentCount
	ms.IntentAge += oms.IntentAge
	ms.GCBytesAge += oms.GCBytesAge
	ms.SysBytes += oms.SysBytes
	ms.SysCount += oms.SysCount
	if oms.LastUpdateNanos > ms.LastUpdateNanos {
		ms.LastUpdateNanos = oms.LastUpdateNanos
	}
}

// Subtract subtracts the values of oms from ms.
func (ms *MVCCStats) Subtract(oms *MVCCStats) {
	ms.LiveBytes -= oms.LiveBytes
	ms.KeyBytes -= oms.KeyBytes
	ms.ValBytes -= oms.ValBytes
	ms.IntentBytes -= oms.IntentBytes
	ms.LiveCount -= oms.LiveCount
	ms.KeyCount -= oms.KeyCount
	ms.ValCount -= oms.ValCount
	ms.IntentCount -= oms.IntentCount
	ms.IntentAge -= oms.IntentAge
	ms.GCBytesAge -= oms.GCBytesAge
	ms.SysBytes -= oms.SysBytes
	ms.SysCount -= oms.SysCount
	if oms.LastUpdateNanos > ms.LastUpdateNanos {
		ms.LastUpdateNanos = oms.LastUpdateNanos
	}
}

// updateStatsForKey returns whether or not the bytes and counts for
// the specified key should be tracked at all, and if so, whether the
// key is system-local.
func updateStatsForKey(ms *MVCCStats, key roachpb.Key) (bool, bool) {
	return ms != nil, key.Compare(keys.LocalMax) < 0
}

// updateStatsForInline updates stat counters for an inline value.
// These are simpler as they don't involve intents or multiple
// versions.
func updateStatsForInline(ms *MVCCStats, key roachpb.Key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize int64) {
	ok, sys := updateStatsForKey(ms, key)
	if !ok {
		return
	}
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
// value.Bytes byte slice. These errors are corrected during splits
// and merges.
func updateStatsOnMerge(ms *MVCCStats, key roachpb.Key, valSize int64) {
	ok, sys := updateStatsForKey(ms, key)
	if !ok {
		return
	}
	if sys {
		ms.SysBytes += valSize
	} else {
		ms.LiveBytes += valSize
		ms.ValBytes += valSize
	}
}

// updateStatsOnPut updates stat counters for a newly put value,
// including both the metadata key & value bytes and the mvcc
// versioned value's key & value bytes. If the value is not a
// deletion tombstone, updates the live stat counters as well.
// If this value is an intent, updates the intent counters.
func updateStatsOnPut(ms *MVCCStats, key roachpb.Key, origMetaKeySize, origMetaValSize,
	metaKeySize, metaValSize int64, orig, meta *MVCCMetadata, origAgeSeconds int64) {
	ok, sys := updateStatsForKey(ms, key)
	if !ok {
		return
	}
	// Remove current live counts for this key.
	if orig != nil {
		if sys {
			ms.SysBytes -= (origMetaKeySize + origMetaValSize)
			ms.SysCount--
		} else {
			// If original version value for this key wasn't deleted, subtract
			// its contribution from live bytes in anticipation of adding in
			// contribution from new version below.
			if !orig.Deleted {
				ms.LiveBytes -= orig.KeyBytes + orig.ValBytes + origMetaKeySize + origMetaValSize
				ms.LiveCount--
				// Also, add the bytes from overwritten value to the GC'able bytes age stat.
				ms.GCBytesAge += MVCCComputeGCBytesAge(orig.KeyBytes+orig.ValBytes, origAgeSeconds)
			} else {
				// Remove the meta byte previously counted for deleted value from GC'able bytes age stat.
				ms.GCBytesAge -= MVCCComputeGCBytesAge(origMetaKeySize+origMetaValSize, origAgeSeconds)
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
				ms.IntentAge -= origAgeSeconds
			}
		}
	}

	// If new version isn't a deletion tombstone, add it to live counters.
	if sys {
		ms.SysBytes += meta.KeyBytes + meta.ValBytes + metaKeySize + metaValSize
		ms.SysCount++
	} else {
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
}

// updateStatsOnResolve updates stat counters with the difference
// between the original and new metadata sizes. The size of the
// resolved value (key & bytes) are subtracted from the intents
// counters if commit=true.
func updateStatsOnResolve(ms *MVCCStats, key roachpb.Key, origMetaKeySize, origMetaValSize,
	metaKeySize, metaValSize int64, meta *MVCCMetadata, commit bool, origAgeSeconds int64) {
	ok, sys := updateStatsForKey(ms, key)
	if !ok {
		return
	}
	// We're pushing or committing an intent; update counts with
	// difference in bytes between old metadata and new.
	keyDiff := metaKeySize - origMetaKeySize
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
			ms.IntentAge -= origAgeSeconds
		}
	}
}

// updateStatsOnAbort updates stat counters by subtracting an
// aborted value's key and value byte sizes. If an earlier version
// was restored, the restored values are added to live bytes and
// count if the restored value isn't a deletion tombstone.
func updateStatsOnAbort(ms *MVCCStats, key roachpb.Key, origMetaKeySize, origMetaValSize,
	restoredMetaKeySize, restoredMetaValSize int64, orig, restored *MVCCMetadata,
	origAgeSeconds, restoredAgeSeconds int64) {
	ok, sys := updateStatsForKey(ms, key)
	if !ok {
		return
	}
	origTotalBytes := orig.KeyBytes + orig.ValBytes + origMetaKeySize + origMetaValSize
	if sys {
		ms.SysBytes -= origTotalBytes
		ms.SysCount--
	} else {
		if !orig.Deleted {
			ms.LiveBytes -= origTotalBytes
			ms.LiveCount--
		} else {
			// Remove the bytes from previously deleted intent from the GC'able bytes age stat.
			ms.GCBytesAge -= MVCCComputeGCBytesAge(origTotalBytes, origAgeSeconds)
		}
		ms.KeyBytes -= (orig.KeyBytes + origMetaKeySize)
		ms.ValBytes -= (orig.ValBytes + origMetaValSize)
		ms.KeyCount--
		ms.ValCount--
		ms.IntentBytes -= (orig.KeyBytes + orig.ValBytes)
		ms.IntentCount--
		ms.IntentAge -= origAgeSeconds
	}
	// If restored version isn't a deletion tombstone, add it to live counters.
	if restored != nil {
		if sys {
			ms.SysBytes += restoredMetaKeySize + restoredMetaValSize
			ms.SysCount++
		} else {
			if !restored.Deleted {
				ms.LiveBytes += restored.KeyBytes + restored.ValBytes + restoredMetaKeySize + restoredMetaValSize
				ms.LiveCount++
				// Also, remove the bytes from previously overwritten value from the GC'able bytes age stat.
				ms.GCBytesAge -= MVCCComputeGCBytesAge(restored.KeyBytes+restored.ValBytes, restoredAgeSeconds)
			} else {
				// Add back in the meta key/value bytes to GC'able bytes age stat.
				ms.GCBytesAge += MVCCComputeGCBytesAge(restoredMetaKeySize+restoredMetaValSize, restoredAgeSeconds)
			}
			ms.KeyBytes += restoredMetaKeySize
			ms.ValBytes += restoredMetaValSize
			ms.KeyCount++
			if restored.Txn != nil {
				panic("restored version should never be an intent")
			}
		}
	}
}

// updateStatsOnGC updates stat counters after garbage collection
// by subtracting key and value byte counts, updating key and
// value counts, and updating the GC'able bytes age. If meta is
// not nil, then the value being GC'd is the mvcc metadata and we
// decrement the key count.
func updateStatsOnGC(ms *MVCCStats, key roachpb.Key, keySize, valSize int64, meta *MVCCMetadata, ageSeconds int64) {
	ok, sys := updateStatsForKey(ms, key)
	if !ok {
		return
	}
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
		ms.GCBytesAge -= MVCCComputeGCBytesAge(keySize+valSize, ageSeconds)
	}
}

// MVCCComputeGCBytesAge comptues the value to assign to the specified
// number of bytes, at the given age (in seconds).
func MVCCComputeGCBytesAge(bytes, ageSeconds int64) int64 {
	return bytes * ageSeconds
}

// MVCCGetRangeStats reads stat counters for the specified range and
// sets the values in the supplied MVCCStats struct.
func MVCCGetRangeStats(engine Engine, rangeID roachpb.RangeID, ms *MVCCStats) error {
	_, err := MVCCGetProto(engine, keys.RangeStatsKey(rangeID), roachpb.ZeroTimestamp, true, nil, ms)
	return err
}

// MVCCSetRangeStats sets stat counters for specified range.
func MVCCSetRangeStats(engine Engine, rangeID roachpb.RangeID, ms *MVCCStats) error {
	return MVCCPutProto(engine, nil, keys.RangeStatsKey(rangeID), roachpb.ZeroTimestamp, nil, ms)
}

// MVCCGetProto fetches the value at the specified key and unmarshals
// it using a protobuf decoder. Returns true on success or false if
// the key was not found. In the event of a WriteIntentError when
// consistent=false, we return the error and the decoded result; for
// all other errors (or when consistent=true) the decoded value is
// invalid.
func MVCCGetProto(engine Engine, key roachpb.Key, timestamp roachpb.Timestamp, consistent bool, txn *roachpb.Transaction, msg proto.Message) (bool, error) {
	// TODO(tschottdorf) Consider returning skipped intents to the caller.
	value, _, mvccGetErr := MVCCGet(engine, key, timestamp, consistent, txn)
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
func MVCCPutProto(engine Engine, ms *MVCCStats, key roachpb.Key, timestamp roachpb.Timestamp, txn *roachpb.Transaction, msg proto.Message) error {
	value := roachpb.Value{}
	if err := value.SetProto(msg); err != nil {
		return err
	}
	value.InitChecksum(key)
	return MVCCPut(engine, ms, key, timestamp, value, txn)
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
func MVCCGet(engine Engine, key roachpb.Key, timestamp roachpb.Timestamp, consistent bool, txn *roachpb.Transaction) (*roachpb.Value, []roachpb.Intent, error) {
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}

	buf := getBufferPool.Get().(*getBuffer)
	defer getBufferPool.Put(buf)

	iter := engine.NewIterator(true /* prefix iteration */)
	defer iter.Close()

	metaKey := MakeMVCCMetadataKey(key)
	ok, _, _, err := mvccGetMetadata(iter, metaKey, &buf.meta)
	if !ok || err != nil {
		return nil, nil, err
	}

	value, intents, err := mvccGetInternal(iter, metaKey, timestamp, consistent, txn, buf)
	if value == &buf.value {
		value = &roachpb.Value{}
		*value = buf.value
		buf.value.Reset()
	}
	return value, intents, err
}

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
	// For values, the size of keys is always account for as
	// mvccVersionTimestampSize. The size of the metadata key is accounted for
	// separately.
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
func mvccGetInternal(iter Iterator, metaKey MVCCKey,
	timestamp roachpb.Timestamp, consistent bool, txn *roachpb.Transaction,
	buf *getBuffer) (*roachpb.Value, []roachpb.Intent, error) {
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
			roachpb.Intent{Span: roachpb.Span{Key: metaKey.Key}, Txn: *meta.Txn})
		timestamp = meta.Timestamp.Prev()
	}

	ownIntent := meta.IsIntentOf(txn) // false if txn == nil
	if !timestamp.Less(meta.Timestamp) && meta.Txn != nil && !ownIntent {
		// Trying to read the last value, but it's another transaction's intent;
		// the reader will have to act on this.
		return nil, nil, &roachpb.WriteIntentError{
			Intents: []roachpb.Intent{{Span: roachpb.Span{Key: metaKey.Key}, Txn: *meta.Txn}},
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
			return nil, nil, &roachpb.ReadWithinUncertaintyIntervalError{
				Timestamp:         timestamp,
				ExistingTimestamp: meta.Timestamp,
			}
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
			return nil, nil, &roachpb.ReadWithinUncertaintyIntervalError{
				Timestamp:         timestamp,
				ExistingTimestamp: unsafeKey.Timestamp,
			}
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
	// Set the timestamp if the value is not nil (i.e. not a deletion tombstone).
	ts := unsafeKey.Timestamp
	value.Timestamp = &ts
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
}

var putBufferPool = sync.Pool{
	New: func() interface{} {
		return &putBuffer{}
	},
}

// MVCCPut sets the value for a specified key. It will save the value
// with different versions according to its timestamp and update the
// key metadata. The timestamp must be passed as a parameter; using
// the Timestamp field on the value results in an error.
//
// If the timestamp is specifed as roachpb.ZeroTimestamp, the value is
// inlined instead of being written as a timestamp-versioned value. A
// zero timestamp write to a key precludes a subsequent write using a
// non-zero timestamp and vice versa. Inlined values require only a
// single row and never accumulate more than a single value. Successive
// zero timestamp writes to a key replace the value and deletes clear
// the value. In addition, zero timestamp values may be merged.
func MVCCPut(engine Engine, ms *MVCCStats, key roachpb.Key, timestamp roachpb.Timestamp,
	value roachpb.Value, txn *roachpb.Transaction) error {
	if value.Timestamp != nil {
		return util.Errorf("cannot have timestamp set in value on Put")
	}

	buf := putBufferPool.Get().(*putBuffer)

	err := mvccPutInternal(engine, ms, key, timestamp, value.RawBytes, txn, buf)

	// Using defer would be more convenient, but it is measurably
	// slower.
	putBufferPool.Put(buf)
	return err
}

// MVCCDelete marks the key deleted so that it will not be returned in
// future get responses.
func MVCCDelete(engine Engine, ms *MVCCStats, key roachpb.Key, timestamp roachpb.Timestamp,
	txn *roachpb.Transaction) error {
	buf := putBufferPool.Get().(*putBuffer)

	err := mvccPutInternal(engine, ms, key, timestamp, nil, txn, buf)

	// Using defer would be more convenient, but it is measurably
	// slower.
	putBufferPool.Put(buf)
	return err
}

// mvccPutInternal adds a new timestamped value to the specified key.
// If value is nil, creates a deletion tombstone value.
func mvccPutInternal(engine Engine, ms *MVCCStats, key roachpb.Key, timestamp roachpb.Timestamp,
	value []byte, txn *roachpb.Transaction, buf *putBuffer) error {
	if len(key) == 0 {
		return emptyKeyError()
	}

	iter := engine.NewIterator(true /* prefix iteration */)
	defer iter.Close()

	metaKey := MakeMVCCMetadataKey(key)
	ok, origMetaKeySize, origMetaValSize, err := mvccGetMetadata(iter, metaKey, &buf.meta)
	if err != nil {
		return err
	}

	// Verify we're not mixing inline and non-inline values.
	putIsInline := timestamp.Equal(roachpb.ZeroTimestamp)
	if ok && putIsInline != buf.meta.IsInline() {
		return util.Errorf("%q: put is inline=%t, but existing value is inline=%t",
			metaKey, putIsInline, buf.meta.IsInline())
	}
	if putIsInline {
		var metaKeySize, metaValSize int64
		if value == nil {
			metaKeySize, metaValSize, err = 0, 0, engine.Clear(metaKey)
		} else {
			buf.meta = MVCCMetadata{RawBytes: value}
			metaKeySize, metaValSize, err = PutProto(engine, metaKey, &buf.meta)
		}
		updateStatsForInline(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize)
		return err
	}

	var meta *MVCCMetadata
	var origAgeSeconds int64
	if ok {
		// There is existing metadata for this key; ensure our write is permitted.
		meta = &buf.meta
		origAgeSeconds = timestamp.WallTime/1E9 - meta.Timestamp.WallTime/1E9

		if meta.Txn != nil {
			// There is an uncommitted write intent.

			if txn == nil || !bytes.Equal(meta.Txn.ID, txn.ID) {
				// The current Put operation does not come from the same
				// transaction.
				return &roachpb.WriteIntentError{Intents: []roachpb.Intent{{Span: roachpb.Span{Key: key}, Txn: *meta.Txn}}}
			} else if txn.Epoch < meta.Txn.Epoch {
				return util.Errorf("put with epoch %d came after put with epoch %d in txn %s",
					txn.Epoch, meta.Txn.Epoch, txn.ID)
			}

			// We are replacing our own older write intent. If we are
			// writing at the same timestamp we can simply overwrite it;
			// otherwise we must explicitly delete the obsolete intent.
			if !timestamp.Equal(meta.Timestamp) {
				versionKey := metaKey
				versionKey.Timestamp = meta.Timestamp
				if err := engine.Clear(versionKey); err != nil {
					return err
				}
			}
		} else {
			// No pending transaction, so we can write as long as it is not
			// in the past.
			if !meta.Timestamp.Less(timestamp) {
				return &roachpb.WriteTooOldError{Timestamp: timestamp, ExistingTimestamp: meta.Timestamp}
			}
		}
	} else {
		// No existing metadata record. If this is a delete, do nothing;
		// otherwise we can perform the write.
		if value == nil {
			return nil
		}
	}
	buf.newMeta = MVCCMetadata{Txn: txn, Timestamp: timestamp}
	newMeta := &buf.newMeta

	versionKey := metaKey
	versionKey.Timestamp = timestamp
	if err := engine.Put(versionKey, value); err != nil {
		return err
	}

	// Write the mvcc metadata now that we have sizes for the latest versioned
	// value. For values, the size of keys is always account for as
	// mvccVersionTimestampSize. The size of the metadata key is accounted for
	// separately.
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
	updateStatsOnPut(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, meta, newMeta, origAgeSeconds)

	return nil
}

// MVCCIncrement fetches the value for key, and assuming the value is
// an "integer" type, increments it by inc and stores the new
// value. The newly incremented value is returned.
//
// An initial value is read from the key using the same operational
// timestamp as we use to write a value.
func MVCCIncrement(engine Engine, ms *MVCCStats, key roachpb.Key, timestamp roachpb.Timestamp, txn *roachpb.Transaction, inc int64) (int64, error) {
	// Use the specified timestamp to read the value. When a write
	// with newer timestamp exists, one of the following will
	// happen:
	// - If the read value is not an integer value or overflow
	//   happens, returns an error with an appropriate message.
	// - Otherwise, either a WriteTooOldError or WriteIntentError is returned,
	//   depending on whether the newer write is an intent.
	value, _, err := MVCCGet(engine, key, timestamp, true /* consistent */, txn)
	if err != nil {
		return 0, err
	}

	var int64Val int64
	if value != nil {
		int64Val, err = value.GetInt()
		if err != nil {
			return 0, util.Errorf("key %q does not contain an integer value", key)
		}
	}

	// Check for overflow and underflow.
	if willOverflow(int64Val, inc) {
		return 0, util.Errorf("key %s with value %d incremented by %d results in overflow", key, int64Val, inc)
	}

	// Skip writing the value in the event the value already exists.
	if inc == 0 && value != nil {
		return int64Val, nil
	}

	r := int64Val + inc
	newValue := roachpb.Value{}
	newValue.SetInt(r)
	newValue.InitChecksum(key)
	return r, MVCCPut(engine, ms, key, timestamp, newValue, txn)
}

// MVCCConditionalPut sets the value for a specified key only if the
// expected value matches. If not, the return a ConditionFailedError
// containing the actual value.
//
// The condition check reads a value from the key using the same operational
// timestamp as we use to write a value.
func MVCCConditionalPut(engine Engine, ms *MVCCStats, key roachpb.Key, timestamp roachpb.Timestamp, value roachpb.Value,
	expVal *roachpb.Value, txn *roachpb.Transaction) error {
	// Use the specified timestamp to read the value. When a write
	// with newer timestamp exists, either of the following will
	// happen:
	// - If the conditional check succeeds, either a WriteTooOldError or WriteIntentError
	//   is returned, depending on whether the newer write is an intent.
	// - If the conditional check fails, a ConditionFailedError is returned.
	existVal, _, err := MVCCGet(engine, key, timestamp, true /* consistent */, txn)
	if err != nil {
		return err
	}

	if expValPresent, existValPresent := expVal != nil, existVal != nil; expValPresent && existValPresent {
		// Every type flows through here, so we can't use the typed getters.
		if !bytes.Equal(expVal.RawBytes, existVal.RawBytes) {
			return &roachpb.ConditionFailedError{
				ActualValue: existVal,
			}
		}
	} else if expValPresent != existValPresent {
		return &roachpb.ConditionFailedError{
			ActualValue: existVal,
		}
	}

	return MVCCPut(engine, ms, key, timestamp, value, txn)
}

// MVCCMerge implements a merge operation. Merge adds integer values,
// concatenates undifferentiated byte slice values, and efficiently
// combines time series observations if the roachpb.Value tag value
// indicates the value byte slice is of type TIMESERIES.
func MVCCMerge(engine Engine, ms *MVCCStats, key roachpb.Key, value roachpb.Value) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	metaKey := MakeMVCCMetadataKey(key)

	// Encode and merge the MVCC metadata with inlined value.
	meta := &MVCCMetadata{RawBytes: value.RawBytes}
	data, err := proto.Marshal(meta)
	if err != nil {
		return err
	}
	if err := engine.Merge(metaKey, data); err != nil {
		return err
	}
	// Every type flows through here, so we can't use the typed getters.
	updateStatsOnMerge(ms, key, int64(len(value.RawBytes)))
	return nil
}

// MVCCDeleteRange deletes the range of key/value pairs specified by
// start and end keys. Specify max=0 for unbounded deletes.
func MVCCDeleteRange(engine Engine, ms *MVCCStats, key, endKey roachpb.Key, max int64, timestamp roachpb.Timestamp, txn *roachpb.Transaction) (int64, error) {
	// In order to detect the potential write intent by another
	// concurrent transaction with a newer timestamp, we need
	// to use the max timestamp for scan.
	kvs, _, err := MVCCScan(engine, key, endKey, max, roachpb.MaxTimestamp, true /* consistent */, txn)
	if err != nil {
		return 0, err
	}

	num := int64(0)
	for _, kv := range kvs {
		if err := MVCCDelete(engine, ms, kv.Key, timestamp, txn); err != nil {
			return num, err
		}
		num++
	}
	return num, nil
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
func mvccScanInternal(engine Engine, key, endKey roachpb.Key, max int64, timestamp roachpb.Timestamp,
	consistent bool, txn *roachpb.Transaction, reverse bool) ([]roachpb.KeyValue, []roachpb.Intent, error) {
	var res []roachpb.KeyValue
	intents, err := MVCCIterate(engine, key, endKey, timestamp, consistent, txn, reverse,
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
func MVCCScan(engine Engine, key, endKey roachpb.Key, max int64, timestamp roachpb.Timestamp,
	consistent bool, txn *roachpb.Transaction) ([]roachpb.KeyValue, []roachpb.Intent, error) {
	return mvccScanInternal(engine, key, endKey, max, timestamp,
		consistent, txn, false /* !reverse */)
}

// MVCCReverseScan scans the key range [start,end) key up to some maximum number of
// results in descending order. Specify max=0 for unbounded scans.
func MVCCReverseScan(engine Engine, key, endKey roachpb.Key, max int64, timestamp roachpb.Timestamp,
	consistent bool, txn *roachpb.Transaction) ([]roachpb.KeyValue, []roachpb.Intent, error) {
	return mvccScanInternal(engine, key, endKey, max, timestamp,
		consistent, txn, true /* reverse */)
}

// MVCCIterate iterates over the key range [start,end). At each step of the
// iteration, f() is invoked with the current key/value pair. If f returns true
// (done) or an error, the iteration stops and the error is propagated. If the
// reverse is flag set the iterator will be moved in reverse order.
func MVCCIterate(engine Engine, startKey, endKey roachpb.Key, timestamp roachpb.Timestamp,
	consistent bool, txn *roachpb.Transaction, reverse bool, f func(roachpb.KeyValue) (bool, error)) ([]roachpb.Intent, error) {
	if !consistent && txn != nil {
		return nil, util.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(endKey) == 0 {
		return nil, emptyKeyError()
	}

	buf := getBufferPool.Get().(*getBuffer)
	defer getBufferPool.Put(buf)

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
	iter := engine.NewIterator(false)
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

		value, newIntents, err := mvccGetInternal(iter, metaKey, timestamp, consistent, txn, buf)
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
func MVCCResolveWriteIntent(engine Engine, ms *MVCCStats, key roachpb.Key, timestamp roachpb.Timestamp, txn *roachpb.Transaction) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	if txn == nil {
		return util.Errorf("no txn specified")
	}

	iter := engine.NewIterator(true /* prefix iteration */)
	defer iter.Close()

	metaKey := MakeMVCCMetadataKey(key)
	meta := &MVCCMetadata{}
	ok, origMetaKeySize, origMetaValSize, err := mvccGetMetadata(iter, metaKey, meta)
	if err != nil {
		return err
	}
	// For cases where there's no write intent to resolve, or one exists
	// which we can't resolve, this is a noop.
	if !ok || !txn.Equal(meta.Txn) {
		return nil
	}
	origAgeSeconds := timestamp.WallTime/1E9 - meta.Timestamp.WallTime/1E9

	// If we're committing, or if the commit timestamp of the intent has
	// been moved forward, and if the proposed epoch matches the existing
	// epoch: update the meta.Txn. For commit, it's set to nil;
	// otherwise, we update its value. We may have to update the actual
	// version value (remove old and create new with proper
	// timestamp-encoded key) if timestamp changed.
	commit := txn.Status == roachpb.COMMITTED
	pushed := txn.Status == roachpb.PENDING && meta.Txn.Timestamp.Less(txn.Timestamp)
	if (commit || pushed) && meta.Txn.Epoch == txn.Epoch {
		newMeta := *meta
		var metaKeySize, metaValSize int64
		var err error
		if pushed {
			// Keep intent if we're pushing timestamp.
			newMeta.Timestamp = txn.Timestamp
			newMeta.Txn = txn
			metaKeySize, metaValSize, err = PutProto(engine, metaKey, &newMeta)
		} else {
			metaKeySize = int64(metaKey.EncodedSize())
			err = engine.Clear(metaKey)
		}
		if err != nil {
			return err
		}

		// Update stat counters related to resolving the intent.
		updateStatsOnResolve(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, &newMeta, commit, origAgeSeconds)

		// If timestamp of value changed, need to rewrite versioned value.
		if !meta.Timestamp.Equal(txn.Timestamp) {
			origKey := MVCCKey{Key: key, Timestamp: meta.Timestamp}
			newKey := MVCCKey{Key: key, Timestamp: txn.Timestamp}
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
	if txn.Status == roachpb.PENDING && meta.Txn.Epoch >= txn.Epoch {
		return nil
	}

	// Otherwise, we're deleting the intent. We must find the next
	// versioned value and reset the metadata's latest timestamp. If
	// there are no other versioned values, we delete the metadata
	// key.

	// First clear the intent value.
	latestKey := MVCCKey{Key: key, Timestamp: meta.Timestamp}
	if err := engine.Clear(latestKey); err != nil {
		return err
	}

	// Compute the next possible mvcc value for this key.
	nextKey := latestKey.Next()
	iter.Seek(nextKey)

	// If there is no other version, we should just clean up the key entirely.
	if !iter.Valid() || !iter.unsafeKey().Key.Equal(key) {
		if err = engine.Clear(metaKey); err != nil {
			return err
		}
		// Clear stat counters attributable to the intent we're aborting.
		updateStatsOnAbort(ms, key, origMetaKeySize, origMetaValSize, 0, 0, meta, nil, origAgeSeconds, 0)
		return nil
	}

	iterKey := iter.Key()
	if !iterKey.IsValue() {
		return util.Errorf("expected an MVCC value key: %s", iterKey)
	}
	// Get the bytes for the next version so we have size for stat counts.
	valueSize := int64(len(iter.unsafeValue()))
	// Update the keyMetadata with the next version.
	newMeta := &MVCCMetadata{
		Deleted:  valueSize == 0,
		KeyBytes: mvccVersionTimestampSize,
		ValBytes: valueSize,
	}
	if err := engine.Clear(metaKey); err != nil {
		return err
	}
	metaKeySize := int64(metaKey.EncodedSize())
	metaValSize := int64(0)
	restoredAgeSeconds := timestamp.WallTime/1E9 - iterKey.Timestamp.WallTime/1E9

	// Update stat counters with older version.
	updateStatsOnAbort(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, meta, newMeta, origAgeSeconds, restoredAgeSeconds)

	return nil
}

// MVCCResolveWriteIntentRange commits or aborts (rolls back) the
// range of write intents specified by start and end keys for a given
// txn. ResolveWriteIntentRange will skip write intents of other
// txns. Specify max=0 for unbounded resolves.
func MVCCResolveWriteIntentRange(engine Engine, ms *MVCCStats, key, endKey roachpb.Key, max int64, timestamp roachpb.Timestamp, txn *roachpb.Transaction) (int64, error) {
	if txn == nil {
		return 0, util.Errorf("no txn specified")
	}

	encKey := MakeMVCCMetadataKey(key)
	encEndKey := MakeMVCCMetadataKey(endKey)
	nextKey := encKey

	num := int64(0)
	for {
		kvs, err := Scan(engine, nextKey, encEndKey, 1)
		if err != nil {
			return num, err
		}
		// No more keys exists in the given range.
		if len(kvs) == 0 {
			break
		}

		key0 := kvs[0].Key
		if !key0.IsValue() {
			err = MVCCResolveWriteIntent(engine, ms, key0.Key, timestamp, txn)
		}
		if err != nil {
			log.Warningf("failed to resolve intent for key %q: %v", key0.Key, err)
		} else {
			num++
			if max != 0 && max == num {
				break
			}
		}

		// In order to efficiently skip the possibly long list of
		// old versions for this key, we make a non-version MVCC key.
		nextKey = MakeMVCCMetadataKey(key0.Key.Next())
	}

	return num, nil
}

// MVCCGarbageCollect creates an iterator on the engine. In parallel
// it iterates through the keys listed for garbage collection by the
// keys slice. The engine iterator is seeked in turn to each listed
// key, clearing all values with timestamps <= to expiration.
// The timestamp parameter is used to compute the intent age on GC.
func MVCCGarbageCollect(engine Engine, ms *MVCCStats, keys []roachpb.GCRequest_GCKey, timestamp roachpb.Timestamp) error {
	iter := engine.NewIterator(false)
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
			return util.Errorf("could not seek to key %q", gcKey.Key)
		}
		implicitMeta := iter.Key().IsValue()
		// First, check whether all values of the key are being deleted.
		if !gcKey.Timestamp.Less(meta.Timestamp) {
			// For version keys, don't allow GC'ing the meta key if it's
			// not marked deleted. However, for inline values we allow it;
			// they are internal and GCing them directly saves the extra
			// deletion step.
			if !meta.Deleted && !gcKey.Timestamp.Equal(roachpb.ZeroTimestamp) {
				return util.Errorf("request to GC non-deleted, latest value of %q", gcKey.Key)
			}
			if meta.Txn != nil {
				return util.Errorf("request to GC intent at %q", gcKey.Key)
			}
			ageSeconds := timestamp.WallTime/1E9 - meta.Timestamp.WallTime/1E9
			updateStatsOnGC(ms, gcKey.Key, metaKeySize, metaValSize, meta, ageSeconds)
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
				ageSeconds := timestamp.WallTime/1E9 - iterKey.Timestamp.WallTime/1E9
				updateStatsOnGC(ms, gcKey.Key, mvccVersionTimestampSize, int64(len(iter.Value())), nil, ageSeconds)
				if err := engine.Clear(iterKey); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// IsValidSplitKey returns whether the key is a valid split key.
// Certain key ranges cannot be split; split keys chosen within
// any of these ranges are considered invalid.
//
//   - \x00\x00meta1 < SplitKey < \x00\x00meta2
//   - \x00zone < SplitKey < \x00zonf
// And split key equal to Meta2KeyMax (\x00\x00meta2\xff\xff) is
// considered invalid.
func IsValidSplitKey(key roachpb.Key) bool {
	if keys.Meta2KeyMax.Equal(key) {
		return false
	}
	return isValidEncodedSplitKey(MakeMVCCMetadataKey(key))
}

// isValidEncodedSplitKey iterates through the illegal ranges and
// returns true if the specified key falls within any; false otherwise.
func isValidEncodedSplitKey(key MVCCKey) bool {
	for _, rng := range illegalSplitKeySpans {
		if rng.start.Less(key) && key.Less(rng.end) {
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
func MVCCFindSplitKey(engine Engine, rangeID roachpb.RangeID, key, endKey roachpb.RKey) (roachpb.Key, error) {
	if key.Less(roachpb.RKey(keys.LocalMax)) {
		key = keys.Addr(keys.LocalMax)
	}
	encStartKey := MakeMVCCMetadataKey(key.AsRawKey())
	encEndKey := MakeMVCCMetadataKey(endKey.AsRawKey())

	// Get range size from stats.
	var ms MVCCStats
	if err := MVCCGetRangeStats(engine, rangeID, &ms); err != nil {
		return nil, err
	}
	rangeSize := ms.KeyBytes + ms.ValBytes

	targetSize := rangeSize / 2
	sizeSoFar := int64(0)
	bestSplitKey := encStartKey
	bestSplitDiff := int64(math.MaxInt64)
	var lastKey roachpb.Key

	if err := engine.Iterate(encStartKey, encEndKey, func(kv MVCCKeyValue) (bool, error) {
		// Is key within a legal key range?
		valid := isValidEncodedSplitKey(kv.Key)

		// Determine if this key would make a better split than last "best" key.
		diff := targetSize - sizeSoFar
		if diff < 0 {
			diff = -diff
		}
		if valid && diff < bestSplitDiff {
			bestSplitKey = kv.Key
			bestSplitDiff = diff
		}

		// Determine whether we've found best key and can exit iteration.
		done := !bestSplitKey.Equal(encStartKey) && diff > bestSplitDiff

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

	if bestSplitKey.Equal(encStartKey) {
		return nil, util.Errorf("the range cannot be split; considered range %q-%q has no valid splits", key, endKey)
	}

	// The key is an MVCC key, so to avoid corrupting MVCC we get the
	// associated mvcc metadata key, which is fine to split in front of.
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
