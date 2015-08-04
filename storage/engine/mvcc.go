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

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// The size of the timestamp portion of MVCC version keys (used to update stats).
	mvccVersionTimestampSize int64 = 12
)

var (
	// MVCCKeyMax is a maximum mvcc-encoded key value which sorts after
	// all other keys.
	MVCCKeyMax = MVCCEncodeKey(proto.KeyMax)
)

// IsInline returns true if the value is inlined in the metadata.
func (meta MVCCMetadata) IsInline() bool {
	return meta.Value != nil
}

// HasWriteIntentError returns whether the metadata has an open intent which
// has not been laid down by the given transaction (which may be nil).
func (meta MVCCMetadata) HasWriteIntentError(txn *proto.Transaction) bool {
	return meta.Txn != nil && (txn == nil || !bytes.Equal(meta.Txn.ID, txn.ID))
}

// IsIntentOf returns true if the meta record is an intent of the supplied
// transaction.
func (meta MVCCMetadata) IsIntentOf(txn *proto.Transaction) bool {
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
func updateStatsForKey(ms *MVCCStats, key proto.Key) (bool, bool) {
	return ms != nil, key.Less(keys.LocalMax)
}

// updateStatsForInline updates stat counters for an inline value.
// These are simpler as they don't involve intents or multiple
// versions.
func updateStatsForInline(ms *MVCCStats, key proto.Key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize int64) {
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
func updateStatsOnMerge(ms *MVCCStats, key proto.Key, valSize int64) {
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
func updateStatsOnPut(ms *MVCCStats, key proto.Key, origMetaKeySize, origMetaValSize,
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
func updateStatsOnResolve(ms *MVCCStats, key proto.Key, origMetaKeySize, origMetaValSize,
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
		} else {
			ms.GCBytesAge += MVCCComputeGCBytesAge(keyDiff+valDiff, origAgeSeconds)
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
func updateStatsOnAbort(ms *MVCCStats, key proto.Key, origMetaKeySize, origMetaValSize,
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
func updateStatsOnGC(ms *MVCCStats, key proto.Key, keySize, valSize int64, meta *MVCCMetadata, ageSeconds int64) {
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
func MVCCGetRangeStats(engine Engine, raftID proto.RaftID, ms *MVCCStats) error {
	_, err := MVCCGetProto(engine, keys.RangeStatsKey(raftID), proto.ZeroTimestamp, true, nil, ms)
	return err
}

// MVCCSetRangeStats sets stat counters for specified range.
func MVCCSetRangeStats(engine Engine, raftID proto.RaftID, ms *MVCCStats) error {
	return MVCCPutProto(engine, nil, keys.RangeStatsKey(raftID), proto.ZeroTimestamp, nil, ms)
}

// MVCCGetProto fetches the value at the specified key and unmarshals
// it using a protobuf decoder. Returns true on success or false if
// the key was not found. In the event of a WriteIntentError when
// consistent=false, we return the error and the decoded result; for
// all other errors (or when consistent=true) the decoded value is
// invalid.
func MVCCGetProto(engine Engine, key proto.Key, timestamp proto.Timestamp, consistent bool, txn *proto.Transaction, msg gogoproto.Message) (bool, error) {
	// TODO(tschottdorf) Consider returning skipped intents to the caller.
	value, _, err := MVCCGet(engine, key, timestamp, consistent, txn)
	found := value != nil && len(value.Bytes) > 0
	// If we found a result, parse it regardless of the error returned by MVCCGet.
	if found && msg != nil {
		unmarshalErr := gogoproto.Unmarshal(value.Bytes, msg)
		// If the unmarshal failed, return its result. Otherwise, pass
		// through the underlying error (which may be a WriteIntentError
		// to be handled specially alongside the returned value).
		if unmarshalErr != nil {
			err = unmarshalErr
		}
	}
	return found, err
}

// MVCCPutProto sets the given key to the protobuf-serialized byte
// string of msg and the provided timestamp.
func MVCCPutProto(engine Engine, ms *MVCCStats, key proto.Key, timestamp proto.Timestamp, txn *proto.Transaction, msg gogoproto.Message) error {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return err
	}
	value := proto.Value{Bytes: data}
	value.InitChecksum(key)
	return MVCCPut(engine, ms, key, timestamp, value, txn)
}

type getBuffer struct {
	meta  MVCCMetadata
	value MVCCValue
	key   [1024]byte
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
// ignored for reading the value (but returned via the proto.Intent slice);
// the previous value (if any) is read instead.
func MVCCGet(engine Engine, key proto.Key, timestamp proto.Timestamp, consistent bool, txn *proto.Transaction) (*proto.Value, []proto.Intent, error) {
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}

	// Create a function which scans for the first key between start and end keys.
	getValue := func(engine Engine, start, end proto.EncodedKey,
		msg gogoproto.Message) (proto.EncodedKey, error) {
		iter := engine.NewIterator()
		defer iter.Close()
		iter.Seek(start)
		if !iter.Valid() {
			return nil, iter.Error()
		}
		key := iter.Key()
		if bytes.Compare(key, end) >= 0 {
			return nil, iter.Error()
		}
		return key, iter.ValueProto(msg)
	}

	buf := getBufferPool.Get().(*getBuffer)
	defer getBufferPool.Put(buf)

	metaKey := mvccEncodeKey(buf.key[0:0], key)
	ok, _, _, err := engine.GetProto(metaKey, &buf.meta)
	if err != nil || !ok {
		return nil, nil, err
	}

	return mvccGetInternal(engine, key, metaKey, timestamp, consistent, txn, getValue, buf)
}

// getValueFunc fetches a version of a key between start and end.
// Returns the key as an encoded byte slice, and error, if applicable.
type getValueFunc func(engine Engine, start, end proto.EncodedKey,
	msg gogoproto.Message) (proto.EncodedKey, error)

// mvccGetInternal parses the MVCCMetadata from the specified raw key
// value, and reads the versioned value indicated by timestamp, taking
// the transaction txn into account. getValue is a helper function to
// get an earlier version of the value when doing historical reads.
//
// The consistent parameter specifies whether reads should ignore any write
// intents (regardless of the actual status of their transaction) and read the
// most recent non-intent value instead. In the event that an inconsistent read
// does encounter an intent (currently there can only be one), it is returned
// via the proto.Intent slice, in addition to the result.
func mvccGetInternal(engine Engine, key proto.Key, metaKey proto.EncodedKey,
	timestamp proto.Timestamp, consistent bool, txn *proto.Transaction,
	getValue getValueFunc, buf *getBuffer) (*proto.Value, []proto.Intent, error) {
	if !consistent && txn != nil {
		return nil, nil, util.Errorf("cannot allow inconsistent reads within a transaction")
	}

	meta := &buf.meta

	// If value is inline, return immediately; txn & timestamp are irrelevant.
	if meta.IsInline() {
		if err := meta.Value.Verify(key); err != nil {
			return nil, nil, err
		}
		return meta.Value, nil, nil
	}
	var ignoredIntents []proto.Intent
	if !consistent && meta.Txn != nil && !timestamp.Less(meta.Timestamp) {
		// If we're doing inconsistent reads and there's an intent, we
		// ignore the intent by insisting that the timestamp we're reading
		// at is a historical timestamp < the intent timestamp. However, we
		// return the intent separately; the caller may want to resolve it.
		ignoredIntents = append(ignoredIntents, proto.Intent{Key: key, Txn: *meta.Txn})
		timestamp = meta.Timestamp.Prev()
	}

	var valueKey proto.EncodedKey
	value := &buf.value

	if !timestamp.Less(meta.Timestamp) && meta.HasWriteIntentError(txn) {
		// Trying to read the last value, but it's another transaction's
		// intent; the reader will have to act on this.
		return nil, nil, &proto.WriteIntentError{
			Intents: []proto.Intent{{Key: key, Txn: *meta.Txn}},
		}
	} else if ownIntent := meta.IsIntentOf(txn); !timestamp.Less(meta.Timestamp) || ownIntent {
		// We are reading the latest value, which is either an intent written
		// by this transaction or not an intent at all (so there's no
		// conflict). Note that when reading the own intent, the timestamp
		// specified is irrelevant; we always want to see the intent (see
		// TestMVCCReadWithPushedTimestamp).
		latestKey := mvccEncodeTimestamp(metaKey, meta.Timestamp)

		// Check for case where we're reading our own txn's intent
		// but it's got a different epoch. This can happen if the
		// txn was restarted and an earlier iteration wrote the value
		// we're now reading. In this case, we skip the intent.
		var err error
		if ownIntent && txn.Epoch != meta.Txn.Epoch {
			if txn.Epoch < meta.Txn.Epoch {
				return nil, nil, util.Errorf("failed to read with epoch %d due to a write intent with epoch %d",
					txn.Epoch, meta.Txn.Epoch)
			}
			valueKey, err = getValue(engine, latestKey.Next(), MVCCEncodeKey(key.Next()), value)
		} else {
			var ok bool
			ok, _, _, err = engine.GetProto(latestKey, value)
			if ok {
				valueKey = latestKey
			}
		}
		if err != nil {
			return nil, nil, err
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
			return nil, nil, &proto.ReadWithinUncertaintyIntervalError{
				Timestamp:         timestamp,
				ExistingTimestamp: meta.Timestamp,
			}
		}

		// We want to know if anything has been written ahead of timestamp, but
		// before MaxTimestamp.
		nextKey := MVCCEncodeVersionKey(key, txn.MaxTimestamp)
		var err error
		valueKey, err = getValue(engine, nextKey, MVCCEncodeKey(key.Next()), value)
		if err != nil {
			return nil, nil, err
		}
		if valueKey != nil {
			_, ts, _ := MVCCDecodeKey(valueKey)
			if timestamp.Less(ts) {
				// Third case: Our read timestamp is sufficiently behind the newest
				// value, but there is another previous write with the same issues
				// as in the second case, so the reader will have to come again
				// with a higher read timestamp.
				return nil, nil, &proto.ReadWithinUncertaintyIntervalError{
					Timestamp:         timestamp,
					ExistingTimestamp: ts,
				}
			}
		}
		// Fourth case: There's no value in our future up to MaxTimestamp, and
		// those are the only ones that we're not certain about. The correct
		// key has already been read above, so there's nothing left to do.
	} else {
		// Fifth case: We're reading a historic value either outside of
		// a transaction, or in the absence of future versions that clock
		// uncertainty would apply to.
		var err error
		nextKey := MVCCEncodeVersionKey(key, timestamp)
		valueKey, err = getValue(engine, nextKey, MVCCEncodeKey(key.Next()), value)
		if err != nil {
			return nil, nil, err
		}
	}

	if valueKey == nil {
		return nil, ignoredIntents, nil
	}

	_, ts, isValue := MVCCDecodeKey(valueKey)
	if !isValue {
		return nil, nil, util.Errorf("expected scan to versioned value reading key %q; got %q", key, valueKey)
	}

	if value.Deleted {
		value.Value = nil
	}

	// Set the timestamp if the value is not nil (i.e. not a deletion tombstone).
	if value.Value != nil {
		value.Value.Timestamp = &ts
		if err := value.Value.Verify(key); err != nil {
			return nil, nil, err
		}
	} else if !value.Deleted {
		// Sanity check.
		panic(fmt.Sprintf("encountered MVCC value at key %q with a nil proto.Value but with !Deleted: %+v", key, value))
	}

	return value.Value, ignoredIntents, nil
}

// putBuffer holds pointer data needed by mvccPutInternal. Bundling
// this data into a single structure reduces memory
// allocations. Managing this temporary buffer using a sync.Pool
// completely eliminates allocation from the put common path.
type putBuffer struct {
	meta    MVCCMetadata
	newMeta MVCCMetadata
	value   MVCCValue
	pvalue  proto.Value
	key     [1024]byte
}

var putBufferPool = sync.Pool{
	New: func() interface{} {
		return &putBuffer{}
	},
}

// MVCCPut sets the value for a specified key. It will save the value
// with different versions according to its timestamp and update the
// key metadata.
//
// If the timestamp is specifed as proto.ZeroTimestamp, the value is
// inlined instead of being written as a timestamp-versioned value. A
// zero timestamp write to a key precludes a subsequent write using a
// non-zero timestamp and vice versa. Inlined values require only a
// single row and never accumulate more than a single value. Successive
// zero timestamp writes to a key replace the value and deletes clear
// the value. In addition, zero timestamp values may be merged.
func MVCCPut(engine Engine, ms *MVCCStats, key proto.Key, timestamp proto.Timestamp,
	value proto.Value, txn *proto.Transaction) error {
	if value.Timestamp != nil && !value.Timestamp.Equal(timestamp) {
		return util.Errorf(
			"the timestamp %+v provided in value does not match the timestamp %+v in request",
			value.Timestamp, timestamp)
	}

	buf := putBufferPool.Get().(*putBuffer)
	buf.pvalue = value
	buf.value.Reset()
	buf.value.Value = &buf.pvalue

	err := mvccPutInternal(engine, ms, key, timestamp, buf.value, txn, buf)

	// Using defer would be more convenient, but it is measurably
	// slower.
	putBufferPool.Put(buf)
	return err
}

// MVCCDelete marks the key deleted so that it will not be returned in
// future get responses.
func MVCCDelete(engine Engine, ms *MVCCStats, key proto.Key, timestamp proto.Timestamp,
	txn *proto.Transaction) error {
	buf := putBufferPool.Get().(*putBuffer)
	buf.value.Reset()
	buf.value.Deleted = true

	err := mvccPutInternal(engine, ms, key, timestamp, buf.value, txn, buf)

	// Using defer would be more convenient, but it is measurably
	// slower.
	putBufferPool.Put(buf)
	return err
}

// mvccPutInternal adds a new timestamped value to the specified key.
// If value is nil, creates a deletion tombstone value.
func mvccPutInternal(engine Engine, ms *MVCCStats, key proto.Key, timestamp proto.Timestamp,
	value MVCCValue, txn *proto.Transaction, buf *putBuffer) error {
	if len(key) == 0 {
		return emptyKeyError()
	}

	metaKey := mvccEncodeKey(buf.key[0:0], key)
	ok, origMetaKeySize, origMetaValSize, err := engine.GetProto(metaKey, &buf.meta)
	if err != nil {
		return err
	}

	// Verify we're not mixing inline and non-inline values.
	putIsInline := timestamp.Equal(proto.ZeroTimestamp)
	if ok && putIsInline != buf.meta.IsInline() {
		return util.Errorf("%q: put is inline=%t, but existing value is inline=%t",
			metaKey, putIsInline, buf.meta.IsInline())
	}
	if putIsInline {
		var metaKeySize, metaValSize int64
		if value.Deleted {
			metaKeySize, metaValSize, err = 0, 0, engine.Clear(metaKey)
		} else {
			buf.meta = MVCCMetadata{Value: value.Value}
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
				return &proto.WriteIntentError{Intents: []proto.Intent{{Key: key, Txn: *meta.Txn}}}
			} else if txn.Epoch < meta.Txn.Epoch {
				return util.Errorf("put with epoch %d came after put with epoch %d in txn %s",
					txn.Epoch, meta.Txn.Epoch, txn.ID)
			}

			// We are replacing our own older write intent. If we are
			// writing at the same timestamp we can simply overwrite it;
			// otherwise we must explicitly delete the obsolete intent.
			if !timestamp.Equal(meta.Timestamp) {
				versionKey := mvccEncodeTimestamp(metaKey, meta.Timestamp)
				if err := engine.Clear(versionKey); err != nil {
					return err
				}
			}
		} else {
			// No pending transaction, so we can write as long as it is not
			// in the past.
			if !meta.Timestamp.Less(timestamp) {
				return &proto.WriteTooOldError{Timestamp: timestamp, ExistingTimestamp: meta.Timestamp}
			}
		}
	} else {
		// No existing metadata record. If this is a delete, do nothing;
		// otherwise we can perform the write.
		if value.Deleted {
			return nil
		}
	}
	buf.newMeta = MVCCMetadata{Txn: txn, Timestamp: timestamp}
	newMeta := &buf.newMeta

	// Make sure to zero the redundant timestamp (timestamp is encoded
	// into the key, so don't need it in both places).
	if value.Value != nil {
		value.Value.Timestamp = nil
	}

	// The metaKey is always the prefix of the versionKey.
	versionKey := mvccEncodeTimestamp(metaKey, timestamp)
	_, valueSize, err := PutProto(engine, versionKey, &buf.value)
	if err != nil {
		return err
	}

	// Write the mvcc metadata now that we have sizes for the latest versioned value.
	newMeta.KeyBytes = mvccVersionTimestampSize
	newMeta.ValBytes = valueSize
	newMeta.Deleted = value.Deleted
	metaKeySize, metaValSize, err := PutProto(engine, metaKey, newMeta)
	if err != nil {
		return err
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
func MVCCIncrement(engine Engine, ms *MVCCStats, key proto.Key, timestamp proto.Timestamp, txn *proto.Transaction, inc int64) (int64, error) {
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

	int64Val, err := value.GetInteger()
	if err != nil {
		return 0, util.Errorf("key %q does not contain an integer value", key)
	}

	// Check for overflow and underflow.
	if encoding.WillOverflow(int64Val, inc) {
		return 0, util.Errorf("key %s with value %d incremented by %d results in overflow", key, int64Val, inc)
	}

	// Skip writing the value in the event the value already exists.
	if inc == 0 && value != nil {
		return int64Val, nil
	}

	r := int64Val + inc
	newValue := proto.Value{}
	newValue.SetInteger(r)
	newValue.InitChecksum(key)
	return r, MVCCPut(engine, ms, key, timestamp, newValue, txn)
}

// MVCCConditionalPut sets the value for a specified key only if the
// expected value matches. If not, the return a ConditionFailedError
// containing the actual value.
//
// The condition check reads a value from the key using the same operational
// timestamp as we use to write a value.
func MVCCConditionalPut(engine Engine, ms *MVCCStats, key proto.Key, timestamp proto.Timestamp, value proto.Value,
	expValue *proto.Value, txn *proto.Transaction) error {
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

	if expValue == nil && existVal != nil {
		return &proto.ConditionFailedError{
			ActualValue: existVal,
		}
	} else if expValue != nil {
		// Handle check for existence when there is no key.
		if existVal == nil {
			return &proto.ConditionFailedError{}
		} else if expValue.Bytes != nil && !bytes.Equal(expValue.Bytes, existVal.Bytes) {
			return &proto.ConditionFailedError{
				ActualValue: existVal,
			}
		}
	}

	return MVCCPut(engine, ms, key, timestamp, value, txn)
}

// MVCCMerge implements a merge operation. Merge adds integer values,
// concatenates undifferentiated byte slice values, and efficiently
// combines time series observations if the proto.Value tag value
// indicates the value byte slice is of type _CR_TS (the internal
// cockroach time series data tag).
func MVCCMerge(engine Engine, ms *MVCCStats, key proto.Key, value proto.Value) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	metaKey := MVCCEncodeKey(key)

	// Encode and merge the MVCC metadata with inlined value.
	meta := &MVCCMetadata{Value: &value}
	data, err := gogoproto.Marshal(meta)
	if err != nil {
		return err
	}
	if err = engine.Merge(metaKey, data); err != nil {
		return err
	}
	updateStatsOnMerge(ms, key, int64(len(value.Bytes)))
	return nil
}

// MVCCDeleteRange deletes the range of key/value pairs specified by
// start and end keys. Specify max=0 for unbounded deletes.
func MVCCDeleteRange(engine Engine, ms *MVCCStats, key, endKey proto.Key, max int64, timestamp proto.Timestamp, txn *proto.Transaction) (int64, error) {
	// In order to detect the potential write intent by another
	// concurrent transaction with a newer timestamp, we need
	// to use the max timestamp for scan.
	kvs, _, err := MVCCScan(engine, key, endKey, max, proto.MaxTimestamp, true /* consistent */, txn)
	if err != nil {
		return 0, err
	}

	num := int64(0)
	for _, kv := range kvs {
		err = MVCCDelete(engine, ms, kv.Key, timestamp, txn)
		if err != nil {
			return num, err
		}
		num++
	}
	return num, nil
}

// MVCCScan scans the key range specified by start key through end key
// up to some maximum number of results. Specify max=0 for unbounded
// scans.
func MVCCScan(engine Engine, key, endKey proto.Key, max int64, timestamp proto.Timestamp,
	consistent bool, txn *proto.Transaction) ([]proto.KeyValue, []proto.Intent, error) {
	res := []proto.KeyValue{}
	intents, err := MVCCIterate(engine, key, endKey, timestamp, consistent, txn, func(kv proto.KeyValue) (bool, error) {
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

// MVCCIterate iterates over the key range specified by start and end
// keys, At each step of the iteration, f() is invoked with the
// current key/value pair. If f returns true (done) or an error, the
// iteration stops and the error is propagated.
func MVCCIterate(engine Engine, startKey, endKey proto.Key, timestamp proto.Timestamp,
	consistent bool, txn *proto.Transaction, f func(proto.KeyValue) (bool, error)) ([]proto.Intent, error) {
	if !consistent && txn != nil {
		return nil, util.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(endKey) == 0 {
		return nil, emptyKeyError()
	}

	buf := getBufferPool.Get().(*getBuffer)
	defer getBufferPool.Put(buf)

	// We store encEndKey and encKey in the same buffer to avoid memory
	// allocations.
	encEndKey := mvccEncodeKey(buf.key[0:0], endKey)
	keyBuf := encEndKey[len(encEndKey):]
	encKey := mvccEncodeKey(keyBuf, startKey)

	// Get a new iterator and define our getEarlierFunc using iter.Seek.
	iter := engine.NewIterator()
	defer iter.Close()
	getValue := func(engine Engine, start, end proto.EncodedKey,
		msg gogoproto.Message) (proto.EncodedKey, error) {
		iter.Seek(start)
		if !iter.Valid() {
			return nil, iter.Error()
		}
		key := iter.Key()
		if bytes.Compare(key, end) >= 0 {
			return nil, iter.Error()
		}
		return key, iter.ValueProto(msg)
	}

	// A slice to gather all encountered intents we skipped, in case of
	// inconsistent iteration.
	var intents []proto.Intent
	// Gathers up all the intents from WriteIntentErrors. We only get those if
	// the scan is consistent.
	var wiErr error

	for {
		iter.Seek(encKey)
		if !iter.Valid() {
			if err := iter.Error(); err != nil {
				return nil, err
			}
			break
		}
		metaKey := iter.Key()
		if bytes.Compare(metaKey, encEndKey) >= 0 {
			if err := iter.Error(); err != nil {
				return nil, err
			}
			break
		}
		key, _, isValue := MVCCDecodeKey(metaKey)
		if isValue {
			return nil, util.Errorf("expected an MVCC metadata key: %q", metaKey)
		}
		if err := iter.ValueProto(&buf.meta); err != nil {
			return nil, err
		}
		value, newIntents, err := mvccGetInternal(engine, key, metaKey, timestamp, consistent, txn, getValue, buf)
		intents = append(intents, newIntents...)
		if value != nil {
			done, err := f(proto.KeyValue{Key: key, Value: *value})
			if err != nil {
				return nil, err
			}
			if done {
				break
			}
		}

		if err != nil {
			switch tErr := err.(type) {
			case *proto.WriteIntentError:
				// In the case of WriteIntentErrors, accumulate affected keys but continue scan.
				if wiErr == nil {
					wiErr = tErr
				} else {
					wiErr.(*proto.WriteIntentError).Intents = append(wiErr.(*proto.WriteIntentError).Intents, tErr.Intents...)
				}
			default:
				return nil, err
			}
		}
		encKey = mvccEncodeKey(keyBuf, key.Next())
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
func MVCCResolveWriteIntent(engine Engine, ms *MVCCStats, key proto.Key, timestamp proto.Timestamp, txn *proto.Transaction) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	if txn == nil {
		return util.Error("no txn specified")
	}

	metaKey := MVCCEncodeKey(key)
	meta := &MVCCMetadata{}
	ok, origMetaKeySize, origMetaValSize, err := engine.GetProto(metaKey, meta)
	if err != nil {
		return err
	}
	// For cases where there's no write intent to resolve, or one exists
	// which we can't resolve, this is a noop.
	if !ok || meta.Txn == nil || !bytes.Equal(meta.Txn.ID, txn.ID) {
		return nil
	}
	origAgeSeconds := timestamp.WallTime/1E9 - meta.Timestamp.WallTime/1E9

	// If we're committing, or if the commit timestamp of the intent has
	// been moved forward, and if the proposed epoch matches the existing
	// epoch: update the meta.Txn. For commit, it's set to nil;
	// otherwise, we update its value. We may have to update the actual
	// version value (remove old and create new with proper
	// timestamp-encoded key) if timestamp changed.
	commit := txn.Status == proto.COMMITTED
	pushed := txn.Status == proto.PENDING && meta.Txn.Timestamp.Less(txn.Timestamp)
	if (commit || pushed) && meta.Txn.Epoch == txn.Epoch {
		origTimestamp := meta.Timestamp
		newMeta := *meta
		newMeta.Timestamp = txn.Timestamp
		if pushed { // keep intent if we're pushing timestamp
			newMeta.Txn = txn
		} else {
			newMeta.Txn = nil
		}
		metaKeySize, metaValSize, err := PutProto(engine, metaKey, &newMeta)
		if err != nil {
			return err
		}

		// Update stat counters related to resolving the intent.
		updateStatsOnResolve(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, &newMeta, commit, origAgeSeconds)

		// If timestamp of value changed, need to rewrite versioned value.
		// TODO(spencer,tobias): think about a new merge operator for
		// updating key of intent value to new timestamp instead of
		// read-then-write.
		if !origTimestamp.Equal(txn.Timestamp) {
			origKey := MVCCEncodeVersionKey(key, origTimestamp)
			newKey := MVCCEncodeVersionKey(key, txn.Timestamp)
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
	if txn.Status == proto.PENDING && meta.Txn.Epoch >= txn.Epoch {
		return nil
	}

	// Otherwise, we're deleting the intent. We must find the next
	// versioned value and reset the metadata's latest timestamp. If
	// there are no other versioned values, we delete the metadata
	// key.

	// First clear the intent value.
	latestKey := MVCCEncodeVersionKey(key, meta.Timestamp)
	if err := engine.Clear(latestKey); err != nil {
		return err
	}

	// Compute the next possible mvcc value for this key.
	nextKey := latestKey.Next()
	// Compute the last possible mvcc value for this key.
	endScanKey := MVCCEncodeKey(key.Next())
	kvs, err := Scan(engine, nextKey, endScanKey, 1)
	if err != nil {
		return err
	}
	// If there is no other version, we should just clean up the key entirely.
	if len(kvs) == 0 {
		if err = engine.Clear(metaKey); err != nil {
			return err
		}
		// Clear stat counters attributable to the intent we're aborting.
		updateStatsOnAbort(ms, key, origMetaKeySize, origMetaValSize, 0, 0, meta, nil, origAgeSeconds, 0)
	} else {
		_, ts, isValue := MVCCDecodeKey(kvs[0].Key)
		if !isValue {
			return util.Errorf("expected an MVCC value key: %s", kvs[0].Key)
		}
		// Get the bytes for the next version so we have size for stat counts.
		value := MVCCValue{}
		var valueSize int64
		ok, _, valueSize, err = engine.GetProto(kvs[0].Key, &value)
		if err != nil || !ok {
			return util.Errorf("unable to fetch previous version for key %q (%t): %s", kvs[0].Key, ok, err)
		}
		// Update the keyMetadata with the next version.
		newMeta := &MVCCMetadata{
			Timestamp: ts,
			Deleted:   value.Deleted,
			KeyBytes:  mvccVersionTimestampSize,
			ValBytes:  valueSize,
		}
		metaKeySize, metaValSize, err := PutProto(engine, metaKey, newMeta)
		if err != nil {
			return err
		}
		restoredAgeSeconds := timestamp.WallTime/1E9 - ts.WallTime/1E9

		// Update stat counters with older version.
		updateStatsOnAbort(ms, key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, meta, newMeta, origAgeSeconds, restoredAgeSeconds)
	}

	return nil
}

// MVCCResolveWriteIntentRange commits or aborts (rolls back) the
// range of write intents specified by start and end keys for a given
// txn. ResolveWriteIntentRange will skip write intents of other
// txns. Specify max=0 for unbounded resolves.
func MVCCResolveWriteIntentRange(engine Engine, ms *MVCCStats, key, endKey proto.Key, max int64, timestamp proto.Timestamp, txn *proto.Transaction) (int64, error) {
	if txn == nil {
		return 0, util.Error("no txn specified")
	}

	encKey := MVCCEncodeKey(key)
	encEndKey := MVCCEncodeKey(endKey)
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

		currentKey, _, isValue := MVCCDecodeKey(kvs[0].Key)
		if isValue {
			return 0, util.Errorf("expected an MVCC metadata key: %s", kvs[0].Key)
		}
		err = MVCCResolveWriteIntent(engine, ms, currentKey, timestamp, txn)
		if err != nil {
			log.Warningf("failed to resolve intent for key %q: %v", currentKey, err)
		} else {
			num++
			if max != 0 && max == num {
				break
			}
		}

		// In order to efficiently skip the possibly long list of
		// old versions for this key, we make a non-version MVCC key.
		nextKey = MVCCEncodeKey(currentKey.Next())
	}

	return num, nil
}

// MVCCGarbageCollect creates an iterator on the engine. In parallel
// it iterates through the keys listed for garbage collection by the
// keys slice. The engine iterator is seeked in turn to each listed
// key, clearing all values with timestamps <= to expiration.
func MVCCGarbageCollect(engine Engine, ms *MVCCStats, keys []proto.InternalGCRequest_GCKey, timestamp proto.Timestamp) error {
	iter := engine.NewIterator()

	// Iterate through specified GC keys.
	for _, gcKey := range keys {
		encKey := MVCCEncodeKey(gcKey.Key)
		iter.Seek(encKey)
		if !iter.Valid() {
			return util.Errorf("could not seek to key %q", gcKey.Key)
		}
		// First, check whether all values of the key are being deleted.
		meta := &MVCCMetadata{}
		if err := gogoproto.Unmarshal(iter.Value(), meta); err != nil {
			return util.Errorf("unable to marshal mvcc meta: %s", err)
		}
		if !gcKey.Timestamp.Less(meta.Timestamp) {
			if !meta.Deleted {
				return util.Errorf("request to GC non-deleted, latest value of %q", gcKey.Key)
			}
			if meta.Txn != nil {
				return util.Errorf("request to GC intent at %q", gcKey.Key)
			}
			ageSeconds := timestamp.WallTime/1E9 - meta.Timestamp.WallTime/1E9
			updateStatsOnGC(ms, gcKey.Key, int64(len(iter.Key())), int64(len(iter.Value())), meta, ageSeconds)
			if err := engine.Clear(iter.Key()); err != nil {
				return err
			}
		}

		// Now, iterate through all values, GC'ing ones which have expired.
		// Note that we start the for loop by iterating once to move past
		// the metadata key.
		for iter.Next(); iter.Valid(); iter.Next() {
			_, ts, isValue := MVCCDecodeKey(iter.Key())
			if !isValue {
				break
			}
			if !gcKey.Timestamp.Less(ts) {
				ageSeconds := timestamp.WallTime/1E9 - ts.WallTime/1E9
				updateStatsOnGC(ms, gcKey.Key, mvccVersionTimestampSize, int64(len(iter.Value())), nil, ageSeconds)
				if err := engine.Clear(iter.Key()); err != nil {
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
//   - \x00acct < SplitKey < \x00accu
//   - \x00perm < SplitKey < \x00pern
//   - \x00zone < SplitKey < \x00zonf
// And split key equal to Meta2KeyMax (\x00\x00meta2\xff\xff) is
// considered invalid.
func IsValidSplitKey(key proto.Key) bool {
	if key.Equal(keys.Meta2KeyMax) {
		return false
	}
	return isValidEncodedSplitKey(MVCCEncodeKey(key))
}

// illegalSplitKeyRanges detail illegal ranges for split keys,
// exclusive of start and end.
var illegalSplitKeyRanges = []struct {
	start, end proto.EncodedKey
}{
	{
		start: MVCCEncodeKey(proto.KeyMin),
		end:   MVCCEncodeKey(keys.Meta2Prefix),
	},
	{
		start: MVCCEncodeKey(keys.ConfigAccountingPrefix),
		end:   MVCCEncodeKey(keys.ConfigAccountingPrefix.PrefixEnd()),
	},
	{
		start: MVCCEncodeKey(keys.ConfigPermissionPrefix),
		end:   MVCCEncodeKey(keys.ConfigPermissionPrefix.PrefixEnd()),
	},
	{
		start: MVCCEncodeKey(keys.ConfigUserPrefix),
		end:   MVCCEncodeKey(keys.ConfigUserPrefix.PrefixEnd()),
	},
	{
		start: MVCCEncodeKey(keys.ConfigZonePrefix),
		end:   MVCCEncodeKey(keys.ConfigZonePrefix.PrefixEnd()),
	},
}

// isValidEncodedSplitKey iterates through the illegal ranges and
// returns true if the specified key falls within any; false otherwise.
func isValidEncodedSplitKey(key proto.EncodedKey) bool {
	for _, rng := range illegalSplitKeyRanges {
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
// illegalSplitKeyRanges.
func MVCCFindSplitKey(engine Engine, raftID proto.RaftID, key, endKey proto.Key) (proto.Key, error) {
	if key.Less(keys.LocalMax) {
		key = keys.LocalMax
	}
	encStartKey := MVCCEncodeKey(key)
	encEndKey := MVCCEncodeKey(endKey)

	// Get range size from stats.
	var ms MVCCStats
	if err := MVCCGetRangeStats(engine, raftID, &ms); err != nil {
		return nil, err
	}
	rangeSize := ms.KeyBytes + ms.ValBytes

	targetSize := rangeSize / 2
	sizeSoFar := int64(0)
	bestSplitKey := encStartKey
	bestSplitDiff := int64(math.MaxInt64)

	if err := engine.Iterate(encStartKey, encEndKey, func(kv proto.RawKeyValue) (bool, error) {
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
		_, _, isValue := MVCCDecodeKey(kv.Key)
		if isValue {
			sizeSoFar += mvccVersionTimestampSize + int64(len(kv.Value))
		} else {
			sizeSoFar += int64(len(kv.Key) + len(kv.Value))
		}

		return done, nil
	}); err != nil {
		return nil, err
	}

	if bestSplitKey.Equal(encStartKey) {
		return nil, util.Errorf("the range cannot be split; considered range %q-%q has no valid splits", key, endKey)
	}

	// The key is an MVCC key, so to avoid corrupting MVCC we get the
	// associated mvcc metadata key, which is fine to split in front of.
	humanKey, _, _ := MVCCDecodeKey(bestSplitKey)
	return humanKey, nil
}

// MVCCComputeStats scans the underlying engine from start to end keys
// and computes stats counters based on the values. This method is
// used after a range is split to recompute stats for each
// subrange. The start key is always adjusted to avoid counting local
// keys in the event stats are being recomputed for the first range
// (i.e. the one with start key == KeyMin). The nowNanos arg specifies
// the wall time in nanoseconds since the epoch and is used to compute
// the total age of all intents.
func MVCCComputeStats(iter Iterator, nowNanos int64) (MVCCStats, error) {
	ms := MVCCStats{LastUpdateNanos: nowNanos}
	first := false
	meta := &MVCCMetadata{}

	for ; iter.Valid(); iter.Next() {
		key, ts, isValue := MVCCDecodeKey(iter.Key())
		_, sys := updateStatsForKey(&ms, key)
		if !isValue {
			totalBytes := int64(len(iter.Value())) + int64(len(iter.Key()))
			first = true
			if err := gogoproto.Unmarshal(iter.Value(), meta); err != nil {
				return ms, util.Errorf("unable to unmarshal MVCC metadata %b: %s", iter.Value(), err)
			}
			if sys {
				ms.SysBytes += int64(len(iter.Key())) + int64(len(iter.Value()))
				ms.SysCount++
			} else {
				if !meta.Deleted {
					ms.LiveBytes += totalBytes
					ms.LiveCount++
				} else {
					// First value is deleted, so it's GC'able; add meta key & value bytes to age stat.
					ms.GCBytesAge += totalBytes * (nowNanos/1E9 - meta.Timestamp.WallTime/1E9)
				}
				ms.KeyBytes += int64(len(iter.Key()))
				ms.ValBytes += int64(len(iter.Value()))
				ms.KeyCount++
				if meta.IsInline() {
					ms.ValCount++
				}
			}
		} else {
			totalBytes := int64(len(iter.Value())) + mvccVersionTimestampSize
			if sys {
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
						return ms, util.Errorf("expected mvcc metadata key bytes to equal %d; got %d", mvccVersionTimestampSize, meta.KeyBytes)
					}
					if meta.ValBytes != int64(len(iter.Value())) {
						return ms, util.Errorf("expected mvcc metadata val bytes to equal %d; got %d", len(iter.Value()), meta.ValBytes)
					}
				} else {
					// Overwritten value; add value bytes to the GC'able bytes age stat.
					ms.GCBytesAge += totalBytes * (nowNanos/1E9 - ts.WallTime/1E9)
				}
				ms.KeyBytes += mvccVersionTimestampSize
				ms.ValBytes += int64(len(iter.Value()))
				ms.ValCount++
			}
		}
	}
	return ms, nil
}

// MVCCEncodeKey makes an MVCC key for storing MVCC metadata or
// for storing raw values directly. Use MVCCEncodeVersionValue for
// storing timestamped version values.
func MVCCEncodeKey(key proto.Key) proto.EncodedKey {
	return mvccEncodeKey(nil, key)
}

// mvccEncodeKey is the internal version of MVCCEncodeKey and takes a
// buffer to append the encoded key to.
func mvccEncodeKey(buf []byte, key proto.Key) proto.EncodedKey {
	return encoding.EncodeBytes(buf, key)
}

// MVCCEncodeVersionKey makes an MVCC version key, which consists
// of a binary-encoding of key, followed by a decreasing encoding
// of the timestamp, so that more recent versions sort first.
func MVCCEncodeVersionKey(key proto.Key, timestamp proto.Timestamp) proto.EncodedKey {
	k := encoding.EncodeBytes(nil, key)
	return mvccEncodeTimestamp(k, timestamp)
}

// mvccEncodeTimestamp encodes the MVCC version info onto an existing
// MVCC key (created using MVCCEncodeKey).
func mvccEncodeTimestamp(key proto.EncodedKey, timestamp proto.Timestamp) proto.EncodedKey {
	if timestamp.WallTime < 0 || timestamp.Logical < 0 {
		panic(fmt.Sprintf("negative values disallowed in timestamps: %+v", timestamp))
	}
	key = encoding.EncodeUint64Decreasing(key, uint64(timestamp.WallTime))
	key = encoding.EncodeUint32Decreasing(key, uint32(timestamp.Logical))
	return key
}

// MVCCDecodeKey decodes encodedKey by binary decoding the leading
// bytes of encodedKey. If there are no remaining bytes, returns the
// decoded key, an empty timestamp, and false, to indicate the key is
// for an MVCC metadata or a raw value. Otherwise, there must be
// exactly 12 trailing bytes and they're decoded into a timestamp.
// The decoded key, timestamp and true are returned to indicate the
// key is for an MVCC versioned value.
func MVCCDecodeKey(encodedKey proto.EncodedKey) (proto.Key, proto.Timestamp, bool) {
	tsBytes, key := encoding.DecodeBytes(encodedKey, nil)
	if len(tsBytes) == 0 {
		return key, proto.Timestamp{}, false
	} else if len(tsBytes) != 12 {
		panic(fmt.Sprintf("there should be 12 bytes for encoded timestamp: %q", tsBytes))
	}
	var walltime uint64
	var logical uint32
	tsBytes, walltime = encoding.DecodeUint64Decreasing(tsBytes)
	tsBytes, logical = encoding.DecodeUint32Decreasing(tsBytes)
	return key, proto.Timestamp{WallTime: int64(walltime), Logical: int32(logical)}, true
}
