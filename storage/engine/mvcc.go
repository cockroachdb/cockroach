// Copyright 2014 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// The size of the reservoir used by FindSplitKey.
	splitReservoirSize = 100
	// The size of the timestamp portion of MVCC version keys (used to update stats).
	mvccVersionTimestampSize int64 = 12
)

// MVCCStats tracks byte and instance counts for:
//  - Live key/values (i.e. what a scan at current time will reveal;
//    note that this includes intent keys and values, but not keys and
//    values with most recent value deleted)
//  - Key bytes (includes all keys, even those with most recent value deleted)
//  - Value bytes (includes all versions)
//  - Key count (count of all keys, including keys with deleted tombstones)
//  - Value count (all versions, including deleted tombstones)
//  - Intents (provisional values written during txns)
type MVCCStats struct {
	LiveBytes, KeyBytes, ValBytes, IntentBytes int64
	LiveCount, KeyCount, ValCount, IntentCount int64
	IntentAge, GCBytesAge, LastUpdateNanos     int64
}

// MergeStats merges accumulated stats to stat counters for specified range.
func (ms *MVCCStats) MergeStats(engine Engine, raftID int64) {
	MVCCMergeRangeStat(engine, raftID, StatLiveBytes, ms.LiveBytes)
	MVCCMergeRangeStat(engine, raftID, StatKeyBytes, ms.KeyBytes)
	MVCCMergeRangeStat(engine, raftID, StatValBytes, ms.ValBytes)
	MVCCMergeRangeStat(engine, raftID, StatIntentBytes, ms.IntentBytes)
	MVCCMergeRangeStat(engine, raftID, StatLiveCount, ms.LiveCount)
	MVCCMergeRangeStat(engine, raftID, StatKeyCount, ms.KeyCount)
	MVCCMergeRangeStat(engine, raftID, StatValCount, ms.ValCount)
	MVCCMergeRangeStat(engine, raftID, StatIntentCount, ms.IntentCount)
	MVCCMergeRangeStat(engine, raftID, StatIntentAge, ms.IntentAge)
	MVCCMergeRangeStat(engine, raftID, StatGCBytesAge, ms.GCBytesAge)
	MVCCMergeRangeStat(engine, raftID, StatLastUpdateNanos, ms.LastUpdateNanos)
}

// SetStats sets stat counters for specified range.
func (ms *MVCCStats) SetStats(engine Engine, raftID int64) {
	MVCCSetRangeStat(engine, raftID, StatLiveBytes, ms.LiveBytes)
	MVCCSetRangeStat(engine, raftID, StatKeyBytes, ms.KeyBytes)
	MVCCSetRangeStat(engine, raftID, StatValBytes, ms.ValBytes)
	MVCCSetRangeStat(engine, raftID, StatIntentBytes, ms.IntentBytes)
	MVCCSetRangeStat(engine, raftID, StatLiveCount, ms.LiveCount)
	MVCCSetRangeStat(engine, raftID, StatKeyCount, ms.KeyCount)
	MVCCSetRangeStat(engine, raftID, StatValCount, ms.ValCount)
	MVCCSetRangeStat(engine, raftID, StatIntentCount, ms.IntentCount)
	MVCCSetRangeStat(engine, raftID, StatIntentAge, ms.IntentAge)
	MVCCSetRangeStat(engine, raftID, StatGCBytesAge, ms.GCBytesAge)
	MVCCSetRangeStat(engine, raftID, StatLastUpdateNanos, ms.LastUpdateNanos)
}

// Accumulate adds values from oms to ms.
func (ms *MVCCStats) Accumulate(oms MVCCStats) {
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
	ms.LastUpdateNanos += oms.LastUpdateNanos
}

// updateStatsForKey returns whether or not the bytes and counts for
// the specified key should be tracked. Local keys are excluded.
func (ms *MVCCStats) updateStatsForKey(key proto.Key) bool {
	return ms != nil && !key.Less(KeyLocalMax)
}

// updateStatsForInline updates stat counters for an inline value.
// These are simpler as they don't involve intents or multiple
// versions.
func (ms *MVCCStats) updateStatsForInline(key proto.Key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize int64) {
	if !ms.updateStatsForKey(key) {
		return
	}
	// Remove counts for this key if the original size is non-zero.
	if origMetaKeySize != 0 {
		ms.LiveBytes -= (origMetaKeySize + origMetaValSize)
		ms.LiveCount--
		ms.KeyBytes -= origMetaKeySize
		ms.ValBytes -= origMetaValSize
		ms.KeyCount--
		ms.ValCount--
	}
	// Add counts for this key if the new size is non-zero.
	if metaKeySize != 0 {
		ms.LiveBytes += metaKeySize + metaValSize
		ms.LiveCount++
		ms.KeyBytes += metaKeySize
		ms.ValBytes += metaValSize
		ms.KeyCount++
		ms.ValCount++
	}
}

// updateStatsOnMerge updates metadata stats while merging inlined
// values. Unfortunately, we're unable to keep accurate stats on merge
// as the actual details of the merge play out asynchronously during
// compaction. Instead, we undercount by adding only the size of the
// value.Bytes byte slice. These errors are corrected during splits
// and merges.
func (ms *MVCCStats) updateStatsOnMerge(key proto.Key, valSize int64) {
	if !ms.updateStatsForKey(key) {
		return
	}
	ms.LiveBytes += valSize
	ms.ValBytes += valSize
}

// updateStatsOnPut updates stat counters for a newly put value,
// including both the metadata key & value bytes and the mvcc
// versioned value's key & value bytes. If the value is not a
// deletion tombstone, updates the live stat counters as well.
// If this value is an intent, updates the intent counters.
func (ms *MVCCStats) updateStatsOnPut(key proto.Key, origMetaKeySize, origMetaValSize,
	metaKeySize, metaValSize int64, orig, meta *proto.MVCCMetadata, origAgeSeconds int64) {
	if !ms.updateStatsForKey(key) {
		return
	}
	// Remove current live counts for this key.
	if orig != nil {
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

// updateStatsOnResolve updates stat counters with the difference
// between the original and new metadata sizes. The size of the
// resolved value (key & bytes) are subtracted from the intents
// counters if commit=true.
func (ms *MVCCStats) updateStatsOnResolve(key proto.Key, origMetaKeySize, origMetaValSize,
	metaKeySize, metaValSize int64, meta *proto.MVCCMetadata, commit bool, origAgeSeconds int64) {
	if !ms.updateStatsForKey(key) {
		return
	}
	// We're pushing or committing an intent; update counts with
	// difference in bytes between old metadata and new.
	keyDiff := metaKeySize - origMetaKeySize
	valDiff := metaValSize - origMetaValSize
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

// updateStatsOnAbort updates stat counters by subtracting an
// aborted value's key and value byte sizes. If an earlier version
// was restored, the restored values are added to live bytes and
// count if the restored value isn't a deletion tombstone.
func (ms *MVCCStats) updateStatsOnAbort(key proto.Key, origMetaKeySize, origMetaValSize,
	restoredMetaKeySize, restoredMetaValSize int64, orig, restored *proto.MVCCMetadata,
	origAgeSeconds, restoredAgeSeconds int64) {
	if !ms.updateStatsForKey(key) {
		return
	}
	origTotalBytes := orig.KeyBytes + orig.ValBytes + origMetaKeySize + origMetaValSize
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

	// If restored version isn't a deletion tombstone, add it to live counters.
	if restored != nil {
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

// updateStatsOnGC updates stat counters after garbage collection
// by subtracting key and value byte counts, updating key and
// value counts, and updating the GC'able bytes age. If meta is
// not nil, then the value being GC'd is the mvcc metadata and we
// decrement the key count.
func (ms *MVCCStats) updateStatsOnGC(key proto.Key, keySize, valSize int64, meta *proto.MVCCMetadata, ageSeconds int64) {
	if !ms.updateStatsForKey(key) {
		return
	}
	ms.KeyBytes -= keySize
	ms.ValBytes -= valSize
	if meta != nil {
		ms.KeyCount--
	} else {
		ms.ValCount--
	}
	ms.GCBytesAge -= MVCCComputeGCBytesAge(keySize+valSize, ageSeconds)
}

// MVCCComputeGCBytesAge comptues the value to assign to the specified
// number of bytes, at the given age (in seconds).
func MVCCComputeGCBytesAge(bytes, ageSeconds int64) int64 {
	return bytes * ageSeconds
}

// MVCCGetRangeStat returns the value for the specified range stat, by
// Raft ID and stat name.
func MVCCGetRangeStat(engine Engine, raftID int64, stat proto.Key) (int64, error) {
	val, err := MVCCGet(engine, RangeStatKey(raftID, stat), proto.ZeroTimestamp, true, nil)
	if err != nil || val == nil {
		return 0, err
	}
	return val.GetInteger(), nil
}

// MVCCSetRangeStat sets the value for the specified range stat, by
// Raft ID and stat name.
func MVCCSetRangeStat(engine Engine, raftID int64, stat proto.Key, statVal int64) error {
	value := proto.Value{Integer: gogoproto.Int64(statVal)}
	if err := MVCCPut(engine, nil, RangeStatKey(raftID, stat), proto.ZeroTimestamp, value, nil); err != nil {
		return err
	}
	return nil
}

// MVCCMergeRangeStat flushes the specified stat to merge counters via
// the provided engine instance.
func MVCCMergeRangeStat(engine Engine, raftID int64, stat proto.Key, statVal int64) error {
	if statVal == 0 {
		return nil
	}
	value := proto.Value{Integer: gogoproto.Int64(statVal)}
	if err := MVCCMerge(engine, nil, RangeStatKey(raftID, stat), value); err != nil {
		return err
	}
	return nil
}

// MVCCGetRangeSize returns the size of the range, equal to the sum of
// the key and value stats.
func MVCCGetRangeSize(engine Engine, raftID int64) (int64, error) {
	keyBytes, err := MVCCGetRangeStat(engine, raftID, StatKeyBytes)
	if err != nil {
		return 0, err
	}
	valBytes, err := MVCCGetRangeStat(engine, raftID, StatValBytes)
	if err != nil {
		return 0, err
	}
	return keyBytes + valBytes, nil
}

// MVCCGetRangeStats reads stat counters for the specified range and
// sets the values in the supplied MVCCStats struct.
func MVCCGetRangeStats(engine Engine, raftID int64, ms *MVCCStats) error {
	var err error
	if ms.LiveBytes, err = MVCCGetRangeStat(engine, raftID, StatLiveBytes); err != nil {
		return err
	}
	if ms.KeyBytes, err = MVCCGetRangeStat(engine, raftID, StatKeyBytes); err != nil {
		return err
	}
	if ms.ValBytes, err = MVCCGetRangeStat(engine, raftID, StatValBytes); err != nil {
		return err
	}
	if ms.IntentBytes, err = MVCCGetRangeStat(engine, raftID, StatIntentBytes); err != nil {
		return err
	}
	if ms.LiveCount, err = MVCCGetRangeStat(engine, raftID, StatLiveCount); err != nil {
		return err
	}
	if ms.KeyCount, err = MVCCGetRangeStat(engine, raftID, StatKeyCount); err != nil {
		return err
	}
	if ms.ValCount, err = MVCCGetRangeStat(engine, raftID, StatValCount); err != nil {
		return err
	}
	if ms.IntentCount, err = MVCCGetRangeStat(engine, raftID, StatIntentCount); err != nil {
		return err
	}
	if ms.IntentAge, err = MVCCGetRangeStat(engine, raftID, StatIntentAge); err != nil {
		return err
	}
	if ms.GCBytesAge, err = MVCCGetRangeStat(engine, raftID, StatGCBytesAge); err != nil {
		return err
	}
	if ms.LastUpdateNanos, err = MVCCGetRangeStat(engine, raftID, StatLastUpdateNanos); err != nil {
		return err
	}
	return nil
}

// MVCCGetProto fetches the value at the specified key and unmarshals
// it using a protobuf decoder. Returns true on success or false if
// the key was not found.
func MVCCGetProto(engine Engine, key proto.Key, timestamp proto.Timestamp, consistent bool, txn *proto.Transaction, msg gogoproto.Message) (bool, error) {
	value, err := MVCCGet(engine, key, timestamp, consistent, txn)
	if err != nil {
		return false, err
	}
	if value == nil || len(value.Bytes) == 0 {
		return false, nil
	}
	if msg != nil {
		if err := gogoproto.Unmarshal(value.Bytes, msg); err != nil {
			return true, err
		}
	}
	return true, nil
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
	meta  proto.MVCCMetadata
	value proto.MVCCValue
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
// keyA : MVCCMetatata of keyA
// keyA_Timestamp_n : value of version_n
// keyA_Timestamp_n-1 : value of version_n-1
// ...
// keyA_Timestamp_0 : value of version_0
// keyB : MVCCMetadata of keyB
// ...
//
// The consistent parameter indicates that intents should cause
// WriteIntentErrors. If set to false, intents are ignored; keys with
// an intent but no earlier committed versions, will be skipped.
func MVCCGet(engine Engine, key proto.Key, timestamp proto.Timestamp, consistent bool, txn *proto.Transaction) (*proto.Value, error) {
	if len(key) == 0 {
		return nil, emptyKeyError()
	}

	// Create a function which scans for the first key between next and end keys.
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
		return nil, err
	}

	return mvccGetInternal(engine, key, metaKey, timestamp, consistent, txn, getValue, buf)
}

// getEarlierFunc fetches an earlier version of a key starting at
// start and ending at end. Returns the value as a byte slice, the
// timestamp of the earlier version, a boolean indicating whether a
// version value or metadata was found, and error, if applicable.
type getValueFunc func(engine Engine, start, end proto.EncodedKey,
	msg gogoproto.Message) (proto.EncodedKey, error)

// mvccGetInternal parses the MVCCMetadata from the specified raw key
// value, and reads the versioned value indicated by timestamp, taking
// the transaction txn into account. getValue is a helper function to
// get an earlier version of the value when doing historical reads.
func mvccGetInternal(engine Engine, key proto.Key, metaKey proto.EncodedKey, timestamp proto.Timestamp,
	consistent bool, txn *proto.Transaction, getValue getValueFunc, buf *getBuffer) (*proto.Value, error) {
	if !consistent && txn != nil {
		return nil, util.Errorf("cannot allow inconsistent reads within a transaction")
	}

	var err error
	meta := &buf.meta

	// If value is inline, return immediately; txn & timestamp are irrelevant.
	if meta.IsInline() {
		if err := meta.Value.Verify(key); err != nil {
			return nil, err
		}
		return meta.Value, nil
	}
	// If we're doing inconsistent reads and there's an intent, we
	// ignore the intent by insisting that the timestamp we're reading
	// at is an historical timestamp < the intent timestamp.
	if !consistent && meta.Txn != nil {
		if !timestamp.Less(meta.Timestamp) {
			timestamp = meta.Timestamp.Prev()
		}
	}

	var valueKey proto.EncodedKey
	value := &buf.value

	// First case: Our read timestamp is ahead of the latest write, or the
	// latest write and current read are within the same transaction.
	if !timestamp.Less(meta.Timestamp) ||
		(meta.Txn != nil && txn != nil && bytes.Equal(meta.Txn.ID, txn.ID)) {
		if meta.Txn != nil && (txn == nil || !bytes.Equal(meta.Txn.ID, txn.ID)) {
			// Trying to read the last value, but it's another transaction's
			// intent; the reader will have to act on this.
			return nil, &proto.WriteIntentError{Key: key, Txn: *meta.Txn}
		}
		latestKey := mvccEncodeTimestamp(metaKey, meta.Timestamp)

		// Check for case where we're reading our own txn's intent
		// but it's got a different epoch. This can happen if the
		// txn was restarted and an earlier iteration wrote the value
		// we're now reading. In this case, we skip the intent.
		if meta.Txn != nil && txn.Epoch != meta.Txn.Epoch {
			valueKey, err = getValue(engine, latestKey.Next(), MVCCEncodeKey(key.Next()), value)
		} else {
			var ok bool
			ok, _, _, err = engine.GetProto(latestKey, value)
			if ok {
				valueKey = latestKey
			}
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
			return nil, &proto.ReadWithinUncertaintyIntervalError{
				Timestamp:         timestamp,
				ExistingTimestamp: meta.Timestamp,
			}
		}

		// We want to know if anything has been written ahead of timestamp, but
		// before MaxTimestamp.
		nextKey := MVCCEncodeVersionKey(key, txn.MaxTimestamp)
		valueKey, err = getValue(engine, nextKey, MVCCEncodeKey(key.Next()), value)
		if err == nil && valueKey != nil {
			_, ts, _ := MVCCDecodeKey(valueKey)
			if timestamp.Less(ts) {
				// Third case: Our read timestamp is sufficiently behind the newest
				// value, but there is another previous write with the same issues
				// as in the second case, so the reader will have to come again
				// with a higher read timestamp.
				return nil, &proto.ReadWithinUncertaintyIntervalError{
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
		nextKey := MVCCEncodeVersionKey(key, timestamp)
		valueKey, err = getValue(engine, nextKey, MVCCEncodeKey(key.Next()), value)
	}

	if err != nil || valueKey == nil {
		return nil, err
	}

	_, ts, isValue := MVCCDecodeKey(valueKey)
	if !isValue {
		return nil, util.Errorf("expected scan to versioned value reading key %q; got %q", key, valueKey)
	}

	if value.Deleted {
		value.Value = nil
	}

	// Set the timestamp if the value is not nil (i.e. not a deletion tombstone).
	if value.Value != nil {
		value.Value.Timestamp = &ts
		if err := value.Value.Verify(key); err != nil {
			return nil, err
		}
	} else if !value.Deleted {
		// Sanity check.
		panic(fmt.Sprintf("encountered MVCC value at key %q with a nil proto.Value but with !Deleted: %+v", key, value))
	}

	return value.Value, nil
}

// putBuffer holds pointer data needed by mvccPutInternal. Bundling
// this data into a single structure reduces memory
// allocations. Managing this temporary buffer using a sync.Pool
// completely eliminates allocation from the put common path.
type putBuffer struct {
	meta    proto.MVCCMetadata
	newMeta proto.MVCCMetadata
	value   proto.MVCCValue
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
// key metadata. We assume the range will check for an existing write
// intent before executing any Put action at the MVCC level.
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
	value proto.MVCCValue, txn *proto.Transaction, buf *putBuffer) error {
	if len(key) == 0 {
		return emptyKeyError()
	}
	if value.Value != nil && value.Value.Bytes != nil && value.Value.Integer != nil {
		return util.Errorf("key %q value contains both a byte slice and an integer value: %+v", key, value)
	}

	meta := &buf.meta
	metaKey := mvccEncodeKey(buf.key[0:0], key)
	ok, origMetaKeySize, origMetaValSize, err := engine.GetProto(metaKey, meta)
	if err != nil {
		return err
	}
	origAgeSeconds := timestamp.WallTime/1E9 - meta.Timestamp.WallTime/1E9

	// Verify we're not mixing inline and non-inline values.
	putIsInline := timestamp.Equal(proto.ZeroTimestamp)
	if ok && putIsInline != meta.IsInline() {
		return util.Errorf("%q: put is inline=%t, but existing value is inline=%t",
			metaKey, putIsInline, meta.IsInline())
	}
	if putIsInline {
		var metaKeySize, metaValSize int64
		if value.Deleted {
			metaKeySize, metaValSize, err = 0, 0, engine.Clear(metaKey)
		} else {
			meta.Value = value.Value
			metaKeySize, metaValSize, err = PutProto(engine, metaKey, meta)
		}
		ms.updateStatsForInline(key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize)
		return err
	}

	var newMeta *proto.MVCCMetadata
	// In case the key metadata exists.
	if ok {
		// There is an uncommitted write intent and the current Put
		// operation does not come from the same transaction.
		// This should not happen since range should check the existing
		// write intent before executing any Put action at MVCC level.
		if meta.Txn != nil && (txn == nil || !bytes.Equal(meta.Txn.ID, txn.ID)) {
			return &proto.WriteIntentError{Key: key, Txn: *meta.Txn}
		}

		// We can update the current metadata only if both the timestamp
		// and epoch of the new intent are greater than or equal to
		// existing. If either of these conditions doesn't hold, it's
		// likely the case that an older RPC is arriving out of order.
		//
		// Note that if meta.Txn!=nil and txn==nil, a WriteIntentError was
		// returned above.
		if !timestamp.Less(meta.Timestamp) &&
			(meta.Txn == nil || txn.Epoch >= meta.Txn.Epoch) {
			// If this is an intent and timestamps have changed,
			// need to remove old version.
			if meta.Txn != nil && !timestamp.Equal(meta.Timestamp) {
				versionKey := mvccEncodeTimestamp(metaKey, meta.Timestamp)
				engine.Clear(versionKey)
			}
			newMeta = &buf.newMeta
			*newMeta = proto.MVCCMetadata{Txn: txn, Timestamp: timestamp}
		} else if timestamp.Less(meta.Timestamp) && meta.Txn == nil {
			// If we receive a Put request to write before an already-
			// committed version, send write tool old error.
			return &proto.WriteTooOldError{Timestamp: timestamp, ExistingTimestamp: meta.Timestamp}
		} else {
			// Otherwise, it's an old write to the current transaction. Just ignore.
			return nil
		}
	} else { // In case the key metadata does not exist yet.
		// If this is a delete, do nothing!
		if value.Deleted {
			return nil
		}
		// Create key metadata.
		meta = nil
		newMeta = &buf.newMeta
		*newMeta = proto.MVCCMetadata{Txn: txn, Timestamp: timestamp}
	}

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
	ms.updateStatsOnPut(key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, meta, newMeta, origAgeSeconds)

	return nil
}

// MVCCIncrement fetches the value for key, and assuming the value is
// an "integer" type, increments it by inc and stores the new
// value. The newly incremented value is returned.
func MVCCIncrement(engine Engine, ms *MVCCStats, key proto.Key, timestamp proto.Timestamp, txn *proto.Transaction, inc int64) (int64, error) {
	// Handle check for non-existence of key. In order to detect
	// the potential write intent by another concurrent transaction
	// with a newer timestamp, we need to use the max timestamp
	// while reading.
	value, err := MVCCGet(engine, key, proto.MaxTimestamp, true, txn)
	if err != nil {
		return 0, err
	}

	var int64Val int64
	// If the value exists, verify it's an integer type not a byte slice.
	if value != nil {
		if value.Bytes != nil || value.Integer == nil {
			return 0, util.Errorf("cannot increment key %q which already has a generic byte value: %+v", key, *value)
		}
		int64Val = value.GetInteger()
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
	newValue := proto.Value{Integer: gogoproto.Int64(r)}
	newValue.InitChecksum(key)
	return r, MVCCPut(engine, ms, key, timestamp, newValue, txn)
}

// MVCCConditionalPut sets the value for a specified key only if the
// expected value matches. If not, the return a ConditionFailedError
// containing the actual value.
func MVCCConditionalPut(engine Engine, ms *MVCCStats, key proto.Key, timestamp proto.Timestamp, value proto.Value,
	expValue *proto.Value, txn *proto.Transaction) error {
	// Handle check for non-existence of key. In order to detect
	// the potential write intent by another concurrent transaction
	// with a newer timestamp, we need to use the max timestamp
	// while reading.
	existVal, err := MVCCGet(engine, key, proto.MaxTimestamp, true, txn)
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
		} else if expValue.Integer != nil && (existVal.Integer == nil || expValue.GetInteger() != existVal.GetInteger()) {
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
	meta := &proto.MVCCMetadata{Value: &value}
	data, err := gogoproto.Marshal(meta)
	if err != nil {
		return err
	}
	engine.Merge(metaKey, data)
	ms.updateStatsOnMerge(key, int64(len(value.Bytes)))
	return nil
}

// MVCCDeleteRange deletes the range of key/value pairs specified by
// start and end keys. Specify max=0 for unbounded deletes.
func MVCCDeleteRange(engine Engine, ms *MVCCStats, key, endKey proto.Key, max int64, timestamp proto.Timestamp, txn *proto.Transaction) (int64, error) {
	// In order to detect the potential write intent by another
	// concurrent transaction with a newer timestamp, we need
	// to use the max timestamp for scan.
	kvs, err := MVCCScan(engine, key, endKey, max, proto.MaxTimestamp, true, txn)
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
	consistent bool, txn *proto.Transaction) ([]proto.KeyValue, error) {
	res := []proto.KeyValue{}
	if err := MVCCIterate(engine, key, endKey, timestamp, consistent, txn, func(kv proto.KeyValue) (bool, error) {
		res = append(res, kv)
		if max != 0 && max == int64(len(res)) {
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	return res, nil
}

// MVCCIterate iterates over the key range specified by start and end
// keys, At each step of the iteration, f() is invoked with the
// current key/value pair. If f returns true (done) or an error, the
// iteration stops and the error is propagated.
func MVCCIterate(engine Engine, key, endKey proto.Key, timestamp proto.Timestamp,
	consistent bool, txn *proto.Transaction, f func(proto.KeyValue) (bool, error)) error {
	if !consistent && txn != nil {
		return util.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(endKey) == 0 {
		return emptyKeyError()
	}

	buf := getBufferPool.Get().(*getBuffer)
	defer getBufferPool.Put(buf)

	// We store encEndKey and encKey in the same buffer to avoid memory
	// allocations.
	encEndKey := mvccEncodeKey(buf.key[0:0], endKey)
	keyBuf := encEndKey[len(encEndKey):]
	encKey := mvccEncodeKey(keyBuf, key)

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

	for {
		iter.Seek(encKey)
		if !iter.Valid() {
			return iter.Error()
		}
		metaKey := iter.Key()
		if bytes.Compare(metaKey, encEndKey) >= 0 {
			return iter.Error()
		}
		key, _, isValue := MVCCDecodeKey(metaKey)
		if isValue {
			return util.Errorf("expected an MVCC metadata key: %q", metaKey)
		}
		if err := iter.ValueProto(&buf.meta); err != nil {
			return err
		}
		value, err := mvccGetInternal(engine, key, metaKey, timestamp, consistent, txn, getValue, buf)
		if err != nil {
			return err
		}
		if value != nil {
			done, err := f(proto.KeyValue{Key: key, Value: *value})
			if done || err != nil {
				return err
			}
		}
		encKey = mvccEncodeKey(keyBuf, key.Next())
	}
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
	meta := &proto.MVCCMetadata{}
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
		ms.updateStatsOnResolve(key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, &newMeta, commit, origAgeSeconds)

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
			engine.Clear(origKey)
			engine.Put(newKey, valBytes)
		}
		return nil
	}

	// This method shouldn't be called with this instance, but there's
	// nothing to do if the epochs match and the state is still PENDING.
	if txn.Status == proto.PENDING && meta.Txn.Epoch == txn.Epoch {
		return nil
	}

	// Otherwise, we're deleting the intent. We must find the next
	// versioned value and reset the metadata's latest timestamp. If
	// there are no other versioned values, we delete the metadata
	// key.

	// First clear the intent value.
	latestKey := MVCCEncodeVersionKey(key, meta.Timestamp)
	engine.Clear(latestKey)

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
		engine.Clear(metaKey)
		// Clear stat counters attributable to the intent we're aborting.
		ms.updateStatsOnAbort(key, origMetaKeySize, origMetaValSize, 0, 0, meta, nil, origAgeSeconds, 0)
	} else {
		_, ts, isValue := MVCCDecodeKey(kvs[0].Key)
		if !isValue {
			return util.Errorf("expected an MVCC value key: %s", kvs[0].Key)
		}
		// Get the bytes for the next version so we have size for stat counts.
		value := proto.MVCCValue{}
		ok, _, valueSize, err := engine.GetProto(kvs[0].Key, &value)
		if err != nil || !ok {
			return util.Errorf("unable to fetch previous version for key %q (%t): %s", kvs[0].Key, ok, err)
		}
		// Update the keyMetadata with the next version.
		newMeta := &proto.MVCCMetadata{
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
		ms.updateStatsOnAbort(key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, meta, newMeta, origAgeSeconds, restoredAgeSeconds)
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
		// old versions for this key; refer to Scan for details.
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
		meta := &proto.MVCCMetadata{}
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
			ms.updateStatsOnGC(gcKey.Key, int64(len(iter.Key())), int64(len(iter.Value())), meta, ageSeconds)
			engine.Clear(iter.Key())
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
				ms.updateStatsOnGC(gcKey.Key, mvccVersionTimestampSize, int64(len(iter.Value())), nil, ageSeconds)
				engine.Clear(iter.Key())
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
func IsValidSplitKey(key proto.Key) bool {
	return isValidEncodedSplitKey(MVCCEncodeKey(key))
}

// illegalSplitKeyRanges detail illegal ranges for split keys,
// exclusive of start and end.
var illegalSplitKeyRanges = []struct {
	start, end proto.EncodedKey
}{
	{
		start: MVCCEncodeKey(KeyMin),
		end:   MVCCEncodeKey(KeyMeta2Prefix),
	},
	{
		start: MVCCEncodeKey(KeyConfigAccountingPrefix),
		end:   MVCCEncodeKey(KeyConfigAccountingPrefix.PrefixEnd()),
	},
	{
		start: MVCCEncodeKey(KeyConfigPermissionPrefix),
		end:   MVCCEncodeKey(KeyConfigPermissionPrefix.PrefixEnd()),
	},
	{
		start: MVCCEncodeKey(KeyConfigZonePrefix),
		end:   MVCCEncodeKey(KeyConfigZonePrefix.PrefixEnd()),
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
func MVCCFindSplitKey(engine Engine, raftID int64, key, endKey proto.Key) (proto.Key, error) {
	if key.Less(KeyLocalMax) {
		key = KeyLocalMax
	}
	encStartKey := MVCCEncodeKey(key)
	encEndKey := MVCCEncodeKey(endKey)

	// Get range size from stats.
	rangeSize, err := MVCCGetRangeSize(engine, raftID)
	if err != nil {
		return nil, err
	}

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
func MVCCComputeStats(engine Engine, key, endKey proto.Key, nowNanos int64) (MVCCStats, error) {
	if key.Less(KeyLocalMax) {
		key = KeyLocalMax
	}
	encStartKey := MVCCEncodeKey(key)
	encEndKey := MVCCEncodeKey(endKey)

	ms := MVCCStats{LastUpdateNanos: nowNanos}
	first := false
	meta := &proto.MVCCMetadata{}
	err := engine.Iterate(encStartKey, encEndKey, func(kv proto.RawKeyValue) (bool, error) {
		_, ts, isValue := MVCCDecodeKey(kv.Key)
		if !isValue {
			totalBytes := int64(len(kv.Value)) + int64(len(kv.Key))
			first = true
			if err := gogoproto.Unmarshal(kv.Value, meta); err != nil {
				return false, util.Errorf("unable to unmarshal MVCC metadata %q: %s", kv.Value, err)
			}
			if !meta.Deleted {
				ms.LiveBytes += totalBytes
				ms.LiveCount++
			} else {
				// First value is deleted, so it's GC'able; add meta key & value bytes to age stat.
				ms.GCBytesAge += totalBytes * (nowNanos/1E9 - meta.Timestamp.WallTime/1E9)
			}
			ms.KeyBytes += int64(len(kv.Key))
			ms.ValBytes += int64(len(kv.Value))
			ms.KeyCount++
			if meta.IsInline() {
				ms.ValCount++
			}
		} else {
			totalBytes := int64(len(kv.Value)) + mvccVersionTimestampSize
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
					return false, util.Errorf("expected mvcc metadata key bytes to equal %d; got %d", mvccVersionTimestampSize, meta.KeyBytes)
				}
				if meta.ValBytes != int64(len(kv.Value)) {
					return false, util.Errorf("expected mvcc metadata val bytes to equal %d; got %d", len(kv.Value), meta.ValBytes)
				}
			} else {
				// Overwritten value; add value bytes to the GC'able bytes age stat.
				ms.GCBytesAge += totalBytes * (nowNanos/1E9 - ts.WallTime/1E9)
			}
			ms.KeyBytes += mvccVersionTimestampSize
			ms.ValBytes += int64(len(kv.Value))
			ms.ValCount++
		}
		return false, nil
	})
	return ms, err
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
	tsBytes, key := encoding.DecodeBytes(encodedKey)
	if len(tsBytes) == 0 {
		return key, proto.Timestamp{}, false
	} else if len(tsBytes) != 12 {
		panic(fmt.Sprintf("there should be 12 bytes for encoded timestamp: %q", tsBytes))
	}
	tsBytes, walltime := encoding.DecodeUint64Decreasing(tsBytes)
	tsBytes, logical := encoding.DecodeUint32Decreasing(tsBytes)
	return key, proto.Timestamp{WallTime: int64(walltime), Logical: int32(logical)}, true
}
