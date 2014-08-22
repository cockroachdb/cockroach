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

package engine

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// MVCC wraps the mvcc operations of a key/value store.
type MVCC struct {
	engine Engine // The underlying key-value store
}

// writeIntentError is a trivial implementation of error.
type writeIntentError struct {
	TxnID string
}

type writeTimestampTooOldError struct {
	Timestamp proto.Timestamp
}

// Constants for system-reserved value prefix.
const (
	// valueNormalPrefix is the prefix for the normal value.
	valueNormalPrefix = byte(0)
	// valueDeletedPrefix is the prefix for the deleted value.
	valueDeletedPrefix = byte(1)
)

func (e *writeIntentError) Error() string {
	return fmt.Sprintf("there exists a write intent from transaction %s", e.TxnID)
}

func (e *writeTimestampTooOldError) Error() string {
	return fmt.Sprintf("cannot write with a timestamp older than %+v", e.Timestamp)
}

// NewMVCC returns a new instance of MVCC.
func NewMVCC(engine Engine) *MVCC {
	return &MVCC{
		engine: engine,
	}
}

// Get returns the value for the key specified in the request, while
// satisfying the given timestamp condition. The key may be
// arbitrarily encoded; it will be binary-encoded to remove any
// internal null characters. txnID in the response is used to indicate
// that the response value belongs to a write intent.
func (mvcc *MVCC) Get(key Key, timestamp proto.Timestamp, txnID string) (Value, error) {
	binKey := encoding.EncodeBinary(nil, key)
	value, ts, _, err := mvcc.getInternal(binKey, timestamp, txnID)
	// In case of error, or the key doesn't exist, or the key was deleted.
	if err != nil || len(value) == 0 || value[0] == valueDeletedPrefix {
		return Value{}, err
	}

	// TODO(Jiang-Ming): use unwrapChecksum here.
	return Value{Bytes: value[1:], Timestamp: ts}, nil
}

// getInternal implements the actual logic of get function.
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
func (mvcc *MVCC) getInternal(key Key, timestamp proto.Timestamp, txnID string) ([]byte, proto.Timestamp, string, error) {
	meta := &proto.MVCCMetadata{}
	ok, err := GetProto(mvcc.engine, key, meta)
	if err != nil || !ok {
		return nil, proto.Timestamp{}, "", err
	}
	// If the read timestamp is greater than the latest one, we can just
	// fetch the value without a scan.
	if !timestamp.Less(meta.Timestamp) {
		if len(meta.TxnID) > 0 && (len(txnID) == 0 || meta.TxnID != txnID) {
			return nil, proto.Timestamp{}, "", &writeIntentError{TxnID: meta.TxnID}
		}

		latestKey := mvccEncodeKey(key, meta.Timestamp)
		val, err := mvcc.engine.Get(latestKey)
		return val, meta.Timestamp, meta.TxnID, err
	}

	nextKey := mvccEncodeKey(key, timestamp)
	// We use the PrefixEndKey(key) as the upper bound for scan.
	// If there is no other version after nextKey, it won't return
	// the value of the next key.
	kvs, err := mvcc.engine.Scan(nextKey, PrefixEndKey(key), 1)
	if len(kvs) > 0 {
		_, ts, _ := mvccDecodeKey(kvs[0].Key)
		return kvs[0].Value, ts, "", err
	}
	return nil, proto.Timestamp{}, "", err
}

// Put sets the value for a specified key. It will save the value with
// different versions according to its timestamp and update the key metadata.
// We assume the range will check for an existing write intent before
// executing any Put action at the MVCC level.
func (mvcc *MVCC) Put(key Key, timestamp proto.Timestamp, value Value, txnID string) error {
	binKey := encoding.EncodeBinary(nil, key)
	if !value.Timestamp.Equal(proto.Timestamp{}) && !value.Timestamp.Equal(timestamp) {
		return util.Errorf(
			"the timestamp %+v provided in value does not match the timestamp %+v in request",
			value.Timestamp, timestamp)
	}

	// TODO(Jiang-Ming): use wrapChecksum here.
	// into val which need to be used in get response.
	val := bytes.Join([][]byte{[]byte{valueNormalPrefix}, value.Bytes}, []byte(""))
	return mvcc.putInternal(binKey, timestamp, val, txnID)
}

// Delete marks the key deleted and will not return in the next get response.
func (mvcc *MVCC) Delete(key Key, timestamp proto.Timestamp, txnID string) error {
	binKey := encoding.EncodeBinary(nil, key)
	return mvcc.putInternal(binKey, timestamp, []byte{valueDeletedPrefix}, txnID)
}

func (mvcc *MVCC) putInternal(key Key, timestamp proto.Timestamp, value []byte, txnID string) error {
	meta := &proto.MVCCMetadata{}
	ok, err := GetProto(mvcc.engine, key, meta)
	if err != nil {
		return err
	}

	// In case the key metadata exists.
	if ok {
		// There is an uncommitted write intent and the current Put
		// operation does not come from the same transaction.
		// This should not happen since range should check the existing
		// write intent before executing any Put action at MVCC level.
		if len(meta.TxnID) > 0 && (len(txnID) == 0 || meta.TxnID != txnID) {
			return &writeIntentError{TxnID: meta.TxnID}
		}

		if meta.Timestamp.Less(timestamp) ||
			(timestamp.Equal(meta.Timestamp) && txnID == meta.TxnID) {
			// Update MVCC metadata.
			meta = &proto.MVCCMetadata{TxnID: txnID, Timestamp: timestamp}
			if err := PutProto(mvcc.engine, key, meta); err != nil {
				return err
			}
		} else {
			// In case we receive a Put request to update an old version,
			// it must be an error since raft should handle any client
			// retry from timeout.
			return &writeTimestampTooOldError{Timestamp: meta.Timestamp}
		}
	} else { // In case the key metadata does not exist yet.
		// Create key metadata.
		meta = &proto.MVCCMetadata{TxnID: txnID, Timestamp: timestamp}
		if err := PutProto(mvcc.engine, key, meta); err != nil {
			return err
		}
	}

	// Save the value with the given version (Key + Timestamp).
	return mvcc.engine.Put(mvccEncodeKey(key, timestamp), value)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (mvcc *MVCC) ConditionalPut(key Key, timestamp proto.Timestamp, value Value, expValue Value, txnID string) (Value, error) {
	// Handle check for non-existence of key. In order to detect
	// the potential write intent by another concurrent transaction
	// with a newer timestamp, we need to use the max timestamp
	// while reading.
	val, err := mvcc.Get(key, proto.MaxTimestamp, txnID)
	if err != nil {
		return Value{}, err
	}

	if expValue.Bytes == nil && val.Bytes != nil {
		return Value{}, util.Errorf("key %q already exists", key)
	} else if expValue.Bytes != nil {
		// Handle check for existence when there is no key.
		if val.Bytes == nil {
			return Value{}, util.Errorf("key %q does not exist", key)
		} else if !bytes.Equal(expValue.Bytes, val.Bytes) {
			return val, util.Errorf("key %q does not match existing", key)
		}
	}

	err = mvcc.Put(key, timestamp, value, txnID)
	return Value{}, err
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys. Specify max=0 for unbounded deletes.
func (mvcc *MVCC) DeleteRange(key Key, endKey Key, max int64, timestamp proto.Timestamp, txnID string) (int64, error) {
	// In order to detect the potential write intent by another
	// concurrent transaction with a newer timestamp, we need
	// to use the max timestamp for scan.
	kvs, txnID, err := mvcc.Scan(key, endKey, max, proto.MaxTimestamp, txnID)
	if err != nil {
		return 0, err
	}

	num := int64(0)
	for _, kv := range kvs {
		err = mvcc.Delete(kv.Key, timestamp, txnID)
		if err != nil {
			return num, err
		}
		num++
	}
	return num, nil
}

// Scan scans the key range specified by start key through end key up
// to some maximum number of results. Specify max=0 for unbounded scans.
func (mvcc *MVCC) Scan(key Key, endKey Key, max int64, timestamp proto.Timestamp, txnID string) ([]KeyValue, string, error) {
	binKey := encoding.EncodeBinary(nil, key)
	binEndKey := encoding.EncodeBinary(nil, endKey)
	nextKey := binKey

	res := []KeyValue{}
	for {
		kvs, err := mvcc.engine.Scan(nextKey, binEndKey, 1)
		if err != nil {
			return nil, "", err
		}
		// No more keys exists in the given range.
		if len(kvs) == 0 {
			break
		}

		remainder, currentKey := encoding.DecodeBinary(kvs[0].Key)
		if len(remainder) != 0 {
			return nil, "", util.Errorf("expected an MVCC metadata key: %s", kvs[0].Key)
		}
		value, err := mvcc.Get(currentKey, timestamp, txnID)
		if err != nil {
			return res, "", err
		}

		if value.Bytes != nil {
			res = append(res, KeyValue{Key: currentKey, Value: value})
		}

		if max != 0 && max == int64(len(res)) {
			break
		}

		// In order to efficiently skip the possibly long list of
		// old versions for this key, we move instead to the next
		// highest key and the for loop continues by scanning again
		// with nextKey.
		// Let's say you have:
		// a
		// a<T=2>
		// a<T=1>
		// aa
		// aa<T=3>
		// aa<T=2>
		// b
		// b<T=5>
		// In this case, if we scan from "a"-"b", we wish to skip
		// a<T=2> and a<T=1> and find "aa'.
		nextKey = encoding.EncodeBinary(nil, NextKey(currentKey))
	}

	return res, txnID, nil
}

// ResolveWriteIntent either commits or aborts (rolls back) an extant
// write intent for a given txnID according to commit parameter.
// ResolveWriteIntent will skip write intents of other txnIDs.
func (mvcc *MVCC) ResolveWriteIntent(key Key, txnID string, commit bool) error {
	if len(txnID) == 0 {
		return util.Error("missing txnID in request")
	}

	binKey := encoding.EncodeBinary(nil, key)
	meta := &proto.MVCCMetadata{}
	ok, err := GetProto(mvcc.engine, binKey, meta)
	if err != nil {
		return err
	}
	if !ok {
		return util.Errorf("key %q does not exist", key)
	}

	if len(meta.TxnID) == 0 {
		return util.Errorf("write intent %q does not exist", key)
	}
	if meta.TxnID != txnID {
		return util.Errorf("cannot commit another TxnID %s from TxnID %s",
			meta.TxnID, txnID)
	}

	if !commit {
		latestKey := mvccEncodeKey(binKey, meta.Timestamp)
		err = mvcc.engine.Clear(latestKey)
		if err != nil {
			return err
		}

		// Compute the next possible mvcc value for this key.
		nextKey := NextKey(latestKey)
		// Compute the last possible mvcc value for this key.
		endScanKey := encoding.EncodeBinary(nil, NextKey(key))
		kvs, err := mvcc.engine.Scan(nextKey, endScanKey, 1)
		if err != nil {
			return err
		}
		// If there is no other version, we should just clean up the key entirely.
		if len(kvs) == 0 {
			return mvcc.engine.Clear(binKey)
		}
		_, ts, isValue := mvccDecodeKey(kvs[0].Key)
		if !isValue {
			return util.Errorf("expected an MVCC value key: %s", kvs[0].Key)
		}
		// Update the keyMetadata with the next version.
		return PutProto(mvcc.engine, binKey, &proto.MVCCMetadata{TxnID: "", Timestamp: ts})
	}

	return PutProto(mvcc.engine, binKey, &proto.MVCCMetadata{TxnID: "", Timestamp: meta.Timestamp})
}

// ResolveWriteIntentRange commits or aborts (rolls back) the range of
// write intents specified by start and end keys for a given txnID
// according to commit parameter.  ResolveWriteIntentRange will skip
// write intents of other txnIDs. Specify max=0 for unbounded
// resolves.
func (mvcc *MVCC) ResolveWriteIntentRange(key Key, endKey Key, max int64, txnID string, commit bool) (int64, error) {
	if len(txnID) == 0 {
		return 0, util.Error("missing txnID in request")
	}

	binKey := encoding.EncodeBinary(nil, key)
	binEndKey := encoding.EncodeBinary(nil, endKey)
	nextKey := binKey

	num := int64(0)
	for {
		kvs, err := mvcc.engine.Scan(nextKey, binEndKey, 1)
		if err != nil {
			return num, err
		}
		// No more keys exists in the given range.
		if len(kvs) == 0 {
			break
		}

		remainder, currentKey := encoding.DecodeBinary(kvs[0].Key)
		if len(remainder) != 0 {
			return 0, util.Errorf("expected an MVCC metadata key: %s", kvs[0].Key)
		}
		_, _, existingTxnID, err := mvcc.getInternal(kvs[0].Key, proto.MaxTimestamp, txnID)
		// Return the error unless its a writeIntentError, which
		// will occur in the event we scan a key with a write
		// intent belonging to a different transaction.
		if _, ok := err.(*writeIntentError); err != nil && !ok {
			return num, err
		}
		// ResolveWriteIntent only needs to deal with the write
		// intents for the given txnID.
		if err == nil && existingTxnID == txnID {
			// commits or aborts (rolls back) the write intent of
			// the given txnID.
			err = mvcc.ResolveWriteIntent(currentKey, txnID, commit)
			if err != nil {
				return num, err
			}
			num++
		}

		if max != 0 && max == num {
			break
		}

		// In order to efficiently skip the possibly long list of
		// old versions for this key; refer to Scan for details.
		nextKey = encoding.EncodeBinary(nil, NextKey(currentKey))
	}

	return num, nil
}

// mvccEncodeKey makes a timestamped key which is the concatenation of
// the given key and the corresponding timestamp. The key is expected
// to have been encoded using EncodeBinary.
func mvccEncodeKey(key Key, timestamp proto.Timestamp) Key {
	if timestamp.WallTime < 0 || timestamp.Logical < 0 {
		panic(fmt.Sprintf("negative values disallowed in timestamps: %+v", timestamp))
	}
	k := append([]byte{}, key...)
	k = encoding.EncodeUint64Decreasing(k, uint64(timestamp.WallTime))
	k = encoding.EncodeUint64Decreasing(k, uint64(timestamp.Logical))
	return k
}

// mvccDecodeKey decodes encodedKey into key and Timestamp. The final
// returned bool is true if this is an MVCC value and false if this is
// MVCC metadata. Note that the returned key is exactly the value of
// key passed to mvccEncodeKey. A separate DecodeBinary step must be
// carried out to decode it if necessary.
func mvccDecodeKey(encodedKey []byte) (Key, proto.Timestamp, bool) {
	tsBytes, _ := encoding.DecodeBinary(encodedKey)
	key := encodedKey[:len(encodedKey)-len(tsBytes)]
	if len(tsBytes) == 0 {
		return key, proto.Timestamp{}, false
	}
	tsBytes, walltime := encoding.DecodeUint64Decreasing(tsBytes)
	tsBytes, logical := encoding.DecodeUint64Decreasing(tsBytes)
	if len(tsBytes) > 0 {
		panic(fmt.Sprintf("leftover bytes on mvcc key decode: %s", tsBytes))
	}
	return key, proto.Timestamp{WallTime: int64(walltime), Logical: int64(logical)}, true
}
