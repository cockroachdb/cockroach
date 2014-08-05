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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)

package engine

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/encoding"
	"github.com/cockroachdb/cockroach/hlc"
	"github.com/cockroachdb/cockroach/util"
)

// MVCC wraps the mvcc operations of a key/value store.
type MVCC struct {
	engine Engine // The underlying key-value store
}

type keyMetadata struct {
	TxnID     string // TODO(spencer): replace the TxID with a Txn struct.
	Timestamp hlc.HLTimestamp
}

// writeIntentError is a trivial implementation of error.
type writeIntentError struct {
	TxnID string
}

type writeTimestampTooOldError struct {
	Timestamp hlc.HLTimestamp
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

// Get returns the value for the key specified in the request and it
// needs to satisfy the given timestamp condition.
// txnID in the response is used to indicate that the response value
// belongs to a write intent.
func (mvcc *MVCC) Get(key Key, timestamp hlc.HLTimestamp, txnID string) (Value, string, error) {
	value, ts, txnID, err := mvcc.getInternal(key, timestamp, txnID)
	// In case of error, or the key doesn't exist, or the key was deleted.
	if err != nil || len(value) == 0 || value[0] == valueDeletedPrefix {
		return Value{}, txnID, err
	}

	// TODO(Jiang-Ming): use unwrapChecksum here.
	return Value{Bytes: value[1:], Timestamp: ts}, txnID, nil
}

// getInternal implements the actual logic of get function.
// The values of multiple versions for the given key should
// be organized as follows:
// ...
// keyA : keyMetatata of keyA
// keyA_Timestamp_n : value of version_n
// keyA_Timestamp_n-1 : value of version_n-1
// ...
// keyA_Timestamp_0 : value of version_0
// keyB : keyMetadata of keyB
// ...
func (mvcc *MVCC) getInternal(key Key, timestamp hlc.HLTimestamp, txnID string) ([]byte, hlc.HLTimestamp, string, error) {
	keyMetadata := &keyMetadata{}
	ok, err := GetI(mvcc.engine, key, keyMetadata)
	if err != nil || !ok {
		return nil, hlc.HLTimestamp{}, "", err
	}

	// If the read timestamp is greater than the latest one, we can just
	// fetch the value without a scan.
	if !timestamp.Less(keyMetadata.Timestamp) {
		if len(keyMetadata.TxnID) > 0 && (len(txnID) == 0 || keyMetadata.TxnID != txnID) {
			return nil, hlc.HLTimestamp{}, "", &writeIntentError{TxnID: keyMetadata.TxnID}
		}

		latestKey := mvccEncodeKey(key, keyMetadata.Timestamp)
		val, err := mvcc.engine.Get(latestKey)
		return val, keyMetadata.Timestamp, keyMetadata.TxnID, err
	}

	nextKey := mvccEncodeKey(key, timestamp)
	// We use the PrefixEndKey(key) as the upper bound for scan.
	// If there is no other version after nextKey, it won't return
	// the value of the next key.
	kvs, err := mvcc.engine.Scan(nextKey, PrefixEndKey(key), 1)
	if len(kvs) > 0 {
		_, ts := mvccDecodeKey(kvs[0].Key)
		return kvs[0].Value, ts, "", err
	}
	return nil, hlc.HLTimestamp{}, "", err
}

// Put sets the value for a specified key. It will save the value with
// different versions according to its timestamp and update the key metadata.
// We assume the range will check for an existing write intent before
// executing any Put action at the MVCC level.
func (mvcc *MVCC) Put(key Key, timestamp hlc.HLTimestamp, value Value, txnID string) error {
	if !value.Timestamp.Equal(hlc.HLTimestamp{}) && !value.Timestamp.Equal(timestamp) {
		return util.Errorf(
			"the timestamp %+v provided in value does not match the timestamp %+v in request",
			value.Timestamp, timestamp)
	}

	// TODO(Jiang-Ming): use wrapChecksum here.
	// into val which need to be used in get response.
	val := bytes.Join([][]byte{[]byte{valueNormalPrefix}, value.Bytes}, []byte(""))
	return mvcc.putInternal(key, timestamp, val, txnID)
}

// Delete marks the key deleted and will not return in the next get response.
func (mvcc *MVCC) Delete(key Key, timestamp hlc.HLTimestamp, txnID string) error {
	return mvcc.putInternal(key, timestamp, []byte{valueDeletedPrefix}, txnID)
}

func (mvcc *MVCC) putInternal(key Key, timestamp hlc.HLTimestamp, value []byte, txnID string) error {
	keyMeta := &keyMetadata{}
	ok, err := GetI(mvcc.engine, key, keyMeta)
	if err != nil {
		return err
	}

	// In case the key metadata exists.
	if ok {
		// There is an uncommitted write intent and the current Put
		// operation does not come from the same transaction.
		// This should not happen since range should check the existing
		// write intent before executing any Put action at MVCC level.
		if len(keyMeta.TxnID) > 0 && (len(txnID) == 0 || keyMeta.TxnID != txnID) {
			return &writeIntentError{TxnID: keyMeta.TxnID}
		}

		if keyMeta.Timestamp.Less(timestamp) ||
			(timestamp.Equal(keyMeta.Timestamp) && txnID == keyMeta.TxnID) {
			// Update key metadata.
			PutI(mvcc.engine, key, &keyMetadata{TxnID: txnID, Timestamp: timestamp})
		} else {
			// In case we receive a Put request to update an old version,
			// it must be an error since raft should handle any client
			// retry from timeout.
			return &writeTimestampTooOldError{Timestamp: keyMeta.Timestamp}
		}
	} else { // In case the key metadata does not exist yet.
		// Create key metadata.
		PutI(mvcc.engine, key, &keyMetadata{TxnID: txnID, Timestamp: timestamp})
	}

	// Save the value with the given version (Key + Timestamp).
	return mvcc.engine.Put(mvccEncodeKey(key, timestamp), value)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (mvcc *MVCC) ConditionalPut(key Key, timestamp hlc.HLTimestamp, value Value, expValue Value, txnID string) (Value, error) {
	// Handle check for non-existence of key. In order to detect
	// the potential write intent by another concurrent transaction
	// with a newer timestamp, we need to use the max timestamp
	// while reading.
	val, _, err := mvcc.Get(key, hlc.MaxHLTimestamp, txnID)
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
func (mvcc *MVCC) DeleteRange(key Key, endKey Key, max int64, timestamp hlc.HLTimestamp, txnID string) (int64, error) {
	// In order to detect the potential write intent by another
	// concurrent transaction with a newer timestamp, we need
	// to use the max timestamp for scan.
	kvs, txnID, err := mvcc.Scan(key, endKey, max, hlc.MaxHLTimestamp, txnID)
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
func (mvcc *MVCC) Scan(key Key, endKey Key, max int64, timestamp hlc.HLTimestamp, txnID string) ([]KeyValue, string, error) {
	nextKey := key
	// TODO(Jiang-Ming): remove this after we put everything via MVCC.
	// Currently, we need to skip the series of reserved system
	// key / value pairs covering accounting, range metadata, node
	// accounting and permissions before the actual key / value pairs
	// since they don't have keyMetadata.
	if nextKey.Less(PrefixEndKey(Key("\x00"))) {
		nextKey = PrefixEndKey(Key("\x00"))
	}

	res := []KeyValue{}
	for {
		kvs, err := mvcc.engine.Scan(nextKey, endKey, 1)
		if err != nil {
			return nil, "", err
		}
		// No more keys exists in the given range.
		if len(kvs) == 0 {
			break
		}

		currentKey := kvs[0].Key
		value, _, err := mvcc.Get(currentKey, timestamp, txnID)
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
		nextKey = NextKey(mvccEncodeKey(currentKey, hlc.MinHLTimestamp))
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

	keyMeta := &keyMetadata{}
	ok, err := GetI(mvcc.engine, key, keyMeta)
	if err != nil {
		return err
	}
	if !ok {
		return util.Errorf("key %q does not exist", key)
	}

	if len(keyMeta.TxnID) == 0 {
		return util.Errorf("write intent does not exist", key)
	}
	if keyMeta.TxnID != txnID {
		return util.Errorf("cannot commit another TxnID %s from TxnID %s",
			keyMeta.TxnID, txnID)
	}

	if !commit {
		latestKey := mvccEncodeKey(key, keyMeta.Timestamp)
		err = mvcc.engine.Clear(latestKey)
		if err != nil {
			return err
		}

		nextKey := NextKey(latestKey)
		kvs, err := mvcc.engine.Scan(nextKey, PrefixEndKey(key), 1)
		if err != nil {
			return err
		}
		// If there is no other version, we should just clean up the key entirely.
		if len(kvs) == 0 {
			return mvcc.engine.Clear(key)
		}

		_, ts := mvccDecodeKey(kvs[0].Key)
		// Update the keyMetadata with the next version.
		return PutI(mvcc.engine, key, &keyMetadata{TxnID: "", Timestamp: ts})
	}

	return PutI(mvcc.engine, key, &keyMetadata{TxnID: "", Timestamp: keyMeta.Timestamp})
}

// ResolveWriteIntentRange commits or aborts (rolls back)
// the range of write intents specified by start and end
// keys for a given txnID according to commit parameter.
// ResolveWriteIntentRange will skip write intents of
// other txnIDs. Specify max=0 for unbounded resolves.
func (mvcc *MVCC) ResolveWriteIntentRange(key Key, endKey Key, max int64, txnID string, commit bool) (int64, error) {
	if len(txnID) == 0 {
		return 0, util.Error("missing txnID in request")
	}

	nextKey := key
	// TODO(Jiang-Ming): remove this after we put everything via MVCC.
	// Currently, we need to skip the series of reserved system
	// key / value pairs covering accounting, range metadata, node
	// accounting and permissions before the actual key / value pairs
	// since they don't have keyMetadata.
	if nextKey.Less(PrefixEndKey(Key("\x00"))) {
		nextKey = PrefixEndKey(Key("\x00"))
	}

	num := int64(0)
	for {
		kvs, err := mvcc.engine.Scan(nextKey, endKey, 1)
		if err != nil {
			return num, err
		}
		// No more keys exists in the given range.
		if len(kvs) == 0 {
			break
		}

		currentKey := kvs[0].Key
		_, existingTxnID, err := mvcc.Get(currentKey, hlc.MaxHLTimestamp, txnID)
		// Return the error unless its a writeIntentError, which
		// will occur in the event we scan a key with a write
		// intent belonging to a different transaction.
		if _, ok := err.(*writeIntentError); err != nil && !ok {
			return num, err
		}
		// endRangTransaction only needs to deal with the write
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
		// old versions for this key, please refer to scan function
		// for details.
		nextKey = NextKey(mvccEncodeKey(currentKey, hlc.MinHLTimestamp))
	}

	return num, nil
}

// makeKeyTimestamped makes a timestamped key which is the concatenation
// of the given key and the corresponding timestamp. It assumes the
// key was already encoded before passed to mvcc layer thus it won't
// encode the key part again.
func mvccEncodeKey(key Key, timestamp hlc.HLTimestamp) Key {
	return Key(bytes.Join([][]byte{key,
		encoding.EncodeIntDecreasing(timestamp.WallTime),
		encoding.EncodeIntDecreasing(timestamp.Logical)},
		[]byte{}))
}

func mvccDecodeKey(encodedKey []byte) (Key, hlc.HLTimestamp) {
	// TODO(Jiang-Ming): implement the real DecodeIntDecreasing.
	l := len(encodedKey)
	idx0 := bytes.Index(encodedKey, []byte{0x00})
	idx1 := bytes.LastIndex(encodedKey[:l-1], []byte{0x00})
	key := encodedKey[:idx0+1]
	encodedWallTime := encodedKey[idx0+2 : idx1+1]
	encodedLogical := encodedKey[idx1+2:]
	wallTime := encoding.DecodeIntDecreasing(encodedWallTime)
	logical := encoding.DecodeIntDecreasing(encodedLogical)

	return key, hlc.HLTimestamp{WallTime: wallTime, Logical: logical}
}
