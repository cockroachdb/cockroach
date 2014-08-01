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

package storage

import (
	"bytes"
	"fmt"
	"math"
	"strconv"

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

// get returns the value for the key specified in the request and it
// needs to satisfy the given timestamp condition.
// txnID in the response is used to indicate that the response value
// belongs to a write intent.
func (mvcc *MVCC) get(key Key, timestamp hlc.HLTimestamp, txnID string) (Value, string, error) {
	value, txnID, err := mvcc.getInternal(key, timestamp, txnID)
	// In case of error, or the key doesn't exist, or the key was deleted.
	if err != nil || len(value) == 0 || value[0] == valueDeletedPrefix {
		return Value{}, txnID, err
	}

	// TODO(Jiang-Ming): use unwrapChecksum here and return the real timestamp.
	return Value{Bytes: value[1:]}, txnID, nil
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
func (mvcc *MVCC) getInternal(key Key, timestamp hlc.HLTimestamp, txnID string) ([]byte, string, error) {
	keyMetadata := &keyMetadata{}
	ok, err := getI(mvcc.engine, key, keyMetadata)
	if err != nil || !ok {
		return nil, "", err
	}

	// If the read timestamp is greater than the latest one, we can just
	// fetch the value without a scan.
	if !timestamp.Less(keyMetadata.Timestamp) {
		if len(keyMetadata.TxnID) > 0 && (len(txnID) == 0 || keyMetadata.TxnID != txnID) {
			return nil, "", &writeIntentError{TxnID: keyMetadata.TxnID}
		}

		latestKey := MakeKey(key, encodeTimestamp(keyMetadata.Timestamp))
		val, err := mvcc.engine.get(latestKey)
		return val, keyMetadata.TxnID, err
	}

	nextKey := MakeKey(key, encodeTimestamp(timestamp))
	// We use the PrefixEndKey(key) as the upper bound for scan.
	// If there is no other version after nextKey, it won't return
	// the value of the next key.
	kvs, err := mvcc.engine.scan(nextKey, PrefixEndKey(key), 1)
	if len(kvs) > 0 {
		return kvs[0].value, "", err
	}
	return nil, "", err
}

// put sets the value for a specified key. It will save the value with
// different versions according to its timestamp and update the key metadata.
// We assume the range will check for an existing write intent before
// executing any Put action at the MVCC level.
func (mvcc *MVCC) put(key Key, timestamp hlc.HLTimestamp, value Value, txnID string) error {
	if !value.Timestamp.Equal(hlc.HLTimestamp{}) && !value.Timestamp.Equal(timestamp) {
		return util.Errorf(
			"the timestamp %+v provided in value does not match the timestamp %+v in request",
			value.Timestamp, timestamp)
	}

	// TODO(Jiang-Ming): use wrapChecksum here and encode the timestamp
	// into val which need to be used in get response.
	val := bytes.Join([][]byte{[]byte{valueNormalPrefix}, value.Bytes}, []byte(""))
	return mvcc.putInternal(key, timestamp, val, txnID)
}

// delete marks the key deleted and will not return in the next get response.
func (mvcc *MVCC) delete(key Key, timestamp hlc.HLTimestamp, txnID string) error {
	return mvcc.putInternal(key, timestamp, []byte{valueDeletedPrefix}, txnID)
}

func (mvcc *MVCC) putInternal(key Key, timestamp hlc.HLTimestamp, value []byte, txnID string) error {
	keyMeta := &keyMetadata{}
	ok, err := getI(mvcc.engine, key, keyMeta)
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
			putI(mvcc.engine, key, &keyMetadata{TxnID: txnID, Timestamp: timestamp})
		} else {
			// In case we receive a Put request to update an old version,
			// it must be an error since raft should handle any client
			// retry from timeout.
			return &writeTimestampTooOldError{Timestamp: keyMeta.Timestamp}
		}
	} else { // In case the key metadata does not exist yet.
		// Create key metadata.
		putI(mvcc.engine, key, &keyMetadata{TxnID: txnID, Timestamp: timestamp})
	}

	// Save the value with the given version (Key + Timestamp).
	return mvcc.engine.put(MakeKey(key, encodeTimestamp(timestamp)), value)
}

// conditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (mvcc *MVCC) conditionalPut(key Key, timestamp hlc.HLTimestamp, value Value, expValue Value, txnID string) (Value, error) {
	// Handle check for non-existence of key. In order to detect
	// the potential write intent by another concurrent transaction
	// with a newer timestamp, we need to use the max timestamp
	// while reading.
	val, _, err := mvcc.get(key, hlc.MaxHLTimestamp, txnID)
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

	err = mvcc.put(key, timestamp, value, txnID)
	return Value{}, err
}

// deleteRange deletes the range of key/value pairs specified by
// start and end keys. Specify max=0 for unbounded deletes.
func (mvcc *MVCC) deleteRange(key Key, endKey Key, max int64, timestamp hlc.HLTimestamp, txnID string) ([]Key, error) {
	// In order to detect the potential write intent by another
	// concurrent transaction with a newer timestamp, we need
	// to use the max timestamp for scan.
	kvs, txnID, err := mvcc.scan(key, endKey, max, hlc.MaxHLTimestamp, txnID)
	if err != nil {
		return []Key{}, err
	}

	keys := []Key{}
	for _, kv := range kvs {
		err = mvcc.delete(kv.Key, timestamp, txnID)
		if err != nil {
			return keys, err
		}
		keys = append(keys, kv.Key)
	}
	return keys, nil
}

// scan scans the key range specified by start key through end key up
// to some maximum number of results. Specify max=0 for unbounded scans.
func (mvcc *MVCC) scan(key Key, endKey Key, max int64, timestamp hlc.HLTimestamp, txnID string) ([]KeyValue, string, error) {
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
		kvs, err := mvcc.engine.scan(nextKey, endKey, 1)
		if err != nil {
			return nil, "", err
		}
		// No more keys exists in the given range.
		if len(kvs) == 0 {
			break
		}

		currentKey := kvs[0].key
		value, _, err := mvcc.get(currentKey, timestamp, txnID)
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
		nextKey = NextKey(MakeKey(currentKey, encodeTimestamp(hlc.MinHLTimestamp)))
	}

	return res, txnID, nil
}

// endTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter.
func (mvcc *MVCC) endTransaction(key Key, txnID string, commit bool) error {
	return util.Error("unimplemented")
}

// TODO(Jiang-Ming): need to use sqlite4 key ordering once it is ready.
func encodeTimestamp(timestamp hlc.HLTimestamp) Key {
	// TODO(jiangmingyang): Need to encode both the hlc "physical" and
	// "logical" timestamps to the byte array, one after the other for
	// correct ordering.
	return []byte(strconv.FormatInt(math.MaxInt64-timestamp.WallTime, 10))
}
