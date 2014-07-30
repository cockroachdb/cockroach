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
	"fmt"
	"math"
	"strconv"

	"github.com/cockroachdb/cockroach/util"
)

// MVCC wraps the mvcc operations of a key/value store.
type MVCC struct {
	engine Engine // The underlying key-value store
}

type keyMetadata struct {
	TxnID     string // TODO(spencer): replace the TxID with a Txn struct.
	Timestamp int64  // TODO(Jiang-Ming): need to switch to HLTimestamp
}

// writeIntentError is a trivial implementation of error.
type writeIntentError struct {
	TxnID string
}

type writeTimestampTooOldError struct {
	Timestamp int64
}

func (e *writeIntentError) Error() string {
	return fmt.Sprintf("there exists a write intent from transaction %s", e.TxnID)
}

func (e *writeTimestampTooOldError) Error() string {
	return fmt.Sprintf("cannot write with a timestamp older than %d", e.Timestamp)
}

// get returns the value for the key specified in the request and it
// needs to satisfy the given timestamp condition. The values of multiple
// versions for the given key should be organized as following:
// ...
// keyA : keyMetatdata of keyA
// keyA_Timestamp_n : value of version_n
// keyA_Timestamp_n-1 : value of version_n-1
// ...
// keyA_Timestamp_0 : value of version_0
// keyB : keyMetatdata of keyB
// ...
// txnID in the response will be used to indicate if the response value
// belongs to a write intent.
func (mvcc *MVCC) get(key Key, timestamp int64) (Value, string, error) {
	keyMetadata := &keyMetadata{}
	ok, _, err := getI(mvcc.engine, key, keyMetadata)
	value := Value{}
	if err != nil {
		return value, "", err
	}
	if !ok {
		return value, "", nil
	}

	// If the read timestamp is greater than the latest one, we can just
	// fetch the value without a scan.
	if timestamp >= keyMetadata.Timestamp { // TODO(Jiang-Ming): need to use 'Less' in hlc
		latestKey := MakeKey(key, encodeTimestamp(keyMetadata.Timestamp))
		value, err = mvcc.engine.get(latestKey)
		return value, keyMetadata.TxnID, err
	}

	nextKey := MakeKey(key, encodeTimestamp(timestamp))
	// We use the PrefixEndKey(key) as the upper bound for scan.
	// If there is no other version after nextKey, it won't return
	// the value of the next key.
	kvs, err := mvcc.engine.scan(nextKey, PrefixEndKey(key), 1)
	if len(kvs) > 0 {
		value = kvs[0].Value
	} else {
		value = Value{}
	}
	return value, "", err
}

// put sets the value for a specified key. It will save the value with
// different versions according to its timestamp and update the key metadata.
// We assume the range will check for an existing write intent before
// executing any Put action at the MVCC level.
func (mvcc *MVCC) put(key Key, timestamp int64, value Value, txnID string) error {
	keyMeta := &keyMetadata{}
	ok, _, err := getI(mvcc.engine, key, keyMeta)
	if err != nil {
		return err
	}

	// In case the key metadata exists.
	if ok {
		// There is an uncommitted write intent and the current Put
		// operation does not come from the same transaction.
		// This should not happen since range should check the existing
		// write intent before executing any Put action at MVCC level.
		if len(keyMeta.TxnID) > 0 &&
			(len(txnID) == 0 || (len(txnID) > 0 && keyMeta.TxnID != txnID)) {
			return &writeIntentError{TxnID: keyMeta.TxnID}
		}

		if timestamp > keyMeta.Timestamp { // TODO(Jiang-Ming): need to use 'Less' in hlc
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

// delete deletes the key and value specified by key.
func (mvcc *MVCC) delete(key Key, timestamp int64, txID string) error {
	return util.Error("unimplemented")
}

// deleteRange deletes the range of key/value pairs specified by
// start and end keys.
func (mvcc *MVCC) deleteRange(key Key, endKey Key, timestamp int64, txID string) error {
	return util.Error("unimplemented")
}

// scan scans the key range specified by start key through end key up
// to some maximum number of results. The last key of the iteration is
// returned with the reply.
func (mvcc *MVCC) scan(key Key, endKey Key, timestamp int64) ([]Value, string, error) {
	return []Value{}, "", util.Error("unimplemented")
}

// endTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter.
func (mvcc *MVCC) endTransaction(key Key, txID string, commit bool) error {
	return util.Error("unimplemented")
}

// TODO(Jiang-Ming): need to use sqlite4 key ordering once it is ready.
func encodeTimestamp(timestamp int64) Key {
	return []byte(strconv.FormatInt(math.MaxInt64-timestamp, 10))
}
