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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"encoding/gob"

	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// A DB interface provides asynchronous methods to access a key value
// store. It is defined in storage as Ranges must use it directly. For
// example, to split using distributed txns, push transactions in the
// event commands collide with existing write intents, and process
// message queues.
type DB interface {
	Contains(args *ContainsRequest) <-chan *ContainsResponse
	Get(args *GetRequest) <-chan *GetResponse
	Put(args *PutRequest) <-chan *PutResponse
	ConditionalPut(args *ConditionalPutRequest) <-chan *ConditionalPutResponse
	Increment(args *IncrementRequest) <-chan *IncrementResponse
	Delete(args *DeleteRequest) <-chan *DeleteResponse
	DeleteRange(args *DeleteRangeRequest) <-chan *DeleteRangeResponse
	Scan(args *ScanRequest) <-chan *ScanResponse
	EndTransaction(args *EndTransactionRequest) <-chan *EndTransactionResponse
	AccumulateTS(args *AccumulateTSRequest) <-chan *AccumulateTSResponse
	ReapQueue(args *ReapQueueRequest) <-chan *ReapQueueResponse
	EnqueueUpdate(args *EnqueueUpdateRequest) <-chan *EnqueueUpdateResponse
	EnqueueMessage(args *EnqueueMessageRequest) <-chan *EnqueueMessageResponse
	InternalHeartbeatTxn(args *InternalHeartbeatTxnRequest) <-chan *InternalHeartbeatTxnResponse
	InternalResolveIntent(args *InternalResolveIntentRequest) <-chan *InternalResolveIntentResponse
}

// GetI fetches the value at the specified key and deserializes it
// into "value". Returns true on success or false if the key was not
// found. The timestamp of the write is returned as the second return
// value. The first result parameter is "ok": true if a value was
// found for the requested key; false otherwise. An error is returned
// on error fetching from underlying storage or deserializing value.
func GetI(db DB, key engine.Key, value interface{}) (bool, hlc.Timestamp, error) {
	gr := <-db.Get(&GetRequest{
		RequestHeader: RequestHeader{
			Key:  key,
			User: UserRoot,
		},
	})
	if gr.Error != nil {
		return false, hlc.Timestamp{}, gr.Error
	}
	if len(gr.Value.Bytes) == 0 {
		return false, hlc.Timestamp{}, nil
	}
	if err := gob.NewDecoder(bytes.NewBuffer(gr.Value.Bytes)).Decode(value); err != nil {
		return true, gr.Value.Timestamp, err
	}
	return true, gr.Value.Timestamp, nil
}

// PutI sets the given key to the serialized byte string of the value
// and the provided timestamp.
func PutI(db DB, key engine.Key, value interface{}, timestamp hlc.Timestamp) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	pr := <-db.Put(&PutRequest{
		RequestHeader: RequestHeader{
			Key:       key,
			User:      UserRoot,
			Timestamp: timestamp,
		},
		Value: engine.Value{Bytes: buf.Bytes()},
	})
	return pr.Error
}

// BootstrapRangeDescriptor sets meta1 and meta2 values for KeyMax,
// using the provided replica.
func BootstrapRangeDescriptor(db DB, desc RangeDescriptor, timestamp hlc.Timestamp) error {
	// Write meta1.
	if err := PutI(db, engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax), desc, timestamp); err != nil {
		return err
	}
	// Write meta2.
	if err := PutI(db, engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMax), desc, timestamp); err != nil {
		return err
	}
	return nil
}

// BootstrapConfigs sets default configurations for accounting,
// permissions, and zones. All configs are specified for the empty key
// prefix, meaning they apply to the entire database. Permissions are
// granted to all users and the zone requires three replicas with no
// other specifications.
func BootstrapConfigs(db DB, timestamp hlc.Timestamp) error {
	// Accounting config.
	acctConfig := &AcctConfig{}
	key := engine.MakeKey(engine.KeyConfigAccountingPrefix, engine.KeyMin)
	if err := PutI(db, key, acctConfig, timestamp); err != nil {
		return err
	}
	// Permission config.
	permConfig := &PermConfig{
		Read:  []string{UserRoot}, // root user
		Write: []string{UserRoot}, // root user
	}
	key = engine.MakeKey(engine.KeyConfigPermissionPrefix, engine.KeyMin)
	if err := PutI(db, key, permConfig, timestamp); err != nil {
		return err
	}
	// Zone config.
	// TODO(spencer): change this when zone specifications change to elect for three
	// replicas with no specific features set.
	zoneConfig := &ZoneConfig{
		Replicas: []engine.Attributes{
			engine.Attributes{},
			engine.Attributes{},
			engine.Attributes{},
		},
		RangeMinBytes: 1048576,
		RangeMaxBytes: 67108864,
	}
	key = engine.MakeKey(engine.KeyConfigZonePrefix, engine.KeyMin)
	if err := PutI(db, key, zoneConfig, timestamp); err != nil {
		return err
	}
	return nil
}

// UpdateRangeDescriptor updates the range locations metadata for the
// range specified by the meta parameter. This always involves a write
// to "meta2", and may require a write to "meta1", in the event that
// meta.EndKey is a "meta2" key (prefixed by KeyMeta2Prefix).
func UpdateRangeDescriptor(db DB, meta RangeMetadata,
	desc RangeDescriptor, timestamp hlc.Timestamp) error {
	// TODO(spencer): a lot more work here to actually implement this.

	// Write meta2.
	key := engine.MakeKey(engine.KeyMeta2Prefix, meta.EndKey)
	if err := PutI(db, key, desc, timestamp); err != nil {
		return err
	}
	return nil
}
