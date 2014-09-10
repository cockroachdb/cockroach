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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"encoding/gob"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

// A DB interface provides asynchronous methods to access a key value
// store. It is defined in storage as Ranges must use it directly. For
// example, to split using distributed txns, push transactions in the
// event commands collide with existing write intents, and process
// message queues.
type DB interface {
	Contains(args *proto.ContainsRequest) <-chan *proto.ContainsResponse
	Get(args *proto.GetRequest) <-chan *proto.GetResponse
	Put(args *proto.PutRequest) <-chan *proto.PutResponse
	ConditionalPut(args *proto.ConditionalPutRequest) <-chan *proto.ConditionalPutResponse
	Increment(args *proto.IncrementRequest) <-chan *proto.IncrementResponse
	Delete(args *proto.DeleteRequest) <-chan *proto.DeleteResponse
	DeleteRange(args *proto.DeleteRangeRequest) <-chan *proto.DeleteRangeResponse
	Scan(args *proto.ScanRequest) <-chan *proto.ScanResponse
	EndTransaction(args *proto.EndTransactionRequest) <-chan *proto.EndTransactionResponse
	AccumulateTS(args *proto.AccumulateTSRequest) <-chan *proto.AccumulateTSResponse
	ReapQueue(args *proto.ReapQueueRequest) <-chan *proto.ReapQueueResponse
	EnqueueUpdate(args *proto.EnqueueUpdateRequest) <-chan *proto.EnqueueUpdateResponse
	EnqueueMessage(args *proto.EnqueueMessageRequest) <-chan *proto.EnqueueMessageResponse
	InternalHeartbeatTxn(args *proto.InternalHeartbeatTxnRequest) <-chan *proto.InternalHeartbeatTxnResponse
	InternalResolveIntent(args *proto.InternalResolveIntentRequest) <-chan *proto.InternalResolveIntentResponse
	InternalSnapshotCopy(args *proto.InternalSnapshotCopyRequest) <-chan *proto.InternalSnapshotCopyResponse
}

// GetI fetches the value at the specified key and gob-deserializes it
// into "value". Returns true on success or false if the key was not
// found. The timestamp of the write is returned as the second return
// value. The first result parameter is "ok": true if a value was
// found for the requested key; false otherwise. An error is returned
// on error fetching from underlying storage or deserializing value.
func GetI(db DB, key engine.Key, iface interface{}) (bool, proto.Timestamp, error) {
	value, err := getInternal(db, key)
	if err != nil || value == nil {
		return false, proto.Timestamp{}, err
	}
	if value.Integer != nil {
		return false, proto.Timestamp{}, util.Errorf("unexpected integer value at key %q: %+v", key, value)
	}
	if err := gob.NewDecoder(bytes.NewBuffer(value.Bytes)).Decode(iface); err != nil {
		return true, value.Timestamp, err
	}
	return true, value.Timestamp, nil
}

// GetProto fetches the value at the specified key and unmarshals it
// using a protobuf decoder. See comments for GetI for details on
// return values.
func GetProto(db DB, key engine.Key, msg gogoproto.Message) (bool, proto.Timestamp, error) {
	value, err := getInternal(db, key)
	if err != nil || value == nil {
		return false, proto.Timestamp{}, err
	}
	if value.Integer != nil {
		return false, proto.Timestamp{}, util.Errorf("unexpected integer value at key %q: %+v", key, value)
	}
	if err := gogoproto.Unmarshal(value.Bytes, msg); err != nil {
		return true, value.Timestamp, err
	}
	return true, value.Timestamp, nil
}

// getInternal fetches the requested key and returns the value.
func getInternal(db DB, key engine.Key) (*proto.Value, error) {
	gr := <-db.Get(&proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: UserRoot,
		},
	})
	if gr.Error != nil {
		return nil, gr.GoError()
	}
	if gr.Value != nil {
		return gr.Value, gr.Value.VerifyChecksum(key)
	}
	return nil, nil
}

// PutI sets the given key to the gob-serialized byte string of value
// and the provided timestamp.
func PutI(db DB, key engine.Key, iface interface{}, timestamp proto.Timestamp) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(iface); err != nil {
		return err
	}
	return putInternal(db, key, proto.Value{Bytes: buf.Bytes(), Timestamp: timestamp})
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg and the provided timestamp.
func PutProto(db DB, key engine.Key, msg gogoproto.Message, timestamp proto.Timestamp) error {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return err
	}
	return putInternal(db, key, proto.Value{Bytes: data, Timestamp: timestamp})
}

// putInternal writes the specified value to key.
func putInternal(db DB, key engine.Key, value proto.Value) error {
	value.InitChecksum(key)
	pr := <-db.Put(&proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:       key,
			User:      UserRoot,
			Timestamp: value.Timestamp,
		},
		Value: value,
	})
	return pr.GoError()
}

// BootstrapRangeDescriptor sets meta1 and meta2 values for KeyMax,
// using the provided replica.
func BootstrapRangeDescriptor(db DB, desc *proto.RangeDescriptor, timestamp proto.Timestamp) error {
	// Write meta1.
	if err := PutProto(db, engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax), desc, timestamp); err != nil {
		return err
	}
	// Write meta2.
	if err := PutProto(db, engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMax), desc, timestamp); err != nil {
		return err
	}
	return nil
}

// BootstrapConfigs sets default configurations for accounting,
// permissions, and zones. All configs are specified for the empty key
// prefix, meaning they apply to the entire database. Permissions are
// granted to all users and the zone requires three replicas with no
// other specifications.
func BootstrapConfigs(db DB, timestamp proto.Timestamp) error {
	// Accounting config.
	acctConfig := &proto.AcctConfig{}
	key := engine.MakeKey(engine.KeyConfigAccountingPrefix, engine.KeyMin)
	if err := PutProto(db, key, acctConfig, timestamp); err != nil {
		return err
	}
	// Permission config.
	permConfig := &proto.PermConfig{
		Read:  []string{UserRoot}, // root user
		Write: []string{UserRoot}, // root user
	}
	key = engine.MakeKey(engine.KeyConfigPermissionPrefix, engine.KeyMin)
	if err := PutProto(db, key, permConfig, timestamp); err != nil {
		return err
	}
	// Zone config.
	// TODO(spencer): change this when zone specifications change to elect for three
	// replicas with no specific features set.
	zoneConfig := &proto.ZoneConfig{
		Replicas: []proto.Attributes{
			proto.Attributes{},
			proto.Attributes{},
			proto.Attributes{},
		},
		RangeMinBytes: 1048576,
		RangeMaxBytes: 67108864,
	}
	key = engine.MakeKey(engine.KeyConfigZonePrefix, engine.KeyMin)
	if err := PutProto(db, key, zoneConfig, timestamp); err != nil {
		return err
	}
	return nil
}

// UpdateRangeDescriptor updates the range locations metadata for the
// range specified by the meta parameter. This always involves a write
// to "meta2", and may require a write to "meta1", in the event that
// meta.EndKey is a "meta2" key (prefixed by KeyMeta2Prefix).
func UpdateRangeDescriptor(db DB, meta proto.RangeMetadata,
	desc *proto.RangeDescriptor, timestamp proto.Timestamp) error {
	// TODO(spencer): a lot more work here to actually implement this.

	// Write meta2.
	key := engine.MakeKey(engine.KeyMeta2Prefix, meta.EndKey)
	if err := PutProto(db, key, desc, timestamp); err != nil {
		return err
	}
	return nil
}
