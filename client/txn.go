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
// Author: Peter Mattis (peter.mattis@gmail.com)

package client

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

// Txn provides serial access to a KV store via Run and parallel
// access via Prepare and Flush. A Txn instance is not thread
// safe.
type Txn struct {
	kv       KV
	prepared []*Call
}

// Run runs the specified calls synchronously in a single batch and
// returns any errors.
func (t *Txn) Run(calls ...*Call) error {
	if len(calls) == 0 {
		return nil
	}
	if len(t.prepared) > 0 || len(calls) > 1 {
		t.Prepare(calls...)
		return t.Flush()
	}
	return t.kv.Run(calls...)
}

// Prepare accepts a KV API call, specified by arguments and a reply
// struct. The call will be buffered locally until the first call to
// Flush(), at which time it will be sent for execution as part of a
// batch call. Using Prepare/Flush parallelizes queries and updates
// and should be used where possible for efficiency.
//
// For clients using an HTTP sender, Prepare/Flush allows multiple
// commands to be sent over the same connection. For transactional
// clients, Prepare/Flush can dramatically improve efficiency by
// compressing multiple writes into a single atomic update in the
// event that the writes are to keys within a single range. However,
// using Prepare/Flush alone will not guarantee atomicity. Clients
// must use a transaction for that purpose.
//
// The supplied reply struct will not be valid until after a call
// to Flush().
func (t *Txn) Prepare(calls ...*Call) {
	for _, c := range calls {
		c.resetClientCmdID(t.kv.clock)
	}
	t.prepared = append(t.prepared, calls...)
}

// Flush sends all previously prepared calls, buffered by invocations
// of Prepare(). The calls are organized into a single batch command
// and sent together. Flush returns nil if all prepared calls are
// executed successfully. Otherwise, Flush returns the first error,
// where calls are executed in the order in which they were prepared.
// After Flush returns, all prepared reply structs will be valid.
func (t *Txn) Flush() error {
	calls := t.prepared
	t.prepared = nil
	return t.kv.Run(calls...)
}

// Get fetches the value at the specified key. Returns true on success
// or false if the key was not found. The timestamp of the write is
// returned as the second return value. The first result parameter is
// "ok": true if a value was found for the requested key; false
// otherwise. An error is returned on error fetching from underlying
// storage or deserializing value.
func (t *Txn) Get(key proto.Key) (bool, []byte, proto.Timestamp, error) {
	value, err := t.getInternal(key)
	if err != nil || value == nil {
		return false, nil, proto.Timestamp{}, err
	}
	if value.Integer != nil {
		return false, nil, proto.Timestamp{}, util.Errorf("unexpected integer value at key %s: %+v", key, value)
	}
	return true, value.Bytes, *value.Timestamp, nil
}

// GetProto fetches the value at the specified key and unmarshals it
// using a protobuf decoder. See comments for GetI for details on
// return values.
func (t *Txn) GetProto(key proto.Key, msg gogoproto.Message) (bool, proto.Timestamp, error) {
	value, err := t.getInternal(key)
	if err != nil || value == nil {
		return false, proto.Timestamp{}, err
	}
	if value.Integer != nil {
		return false, proto.Timestamp{}, util.Errorf("unexpected integer value at key %q: %+v", key, value)
	}
	if err := gogoproto.Unmarshal(value.Bytes, msg); err != nil {
		return true, *value.Timestamp, err
	}
	return true, *value.Timestamp, nil
}

// getInternal fetches the requested key and returns the value.
func (t *Txn) getInternal(key proto.Key) (*proto.Value, error) {
	call := GetCall(key)
	reply := call.Reply.(*proto.GetResponse)
	if err := t.Run(call); err != nil {
		return nil, err
	}
	if reply.Value != nil {
		return reply.Value, reply.Value.Verify(key)
	}
	return nil, nil
}

// Put writes the specified byte slice value to key.
func (t *Txn) Put(key proto.Key, value []byte) error {
	return t.Run(PutCall(key, value))
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg.
func (t *Txn) PutProto(key proto.Key, msg gogoproto.Message) error {
	return t.Run(PutProtoCall(key, msg))
}
