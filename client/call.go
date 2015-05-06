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

package client

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

// A Callable can be converted into a Call.
type Callable interface {
	Call() Call
}

// A Call is a pending database API call.
type Call struct {
	Args  proto.Request  // The argument to the command
	Reply proto.Response // The reply from the command
	Err   error          // Error during call creation
	Post  func() error   // Function to be called after successful completion
}

// resetClientCmdID sets the client command ID if the call is for a
// read-write method. The client command ID provides idempotency
// protection in conjunction with the server.
func (c Call) resetClientCmdID(clock Clock) {
	c.Args.Header().CmdID = proto.ClientCmdID{
		WallTime: clock.Now(),
		Random:   rand.Int63(),
	}
}

// Method returns the method of the database command for the call.
func (c Call) Method() proto.Method {
	return c.Args.Method()
}

// Call implements Callable
func (c Call) Call() Call {
	return c
}

// GetCall is a type-safe Callable for Get operations.
type GetCall struct {
	Args  *proto.GetRequest
	Reply *proto.GetResponse
	Post  func() error
}

var _ Callable = GetCall{}

// Call implements Callable.
func (c GetCall) Call() Call {
	return Call{
		Args:  c.Args,
		Reply: c.Reply,
		Post:  c.Post,
	}
}

// Get returns a Call object initialized to get the value at key.
func Get(key proto.Key) GetCall {
	return GetCall{
		Args: &proto.GetRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
		},
		Reply: &proto.GetResponse{},
	}
}

// GetProto returns a Call object initialized to get the value at key
// and then to decode it as a protobuf message.
func GetProto(key proto.Key, msg gogoproto.Message) GetCall {
	c := Get(key)
	c.Post = func() error {
		reply := c.Reply
		if reply.Value == nil {
			return util.Errorf("%s: no value present", key)
		}
		if reply.Value.Integer != nil {
			return util.Errorf("%s: unexpected integer value: %+v", key, reply.Value)
		}
		return gogoproto.Unmarshal(reply.Value.Bytes, msg)
	}
	return c
}

// Increment returns a Call object initialized to increment the
// value at key by increment.
func Increment(key proto.Key, increment int64) Call {
	return Call{
		Args: &proto.IncrementRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
			Increment: increment,
		},
		Reply: &proto.IncrementResponse{},
	}
}

// Put returns a Call object initialized to put value
// as a byte slice at key.
func Put(key proto.Key, valueBytes []byte) Call {
	value := proto.Value{Bytes: valueBytes}
	value.InitChecksum(key)
	return Call{
		Args: &proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
			Value: value,
		},
		Reply: &proto.PutResponse{},
	}
}

// ConditionalPut returns a Call object initialized to put value as a
// byte slice at key if the existing value at key equals
// expValueBytes.
func ConditionalPut(key proto.Key, valueBytes, expValueBytes []byte) Call {
	value := proto.Value{Bytes: valueBytes}
	value.InitChecksum(key)
	var expValue *proto.Value
	if expValueBytes != nil {
		expValue = &proto.Value{Bytes: expValueBytes}
		expValue.InitChecksum(key)
	}
	return Call{
		Args: &proto.ConditionalPutRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
			Value:    value,
			ExpValue: expValue,
		},
		Reply: &proto.ConditionalPutResponse{},
	}
}

// PutProto returns a Call object initialized to put the proto
// message as a byte slice at key.
func PutProto(key proto.Key, msg gogoproto.Message) Call {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return Call{Err: err}
	}
	value := proto.Value{Bytes: data}
	value.InitChecksum(key)
	return Call{
		Args: &proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
			Value: value,
		},
		Reply: &proto.PutResponse{},
	}
}

// Delete returns a Call object initialized to delete the value at
// key.
func Delete(key proto.Key) Call {
	return Call{
		Args: &proto.DeleteRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
		},
		Reply: &proto.DeleteResponse{},
	}
}

// DeleteRange returns a Call object initialized to delete the
// values in the given key range (excluding the endpoint).
func DeleteRange(startKey, endKey proto.Key) Call {
	return Call{
		Args: &proto.DeleteRangeRequest{
			RequestHeader: proto.RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
		},
		Reply: &proto.DeleteRangeResponse{},
	}
}

// Scan returns a Call object initialized to scan from start to
// end keys with max results.
func Scan(key, endKey proto.Key, maxResults int64) Call {
	return Call{
		Args: &proto.ScanRequest{
			RequestHeader: proto.RequestHeader{
				Key:    key,
				EndKey: endKey,
			},
			MaxResults: maxResults,
		},
		Reply: &proto.ScanResponse{},
	}
}
