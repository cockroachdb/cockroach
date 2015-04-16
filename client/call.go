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
	gogoproto "github.com/gogo/protobuf/proto"
)

// A Call is a pending database API call.
type Call struct {
	Args  proto.Request  // The argument to the command
	Reply proto.Response // The reply from the command
	Err   error          // Error during call creation
}

// resetClientCmdID sets the client command ID if the call is for a
// read-write method. The client command ID provides idempotency
// protection in conjunction with the server.
func (c *Call) resetClientCmdID(clock Clock) {
	c.Args.Header().CmdID = proto.ClientCmdID{
		WallTime: clock.Now(),
		Random:   rand.Int63(),
	}
}

// Method returns the name of the database command for the call.
func (c *Call) Method() string {
	return c.Args.Method()
}

// GetCall returns a Call object initialized to get the value at key.
func GetCall(key proto.Key, reply *proto.GetResponse) *Call {
	if reply == nil {
		reply = &proto.GetResponse{}
	}
	return &Call{
		Args: &proto.GetRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
		},
		Reply: reply,
	}
}

// IncrementCall returns a Call object initialized to increment the
// value at key by increment.
func IncrementCall(key proto.Key, increment int64, reply *proto.IncrementResponse) *Call {
	if reply == nil {
		reply = &proto.IncrementResponse{}
	}
	return &Call{
		Args: &proto.IncrementRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
			Increment: increment,
		},
		Reply: reply,
	}
}

// PutCall returns a Call object initialized to put value
// as a byte slice at key.
func PutCall(key proto.Key, valueBytes []byte, reply *proto.PutResponse) *Call {
	if reply == nil {
		reply = &proto.PutResponse{}
	}
	value := proto.Value{Bytes: valueBytes}
	value.InitChecksum(key)
	return &Call{
		Args: &proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
			Value: value,
		},
		Reply: reply,
	}
}

// PutProtoCall returns a Call object initialized to put the proto
// message as a byte slice at key.
func PutProtoCall(key proto.Key, msg gogoproto.Message, reply *proto.PutResponse) *Call {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return &Call{Err: err}
	}
	if reply == nil {
		reply = &proto.PutResponse{}
	}
	value := proto.Value{Bytes: data}
	value.InitChecksum(key)
	return &Call{
		Args: &proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
			Value: value,
		},
		Reply: reply,
	}
}

// DeleteCall returns a Call object initialized to delete the value at
// key.
func DeleteCall(key proto.Key, reply *proto.DeleteResponse) *Call {
	if reply == nil {
		reply = &proto.DeleteResponse{}
	}
	return &Call{
		Args: &proto.DeleteRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
		},
		Reply: reply,
	}
}

// DeleteRangeCall returns a Call object initialized to delete the
// values in the given key range (excluding the endpoint).
func DeleteRangeCall(startKey, endKey proto.Key, reply *proto.DeleteRangeResponse) *Call {
	if reply == nil {
		reply = &proto.DeleteRangeResponse{}
	}
	return &Call{
		Args: &proto.DeleteRangeRequest{
			RequestHeader: proto.RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
		},
		Reply: reply,
	}
}

// ScanCall returns a Call object initialized to scan from start to
// end keys with max results.
func ScanCall(key, endKey proto.Key, maxResults int64, reply *proto.ScanResponse) *Call {
	if reply == nil {
		reply = &proto.ScanResponse{}
	}
	return &Call{
		Args: &proto.ScanRequest{
			RequestHeader: proto.RequestHeader{
				Key:    key,
				EndKey: endKey,
			},
			MaxResults: maxResults,
		},
		Reply: reply,
	}
}
