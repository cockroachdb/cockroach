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

package proto

import (
	"fmt"

	gogoproto "github.com/gogo/protobuf/proto"
)

// A Call is a pending database API call.
type Call struct {
	Args  Request      // The argument to the command
	Reply Response     // The reply from the command
	Err   error        // Error during call creation
	Post  func() error // Function to be called after successful completion
}

// Method returns the method of the database command for the call.
func (c *Call) Method() Method {
	return c.Args.Method()
}

// GetCall returns a Call object initialized to get the value at key.
func GetCall(key Key) Call {
	return Call{
		Args: &GetRequest{
			RequestHeader: RequestHeader{
				Key: key,
			},
		},
		Reply: &GetResponse{},
	}
}

// GetProtoCall returns a Call object initialized to get the value at key and
// then to decode it as a protobuf message.
func GetProtoCall(key Key, msg gogoproto.Message) Call {
	c := GetCall(key)
	c.Post = func() error {
		reply := c.Reply.(*GetResponse)
		if reply.Value == nil {
			return fmt.Errorf("%s: no value present", key)
		}
		return gogoproto.Unmarshal(reply.Value.Bytes, msg)
	}
	return c
}

// IncrementCall returns a Call object initialized to increment the value at
// key by increment.
func IncrementCall(key Key, increment int64) Call {
	return Call{
		Args: &IncrementRequest{
			RequestHeader: RequestHeader{
				Key: key,
			},
			Increment: increment,
		},
		Reply: &IncrementResponse{},
	}
}

// PutCall returns a Call object initialized to put the value at key.
func PutCall(key Key, value Value) Call {
	value.InitChecksum(key)
	return Call{
		Args: &PutRequest{
			RequestHeader: RequestHeader{
				Key: key,
			},
			Value: value,
		},
		Reply: &PutResponse{},
	}
}

// ConditionalPutCall returns a Call object initialized to put value as a byte
// slice at key if the existing value at key equals expValueBytes.
func ConditionalPutCall(key Key, valueBytes, expValueBytes []byte) Call {
	value := Value{Bytes: valueBytes}
	value.InitChecksum(key)
	var expValue *Value
	if expValueBytes != nil {
		expValue = &Value{Bytes: expValueBytes}
		expValue.InitChecksum(key)
	}
	return Call{
		Args: &ConditionalPutRequest{
			RequestHeader: RequestHeader{
				Key: key,
			},
			Value:    value,
			ExpValue: expValue,
		},
		Reply: &ConditionalPutResponse{},
	}
}

// PutProtoCall returns a Call object initialized to put the proto message as a
// byte slice at key.
func PutProtoCall(key Key, msg gogoproto.Message) Call {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return Call{Err: err}
	}
	return PutCall(key, Value{Bytes: data})
}

// DeleteCall returns a Call object initialized to delete the value at key.
func DeleteCall(key Key) Call {
	return Call{
		Args: &DeleteRequest{
			RequestHeader: RequestHeader{
				Key: key,
			},
		},
		Reply: &DeleteResponse{},
	}
}

// DeleteRangeCall returns a Call object initialized to delete the values in
// the given key range (excluding the endpoint).
func DeleteRangeCall(startKey, endKey Key) Call {
	return Call{
		Args: &DeleteRangeRequest{
			RequestHeader: RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
		},
		Reply: &DeleteRangeResponse{},
	}
}

// ScanCall returns a Call object initialized to scan from start to end keys
// with max results.
func ScanCall(key, endKey Key, maxResults int64) Call {
	return Call{
		Args: &ScanRequest{
			RequestHeader: RequestHeader{
				Key:    key,
				EndKey: endKey,
			},
			MaxResults: maxResults,
		},
		Reply: &ScanResponse{},
	}
}
