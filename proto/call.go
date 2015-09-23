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

// A Call is a pending database API call.
type Call struct {
	Args  Request      // The argument to the command
	Reply Response     // The reply from the command
	Post  func() error // Function to be called after successful completion
}

// Method returns the method of the database command for the call.
func (c *Call) Method() Method {
	return c.Args.Method()
}

// NewGet returns a Request initialized to get the value at key.
func NewGet(key Key) Request {
	return &GetRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
	}
}

// NewIncrement returns a Request initialized to increment the value at
// key by increment.
func NewIncrement(key Key, increment int64) Request {
	return &IncrementRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Increment: increment,
	}
}

// NewPut returns a Request initialized to put the value at key.
func NewPut(key Key, value Value) Request {
	value.InitChecksum(key)
	return &PutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value: value,
	}
}

// NewConditionalPut returns a Request initialized to put value as a byte
// slice at key if the existing value at key equals expValueBytes.
func NewConditionalPut(key Key, value, expValue Value) Request {
	value.InitChecksum(key)
	var expValuePtr *Value
	if expValue.Bytes != nil {
		expValuePtr = &expValue
		expValue.InitChecksum(key)
	}
	return &ConditionalPutRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
		Value:    value,
		ExpValue: expValuePtr,
	}
}

// NewDelete returns a Request initialized to delete the value at key.
func NewDelete(key Key) Request {
	return &DeleteRequest{
		RequestHeader: RequestHeader{
			Key: key,
		},
	}
}

// NewDeleteRange returns a Request initialized to delete the values in
// the given key range (excluding the endpoint).
func NewDeleteRange(startKey, endKey Key) Request {
	return &DeleteRangeRequest{
		RequestHeader: RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
	}
}

// NewScan returns a Request initialized to scan from start to end keys
// with max results.
func NewScan(key, endKey Key, maxResults int64) Request {
	return &ScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		MaxResults: maxResults,
	}
}

// NewReverseScan returns a Request initialized to reverse scan from end to
// start keys with max results.
func NewReverseScan(key, endKey Key, maxResults int64) Request {
	return &ReverseScanRequest{
		RequestHeader: RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		MaxResults: maxResults,
	}
}
