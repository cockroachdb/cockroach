// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.

package protoutil

import (
	"github.com/gogo/protobuf/proto"
)

// Message extends the proto.Message interface with the MarshalTo and Size
// methods we tell gogoproto to generate for us.
type Message interface {
	proto.Message
	MarshalTo(data []byte) (int, error)
	Unmarshal(data []byte) error
	Size() int
}

// Interceptor will be called with every proto before it is marshalled.
// Interceptor is not safe to modify concurrently with calls to Marshal.
var Interceptor = func(_ Message) {}

// Marshal encodes pb into the wire format. It is used throughout the code base
// to intercept calls to proto.Marshal.
func Marshal(pb Message) ([]byte, error) {
	dest := make([]byte, pb.Size())
	if _, err := MarshalTo(pb, dest); err != nil {
		return nil, err
	}
	return dest, nil
}

// MarshalTo encodes pb into the wire format. It is used throughout the code
// base to intercept calls to pb.MarshalTo.
func MarshalTo(pb Message, dest []byte) (int, error) {
	Interceptor(pb)

	return pb.MarshalTo(dest)
}

// Unmarshal parses the protocol buffer representation in buf and places the
// decoded result in pb. If the struct underlying pb does not match the data in
// buf, the results can be unpredictable.
//
// Unmarshal resets pb before starting to unmarshal, so any existing data in pb
// is always removed.
func Unmarshal(data []byte, pb Message) error {
	pb.Reset()
	return pb.Unmarshal(data)
}
