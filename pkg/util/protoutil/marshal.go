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
	"reflect"

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

// MaybeFuzz takes the given proto and, if nullability fuzzing is enabled, walks it using a
// RandomZeroInsertingVisitor. A suitable copy is made and returned if fuzzing took place.
func MaybeFuzz(pb Message) Message {
	if fuzzEnabled {
		_, noClone := uncloneable(pb)
		if !noClone {
			pb = Clone(pb).(Message)
		} else {
			// Perform a more expensive clone. Unfortunately this is the code path
			// hit by anything that holds a UUID (most things).
			b, err := proto.Marshal(pb)
			if err != nil {
				panic(err)
			}
			typ := reflect.TypeOf(pb).Elem()
			target := reflect.New(typ).Interface().(Message)
			if err := proto.Unmarshal(b, target); err != nil {
				panic(err)
			}
			pb = target
		}
		Walk(pb, RandomZeroInsertingVisitor)
	}
	return pb
}

// Interceptor will be called with every proto before it is marshaled.
// Interceptor is not safe to modify concurrently with calls to Marshal.
var Interceptor = func(_ Message) {}

// Marshal encodes pb into the wire format. It is used throughout the code base
// to intercept calls to proto.Marshal.
func Marshal(pb Message) ([]byte, error) {
	pb = MaybeFuzz(pb)

	dest := make([]byte, pb.Size())
	if _, err := MarshalToWithoutFuzzing(pb, dest); err != nil {
		return nil, err
	}
	return dest, nil
}

// MarshalToWithoutFuzzing encodes pb into the wire format. It is used throughout the code base to
// intercept calls to pb.MarshalTo.
func MarshalToWithoutFuzzing(pb Message, dest []byte) (int, error) {
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
