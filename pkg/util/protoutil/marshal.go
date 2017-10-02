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

import "github.com/gogo/protobuf/proto"

// Interceptor will be called with every proto before it is marshalled.
// Interceptor is not safe to modify concurrently with calls to Marshal.
var Interceptor = func(_ proto.Message) {}

// MaybeFuzz takes the given proto and, if nullability fuzzing is enabled, walks it using a
// RandomZeroInsertingVisitor. A suitable copy is made and returned if fuzzing took place.
func MaybeFuzz(pb proto.Message) proto.Message {
	if fuzzEnabled {
		_, noClone := uncloneable(pb)
		if !noClone {
			pb = Clone(pb)
		} else {
			// Perform a more expensive clone. Unfortunately this is the code path
			// hit by anything that holds a UUID (most things).
			b, err := proto.Marshal(pb)
			if err != nil {
				panic(err)
			}
			if err := proto.Unmarshal(b, pb); err != nil {
				panic(err)
			}
		}
		Walk(pb, RandomZeroInsertingVisitor)
	}
	return pb
}

// Marshal uses proto.Marshal to encode pb into the wire format. It is used in
// some tests to intercept calls to proto.Marshal.
func Marshal(pb proto.Message) ([]byte, error) {
	Interceptor(pb)

	pb = MaybeFuzz(pb)
	return proto.Marshal(pb)
}
