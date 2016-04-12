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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package protoutil

import (
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
)

var interceptor atomic.Value

// SetInterceptor stores a function that will be called with every serialized
// proto.
func SetInterceptor(fn func(proto.Message)) {
	interceptor.Store(fn)
}

func init() {
	SetInterceptor(func(proto.Message) {})
}

// Marshal uses proto.Marshal to encode pb into the wire format. It is used in
// some tests to intercept calls to proto.Marshal.
func Marshal(pb proto.Message) ([]byte, error) {
	interceptor.Load().(func(proto.Message))(pb)

	return proto.Marshal(pb)
}
