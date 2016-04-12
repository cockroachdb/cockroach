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
	"reflect"
	"sync"

	"github.com/gogo/protobuf/proto"
)

var tracker struct {
	sync.Mutex
	isTrack func(proto.Message) bool
	tracked map[reflect.Type]struct{}
}

// Marshal uses proto.Marshal to encode pb into the wire format. It is used in
// some tests to intercept calls to proto.Marshal.
func Marshal(pb proto.Message) ([]byte, error) {
	tracker.Lock()
	if isTrack := tracker.isTrack; isTrack != nil && isTrack(pb) {
		tracker.tracked[reflect.TypeOf(pb)] = struct{}{}
	}
	tracker.Unlock()

	return proto.Marshal(pb)
}

// SetTrackMessageFunc instruments proto marshalling to track protos for which
// fn returns true. It returns a function that removes the instrumentation and
// returns the list of tracked protos.
func SetTrackMessageFunc(fn func(proto.Message) bool) func() []reflect.Type {
	tracker.Lock()
	tracker.isTrack = fn
	tracker.tracked = make(map[reflect.Type]struct{})
	tracker.Unlock()

	return func() []reflect.Type {
		tracker.Lock()
		types := make([]reflect.Type, 0, len(tracker.tracked))
		for t := range tracker.tracked {
			types = append(types, t)
		}
		tracker.isTrack = nil
		tracker.tracked = nil
		tracker.Unlock()

		return types
	}
}
