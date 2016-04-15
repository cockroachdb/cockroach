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

package storage

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

// TrackRaftProtos instruments proto marshalling to track protos which are
// marshalled downstream of raft. It returns a function that removes the
// instrumentation and returns the list of downstream-of-raft protos.
func TrackRaftProtos() func() []reflect.Type {
	// Grab the name of the function that roots all raft operations.
	applyRaftFunc := runtime.FuncForPC(reflect.ValueOf((*Replica).executeBatch).Pointer()).Name()
	// Some raft operations trigger gossip, but we don't care about proto
	// serialization in gossip, so we're going to ignore it.
	addInfoFunc := runtime.FuncForPC(reflect.ValueOf((*gossip.Gossip).AddInfoProto).Pointer()).Name()

	belowRaftProtos := struct {
		sync.Mutex
		inner map[reflect.Type]struct{}
	}{
		inner: make(map[reflect.Type]struct{}),
	}

	protoutil.Interceptor = func(pb proto.Message) {
		t := reflect.TypeOf(pb)

		belowRaftProtos.Lock()
		_, ok := belowRaftProtos.inner[t]
		belowRaftProtos.Unlock()
		if ok {
			return
		}

		pcs := make([]uintptr, 100)
		numCallers := runtime.Callers(0, pcs)
		if numCallers == len(pcs) {
			panic(fmt.Sprintf("number of callers %d might have exceeded slice size %d", numCallers, len(pcs)))
		}
		for _, pc := range pcs[:numCallers] {
			funcName := runtime.FuncForPC(pc).Name()
			if strings.Contains(funcName, addInfoFunc) {
				break
			}
			if strings.Contains(funcName, applyRaftFunc) {
				belowRaftProtos.Lock()
				belowRaftProtos.inner[t] = struct{}{}
				belowRaftProtos.Unlock()

				break
			}
		}
	}

	return func() []reflect.Type {
		protoutil.Interceptor = func(_ proto.Message) {}

		belowRaftProtos.Lock()
		types := make([]reflect.Type, 0, len(belowRaftProtos.inner))
		for t := range belowRaftProtos.inner {
			types = append(types, t)
		}
		belowRaftProtos.Unlock()

		return types
	}
}
