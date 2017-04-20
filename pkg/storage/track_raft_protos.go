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

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func funcName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// TrackRaftProtos instruments proto marshalling to track protos which are
// marshalled downstream of raft. It returns a function that removes the
// instrumentation and returns the list of downstream-of-raft protos.
func TrackRaftProtos() func() []reflect.Type {
	// Grab the name of the function that roots all raft operations.
	processRaftFunc := funcName((*Replica).processRaftCommand)
	// We only need to track protos that could cause replica divergence
	// by being written to disk downstream of raft.
	whitelist := []string{
		// Some raft operations trigger gossip, but we don't require
		// strict consistency there.
		funcName((*gossip.Gossip).AddInfoProto),
		// Replica destroyed errors are written to disk, but they are
		// deliberately per-replica values.
		funcName((replicaStateLoader).setReplicaDestroyedError),
	}

	belowRaftProtos := struct {
		syncutil.Mutex
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

		var pcs [100]uintptr
		if numCallers := runtime.Callers(0, pcs[:]); numCallers == len(pcs) {
			panic(fmt.Sprintf("number of callers %d might have exceeded slice size %d", numCallers, len(pcs)))
		}
		frames := runtime.CallersFrames(pcs[:])
		for {
			f, more := frames.Next()

			whitelisted := false
			for _, s := range whitelist {
				if strings.Contains(f.Function, s) {
					whitelisted = true
					break
				}
			}
			if whitelisted {
				break
			}

			if strings.Contains(f.Function, processRaftFunc) {
				belowRaftProtos.Lock()
				belowRaftProtos.inner[t] = struct{}{}
				belowRaftProtos.Unlock()
				break
			}
			if !more {
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
