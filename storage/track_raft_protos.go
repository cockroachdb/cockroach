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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

// TrackRaftProtos instruments proto marshalling to track protos which are
// marshalled downstream of raft. It returns a function that removes the
// instrumentation and returns the list of downstream-of-raft protos.
func TrackRaftProtos() func() []reflect.Type {
	applyRaftFunc := runtime.FuncForPC(reflect.ValueOf((*Replica).applyRaftCommandInBatch).Pointer()).Name()
	addInfoFunc := runtime.FuncForPC(reflect.ValueOf((*gossip.Gossip).AddInfoProto).Pointer()).Name()
	pcs := make([]uintptr, 100)
	// Calls to this function are synchronized, so it's OK to reuse `pcs`.
	return protoutil.SetTrackMessageFunc(func(_ proto.Message) bool {
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
				return true
			}
		}
		return false
	})
}
