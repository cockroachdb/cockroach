// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func funcName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// TrackRaftProtos instruments proto marshaling to track protos which are
// marshaled downstream of raft. It returns a function that removes the
// instrumentation and returns the list of downstream-of-raft protos.
func TrackRaftProtos() func() []reflect.Type {
	// Grab the name of the function that roots all raft application.
	applyRaftEntryFunc := funcName((*apply.Task).ApplyCommittedEntries)
	// We only need to track protos that could cause replica divergence
	// by being written to disk downstream of raft.
	allowlist := []string{
		// Some raft operations trigger gossip, but we don't require
		// strict consistency there.
		funcName((*gossip.Gossip).AddInfoProto),
		// Range merges destroy replicas beneath Raft and write replica tombstones,
		// but tombstones are unreplicated and thus not subject to the strict
		// consistency requirements.
		funcName((*Replica).setTombstoneKey),
		// tryReproposeWithNewLeaseIndex is only run on the replica that
		// proposed the command.
		funcName((*Replica).tryReproposeWithNewLeaseIndex),
	}

	belowRaftProtos := struct {
		syncutil.Mutex
		inner map[reflect.Type]struct{}
	}{
		inner: make(map[reflect.Type]struct{}),
	}

	protoutil.Interceptor = func(pb protoutil.Message) {
		t := reflect.TypeOf(pb)

		// Special handling for MVCCMetadata: we expect MVCCMetadata to be
		// marshaled below raft, but MVCCMetadata.Txn should always be nil in such
		// cases.
		if meta, ok := pb.(*enginepb.MVCCMetadata); ok && meta.Txn != nil {
			protoutil.Interceptor(meta.Txn)
		}

		belowRaftProtos.Lock()
		_, ok := belowRaftProtos.inner[t]
		belowRaftProtos.Unlock()
		if ok {
			return
		}

		var pcs [256]uintptr
		if numCallers := runtime.Callers(0, pcs[:]); numCallers == len(pcs) {
			panic(fmt.Sprintf("number of callers %d might have exceeded slice size %d", numCallers, len(pcs)))
		}
		frames := runtime.CallersFrames(pcs[:])
		for {
			f, more := frames.Next()

			allowlisted := false
			for _, s := range allowlist {
				if strings.Contains(f.Function, s) {
					allowlisted = true
					break
				}
			}
			if allowlisted {
				break
			}

			if strings.Contains(f.Function, applyRaftEntryFunc) {
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
		protoutil.Interceptor = func(_ protoutil.Message) {}

		belowRaftProtos.Lock()
		types := make([]reflect.Type, 0, len(belowRaftProtos.inner))
		for t := range belowRaftProtos.inner {
			types = append(types, t)
		}
		belowRaftProtos.Unlock()

		return types
	}
}
