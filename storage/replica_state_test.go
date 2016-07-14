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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"reflect"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestSynthesizeHardState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	tHS := raftpb.HardState{Term: 2, Vote: 3, Commit: 4}

	testCases := []struct {
		TruncTerm, RaftAppliedIndex uint64
		OldHS                       *raftpb.HardState
		NewHS                       raftpb.HardState
		Err                         string
	}{
		{OldHS: nil, TruncTerm: 42, RaftAppliedIndex: 24, NewHS: raftpb.HardState{Term: 42, Vote: 0, Commit: 24}},
		// Can't wind back the committed index of the new HardState.
		{OldHS: &tHS, RaftAppliedIndex: tHS.Commit - 1, Err: "can't decrease HardState.Commit"},
		{OldHS: &tHS, RaftAppliedIndex: tHS.Commit, NewHS: tHS},
		{OldHS: &tHS, RaftAppliedIndex: tHS.Commit + 1, NewHS: raftpb.HardState{Term: tHS.Term, Vote: 3, Commit: tHS.Commit + 1}},
		// Higher Term is picked up, but vote isn't carried over when the term
		// changes.
		{OldHS: &tHS, RaftAppliedIndex: tHS.Commit, TruncTerm: 11, NewHS: raftpb.HardState{Term: 11, Vote: 0, Commit: tHS.Commit}},
	}

	for i, test := range testCases {
		func() {
			batch := eng.NewBatch()
			defer batch.Close()
			testState := storagebase.ReplicaState{
				Desc:             testRangeDescriptor(),
				TruncatedState:   &roachpb.RaftTruncatedState{Term: test.TruncTerm},
				RaftAppliedIndex: test.RaftAppliedIndex,
			}

			if test.OldHS != nil {
				if err := setHardState(context.Background(), batch, testState.Desc.RangeID, *test.OldHS); err != nil {
					t.Fatal(err)
				}
			}

			oldHS, err := loadHardState(context.Background(), batch, testState.Desc.RangeID)
			if err != nil {
				t.Fatal(err)
			}

			if err := synthesizeHardState(context.Background(), batch, testState, oldHS); ((err == nil) != (test.Err == "")) ||
				(err != nil && !testutils.IsError(err, test.Err)) {
				t.Fatalf("%d: %v (expected '%s')", i, err, test.Err)
			} else if err != nil {
				// No further checking if we expected an error and got it.
				return
			}

			hs, err := loadHardState(context.Background(), batch, testState.Desc.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(hs, test.NewHS) {
				t.Fatalf("%d: expected %+v, got %+v", i, &test.NewHS, &hs)
			}
		}()
	}
}
