// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stateloader

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestSynthesizeHardState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)

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
			rsl := Make(roachpb.RangeID(1))

			if test.OldHS != nil {
				if err := rsl.SetHardState(context.Background(), batch, *test.OldHS); err != nil {
					t.Fatal(err)
				}
			}

			oldHS, err := rsl.LoadHardState(context.Background(), batch)
			if err != nil {
				t.Fatal(err)
			}

			err = rsl.SynthesizeHardState(
				context.Background(), batch, oldHS, roachpb.RaftTruncatedState{Term: test.TruncTerm}, test.RaftAppliedIndex,
			)
			if !testutils.IsError(err, test.Err) {
				t.Fatalf("%d: expected %q got %v", i, test.Err, err)
			} else if err != nil {
				// No further checking if we expected an error and got it.
				return
			}

			hs, err := rsl.LoadHardState(context.Background(), batch)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(hs, test.NewHS) {
				t.Fatalf("%d: expected %+v, got %+v", i, &test.NewHS, &hs)
			}
		}()
	}
}
