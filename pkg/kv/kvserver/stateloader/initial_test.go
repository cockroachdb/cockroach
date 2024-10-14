// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stateloader

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestSynthesizeHardState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)

	tHS := raftpb.HardState{Term: 2, Vote: 3, Commit: 4, Lead: 5, LeadEpoch: 6}

	testCases := []struct {
		TruncTerm        kvpb.RaftTerm
		RaftAppliedIndex kvpb.RaftIndex
		OldHS            *raftpb.HardState
		NewHS            raftpb.HardState
		Err              string
	}{
		{OldHS: nil, TruncTerm: 42, RaftAppliedIndex: 24, NewHS: raftpb.HardState{Term: 42, Vote: 0, Commit: 24}},
		// Can't wind back the committed index of the new HardState.
		{OldHS: &tHS, RaftAppliedIndex: kvpb.RaftIndex(tHS.Commit - 1), Err: "can't decrease HardState.Commit"},
		{OldHS: &tHS, RaftAppliedIndex: kvpb.RaftIndex(tHS.Commit), NewHS: tHS},
		{OldHS: &tHS, RaftAppliedIndex: kvpb.RaftIndex(tHS.Commit + 1), NewHS: raftpb.HardState{Term: tHS.Term, Vote: 3, Commit: tHS.Commit + 1, Lead: 5, LeadEpoch: 6}},
		// Higher Term is picked up, but Vote, Lead, and LeadEpoch aren't carried
		// over when the term changes.
		{OldHS: &tHS, RaftAppliedIndex: kvpb.RaftIndex(tHS.Commit), TruncTerm: 11, NewHS: raftpb.HardState{Term: 11, Vote: 0, Commit: tHS.Commit, Lead: 0}},
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
				context.Background(), batch, oldHS, kvserverpb.RaftTruncatedState{Term: test.TruncTerm}, test.RaftAppliedIndex,
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
