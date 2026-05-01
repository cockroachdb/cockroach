// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestWriteInitialRangeState captures the typical initial range state written
// to storage at cluster bootstrap.
func TestWriteInitialRangeState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t) // for deterministic output

	desc := roachpb.RangeDescriptor{
		RangeID:       5,
		StartKey:      roachpb.RKey("a"),
		EndKey:        roachpb.RKey("z"),
		NextReplicaID: 4,
	}
	const replicaID = roachpb.ReplicaID(3)
	// Use an arbitrary version instead of things like clusterversion.Latest, so
	// that the test doesn't sporadically fail when version bumps occur.
	replicaVersion := roachpb.Version{Major: 10, Minor: 2, Patch: 17}

	runWithEngines(t, func(t *testing.T, e Engines) {
		ctx := context.Background()
		var seq wag.Seq
		factory := MakeBatchFactory(&e, &seq)
		batch := factory.NewBatch()
		defer batch.Close()

		require.NoError(t, WriteInitialRangeState(
			ctx, batch.State(), batch.Raft(), batch.WagWriter(),
			desc, replicaID, replicaVersion,
		))

		var out string
		if e.Separated() {
			require.NoError(t, batch.TestingFlushWAG())
			require.True(t, wag.AssertMutationBatch(
				t, batch.Raft().Repr(), batch.State().Repr(),
			))
			out = ">> init/state:\n(matches WAG node)\n"
			raftStr, err := print.DecodeWriteBatch(batch.Raft().Repr())
			require.NoError(t, err)
			out += fmt.Sprintf(">> init/raft:\n%s", raftStr)
		} else {
			str, err := print.DecodeWriteBatch(batch.State().Repr())
			require.NoError(t, err)
			out = fmt.Sprintf(">> init:\n%s", str)
		}
		echotestRequire(t, out)
	})
}

func TestSynthesizeHardState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)

	tHS := raftpb.HardState{Term: 2, Vote: 3, Commit: 4, Lead: 5, LeadEpoch: 6}

	testCases := []struct {
		AppliedTerm  kvpb.RaftTerm
		AppliedIndex kvpb.RaftIndex
		OldHS        *raftpb.HardState
		NewHS        raftpb.HardState
		Err          string
	}{
		{OldHS: nil, AppliedTerm: 42, AppliedIndex: 24, NewHS: raftpb.HardState{Term: 42, Vote: 0, Commit: 24}},
		// Can't wind back the committed index of the new HardState.
		{OldHS: &tHS, AppliedIndex: kvpb.RaftIndex(tHS.Commit - 1), Err: "can't decrease HardState.Commit"},
		{OldHS: &tHS, AppliedIndex: kvpb.RaftIndex(tHS.Commit), NewHS: tHS},
		{OldHS: &tHS, AppliedIndex: kvpb.RaftIndex(tHS.Commit + 1), NewHS: raftpb.HardState{Term: tHS.Term, Vote: 3, Commit: tHS.Commit + 1, Lead: 5, LeadEpoch: 6}},
		// Higher Term is picked up, but Vote, Lead, and LeadEpoch aren't carried
		// over when the term changes.
		{OldHS: &tHS, AppliedIndex: kvpb.RaftIndex(tHS.Commit), AppliedTerm: 11, NewHS: raftpb.HardState{Term: 11, Vote: 0, Commit: tHS.Commit, Lead: 0}},
	}

	for i, test := range testCases {
		func() {
			batch := eng.NewBatch()
			defer batch.Close()
			rsl := MakeStateLoader(roachpb.RangeID(1))

			if test.OldHS != nil {
				if err := rsl.SetHardState(context.Background(), batch, *test.OldHS); err != nil {
					t.Fatal(err)
				}
			}

			oldHS, err := rsl.LoadHardState(context.Background(), batch)
			if err != nil {
				t.Fatal(err)
			}

			err = rsl.SynthesizeHardState(context.Background(), batch, oldHS,
				logstore.EntryID{Index: test.AppliedIndex, Term: test.AppliedTerm})
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
