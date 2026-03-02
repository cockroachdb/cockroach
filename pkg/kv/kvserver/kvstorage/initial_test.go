// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestWriteInitialRangeState captures the typical initial range state written
// to storage at cluster bootstrap.
func TestWriteInitialRangeState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	b := eng.NewBatch() // TODO(pav-kv): make it write-only batch
	defer b.Close()

	require.NoError(t, WriteInitialRangeState(
		context.Background(), b, b,
		roachpb.RangeDescriptor{
			RangeID:       5,
			StartKey:      roachpb.RKey("a"),
			EndKey:        roachpb.RKey("z"),
			NextReplicaID: 4,
		},
		roachpb.ReplicaID(3),
		// Use arbitrary version instead of things like clusterversion.Latest, so
		// that the test doesn't sporadically fail when version bumps occur.
		roachpb.Version{Major: 10, Minor: 2, Patch: 17},
	))

	str, err := print.DecodeWriteBatch(b.Repr())
	require.NoError(t, err)
	echotest.Require(t, str, filepath.Join("testdata", t.Name()+".txt"))
}

func TestSynthesizeRaftState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	init := logstore.EntryID{Index: RaftInitialLogIndex, Term: RaftInitialLogTerm}

	for _, tc := range []struct {
		oldHS   raftpb.HardState
		applied logstore.EntryID
		want    raftpb.HardState
		wantErr string
	}{{
		oldHS: raftpb.HardState{Term: 2, Commit: 1}, applied: init,
		wantErr: "non-zero commit index 1",
	}, {
		oldHS: raftpb.HardState{Term: 1}, applied: logstore.EntryID{Index: 123},
		wantErr: "mismatches {Index:10 Term:5}",
	}, {
		oldHS: raftpb.HardState{Term: 1}, applied: init,
		want: raftpb.HardState{Term: 5, Commit: 10},
	}, {
		oldHS: raftpb.HardState{Term: 4, Vote: 1, Lead: 5}, applied: init,
		want: raftpb.HardState{Term: 5, Commit: 10},
	}, {
		oldHS: raftpb.HardState{Term: 5, Vote: 1, Lead: 2, LeadEpoch: 10}, applied: init,
		want: raftpb.HardState{Term: 5, Commit: 10, Vote: 1, Lead: 2, LeadEpoch: 10},
	}, {
		oldHS: raftpb.HardState{Term: 9, Lead: 1, LeadEpoch: 10}, applied: init,
		want: raftpb.HardState{Term: 9, Commit: 10, Lead: 1, LeadEpoch: 10},
	}} {
		t.Run("", func(t *testing.T) {
			hs, ts, err := SynthesizeRaftState(tc.oldHS, tc.applied)
			require.Equal(t, tc.want, hs)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, init, ts)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
				require.Equal(t, kvserverpb.RaftTruncatedState{}, ts)
			}
		})
	}
}
