// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSubsumeReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t) // for deterministic output

	r := replicaInfo{
		id:      roachpb.FullReplicaID{RangeID: 123, ReplicaID: 3},
		hs:      raftpb.HardState{Term: 5, Commit: 14},
		ts:      kvserverpb.RaftTruncatedState{Index: 10, Term: 5},
		keys:    roachpb.RSpan{Key: []byte("a"), EndKey: []byte("z")},
		last:    15,
		applied: 12,
	}

	runWithEngines(t, func(t *testing.T, e Engines) {
		ctx := context.Background()
		out := testMutate(t, "raft", e.LogEngine(), func(w storage.Writer) {
			r.createRaftState(ctx, t, w)
		}) + testMutate(t, "state", e.StateEngine(), func(w storage.Writer) {
			r.createStateMachine(ctx, t, w)
		}) + testMutateSep(t, "subsume", e, func(rw ReadWriter, _ *wag.Writer) {
			require.NoError(t, SubsumeReplica(ctx, rw, DestroyReplicaInfo{
				FullReplicaID:    r.id,
				RaftAppliedIndex: r.applied,
				Keys:             r.keys,
				Separated:        e.Separated(),
			}))
		})
		echotestRequire(t, out)
	})
}
