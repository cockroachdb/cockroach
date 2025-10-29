// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestStateLoader tests the various methods of StateLoader.
//
// TODO(pav-kv): extend it to test keys other than RaftTruncatedState.
func TestStateLoader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	const rangeID = roachpb.RangeID(123)
	sl := NewStateLoader(rangeID)

	// Test that RaftTruncatedState can be read after it is written.
	ts := kvserverpb.RaftTruncatedState{Index: 100, Term: 10}
	require.NoError(t, sl.SetRaftTruncatedState(ctx, eng, &ts))
	got, err := sl.LoadRaftTruncatedState(ctx, eng)
	require.NoError(t, err)
	require.Equal(t, ts, got)
	// Test that RaftTruncatedState is correctly cleared.
	require.NoError(t, sl.ClearRaftTruncatedState(eng))
	got, err = sl.LoadRaftTruncatedState(ctx, eng)
	require.NoError(t, err)
	require.Equal(t, kvserverpb.RaftTruncatedState{}, got)
}
