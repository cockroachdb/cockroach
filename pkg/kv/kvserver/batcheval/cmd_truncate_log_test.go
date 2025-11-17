// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putTruncatedState(
	t *testing.T,
	eng storage.Engine,
	rangeID roachpb.RangeID,
	truncState kvserverpb.RaftTruncatedState,
) {
	key := keys.RaftTruncatedStateKey(rangeID)
	if err := storage.MVCCPutProto(
		context.Background(), eng, key,
		hlc.Timestamp{}, &truncState, storage.MVCCWriteOptions{},
	); err != nil {
		t.Fatal(err)
	}
}

func readTruncStates(
	t *testing.T, eng storage.Engine, rangeID roachpb.RangeID,
) (truncatedState kvserverpb.RaftTruncatedState) {
	t.Helper()
	found, err := storage.MVCCGetProto(
		context.Background(), eng, keys.RaftTruncatedStateKey(rangeID),
		hlc.Timestamp{}, &truncatedState, storage.MVCCGetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	require.True(t, found)
	return
}

func TestTruncateLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const (
		rangeID        = 12
		term           = 10
		compactedIndex = 99
	)

	st := cluster.MakeTestingClusterSettings()

	// The truncate command takes the stateEng as any other command. However, it
	// also accepts the log engine via EvalCtx for the stats computation.
	stateEng := storage.NewDefaultInMemForTesting()
	logEng := storage.NewDefaultInMemForTesting()
	defer stateEng.Close()
	defer logEng.Close()

	evalCtx := &MockEvalCtx{
		ClusterSettings: st,
		Desc:            &roachpb.RangeDescriptor{RangeID: rangeID},
		Term:            term,
		CompactedIndex:  compactedIndex,
		LogEngine:       logEng,
	}

	truncState := kvserverpb.RaftTruncatedState{
		Index: compactedIndex + 2,
		Term:  term,
	}

	putTruncatedState(t, stateEng, rangeID, truncState)

	// Write some raft log entries to the log engine to create non-zero stats.
	// These entries will be "truncated" by our request.
	for i := compactedIndex + 1; i < compactedIndex+8; i++ {
		key := keys.RaftLogKey(rangeID, kvpb.RaftIndex(i))
		require.NoError(t, logEng.PutUnversioned(key, []byte("some-data")))
	}

	// Send a truncation request.
	req := kvpb.TruncateLogRequest{
		RangeID: rangeID,
		Index:   compactedIndex + 8,
	}
	cArgs := CommandArgs{
		EvalCtx: evalCtx.EvalContext(),
		Args:    &req,
	}
	resp := &kvpb.TruncateLogResponse{}
	res, err := TruncateLog(ctx, stateEng, cArgs, resp)
	require.NoError(t, err)

	expTruncState := kvserverpb.RaftTruncatedState{
		Index: req.Index - 1,
		Term:  term,
	}

	// The unreplicated key that we see should be the initial truncated
	// state (it's only updated below Raft).
	gotTruncatedState := readTruncStates(t, stateEng, rangeID)
	assert.Equal(t, truncState, gotTruncatedState)

	assert.NotNil(t, res.Replicated.RaftTruncatedState)
	assert.Equal(t, expTruncState, *res.Replicated.RaftTruncatedState)

	// Verify the stats match what's actually in the log engine.
	start := keys.RaftLogKey(rangeID, compactedIndex+1)
	end := keys.RaftLogKey(rangeID, compactedIndex+8)
	expectedStats, err := storage.ComputeStats(ctx, logEng, start, end, 0)
	require.NoError(t, err)
	assert.Equal(t, -expectedStats.SysBytes, res.Replicated.RaftLogDelta,
		"RaftLogDelta should match stats computed from log engine")

	// The state machine engine's stats should be zero.
	zeroStats, err := storage.ComputeStats(ctx, stateEng, start, end, 0)
	require.NoError(t, err)
	require.Zero(t, zeroStats.SysBytes)
}
