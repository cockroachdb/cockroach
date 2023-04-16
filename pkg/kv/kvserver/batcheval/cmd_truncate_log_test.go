// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
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
	t *testing.T, eng storage.Engine, rangeID roachpb.RangeID, truncState roachpb.RaftTruncatedState,
) {
	key := keys.RaftTruncatedStateKey(rangeID)
	if err := storage.MVCCPutProto(
		context.Background(), eng, nil, key,
		hlc.Timestamp{}, nil /* txn */, &truncState,
	); err != nil {
		t.Fatal(err)
	}
}

func readTruncStates(
	t *testing.T, eng storage.Engine, rangeID roachpb.RangeID,
) (truncatedState roachpb.RaftTruncatedState) {
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
		rangeID    = 12
		term       = 10
		firstIndex = 100
	)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := &MockEvalCtx{
		ClusterSettings: st,
		Desc:            &roachpb.RangeDescriptor{RangeID: rangeID},
		Term:            term,
		FirstIndex:      firstIndex,
	}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	truncState := roachpb.RaftTruncatedState{
		Index: firstIndex + 1,
		Term:  term,
	}

	putTruncatedState(t, eng, rangeID, truncState)

	// Send a truncation request.
	req := roachpb.TruncateLogRequest{
		RangeID: rangeID,
		Index:   firstIndex + 7,
	}
	cArgs := CommandArgs{
		EvalCtx: evalCtx.EvalContext(),
		Args:    &req,
	}
	resp := &roachpb.TruncateLogResponse{}
	res, err := TruncateLog(ctx, eng, cArgs, resp)
	if err != nil {
		t.Fatal(err)
	}

	expTruncState := roachpb.RaftTruncatedState{
		Index: req.Index - 1,
		Term:  term,
	}

	// The unreplicated key that we see should be the initial truncated
	// state (it's only updated below Raft).
	gotTruncatedState := readTruncStates(t, eng, rangeID)
	assert.Equal(t, truncState, gotTruncatedState)

	assert.NotNil(t, res.Replicated.State)
	assert.NotNil(t, res.Replicated.State.TruncatedState)
	assert.Equal(t, expTruncState, *res.Replicated.State.TruncatedState)

}
