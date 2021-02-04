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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

func putTruncatedState(
	t *testing.T,
	eng storage.Engine,
	rangeID roachpb.RangeID,
	truncState roachpb.RaftTruncatedState,
	legacy bool,
) {
	key := keys.RaftTruncatedStateKey(rangeID)
	if legacy {
		key = keys.RaftTruncatedStateLegacyKey(rangeID)
	}
	if err := storage.MVCCPutProto(
		context.Background(), eng, nil, key,
		hlc.Timestamp{}, nil /* txn */, &truncState,
	); err != nil {
		t.Fatal(err)
	}
}

func readTruncStates(
	t *testing.T, eng storage.Engine, rangeID roachpb.RangeID,
) (legacy roachpb.RaftTruncatedState, unreplicated roachpb.RaftTruncatedState) {
	t.Helper()
	legacyFound, err := storage.MVCCGetProto(
		context.Background(), eng, keys.RaftTruncatedStateLegacyKey(rangeID),
		hlc.Timestamp{}, &legacy, storage.MVCCGetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if legacyFound != (legacy != roachpb.RaftTruncatedState{}) {
		t.Fatalf("legacy key found=%t but state is %+v", legacyFound, legacy)
	}

	unreplicatedFound, err := storage.MVCCGetProto(
		context.Background(), eng, keys.RaftTruncatedStateKey(rangeID),
		hlc.Timestamp{}, &unreplicated, storage.MVCCGetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if unreplicatedFound != (unreplicated != roachpb.RaftTruncatedState{}) {
		t.Fatalf("unreplicated key found=%t but state is %+v", unreplicatedFound, unreplicated)
	}
	return
}

const (
	expectationNeither = iota
	expectationLegacy
	expectationUnreplicated
)

type unreplicatedTruncStateTest struct {
	startsWithLegacy bool
	exp              int // see consts above
}

func TestTruncateLogUnreplicatedTruncatedState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Check out the old clusterversion.VersionUnreplicatedRaftTruncatedState
	// for information on what's being tested. The cluster version is gone, but
	// the migration is done range by range and so it still exists.

	const (
		startsLegacy       = true
		startsUnreplicated = false
	)

	testCases := []unreplicatedTruncStateTest{
		// This is the case where we've already migrated.
		{startsUnreplicated, expectationUnreplicated},
		// This is the case in which the migration is triggered. As a result,
		// we see neither of the keys written. The new key will be written
		// atomically as a side effect (outside of the scope of this test).
		{startsLegacy, expectationNeither},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			runUnreplicatedTruncatedState(t, tc)
		})
	}
}

func runUnreplicatedTruncatedState(t *testing.T, tc unreplicatedTruncStateTest) {
	ctx := context.Background()

	const (
		rangeID    = 12
		term       = 10
		firstIndex = 100
	)

	evalCtx := &MockEvalCtx{
		Desc:       &roachpb.RangeDescriptor{RangeID: rangeID},
		Term:       term,
		FirstIndex: firstIndex,
	}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	truncState := roachpb.RaftTruncatedState{
		Index: firstIndex + 1,
		Term:  term,
	}

	// Put down the TruncatedState specified by the test case.
	putTruncatedState(t, eng, rangeID, truncState, tc.startsWithLegacy)

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

	legacy, unreplicated := readTruncStates(t, eng, rangeID)

	switch tc.exp {
	case expectationLegacy:
		assert.Equal(t, expTruncState, legacy)
		assert.Zero(t, unreplicated)
	case expectationUnreplicated:
		// The unreplicated key that we see should be the initial truncated
		// state (it's only updated below Raft).
		assert.Equal(t, truncState, unreplicated)
		assert.Zero(t, legacy)
	case expectationNeither:
		assert.Zero(t, unreplicated)
		assert.Zero(t, legacy)
	default:
		t.Fatalf("unknown expectation %d", tc.exp)
	}

	assert.NotNil(t, res.Replicated.State)
	assert.NotNil(t, res.Replicated.State.TruncatedState)
	assert.Equal(t, expTruncState, *res.Replicated.State.TruncatedState)
}
