// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestExciseEval tests basic Excise evaluation.
func TestExciseEval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	evalCtx := (&batcheval.MockEvalCtx{Clock: clock}).EvalContext()

	testCases := []struct {
		name      string
		startKey  roachpb.Key
		endKey    roachpb.Key
		expectErr string
	}{
		{
			// Valid range.
			startKey:  roachpb.Key("a"),
			endKey:    roachpb.Key("z"),
			expectErr: "",
		},
		{
			// Only the start key is a non-user key.
			startKey:  keys.RangeDescriptorKey(roachpb.RKey("a")),
			endKey:    roachpb.Key("z"),
			expectErr: "excise can only be run against global keys",
		},
		{
			// Only the end key is a non-user key.
			startKey:  roachpb.Key("a"),
			endKey:    keys.RangeDescriptorKey(roachpb.RKey("z")),
			expectErr: "excise can only be run against global keys",
		},
		{
			// Both keys are non-user keys.
			startKey:  keys.RangeDescriptorKey(roachpb.RKey("a")),
			endKey:    keys.RangeDescriptorKey(roachpb.RKey("z")),
			expectErr: "excise can only be run against global keys",
		},
	}

	for _, tc := range testCases {
		resp := &kvpb.ExciseResponse{}
		res, err := batcheval.EvalExcise(ctx, nil, batcheval.CommandArgs{
			EvalCtx: evalCtx,
			Stats:   &enginepb.MVCCStats{},
			Args: &kvpb.ExciseRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    tc.startKey,
					EndKey: tc.endKey,
				},
			},
		}, resp)

		// If there are no errors, we expect the result to be populated.
		if tc.expectErr == "" {
			require.NoError(t, err)

			userSpan := roachpb.Span{Key: tc.startKey, EndKey: tc.endKey}
			ltStart, _ := keys.LockTableSingleKey(tc.startKey, nil)
			ltEnd, _ := keys.LockTableSingleKey(tc.endKey, nil)
			LockTableSpan := roachpb.Span{Key: ltStart, EndKey: ltEnd}

			require.NotNil(t, res.Replicated.Excise)
			require.Equal(t, userSpan, res.Replicated.Excise.Span)
			require.Equal(t, LockTableSpan, res.Replicated.Excise.LockTableSpan)
		} else {
			require.Regexp(t, tc.expectErr, err)
			require.Nil(t, res.Replicated.Excise)
		}
	}
}
