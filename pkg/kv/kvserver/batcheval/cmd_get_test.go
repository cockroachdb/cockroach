// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestGetResumeSpan tests that a GetRequest with a target bytes or max span
// request keys is properly handled.
func TestGetResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	key := roachpb.Key([]byte{'a'})
	value := roachpb.MakeValueFromString("woohoo")
	resp := roachpb.GetResponse{}

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	// This has a size of 11 bytes.
	_, err := Put(ctx, db, CommandArgs{
		EvalCtx: (&MockEvalCtx{}).EvalContext(),
		Header:  roachpb.Header{TargetBytes: -1},
		Args: &roachpb.PutRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
			Value: value,
		},
	}, &resp)
	require.NoError(t, err)

	testCases := []struct {
		maxKeys      int64
		targetBytes  int64
		allowEmpty   bool
		expectResume bool
	}{
		{targetBytes: -1, expectResume: true},
		{maxKeys: -1, expectResume: true},
		{maxKeys: 10, targetBytes: 100, expectResume: false},
		{targetBytes: 1, allowEmpty: false, expectResume: false},
		{targetBytes: 11, allowEmpty: false, expectResume: false},
		{targetBytes: 12, allowEmpty: false, expectResume: false},
		{targetBytes: 1, allowEmpty: true, expectResume: true},
		{targetBytes: 11, allowEmpty: true, expectResume: false},
		{targetBytes: 12, allowEmpty: true, expectResume: false},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("maxKeys=%d targetBytes=%d allowEmpty=%t", tc.maxKeys, tc.targetBytes, tc.allowEmpty)
		t.Run(name, func(t *testing.T) {
			resp := roachpb.GetResponse{}
			_, err := Get(ctx, db, CommandArgs{
				EvalCtx: (&MockEvalCtx{}).EvalContext(),
				Header: roachpb.Header{
					MaxSpanRequestKeys:    tc.maxKeys,
					TargetBytes:           tc.targetBytes,
					TargetBytesAllowEmpty: tc.allowEmpty,
				},
				Args: &roachpb.GetRequest{
					RequestHeader: roachpb.RequestHeader{Key: key},
				},
			}, &resp)
			require.NoError(t, err)

			if tc.expectResume {
				require.NotNil(t, resp.ResumeSpan)
				require.Equal(t, &roachpb.Span{Key: key}, resp.ResumeSpan)
				require.Nil(t, resp.Value)
			} else {
				require.Nil(t, resp.ResumeSpan)
				require.NotNil(t, resp.Value)
				require.Equal(t, resp.Value.RawBytes, value.RawBytes)
				require.EqualValues(t, 1, resp.NumKeys)
				require.Len(t, resp.Value.RawBytes, int(resp.NumBytes))
			}
		})
	}
}
