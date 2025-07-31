// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestGetResumeSpan tests that a GetRequest with a target bytes or max span
// request keys is properly handled.
func TestGetResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	resp := kvpb.GetResponse{}
	key := roachpb.Key([]byte{'a'})
	value := roachpb.MakeValueFromString("woohoo")

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	// This has a size of 11 bytes.
	_, err := Put(ctx, db, CommandArgs{
		EvalCtx: (&MockEvalCtx{
			ClusterSettings: cluster.MakeTestingClusterSettings(),
		}).EvalContext(),
		Header: kvpb.Header{TargetBytes: -1},
		Args: &kvpb.PutRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: key,
			},
			Value: value,
		},
	}, &resp)
	require.NoError(t, err)

	testCases := []struct {
		maxKeys         int64
		targetBytes     int64
		allowEmpty      bool
		expectResume    bool
		expectReason    kvpb.ResumeReason
		expectNextBytes int64
	}{
		{maxKeys: -1, expectResume: true, expectReason: kvpb.RESUME_KEY_LIMIT, expectNextBytes: 0},
		{maxKeys: 0, expectResume: false},
		{maxKeys: 1, expectResume: false},
		{maxKeys: 1, allowEmpty: true, expectResume: false},

		{targetBytes: -1, expectResume: true, expectReason: kvpb.RESUME_BYTE_LIMIT, expectNextBytes: 0},
		{targetBytes: 0, expectResume: false},
		{targetBytes: 1, expectResume: false},
		{targetBytes: 11, expectResume: false},
		{targetBytes: 12, expectResume: false},
		{targetBytes: 1, allowEmpty: true, expectResume: true, expectReason: kvpb.RESUME_BYTE_LIMIT, expectNextBytes: 11},
		{targetBytes: 11, allowEmpty: true, expectResume: false},
		{targetBytes: 12, allowEmpty: true, expectResume: false},

		{maxKeys: -1, targetBytes: -1, expectResume: true, expectReason: kvpb.RESUME_KEY_LIMIT, expectNextBytes: 0},
		{maxKeys: 10, targetBytes: 100, expectResume: false},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("maxKeys=%d targetBytes=%d allowEmpty=%t",
			tc.maxKeys, tc.targetBytes, tc.allowEmpty)
		t.Run(name, func(t *testing.T) {
			settings := cluster.MakeTestingClusterSettings()

			resp := kvpb.GetResponse{}
			_, err := Get(ctx, db, CommandArgs{
				EvalCtx: (&MockEvalCtx{ClusterSettings: settings}).EvalContext(),
				Header: kvpb.Header{
					MaxSpanRequestKeys: tc.maxKeys,
					TargetBytes:        tc.targetBytes,
					AllowEmpty:         tc.allowEmpty,
				},
				Args: &kvpb.GetRequest{
					RequestHeader: kvpb.RequestHeader{Key: key},
				},
			}, &resp)
			require.NoError(t, err)

			if tc.expectResume {
				require.NotNil(t, resp.ResumeSpan)
				require.Equal(t, &roachpb.Span{Key: key}, resp.ResumeSpan)
				require.Equal(t, tc.expectReason, resp.ResumeReason)
				require.Equal(t, tc.expectNextBytes, resp.ResumeNextBytes)
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
