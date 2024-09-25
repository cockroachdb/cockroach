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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

// TestGetReturnRawMVCC tests that a GetRequest with the ReturnRawMVCC
// option set should return the full bytes of the MVCCValue in the
// RawBytes field of returned roachpb.Values.
func TestGetReturnRawMVCC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	key := roachpb.Key([]byte{'a'})
	value := roachpb.MakeValueFromString("woohoo")

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	// Write a value with WriteOptions that will produce an
	// MVCCValueHeader.
	clock := hlc.NewClockForTesting(nil)
	settings := cluster.MakeTestingClusterSettings()
	evalCtx := (&MockEvalCtx{Clock: clock, ClusterSettings: settings}).EvalContext()
	expectedOriginID := uint32(42)
	putResp := kvpb.PutResponse{}
	_, err := Put(ctx, db, CommandArgs{
		EvalCtx: evalCtx,
		Header: kvpb.Header{
			Timestamp: clock.Now(),
			WriteOptions: &kvpb.WriteOptions{
				OriginID: expectedOriginID,
			},
		},
		Args: &kvpb.PutRequest{
			RequestHeader: kvpb.RequestHeader{Key: key},
			Value:         value,
		},
	}, &putResp)
	require.NoError(t, err)

	// Test that by default we get back the OriginID.
	getResp := kvpb.GetResponse{}
	_, err = Get(ctx, db, CommandArgs{
		EvalCtx: evalCtx,
		Header: kvpb.Header{
			Timestamp: clock.Now(),
		},
		Args: &kvpb.GetRequest{
			ReturnRawMVCCValues: true,
			RequestHeader:       kvpb.RequestHeader{Key: key},
		},
	}, &getResp)
	require.NoError(t, err)
	require.NotNil(t, getResp.Value)
	mvccValue, err := storage.DecodeMVCCValue(getResp.Value.RawBytes)
	require.NoError(t, err)
	require.Equal(t, expectedOriginID, mvccValue.OriginID)

	// Without the ReturnRawMVCCValues option set we expect to get
	// back exactly what we put in.
	getResp = kvpb.GetResponse{}
	_, err = Get(ctx, db, CommandArgs{
		EvalCtx: evalCtx,
		Header: kvpb.Header{
			Timestamp: clock.Now(),
		},
		Args: &kvpb.GetRequest{
			RequestHeader: kvpb.RequestHeader{Key: key},
		},
	}, &getResp)
	require.NoError(t, err)
	require.Equal(t, value.RawBytes, getResp.Value.RawBytes)
	mvccValue, err = storage.DecodeMVCCValue(getResp.Value.RawBytes)
	require.NoError(t, err)
	require.Equal(t, uint32(0), mvccValue.OriginID)
}
