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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// TestGetResumeSpan tests that a GetRequest with a target bytes or max span
// request keys is properly handled by returning no result with a resume span.
func TestGetResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	resp := &roachpb.GetResponse{}
	key := roachpb.Key([]byte{'a'})
	value := roachpb.MakeValueFromString("woohoo")

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	_, err := Put(ctx, db, CommandArgs{
		Header: roachpb.Header{TargetBytes: -1},
		Args: &roachpb.PutRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
			Value: value,
		},
	}, resp)
	assert.NoError(t, err)

	// Case 1: Check that a negative TargetBytes causes a resume span.
	_, err = Get(ctx, db, CommandArgs{
		Header: roachpb.Header{TargetBytes: -1},
		Args: &roachpb.GetRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
		},
	}, resp)
	assert.NoError(t, err)

	assert.NotNil(t, resp.ResumeSpan)
	assert.Equal(t, key, resp.ResumeSpan.Key)
	assert.Nil(t, resp.ResumeSpan.EndKey)
	assert.Nil(t, resp.Value)

	resp = &roachpb.GetResponse{}
	// Case 2: Check that a negative MaxSpanRequestKeys causes a resume span.
	_, err = Get(ctx, db, CommandArgs{
		Header: roachpb.Header{MaxSpanRequestKeys: -1},
		Args: &roachpb.GetRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
		},
	}, resp)
	assert.NoError(t, err)

	assert.NotNil(t, resp.ResumeSpan)
	assert.Equal(t, key, resp.ResumeSpan.Key)
	assert.Nil(t, resp.ResumeSpan.EndKey)
	assert.Nil(t, resp.Value)

	resp = &roachpb.GetResponse{}
	// Case 3: Check that a positive limit causes a normal return.
	_, err = Get(ctx, db, CommandArgs{
		Header: roachpb.Header{MaxSpanRequestKeys: 10, TargetBytes: 100},
		Args: &roachpb.GetRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
		},
	}, resp)
	assert.NoError(t, err)

	assert.Nil(t, resp.ResumeSpan)
	assert.NotNil(t, resp.Value)
	assert.Equal(t, resp.Value.RawBytes, value.RawBytes)
	assert.Equal(t, 1, int(resp.NumKeys))
	assert.Equal(t, len(resp.Value.RawBytes), int(resp.NumBytes))
}
