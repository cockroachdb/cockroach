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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestGetResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	resp := &roachpb.GetResponse{}
	key := roachpb.Key([]byte{'a'})
	_, err := Get(ctx, nil, CommandArgs{
		Header: roachpb.Header{TargetBytes: -1},
		Args: &roachpb.GetRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
		},
	}, resp)
	assert.NoError(t, err)

	if resp.ResumeSpan == nil {
		t.Fatalf("expected a resumespan, found nothing")
	}
	assert.Equal(t, key, resp.ResumeSpan.Key)
	assert.Nil(t, resp.ResumeSpan.EndKey)
}
