// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestValidateUpdateSpanConfigsRequest(t *testing.T) {
	for _, tc := range []struct {
		req    roachpb.UpdateSpanConfigsRequest
		expErr string
	}{
		{
			req:    roachpb.UpdateSpanConfigsRequest{},
			expErr: "",
		},
		{
			req: roachpb.UpdateSpanConfigsRequest{
				ToDelete: []roachpb.Span{
					{Key: roachpb.Key("a")}, // empty end key in delete list
				},
			},
			expErr: "invalid span: a",
		},
		{
			req: roachpb.UpdateSpanConfigsRequest{
				ToUpsert: []roachpb.SpanConfigEntry{
					{
						Span: roachpb.Span{Key: roachpb.Key("a")}, // empty end key in update list
					},
				},
			},
			expErr: "invalid span: a",
		},
		{
			req: roachpb.UpdateSpanConfigsRequest{
				ToUpsert: []roachpb.SpanConfigEntry{
					{
						Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
					},
				},
			},
			expErr: "invalid span: {b-a}",
		},
		{
			req: roachpb.UpdateSpanConfigsRequest{
				ToDelete: []roachpb.Span{
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
				},
			},
			expErr: "invalid span: {b-a}",
		},
		{
			req: roachpb.UpdateSpanConfigsRequest{
				ToDelete: []roachpb.Span{
					{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}, // overlapping spans in the same list
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
		{
			req: roachpb.UpdateSpanConfigsRequest{
				ToUpsert: []roachpb.SpanConfigEntry{ // overlapping spans in the same list
					{
						Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
					},
					{
						Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					},
				},
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
	} {
		if err := tc.req.Validate(); err != nil {
			t.Logf("error: %v", err)
		}
		require.True(t, testutils.IsError(tc.req.Validate(), tc.expErr))
	}
}
