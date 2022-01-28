// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvaccessor

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestValidateUpdateArgs ensures we validate arguments to
// UpdateSpanConfigEntries correctly.
func TestValidateUpdateArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		toDelete []roachpb.Span
		toUpsert []roachpb.SpanConfigEntry
		expErr   string
	}{
		{
			toUpsert: nil, toDelete: nil,
			expErr: "",
		},
		{
			toDelete: []roachpb.Span{
				{Key: roachpb.Key("a")}, // empty end key in delete list
			},
			expErr: "invalid span: a",
		},
		{
			toUpsert: []roachpb.SpanConfigEntry{
				{
					Span: roachpb.Span{Key: roachpb.Key("a")}, // empty end key in update list
				},
			},
			expErr: "invalid span: a",
		},
		{
			toUpsert: []roachpb.SpanConfigEntry{
				{
					Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
				},
			},
			expErr: "invalid span: {b-a}",
		},
		{
			toDelete: []roachpb.Span{
				{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
			},
			expErr: "invalid span: {b-a}",
		},
		{
			toDelete: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}, // overlapping spans in the same list
				{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
		{
			toUpsert: []roachpb.SpanConfigEntry{ // overlapping spans in the same list
				{
					Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
				},
				{
					Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
		{
			toDelete: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			},
			toUpsert: []roachpb.SpanConfigEntry{ // overlapping spans in different lists
				{
					Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
				},
				{
					Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
			},
			expErr: "",
		},
	} {
		require.True(t, testutils.IsError(validateUpdateArgs(tc.toDelete, tc.toUpsert), tc.expErr))
	}
}

// TestValidateSystemSpanConfigUpdateArgs ensures we validate arguments to
// UpdateSystemSpanConfigEntries correctly.
func TestValidateSystemSpanConfigUpdateArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	makeTarget := func(id uint64) roachpb.SystemSpanConfigTarget {
		tenID := roachpb.MakeTenantID(id)
		return roachpb.SystemSpanConfigTarget{
			TenantID: &tenID,
		}
	}
	clusterTarget := roachpb.SystemSpanConfigTarget{}
	for _, tc := range []struct {
		toDelete []roachpb.SystemSpanConfigTarget
		toUpsert []roachpb.SystemSpanConfigEntry
		expErr   string
	}{
		{
			// Duplicate across toDelete.
			toDelete: []roachpb.SystemSpanConfigTarget{makeTarget(10), makeTarget(10)},
			expErr:   "duplicate target found",
		},
		{
			// Duplicate across both lists.
			toUpsert: []roachpb.SystemSpanConfigEntry{
				{
					SystemSpanConfigTarget: makeTarget(10),
				},
				{
					SystemSpanConfigTarget: makeTarget(10),
				},
			},
			expErr: "duplicate target found",
		},
		{
			// Duplicate across both lists.
			toDelete: []roachpb.SystemSpanConfigTarget{makeTarget(10), makeTarget(20)},
			toUpsert: []roachpb.SystemSpanConfigEntry{
				{
					SystemSpanConfigTarget: makeTarget(10),
				},
			},
			expErr: "duplicate target found",
		},
		{
			// Duplicate cluster target in toDelete.
			toDelete: []roachpb.SystemSpanConfigTarget{clusterTarget, clusterTarget},
			expErr:   "duplicate cluster target found",
		},
		{
			// Duplicate cluster target in toUpsert.
			toUpsert: []roachpb.SystemSpanConfigEntry{
				{SystemSpanConfigTarget: clusterTarget},
				{SystemSpanConfigTarget: clusterTarget},
			},
			expErr: "duplicate cluster target found",
		},
		{
			// Duplicate cluster target across lists.
			toDelete: []roachpb.SystemSpanConfigTarget{makeTarget(10), clusterTarget},
			toUpsert: []roachpb.SystemSpanConfigEntry{
				{SystemSpanConfigTarget: makeTarget(20)},
				{SystemSpanConfigTarget: clusterTarget},
			},
			expErr: "duplicate cluster target found",
		},
		{
			// Should work.
			toDelete: []roachpb.SystemSpanConfigTarget{makeTarget(10), clusterTarget},
			toUpsert: []roachpb.SystemSpanConfigEntry{
				{SystemSpanConfigTarget: makeTarget(20)},
			},
		},
	} {
		require.True(
			t,
			testutils.IsError(validateUpdateSystemSpanConfigArgs(tc.toDelete, tc.toUpsert), tc.expErr),
		)
	}
}
