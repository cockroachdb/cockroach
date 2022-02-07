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
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtarget"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestValidateUpdateArgs ensures we validate arguments to
// UpdateSpanConfigEntries correctly.
func TestValidateUpdateArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makeTenantClusterTarget := func(id uint64) spanconfig.Target {
		tenID := roachpb.MakeTenantID(id)
		target, err := spanconfigtarget.NewSystemTarget(tenID, tenID)
		require.NoError(t, err)
		return target
	}

	for _, tc := range []struct {
		toDelete []spanconfig.Target
		toUpsert []spanconfig.Record
		expErr   string
	}{
		{
			toUpsert: nil, toDelete: nil,
			expErr: "",
		},
		{
			toDelete: []spanconfig.Target{
				spanconfigtarget.NewSpanTarget(
					roachpb.Span{Key: roachpb.Key("a")}, // empty end key in delete list
				),
			},
			expErr: "invalid span: a",
		},
		{
			toUpsert: []spanconfig.Record{
				{
					Target: spanconfigtarget.NewSpanTarget(
						roachpb.Span{Key: roachpb.Key("a")}, // empty end key in update list
					),
				},
			},
			expErr: "invalid span: a",
		},
		{
			toUpsert: []spanconfig.Record{
				{
					Target: spanconfigtarget.NewSpanTarget(
						roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
					),
				},
			},
			expErr: "invalid span: {b-a}",
		},
		{
			toDelete: []spanconfig.Target{
				spanconfigtarget.NewSpanTarget(
					roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
				),
			},
			expErr: "invalid span: {b-a}",
		},
		{
			toDelete: []spanconfig.Target{
				// overlapping spans in the same list.
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}),
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
		{
			toUpsert: []spanconfig.Record{ // overlapping spans in the same list
				{
					Target: spanconfigtarget.NewSpanTarget(
						roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
					),
				},
				{
					Target: spanconfigtarget.NewSpanTarget(
						roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					),
				},
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
		{
			// Overlapping spans in different lists.
			toDelete: []spanconfig.Target{
				// overlapping spans in the same list.
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
			},
			toUpsert: []spanconfig.Record{
				{
					Target: spanconfigtarget.NewSpanTarget(
						roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					),
				},
				{
					Target: spanconfigtarget.NewSpanTarget(
						roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					),
				},
			},
			expErr: "",
		},

		// Tests for system span configurations.
		{
			// Duplicate in toDelete.
			toDelete: []spanconfig.Target{makeTenantClusterTarget(10), makeTenantClusterTarget(10)},
			expErr:   "duplicate target",
		},
		{
			// Duplicate in toUpsert.
			toUpsert: []spanconfig.Record{
				{
					Target: makeTenantClusterTarget(10),
				},
				{
					Target: makeTenantClusterTarget(10)},
			},
			expErr: "duplicate target",
		},
		{
			// Duplicate in toDelete with some span targets.
			toDelete: []spanconfig.Target{
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				makeTenantClusterTarget(roachpb.SystemTenantID.InternalValue),
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}),
				makeTenantClusterTarget(roachpb.SystemTenantID.InternalValue),
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}),
			},
			expErr: "duplicate target",
		},
		{
			// Duplicate in toDelete with some span targets.
			toDelete: []spanconfig.Target{
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				makeTenantClusterTarget(roachpb.SystemTenantID.InternalValue),
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}),
				makeTenantClusterTarget(roachpb.SystemTenantID.InternalValue),
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}),
			},
			expErr: "duplicate target",
		},
		{
			// Duplicate some span/system target entries across different lists;
			// should work.
			toDelete: []spanconfig.Target{
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				makeTenantClusterTarget(20),
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}),
				makeTenantClusterTarget(roachpb.SystemTenantID.InternalValue),
				spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}),
			},
			toUpsert: []spanconfig.Record{
				{
					Target: makeTenantClusterTarget(20),
				},
				{
					Target: spanconfigtarget.NewSpanTarget(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				},
			},
			expErr: "",
		},
	} {
		require.True(t, testutils.IsError(validateUpdateArgs(tc.toDelete, tc.toUpsert), tc.expErr))
	}
}
