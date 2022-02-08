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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestValidateUpdateArgs ensures we validate arguments to
// UpdateSpanConfigRecords correctly.
func TestValidateUpdateArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	entireKeyspaceTarget := spanconfig.MakeTargetFromSystemTarget(
		spanconfig.MakeEntireKeyspaceTarget(),
	)

	makeTenantTarget := func(id uint64) spanconfig.Target {
		target, err := spanconfig.MakeTenantKeyspaceTarget(roachpb.MakeTenantID(id), roachpb.MakeTenantID(id))
		require.NoError(t, err)
		return spanconfig.MakeTargetFromSystemTarget(target)
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
				spanconfig.MakeTargetFromSpan(
					roachpb.Span{Key: roachpb.Key("a")}, // empty end key in delete list
				),
			},
			expErr: "invalid span: a",
		},
		{
			toUpsert: []spanconfig.Record{
				{
					Target: spanconfig.MakeTargetFromSpan(
						roachpb.Span{Key: roachpb.Key("a")}, // empty end key in update list
					),
				},
			},
			expErr: "invalid span: a",
		},
		{
			toUpsert: []spanconfig.Record{
				{
					Target: spanconfig.MakeTargetFromSpan(
						roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
					),
				},
			},
			expErr: "invalid span: {b-a}",
		},
		{
			toDelete: []spanconfig.Target{
				spanconfig.MakeTargetFromSpan(
					roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
				),
			},
			expErr: "invalid span: {b-a}",
		},
		{
			toDelete: []spanconfig.Target{
				// overlapping spans in the same list.
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}),
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
		{
			toUpsert: []spanconfig.Record{ // overlapping spans in the same list
				{
					Target: spanconfig.MakeTargetFromSpan(
						roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
					),
				},
				{
					Target: spanconfig.MakeTargetFromSpan(
						roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					),
				},
			},
			expErr: "overlapping spans {a-c} and {b-c} in same list",
		},
		{
			// Overlapping spans in different lists.
			toDelete: []spanconfig.Target{
				// Overlapping spans in the same list.
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
			},
			toUpsert: []spanconfig.Record{
				{
					Target: spanconfig.MakeTargetFromSpan(
						roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					),
				},
				{
					Target: spanconfig.MakeTargetFromSpan(
						roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					),
				},
			},
			expErr: "",
		},
		// Tests for system span configurations.
		{
			// Duplicate in toDelete.
			toDelete: []spanconfig.Target{makeTenantTarget(10), makeTenantTarget(10)},
			expErr:   "duplicate system targets .* in the same list",
		},
		{
			// Duplicate in toUpsert.
			toUpsert: []spanconfig.Record{
				{
					Target: makeTenantTarget(10),
				},
				{
					Target: makeTenantTarget(10)},
			},
			expErr: "duplicate system targets .* in the same list",
		},
		{
			// Duplicate in toDelete with some span targets.
			toDelete: []spanconfig.Target{
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				entireKeyspaceTarget,
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}),
				entireKeyspaceTarget,
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}),
			},
			expErr: "duplicate system targets .* in the same list",
		},
		{
			// Duplicate in toDelete with some span targets.
			toDelete: []spanconfig.Target{
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				entireKeyspaceTarget,
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}),
				entireKeyspaceTarget,
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}),
			},
			expErr: "duplicate system targets .* in the same list",
		},
		{
			// Duplicate some span/system target entries across different lists;
			// should work.
			toDelete: []spanconfig.Target{
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				makeTenantTarget(20),
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}),
				entireKeyspaceTarget,
				spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}),
			},
			toUpsert: []spanconfig.Record{
				{
					Target: makeTenantTarget(20),
				},
				{
					Target: spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
				},
			},
			expErr: "",
		},
		{
			// Read only targets are not valid delete args.
			toDelete: []spanconfig.Target{
				spanconfig.MakeTargetFromSystemTarget(
					spanconfig.MakeAllTenantKeyspaceTargetsSet(roachpb.SystemTenantID),
				),
			},
			expErr: "cannot use read only system target .* as an update argument",
		},
		{
			// Read only targets are not valid upsert args.
			toUpsert: []spanconfig.Record{
				{
					Target: spanconfig.MakeTargetFromSystemTarget(
						spanconfig.MakeAllTenantKeyspaceTargetsSet(roachpb.SystemTenantID),
					),
				},
			},
			expErr: "cannot use read only system target .* as an update argument",
		},
		{
			// Read only target validation also applies when the source is a secondary
			// tenant.
			toDelete: []spanconfig.Target{
				spanconfig.MakeTargetFromSystemTarget(
					spanconfig.MakeAllTenantKeyspaceTargetsSet(roachpb.MakeTenantID(10)),
				),
			},
			expErr: "cannot use read only system target .* as an update argument",
		},
	} {
		err := validateUpdateArgs(tc.toDelete, tc.toUpsert)
		require.True(t, testutils.IsError(err, tc.expErr), "exp %s; got %s", tc.expErr, err)
	}
}
