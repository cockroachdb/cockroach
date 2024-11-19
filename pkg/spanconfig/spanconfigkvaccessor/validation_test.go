// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		target, err := spanconfig.MakeTenantKeyspaceTarget(roachpb.MustMakeTenantID(id), roachpb.MustMakeTenantID(id))
		require.NoError(t, err)
		return spanconfig.MakeTargetFromSystemTarget(target)
	}

	makeRecord := func(target spanconfig.Target, cfg roachpb.SpanConfig) spanconfig.Record {
		record, err := spanconfig.MakeRecord(target, cfg)
		require.NoError(t, err)
		return record
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
				makeRecord(spanconfig.MakeTargetFromSpan(
					roachpb.Span{Key: roachpb.Key("a")}, // empty end key in update list
				), roachpb.SpanConfig{}),
			},
			expErr: "invalid span: a",
		},
		{
			toUpsert: []spanconfig.Record{
				makeRecord(spanconfig.MakeTargetFromSpan(
					roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("a")}, // invalid span; end < start
				), roachpb.SpanConfig{}),
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
				makeRecord(spanconfig.MakeTargetFromSpan(
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
				), roachpb.SpanConfig{}),
				makeRecord(spanconfig.MakeTargetFromSpan(
					roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				), roachpb.SpanConfig{}),
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
				makeRecord(spanconfig.MakeTargetFromSpan(
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
				), roachpb.SpanConfig{}),
				makeRecord(spanconfig.MakeTargetFromSpan(
					roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				), roachpb.SpanConfig{}),
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
				makeRecord(makeTenantTarget(10), roachpb.SpanConfig{}),
				makeRecord(makeTenantTarget(10), roachpb.SpanConfig{}),
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
				makeRecord(makeTenantTarget(10), roachpb.SpanConfig{}),
				makeRecord(makeTenantTarget(20), roachpb.SpanConfig{}),
				makeRecord(spanconfig.MakeTargetFromSpan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}),
					roachpb.SpanConfig{}),
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
				makeRecord(spanconfig.MakeTargetFromSystemTarget(
					spanconfig.MakeAllTenantKeyspaceTargetsSet(roachpb.SystemTenantID),
				), roachpb.SpanConfig{}),
			},
			expErr: "cannot use read only system target .* as an update argument",
		},
		{
			// Read only target validation also applies when the source is a secondary
			// tenant.
			toDelete: []spanconfig.Target{
				spanconfig.MakeTargetFromSystemTarget(
					spanconfig.MakeAllTenantKeyspaceTargetsSet(roachpb.MustMakeTenantID(10)),
				),
			},
			expErr: "cannot use read only system target .* as an update argument",
		},
	} {
		err := validateUpdateArgs(tc.toDelete, tc.toUpsert)
		require.True(t, testutils.IsError(err, tc.expErr), "exp %s; got %s", tc.expErr, err)
	}
}
