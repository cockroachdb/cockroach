// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstore

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestSystemSpanConfigStoreCombine tests that we correctly hydrate span
// configurations with the system span configurations that apply to a given key.
func TestSystemSpanConfigStoreCombine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}
	makeSystemSpanConfig := func(nanos int) roachpb.SpanConfig {
		return roachpb.SpanConfig{
			GCPolicy: roachpb.GCPolicy{
				ProtectionPolicies: []roachpb.ProtectionPolicy{
					{
						ProtectedTimestamp: ts(nanos),
					},
				},
			},
		}
	}

	// Setup:
	// System span configurations:
	// Entire Keyspace: 100
	// System Tenant -> System Tenant: 120
	// System Tenant -> Ten 10: 150
	// System Tenant -> Ten 30: 500
	// Ten 10 -> Ten 10: 200
	// Ten 20 -> Ten 20: 300
	//
	// Span config: 1

	updates := []spanconfig.Update{
		spanconfig.Addition(
			spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()),
			makeSystemSpanConfig(100),
		),
		spanconfig.Addition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.SystemTenantID, roachpb.SystemTenantID,
			),
		),
			makeSystemSpanConfig(120),
		),
		spanconfig.Addition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.SystemTenantID, roachpb.MakeTenantID(10),
			),
		),
			makeSystemSpanConfig(150),
		),
		spanconfig.Addition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.MakeTenantID(10), roachpb.MakeTenantID(10),
			),
		),
			makeSystemSpanConfig(200),
		),
		spanconfig.Addition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.MakeTenantID(20), roachpb.MakeTenantID(20),
			),
		),
			makeSystemSpanConfig(300),
		),
		spanconfig.Addition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.SystemTenantID, roachpb.MakeTenantID(30),
			),
		),
			makeSystemSpanConfig(500),
		),
	}

	store := newSystemSpanConfigStore()

	_, _, err := store.apply(updates...)
	require.NoError(t, err)

	for _, tc := range []struct {
		key          roachpb.RKey
		expectedPTSs []hlc.Timestamp
	}{
		{
			key:          roachpb.RKey(append(keys.MakeTenantPrefix(roachpb.SystemTenantID), byte('a'))),
			expectedPTSs: []hlc.Timestamp{ts(1), ts(100), ts(120)},
		},
		{
			key:          roachpb.RKey(append(keys.MakeTenantPrefix(roachpb.MakeTenantID(10)), byte('a'))),
			expectedPTSs: []hlc.Timestamp{ts(1), ts(100), ts(150), ts(200)},
		},
		{
			key:          roachpb.RKey(append(keys.MakeTenantPrefix(roachpb.MakeTenantID(20)), byte('a'))),
			expectedPTSs: []hlc.Timestamp{ts(1), ts(100), ts(300)},
		},
		{
			key:          roachpb.RKey(append(keys.MakeTenantPrefix(roachpb.MakeTenantID(30)), byte('a'))),
			expectedPTSs: []hlc.Timestamp{ts(1), ts(100), ts(500)},
		},
		{
			// Only the system span config over the entire keyspace should apply to
			// tenant 40.
			key:          roachpb.RKey(append(keys.MakeTenantPrefix(roachpb.MakeTenantID(40)), byte('a'))),
			expectedPTSs: []hlc.Timestamp{ts(1), ts(100)},
		},
	} {
		// Combine with a span config that already has a PTS set on it.
		conf, err := store.combine(tc.key, makeSystemSpanConfig(1))
		require.NoError(t, err)
		require.Equal(t, len(tc.expectedPTSs), len(conf.GCPolicy.ProtectionPolicies))
		sort.Slice(conf.GCPolicy.ProtectionPolicies, func(i, j int) bool {
			return conf.GCPolicy.ProtectionPolicies[i].ProtectedTimestamp.Less(
				conf.GCPolicy.ProtectionPolicies[j].ProtectedTimestamp,
			)
		})
		for i, expPTS := range tc.expectedPTSs {
			require.Equal(t, expPTS, conf.GCPolicy.ProtectionPolicies[i].ProtectedTimestamp)
		}
	}
}
