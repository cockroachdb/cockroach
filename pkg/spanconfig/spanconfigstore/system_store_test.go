// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	makeSpanConfigAddition := func(target spanconfig.Target, conf roachpb.SpanConfig) spanconfig.Update {
		addition, err := spanconfig.Addition(target, conf)
		require.NoError(t, err)
		return addition
	}

	updates := []spanconfig.Update{
		makeSpanConfigAddition(
			spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()),
			makeSystemSpanConfig(100),
		),
		makeSpanConfigAddition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.SystemTenantID, roachpb.SystemTenantID,
			),
		),
			makeSystemSpanConfig(120),
		),
		makeSpanConfigAddition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.SystemTenantID, roachpb.MustMakeTenantID(10),
			),
		),
			makeSystemSpanConfig(150),
		),
		makeSpanConfigAddition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.MustMakeTenantID(10), roachpb.MustMakeTenantID(10),
			),
		),
			makeSystemSpanConfig(200),
		),
		makeSpanConfigAddition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.MustMakeTenantID(20), roachpb.MustMakeTenantID(20),
			),
		),
			makeSystemSpanConfig(300),
		),
		makeSpanConfigAddition(spanconfig.MakeTargetFromSystemTarget(
			spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.SystemTenantID, roachpb.MustMakeTenantID(30),
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
			key:          roachpb.RKey(append(keys.MakeTenantPrefix(roachpb.MustMakeTenantID(10)), byte('a'))),
			expectedPTSs: []hlc.Timestamp{ts(1), ts(100), ts(150), ts(200)},
		},
		{
			key:          roachpb.RKey(append(keys.MakeTenantPrefix(roachpb.MustMakeTenantID(20)), byte('a'))),
			expectedPTSs: []hlc.Timestamp{ts(1), ts(100), ts(300)},
		},
		{
			key:          roachpb.RKey(append(keys.MakeTenantPrefix(roachpb.MustMakeTenantID(30)), byte('a'))),
			expectedPTSs: []hlc.Timestamp{ts(1), ts(100), ts(500)},
		},
		{
			// Only the system span config over the entire keyspace should apply to
			// tenant 40.
			key:          roachpb.RKey(append(keys.MakeTenantPrefix(roachpb.MustMakeTenantID(40)), byte('a'))),
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
