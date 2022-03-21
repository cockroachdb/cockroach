// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvsubscriber

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestingRunInner exports the inner run method for testing purposes.
func (s *KVSubscriber) TestingRunInner(ctx context.Context) error {
	return s.rfc.Run(ctx)
}

func TestGetProtectionTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tableSpan := func(tableID uint32) roachpb.Span {
		return roachpb.Span{
			Key:    keys.SystemSQLCodec.TablePrefix(tableID),
			EndKey: keys.SystemSQLCodec.TablePrefix(tableID).PrefixEnd(),
		}
	}

	makeSpanAndSpanConfigWithProtectionPolicies := func(
		span roachpb.Span,
		pp []roachpb.ProtectionPolicy,
	) roachpb.SpanConfigEntry {
		return roachpb.SpanConfigEntry{
			Target: spanconfig.MakeTargetFromSpan(span).ToProto(),
			Config: roachpb.SpanConfig{
				GCPolicy: roachpb.GCPolicy{
					ProtectionPolicies: pp,
				},
			},
		}
	}

	ctx := context.Background()
	ts1 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	ts2 := ts1.Add(1, 0)
	ts3 := ts2.Add(1, 0)
	ts4 := ts3.Add(1, 0)

	sp42 := tableSpan(42)
	sp43 := tableSpan(43)
	sp4243 := roachpb.Span{Key: sp42.Key, EndKey: sp43.EndKey}

	sp42Cfg := makeSpanAndSpanConfigWithProtectionPolicies(sp42, []roachpb.ProtectionPolicy{
		{ProtectedTimestamp: ts1},
		{ProtectedTimestamp: ts2, IgnoreIfExcludedFromBackup: true},
	})

	sp43Cfg := makeSpanAndSpanConfigWithProtectionPolicies(sp43, []roachpb.ProtectionPolicy{
		{ProtectedTimestamp: ts3, IgnoreIfExcludedFromBackup: true},
		{ProtectedTimestamp: ts4},
	})
	// Mark sp43 as excluded from backup.
	sp43Cfg.Config.ExcludeDataFromBackup = true

	subscriber := New(
		nil, /* clock */
		nil, /* rangeFeedFactory */
		keys.SpanConfigurationsTableID,
		1<<20, /* 1 MB */
		roachpb.SpanConfig{},
		cluster.MakeTestingClusterSettings(),
		&spanconfig.TestingKnobs{
			StoreForEachOverlappingSpanConfigOverride: func() []roachpb.SpanConfigEntry {
				return []roachpb.SpanConfigEntry{sp42Cfg, sp43Cfg}
			},
		},
	)

	for _, testCase := range []struct {
		name string
		test func(t *testing.T, subscriber *KVSubscriber)
	}{
		{
			"span not excluded from backup",
			func(t *testing.T, subscriber *KVSubscriber) {
				protections, _, err := subscriber.GetProtectionTimestamps(ctx, sp42)
				require.NoError(t, err)
				sort.SliceIsSorted(protections, func(i, j int) bool {
					return protections[i].Less(protections[j])
				})
				require.Equal(t, []hlc.Timestamp{ts1, ts2}, protections)
			},
		},
		{
			"span excluded from backup",
			func(t *testing.T, subscriber *KVSubscriber) {
				protections, _, err := subscriber.GetProtectionTimestamps(ctx, sp43)
				require.NoError(t, err)
				sort.SliceIsSorted(protections, func(i, j int) bool {
					return protections[i].Less(protections[j])
				})
				require.Equal(t, []hlc.Timestamp{ts4}, protections)
			},
		},
		{
			"span across two table spans",
			func(t *testing.T, subscriber *KVSubscriber) {
				protections, _, err := subscriber.GetProtectionTimestamps(ctx, sp4243)
				require.NoError(t, err)
				sort.SliceIsSorted(protections, func(i, j int) bool {
					return protections[i].Less(protections[j])
				})
				require.Equal(t, []hlc.Timestamp{ts1, ts2, ts4}, protections)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.test(t, subscriber)
		})
	}
}
