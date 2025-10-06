// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvsubscriber

import (
	"context"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestingRunInner exports the inner run method for testing purposes.
func (s *KVSubscriber) TestingRunInner(ctx context.Context) error {
	return s.rfc.Run(ctx)
}

// TestingUpdateMetrics exports the inner updateMetrics method for testing purposes.
func (s *KVSubscriber) TestingUpdateMetrics(ctx context.Context) {
	s.updateMetrics(ctx)
}

func TestGetProtectionTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tableSpan := func(tableID uint32) roachpb.Span {
		return roachpb.Span{
			Key:    keys.SystemSQLCodec.TablePrefix(tableID),
			EndKey: keys.SystemSQLCodec.TablePrefix(tableID).PrefixEnd(),
		}
	}

	makeSpanAndSpanConfigWithProtectionPolicies := func(span roachpb.Span,
		pp []roachpb.ProtectionPolicy,
	) spanAndSpanConfig {
		return spanAndSpanConfig{
			span: span,
			cfg: roachpb.SpanConfig{
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
	sp43Cfg.cfg.ExcludeDataFromBackup = true

	excludedKeyspaceConfig := makeSpanAndSpanConfigWithProtectionPolicies(keys.ExcludeFromBackupSpan, []roachpb.ProtectionPolicy{
		{ProtectedTimestamp: ts4},
	})

	nodelivenessKeyspaceConfig := makeSpanAndSpanConfigWithProtectionPolicies(keys.NodeLivenessSpan, []roachpb.ProtectionPolicy{
		{ProtectedTimestamp: ts4},
	})

	const timeDeltaFromTS1 = 10
	mt := timeutil.NewManualTime(ts1.GoTime())
	mt.AdvanceTo(ts1.Add(timeDeltaFromTS1, 0).GoTime())

	subscriber := New(
		hlc.NewClockForTesting(mt),
		nil, /* rangeFeedFactory */
		keys.SpanConfigurationsTableID,
		1<<20, /* 1 MB */
		roachpb.SpanConfig{},
		cluster.MakeTestingClusterSettings(),
		spanconfigstore.NewEmptyBoundsReader(),
		nil,
		nil,
	)
	m := &manualStore{
		spanAndConfigs: []spanAndSpanConfig{sp42Cfg, sp43Cfg, excludedKeyspaceConfig, nodelivenessKeyspaceConfig},
	}
	subscriber.mu.internal = m

	for _, testCase := range []struct {
		name string
		test func(t *testing.T, m *manualStore, subscriber *KVSubscriber)
	}{
		{
			"span not excluded from backup",
			func(t *testing.T, m *manualStore, subscriber *KVSubscriber) {
				protections, _, err := subscriber.GetProtectionTimestamps(ctx, sp42)
				require.NoError(t, err)
				slices.IsSortedFunc(protections, func(a, b hlc.Timestamp) int {
					return a.Compare(b)
				})
				require.Equal(t, []hlc.Timestamp{ts1, ts2}, protections)
			},
		},
		{
			"span excluded from backup",
			func(t *testing.T, m *manualStore, subscriber *KVSubscriber) {
				protections, _, err := subscriber.GetProtectionTimestamps(ctx, sp43)
				require.NoError(t, err)
				slices.IsSortedFunc(protections, func(a, b hlc.Timestamp) int {
					return a.Compare(b)
				})
				require.Equal(t, []hlc.Timestamp{ts4}, protections)
			},
		},
		{
			"span across two table spans",
			func(t *testing.T, m *manualStore, subscriber *KVSubscriber) {
				protections, _, err := subscriber.GetProtectionTimestamps(ctx, sp4243)
				require.NoError(t, err)
				slices.IsSortedFunc(protections, func(a, b hlc.Timestamp) int {
					return a.Compare(b)
				})
				require.Equal(t, []hlc.Timestamp{ts1, ts2, ts4}, protections)
			},
		},
		{
			"ExcludeFromBackupSpan does not include PTS records",
			func(t *testing.T, m *manualStore, subscriber *KVSubscriber) {
				protections, _, err := subscriber.GetProtectionTimestamps(ctx, keys.ExcludeFromBackupSpan)
				require.NoError(t, err)
				slices.IsSortedFunc(protections, func(a, b hlc.Timestamp) int {
					return a.Compare(b)
				})
				require.Empty(t, protections)
			},
		},
		{
			"NodeLivenessSpan does not include PTS records",
			func(t *testing.T, m *manualStore, subscriber *KVSubscriber) {
				protections, _, err := subscriber.GetProtectionTimestamps(ctx, keys.NodeLivenessSpan)
				require.NoError(t, err)
				slices.IsSortedFunc(protections, func(a, b hlc.Timestamp) int {
					return a.Compare(b)
				})
				require.Empty(t, protections)
			},
		},
		{
			"span across back up boundary includes PTS records",
			func(t *testing.T, m *manualStore, subscriber *KVSubscriber) {
				protections, _, err := subscriber.GetProtectionTimestamps(
					ctx,
					roachpb.Span{Key: keys.MinKey, EndKey: sp43.EndKey},
				)
				require.NoError(t, err)
				slices.IsSortedFunc(protections, func(a, b hlc.Timestamp) int {
					return a.Compare(b)
				})
				require.Equal(t, []hlc.Timestamp{ts1, ts2, ts4}, protections)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.test(t, m, subscriber)
		})
	}

	// Test internal metrics. We should expect a protected record count of 3,
	// ignoring the one from ts3 since it has both
	// {IgnoreIfExcludedFromBackup,ExcludeDataFromBackup} are true. We should
	// also observe the right delta between the oldest protected timestamp and
	// current wall clock time.
	subscriber.TestingUpdateMetrics(ctx)
	require.Equal(t, int64(3), subscriber.metrics.ProtectedRecordCount.Value())
	require.Equal(t, int64(timeDeltaFromTS1), subscriber.metrics.OldestProtectedRecordNanos.Value())
}

var _ spanconfig.Store = &manualStore{}

type spanAndSpanConfig struct {
	span roachpb.Span
	cfg  roachpb.SpanConfig
}

type manualStore struct {
	spanAndConfigs []spanAndSpanConfig
}

// Apply implements the spanconfig.Store interface.
func (m *manualStore) Apply(
	context.Context, ...spanconfig.Update,
) (deleted []spanconfig.Target, added []spanconfig.Record) {
	panic("unimplemented")
}

// NeedsSplit implements the spanconfig.Store interface.
func (m *manualStore) NeedsSplit(context.Context, roachpb.RKey, roachpb.RKey) (bool, error) {
	panic("unimplemented")
}

// ComputeSplitKey implements the spanconfig.Store interface.
func (m *manualStore) ComputeSplitKey(
	context.Context, roachpb.RKey, roachpb.RKey,
) (roachpb.RKey, error) {
	panic("unimplemented")
}

// GetSpanConfigForKey implements the spanconfig.Store interface.
func (m *manualStore) GetSpanConfigForKey(
	context.Context, roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
	panic("unimplemented")
}

// ForEachOverlappingSpanConfig implements the spanconfig.Store interface.
func (m *manualStore) ForEachOverlappingSpanConfig(
	_ context.Context, span roachpb.Span, f func(roachpb.Span, roachpb.SpanConfig) error,
) error {
	for _, spanAndConfig := range m.spanAndConfigs {
		if spanAndConfig.span.Overlaps(span) {
			if err := f(spanAndConfig.span, spanAndConfig.cfg); err != nil {
				return err
			}
		}
	}
	return nil
}
