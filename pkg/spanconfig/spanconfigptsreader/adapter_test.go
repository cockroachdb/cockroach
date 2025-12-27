// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigptsreader

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestAdapter ensures that the spanconfigptsreader.adapter correctly returns
// protected timestamp information
func TestAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}
	validateProtectedTimestamps := func(expected, actual []hlc.Timestamp) {
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].Less(actual[j])
		})
		require.Equal(t, expected, actual)
	}

	ms := &manualSubscriber{}

	version := clusterversion.Latest.Version()
	adapter := NewAdapter(ms, cluster.MakeTestingClusterSettingsWithVersions(version, version, true))
	ctx := context.Background()

	// Initially no protected timestamps.
	ms.updatedTS = ts(14)
	timestamps, asOf, err := adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	require.NoError(t, err)
	require.Empty(t, timestamps)
	require.Equal(t, ts(14), asOf)

	// Add some records to the subscriber.
	// The freshness of the adapter is the freshness of the subscriber.
	ms.protectedTimestamps = append(ms.protectedTimestamps, ts(7), ts(12))
	ms.updatedTS = ts(21)
	timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	require.NoError(t, err)
	require.Equal(t, ts(21), asOf)
	validateProtectedTimestamps([]hlc.Timestamp{ts(7), ts(12)}, timestamps)

	// Clear out records from the subscriber, bump its freshness.
	ms.protectedTimestamps = nil
	ms.updatedTS = ts(25)
	timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	require.NoError(t, err)
	require.Equal(t, ts(25), asOf)
	require.Empty(t, timestamps)
}

type manualSubscriber struct {
	protectedTimestamps []hlc.Timestamp
	updatedTS           hlc.Timestamp
}

var _ spanconfig.KVSubscriber = &manualSubscriber{}

func (m *manualSubscriber) GetProtectionTimestamps(
	context.Context, roachpb.Span,
) ([]hlc.Timestamp, hlc.Timestamp, error) {
	return m.protectedTimestamps, m.updatedTS, nil
}

func (m *manualSubscriber) Start(context.Context, *stop.Stopper) error {
	panic("unimplemented")
}

func (m *manualSubscriber) NeedsSplit(ctx context.Context, start, end roachpb.RKey) (bool, error) {
	panic("unimplemented")
}

func (m *manualSubscriber) ComputeSplitKey(
	context.Context, roachpb.RKey, roachpb.RKey,
) (roachpb.RKey, error) {
	panic("unimplemented")
}

func (m *manualSubscriber) GetSpanConfigForKey(
	context.Context, roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
	panic("unimplemented")
}

func (m *manualSubscriber) LastUpdated() hlc.Timestamp {
	return m.updatedTS
}

func (m *manualSubscriber) Subscribe(callback func(context.Context, roachpb.Span)) {
	panic("unimplemented")
}
