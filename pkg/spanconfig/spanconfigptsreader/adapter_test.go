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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

	mc := &manualCache{}
	ms := &manualSubscriber{}

	t.Run("without pts cache", func(t *testing.T) {
		version := clusterversion.Latest.Version()
		adapter := NewAdapter(mc, ms, cluster.MakeTestingClusterSettingsWithVersions(version, version, true))
		ctx := context.Background()

		// Even though the cache has lower freshness, ignore it.
		mc.asOf = ts(10)
		ms.updatedTS = ts(14)
		timestamps, asOf, err := adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
		require.NoError(t, err)
		require.Empty(t, timestamps)
		require.Equal(t, ts(14), asOf)

		// Forward the freshness of the cache past the subscriber's. The freshness should be the same.
		mc.asOf = ts(18)
		timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
		require.NoError(t, err)
		require.Empty(t, timestamps)
		require.Equal(t, ts(14), asOf)

		// Add some records to the cache; ensure they aren't returned.
		mc.protectedTimestamps = append(mc.protectedTimestamps, ts(6), ts(10))
		mc.asOf = ts(20)
		timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
		require.NoError(t, err)
		require.Equal(t, ts(14), asOf)
		require.Empty(t, timestamps)

		// Add some records to the subscriber, ensure they're returned only.
		// The freshness of the adapter is the freshness of the subscriber.
		ms.protectedTimestamps = append(ms.protectedTimestamps, ts(7), ts(12))
		ms.updatedTS = ts(21)
		timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
		require.NoError(t, err)
		require.Equal(t, ts(21), asOf)
		validateProtectedTimestamps([]hlc.Timestamp{ts(7), ts(12)}, timestamps)

		// Clear out records from the cache, bump its freshness.
		mc.protectedTimestamps = nil
		mc.asOf = ts(22)
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
	})
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

type manualCache struct {
	asOf                hlc.Timestamp
	protectedTimestamps []hlc.Timestamp
}

var _ protectedts.Cache = (*manualCache)(nil)

func (c *manualCache) GetProtectionTimestamps(
	context.Context, roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, err error) {
	return c.protectedTimestamps, c.asOf, nil
}

func (c *manualCache) Iterate(
	_ context.Context, start, end roachpb.Key, it protectedts.Iterator,
) hlc.Timestamp {
	panic("unimplemented")
}

func (c *manualCache) Refresh(ctx context.Context, asOf hlc.Timestamp) error {
	panic("unimplemented")
}

func (c *manualCache) QueryRecord(context.Context, uuid.UUID) (exists bool, asOf hlc.Timestamp) {
	panic("unimplemented")
}
