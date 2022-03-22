// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigptsreader

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	adapter := NewAdapter(mc, ms)
	ctx := context.Background()

	// Setup with an empty subscriber and cache; ensure no records are returned
	// and the freshness timestamp is the minimum of the two.
	mc.asOf = ts(10)
	ms.updatedTS = ts(14)
	timestamps, asOf, err := adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	require.NoError(t, err)
	require.Empty(t, timestamps)
	require.Equal(t, ts(10), asOf)

	// Forward the freshness of the cache past the subscriber's.
	mc.asOf = ts(18)
	timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	require.NoError(t, err)
	require.Empty(t, timestamps)
	require.Equal(t, ts(14), asOf)

	// Add some records to the cache; ensure they're returned.
	mc.protectedTimestamps = append(mc.protectedTimestamps, ts(6), ts(10))
	mc.asOf = ts(20)

	timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	require.NoError(t, err)
	require.Equal(t, ts(14), asOf)
	validateProtectedTimestamps([]hlc.Timestamp{ts(6), ts(10)}, timestamps)

	// Add some records to the subscriber, ensure they're returned as well.
	ms.protectedTimestamps = append(ms.protectedTimestamps, ts(7), ts(12))
	ms.updatedTS = ts(19)
	timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	require.NoError(t, err)
	require.Equal(t, ts(19), asOf)
	validateProtectedTimestamps([]hlc.Timestamp{ts(6), ts(7), ts(10), ts(12)}, timestamps)

	// Clear out records from the cache, bump its freshness.
	mc.protectedTimestamps = nil
	mc.asOf = ts(22)
	timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	require.NoError(t, err)
	require.Equal(t, ts(19), asOf)
	validateProtectedTimestamps([]hlc.Timestamp{ts(7), ts(12)}, timestamps)

	// Clear out records from the subscriber, bump its freshness.
	ms.protectedTimestamps = nil
	ms.updatedTS = ts(25)
	timestamps, asOf, err = adapter.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	require.NoError(t, err)
	require.Equal(t, ts(22), asOf)
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

func (m *manualSubscriber) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	panic("unimplemented")
}

func (m *manualSubscriber) ComputeSplitKey(
	context.Context, roachpb.RKey, roachpb.RKey,
) roachpb.RKey {
	panic("unimplemented")
}

func (m *manualSubscriber) GetSpanConfigForKey(
	context.Context, roachpb.RKey,
) (roachpb.SpanConfig, error) {
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
