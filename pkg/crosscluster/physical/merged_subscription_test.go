// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMergeSubscriptionsRun(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	events := func(partition string) []crosscluster.Event {
		return []crosscluster.Event{
			crosscluster.MakeKVEventFromKVs([]roachpb.KeyValue{{
				Key: []byte(partition + "_key1"),
			}}),
			crosscluster.MakeKVEventFromKVs([]roachpb.KeyValue{{
				Key: []byte(partition + "_key2"),
			}}),
		}
	}
	mockClient := &streamclient.MockStreamClient{
		PartitionEvents: map[string][]crosscluster.Event{
			"partition1": events("partition1"),
			"partition2": events("partition2"),
		},
	}
	defer func() { _ = mockClient.Close(ctx) }()
	sortedExpectedKeys := []string{
		"partition1_key1",
		"partition1_key2",
		"partition2_key1",
		"partition2_key2",
	}

	t.Run("returns without close when all all events are consumed", func(t *testing.T) {
		sub1, err := mockClient.Subscribe(ctx, 0, 0, 0, streamclient.SubscriptionToken("partition1"), hlc.Timestamp{}, nil)
		require.NoError(t, err)
		sub2, err := mockClient.Subscribe(ctx, 0, 0, 0, streamclient.SubscriptionToken("partition2"), hlc.Timestamp{}, nil)
		require.NoError(t, err)

		merged := MergeSubscriptions(ctx, map[string]streamclient.Subscription{
			"partition1": sub1,
			"partition2": sub2,
		})

		g := ctxgroup.WithContext(ctx)
		events := []string{}
		g.Go(func() error {
			for ev := range merged.Events() {
				events = append(events, string(ev.GetKVs()[0].KeyValue.Key))
			}
			return nil
		})
		require.NoError(t, merged.Run())
		require.NoError(t, g.Wait())

		sort.Strings(events)
		require.Equal(t, sortedExpectedKeys, events)
	})
	t.Run("returns after close when there is no reader", func(t *testing.T) {
		sub1, err := mockClient.Subscribe(ctx, 0, 0, 0, streamclient.SubscriptionToken("partition1"), hlc.Timestamp{}, nil)
		require.NoError(t, err)
		sub2, err := mockClient.Subscribe(ctx, 0, 0, 0, streamclient.SubscriptionToken("partition2"), hlc.Timestamp{}, nil)
		require.NoError(t, err)

		merged := MergeSubscriptions(ctx, map[string]streamclient.Subscription{
			"partition1": sub1,
			"partition2": sub2,
		})

		g := ctxgroup.WithContext(ctx)
		g.Go(func() error {
			// Read a single event, to ensure we are started
			<-merged.Events()
			return nil
		})
		require.NoError(t, g.Wait())

		merged.Close()
		require.Error(t, context.Canceled, merged.Run())
	})
}
