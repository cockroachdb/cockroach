// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSpanConfigUpdateAppliedToReplica ensures that when a store learns of a
// span config update, it installs the corresponding config on the right
// replica.
func TestSpanConfigUpdateAppliedToReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	spanConfigStore := spanconfigstore.New(
		roachpb.TestingDefaultSpanConfig(),
		cluster.MakeTestingClusterSettings(),
		spanconfigstore.NewEmptyBoundsReader(),
		nil,
	)
	var t0 = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mockSubscriber := newMockSpanConfigSubscriber(t0, spanConfigStore)

	ctx := context.Background()

	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
				DisableGCQueue:    true,
			},
			SpanConfig: &spanconfig.TestingKnobs{
				StoreKVSubscriberOverride: mockSubscriber,
			},
		},
	}
	s := serverutils.StartServerOnly(t, args)
	defer s.Stopper().Stop(context.Background())

	_, err := s.InternalExecutor().(isql.Executor).ExecEx(ctx, "inline-exec", nil,
		sessiondata.RootUserSessionDataOverride,
		`SET CLUSTER SETTING spanconfig.store.enabled = true`)
	require.NoError(t, err)

	key, err := s.ScratchRange()
	require.NoError(t, err)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	repl := store.LookupReplica(keys.MustAddr(key))
	span := repl.Desc().RSpan().AsRawSpanWithNoLocals()
	conf := roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3}

	add, err := spanconfig.Addition(spanconfig.MakeTargetFromSpan(span), conf)
	require.NoError(t, err)
	deleted, added := spanConfigStore.Apply(
		ctx,
		false, /* dryrun */
		add,
	)
	require.Empty(t, deleted)
	require.Len(t, added, 1)
	require.True(t, added[0].GetTarget().GetSpan().Equal(span))
	addedCfg := added[0].GetConfig()
	require.True(t, addedCfg.Equal(conf))

	require.NotNil(t, mockSubscriber.callback)
	mockSubscriber.callback(ctx, span) // invoke the callback
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(keys.MustAddr(key))
		gotConfig, err := repl.SpanConfig()
		if err != nil {
			return err
		}
		if !gotConfig.Equal(conf) {
			return errors.Newf("expected config=%s, got config=%s", conf.String(), gotConfig.String())
		}
		return nil
	})
}

// TestFallbackSpanConfigOverride ensures that
// spanconfig.store.fallback_config_override works as expected.
func TestFallbackSpanConfigOverride(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	spanConfigStore := spanconfigstore.New(
		roachpb.TestingDefaultSpanConfig(),
		st,
		spanconfigstore.NewEmptyBoundsReader(),
		nil,
	)
	var t0 = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	mockSubscriber := newMockSpanConfigSubscriber(t0, spanConfigStore)

	ctx := context.Background()
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
				DisableGCQueue:    true,
			},
			SpanConfig: &spanconfig.TestingKnobs{
				StoreKVSubscriberOverride: mockSubscriber,
			},
		},
	}
	s := serverutils.StartServerOnly(t, args)
	defer s.Stopper().Stop(context.Background())

	_, err := s.InternalDB().(isql.DB).Executor().ExecEx(ctx, "inline-exec", nil,
		sessiondata.RootUserSessionDataOverride,
		`SET CLUSTER SETTING spanconfig.store.enabled = true`)
	require.NoError(t, err)

	key, err := s.ScratchRange()
	require.NoError(t, err)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	repl := store.LookupReplica(keys.MustAddr(key))
	span := repl.Desc().RSpan().AsRawSpanWithNoLocals()

	conf := roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3}
	spanconfigstore.FallbackConfigOverride.Override(ctx, &st.SV, &conf)

	require.NotNil(t, mockSubscriber.callback)
	mockSubscriber.callback(ctx, span) // invoke the callback
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(keys.MustAddr(key))
		gotConfig, err := repl.SpanConfig()
		if err != nil {
			return err
		}
		if !gotConfig.Equal(conf) {
			return errors.Newf("expected config=%s, got config=%s", conf.String(), gotConfig.String())
		}
		return nil
	})
}

type mockSpanConfigSubscriber struct {
	callback    func(ctx context.Context, config roachpb.Span)
	lastUpdated time.Time
	spanconfig.Store
}

var _ spanconfig.KVSubscriber = &mockSpanConfigSubscriber{}

func newMockSpanConfigSubscriber(
	lastUpdated time.Time, store spanconfig.Store,
) *mockSpanConfigSubscriber {
	return &mockSpanConfigSubscriber{
		lastUpdated: lastUpdated,
		Store:       store,
	}
}

func (m *mockSpanConfigSubscriber) NeedsSplit(
	ctx context.Context, start, end roachpb.RKey,
) (bool, error) {
	return m.Store.NeedsSplit(ctx, start, end)
}

func (m *mockSpanConfigSubscriber) ComputeSplitKey(
	ctx context.Context, start, end roachpb.RKey,
) (roachpb.RKey, error) {
	return m.Store.ComputeSplitKey(ctx, start, end)
}

func (m *mockSpanConfigSubscriber) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	return m.Store.GetSpanConfigForKey(ctx, key)
}

func (m *mockSpanConfigSubscriber) GetProtectionTimestamps(
	context.Context, roachpb.Span,
) ([]hlc.Timestamp, hlc.Timestamp, error) {
	panic("unimplemented")
}

func (m *mockSpanConfigSubscriber) LastUpdated() hlc.Timestamp {
	return hlc.Timestamp{WallTime: m.lastUpdated.UnixNano()}
}

func (m *mockSpanConfigSubscriber) Subscribe(callback func(context.Context, roachpb.Span)) {
	m.callback = callback
}
