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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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

	spanConfigStore := spanconfigstore.New(roachpb.TestingDefaultSpanConfig())
	mockSubscriber := newMockSpanConfigSubscriber(spanConfigStore)

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
	s, _, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(context.Background())

	_, err := s.InternalExecutor().(sqlutil.InternalExecutor).ExecEx(ctx, "inline-exec", nil,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		`SET CLUSTER SETTING spanconfig.store.enabled = true`)
	require.NoError(t, err)

	key, err := s.ScratchRange()
	require.NoError(t, err)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	repl := store.LookupReplica(keys.MustAddr(key))
	span := repl.Desc().RSpan().AsRawSpanWithNoLocals()
	conf := roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3}

	deleted, added := spanConfigStore.Apply(
		ctx,
		false, /* dryrun */
		spanconfig.Addition(spanconfig.MakeTargetFromSpan(span), conf),
	)
	require.Empty(t, deleted)
	require.Len(t, added, 1)
	require.True(t, added[0].Target.GetSpan().Equal(span))
	require.True(t, added[0].Config.Equal(conf))

	require.NotNil(t, mockSubscriber.callback)
	mockSubscriber.callback(ctx, span) // invoke the callback
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(keys.MustAddr(key))
		gotConfig := repl.SpanConfig()
		if !gotConfig.Equal(conf) {
			return errors.Newf("expected config=%s, got config=%s", conf.String(), gotConfig.String())
		}
		return nil
	})
}

type mockSpanConfigSubscriber struct {
	callback func(ctx context.Context, config roachpb.Span)
	spanconfig.Store
}

var _ spanconfig.KVSubscriber = &mockSpanConfigSubscriber{}

func newMockSpanConfigSubscriber(store spanconfig.Store) *mockSpanConfigSubscriber {
	return &mockSpanConfigSubscriber{Store: store}
}

func (m *mockSpanConfigSubscriber) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	return m.Store.NeedsSplit(ctx, start, end)
}

func (m *mockSpanConfigSubscriber) ComputeSplitKey(
	ctx context.Context, start, end roachpb.RKey,
) roachpb.RKey {
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
	panic("unimplemented")
}

func (m *mockSpanConfigSubscriber) Subscribe(callback func(context.Context, roachpb.Span)) {
	m.callback = callback
}
