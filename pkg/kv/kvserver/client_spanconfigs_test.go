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
		EnableSpanConfigs: true,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
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
		`SET CLUSTER SETTING spanconfig.experimental_store.enabled = true`)
	require.NoError(t, err)

	key, err := s.ScratchRange()
	require.NoError(t, err)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	repl := store.LookupReplica(keys.MustAddr(key))
	span := repl.Desc().RSpan().AsRawSpanWithNoLocals()
	conf := roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3}

	deleted, added := spanConfigStore.Apply(ctx, spanconfig.Update{Span: span, Config: conf}, false /* dryrun */)
	require.Empty(t, deleted)
	require.Len(t, added, 1)
	require.True(t, added[0].Span.Equal(span))
	require.True(t, added[0].Config.Equal(conf))

	require.NotNil(t, mockSubscriber.callback)
	mockSubscriber.callback(span) // invoke the callback
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(keys.MustAddr(key))
		gotConfig := repl.SpanConfig()
		if !gotConfig.Equal(conf) {
			return errors.Newf("expected config=%s, got config=%s", conf.String(), gotConfig.String())
		}
		return nil
	})
}

func newMockSpanConfigSubscriber(store spanconfig.Store) *mockSpanConfigSubscriber {
	return &mockSpanConfigSubscriber{Store: store}
}

type mockSpanConfigSubscriber struct {
	callback func(config roachpb.Span)
	spanconfig.Store
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

func (m *mockSpanConfigSubscriber) OnSpanConfigUpdate(
	_ context.Context, callback func(roachpb.Span),
) {
	m.callback = callback
}

var _ spanconfig.KVSubscriber = &mockSpanConfigSubscriber{}
