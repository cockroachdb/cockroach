// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigwatcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigmanager"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestWatcher verifies that the watcher emits the right updates following
// changes made to the system.span_configurations table.
func TestWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfigManager: &spanconfigmanager.TestingKnobs{
					DisableJobCreation: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	node := ts.Node().(*server.Node)
	span := func(start, end string) roachpb.Span {
		return roachpb.Span{
			Key:    roachpb.Key(start),
			EndKey: roachpb.Key(end),
		}
	}

	// ops is a list of operations to execute in order.
	ops := []struct {
		span         roachpb.Span
		config, prev roachpb.SpanConfig
		delete       bool
	}{
		{
			span:   span("a", "c"),
			config: roachpb.SpanConfig{},
			delete: false,
		},
		{
			span:   span("d", "f"),
			config: roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3},
			delete: false,
		},
		{
			span:   span("d", "f"),
			prev:   roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3},
			delete: true,
		},
	}

	// initialWatcher is set up to watch the table before any updates are made
	// to it. It should observe all changes, in order, including the deletions.
	initialWatcher := spanconfigwatcher.New(ts.DB(), ts.Clock(), ts.RangeFeedFactory().(*rangefeed.Factory))
	initialUpdateCh, err := initialWatcher.Watch(ctx, tc.Stopper())
	require.NoError(t, err)

	for _, op := range ops {
		if op.delete {
			require.NoError(t, node.UpdateSpanConfigEntries(ctx, nil, roachpb.Spans{op.span}))
		} else {
			update := []roachpb.SpanConfigEntry{{Span: op.span, Config: op.config}}
			require.NoError(t, node.UpdateSpanConfigEntries(ctx, update, nil))
		}
	}

	for _, op := range ops {
		select {
		case update := <-initialUpdateCh:
			require.True(t, update.Entry.Span.Equal(op.span))
			require.Equal(t, update.Deleted, op.delete)
			if op.delete {
				require.True(t, update.Entry.Config.Equal(op.prev))
			} else {
				require.True(t, update.Entry.Config.Equal(op.config))
			}
		case <-time.After(5 * time.Second):
			t.Errorf("test timed out")
		}
	}

	// finalWatcher is set up to watch the table after the deletes have
	// occurred. It should only observe the final state of the table.
	finalWatcher := spanconfigwatcher.New(ts.DB(), ts.Clock(), ts.RangeFeedFactory().(*rangefeed.Factory))
	finalUpdateCh, err := finalWatcher.Watch(ctx, tc.Stopper())
	require.NoError(t, err)

	select {
	case update := <-finalUpdateCh:
		finalOp := ops[0]
		require.True(t, update.Entry.Span.Equal(finalOp.span))
		require.True(t, update.Entry.Config.Equal(finalOp.config))
		require.False(t, update.Deleted)
	case <-time.After(5 * time.Second):
		t.Errorf("test timed out")
	}
}
