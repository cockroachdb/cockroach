// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvwatcher_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestKVWatcher verifies that the watcher emits the right updates following
// changes made to the system.span_configurations table.
func TestKVWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			EnableSpanConfigs: true,
		},
	})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.experimental_kvaccessor.enabled = true`)

	const dummyTableName = "dummy_span_configurations"
	tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummyTableName))

	var dummyTableID uint32
	tdb.QueryRow(t, fmt.Sprintf(
		`SELECT table_id from crdb_internal.tables WHERE name = '%s'`, dummyTableName),
	).Scan(&dummyTableID)

	accessor := spanconfigkvaccessor.New(
		tc.Server(0).DB(),
		tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
		tc.Server(0).ClusterSettings(),
		fmt.Sprintf("defaultdb.public.%s", dummyTableName),
	)

	span := func(start, end string) roachpb.Span {
		return roachpb.Span{
			Key:    roachpb.Key(start),
			EndKey: roachpb.Key(end),
		}
	}
	// ops is a list of operations to execute in order.
	ops := []struct {
		span   roachpb.Span
		config roachpb.SpanConfig
		delete bool
	}{
		{
			span:   span("a", "c"),
			config: roachpb.SpanConfig{NumReplicas: 3},
		},
		{
			span:   span("d", "f"),
			config: roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3},
		},
		{
			span:   span("d", "f"),
			delete: true,
		},
	}

	ts := tc.Server(0)

	// initialWatcher is set up to watch the table before any updates are made
	// to it. It should observe all changes, in order, including the deletions.
	initialWatcher := spanconfigkvwatcher.New(
		tc.Stopper(),
		ts.DB(),
		ts.Clock(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		dummyTableID,
		10<<20, /* 10 MB */
	)
	// XXX: Change these to be callback oriented.
	initialUpdateCh, err := initialWatcher.WatchForKVUpdates(ctx)
	require.NoError(t, err)

	for _, op := range ops {
		if op.delete {
			require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, roachpb.Spans{op.span}, nil))
		} else {
			update := []roachpb.SpanConfigEntry{{Span: op.span, Config: op.config}}
			require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, update))
		}
	}

	for _, op := range ops {
		select {
		case update := <-initialUpdateCh:
			require.Truef(t, update.Span.Equal(op.span), "expected %s to equal %s", update.Span, op.span)
			require.Equal(t, update.Deletion(), op.delete)
			if op.delete {
				require.True(t, update.Config.Equal(roachpb.SpanConfig{}))
			} else {
				require.True(t, update.Config.Equal(op.config))
			}
		case <-time.After(5 * time.Second):
			t.Errorf("test timed out")
		}
	}

	// finalWatcher is set up to watch the table after the deletes have
	// occurred. It should only observe the final state of the table.
	finalWatcher := spanconfigkvwatcher.New(
		tc.Stopper(),
		ts.DB(),
		ts.Clock(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		dummyTableID,
		10<<20, /* 10 MB */
	)
	finalUpdateCh, err := finalWatcher.WatchForKVUpdates(ctx)
	require.NoError(t, err)

	select {
	case update := <-finalUpdateCh:
		finalOp := ops[0]
		require.True(t, update.Span.Equal(finalOp.span))
		require.True(t, update.Config.Equal(finalOp.config))
		require.False(t, update.Deletion())
	case <-time.After(5 * time.Second):
		t.Errorf("test timed out")
	}
}
