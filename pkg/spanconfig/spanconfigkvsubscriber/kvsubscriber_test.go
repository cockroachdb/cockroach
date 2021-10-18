// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvsubscriber_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestKVWatcher verifies that the watcher emits the right updates following
// changes made to the system.span_configurations table.
func TestKVSubscriber(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			EnableSpanConfigs: true,
		},
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
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

	fallback := roachpb.SpanConfig{}

	// ops is a list of operations to execute in order.
	ops := []struct {
		span   roachpb.Span
		config roachpb.SpanConfig
		delete bool
	}{
		{
			span:   span("a", "c"),
			config: roachpb.SpanConfig{NumReplicas: 2},
		},
		{
			span:   span("d", "f"),
			config: roachpb.SpanConfig{NumReplicas: 5, NumVoters: 3},
		},
		// XXX: Re-introduce deletion of existing span. Probably easiest as a
		// datadriven test. Write one for kvaccessor while here and re-use bits
		// here too.

		// {
		// 	span:   span("d", "f"),
		// 	delete: true,
		// },
	}

	expect := func(updateCh chan roachpb.Span, f func(update roachpb.Span)) {
		select {
		case update := <-updateCh:
			f(update)
		case <-time.After(5 * time.Second):
			t.Error("timed out; didn't receive update")
		}
	}

	// initialSubscriber is set up to watch the table before any updates are made
	// to it. It should observe all changes, in order, including the deletions.
	initialSubscriber := spanconfigkvsubscriber.New(
		tc.Stopper(),
		ts.DB(),
		ts.Clock(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		dummyTableID,
		10<<20, /* 10 MB */
		fallback,
	)
	require.NoError(t, initialSubscriber.Start(ctx))

	initialUpdatesCh := make(chan roachpb.Span)
	initialSubscriber.SubscribeToKVUpdates(ctx, func(update roachpb.Span) {
		initialUpdatesCh <- update
	})

	for _, op := range ops {
		if op.delete {
			require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, roachpb.Spans{op.span}, nil))
		} else {
			update := []roachpb.SpanConfigEntry{{Span: op.span, Config: op.config}}
			require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, update))
		}
	}

	expect(initialUpdatesCh, func(update roachpb.Span) {
		require.Truef(t, update.Equal(keys.EverythingSpan),
			"expected %s to equal %s", update, keys.EverythingSpan)
	})

	for _, op := range ops {
		expect(initialUpdatesCh, func(update roachpb.Span) {
			require.Truef(t, update.Equal(op.span), "expected %s to equal %s", update, op.span)
			conf, err := initialSubscriber.GetSpanConfigForKey(ctx, keys.MustAddr(update.Key))
			require.NoError(t, err)
			if op.delete {
				require.True(t, conf.Equal(fallback))
			} else {
				require.True(t, conf.Equal(op.config), "%s: expected %s got %s", op.span, op.config.String(), conf.String())
			}
		})
	}

	// finalSubscriber is set up to subscribe only after the deletes have
	// occurred. It should only observe the final state of the table.
	finalSubscriber := spanconfigkvsubscriber.New(
		tc.Stopper(),
		ts.DB(),
		ts.Clock(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		dummyTableID,
		10<<20, /* 10 MB */
		fallback,
	)
	require.NoError(t, finalSubscriber.Start(ctx))

	finalUpdatesCh := make(chan roachpb.Span)
	finalSubscriber.SubscribeToKVUpdates(ctx, func(update roachpb.Span) {
		finalUpdatesCh <- update
	})

	expect(finalUpdatesCh, func(update roachpb.Span) {
		require.Truef(t, update.Equal(keys.EverythingSpan),
			"expected %s to equal %s", update, keys.EverythingSpan)
	})

	remainingOp := ops[0]
	remainingConf, err := finalSubscriber.GetSpanConfigForKey(ctx, keys.MustAddr(remainingOp.span.Key))
	require.NoError(t, err)
	require.True(t, remainingConf.Equal(remainingOp.config))
}
