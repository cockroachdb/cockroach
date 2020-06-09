// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestRangefeedWorksOnSystemRangesUnconditionally ensures that a rangefeed will
// not return an error when operating on a system span even if the setting is
// disabled. The test also ensures that an error is received if a rangefeed is
// run on a user table.
func TestRangefeedWorksOnSystemRangesUnconditionally(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Make sure the rangefeed setting really is disabled.
	_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = false")
	require.NoError(t, err)

	db := tc.Server(0).DB()
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)

	t.Run("works on system ranges", func(t *testing.T) {
		startTS := db.Clock().Now()
		descTableKey := keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID)
		descTableSpan := roachpb.Span{
			Key:    descTableKey,
			EndKey: descTableKey.PrefixEnd(),
		}

		evChan := make(chan *roachpb.RangeFeedEvent)
		rangefeedErrChan := make(chan error, 1)
		ctxToCancel, cancel := context.WithCancel(ctx)
		go func() {
			rangefeedErrChan <- ds.RangeFeed(ctxToCancel, descTableSpan, startTS, false /* withDiff */, evChan)
		}()

		// Note: 42 is a system descriptor.
		const junkDescriptorID = 42
		require.GreaterOrEqual(t, keys.MaxReservedDescID, junkDescriptorID)
		junkDescriptorKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, junkDescriptorID)
		junkDescriptor := sqlbase.NewInitialDatabaseDescriptor(junkDescriptorID, "junk")
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if err := txn.SetSystemConfigTrigger(); err != nil {
				return err
			}
			return txn.Put(ctx, junkDescriptorKey, junkDescriptor.DescriptorProto())
		}))
		after := db.Clock().Now()
		for {
			ev := <-evChan
			if ev.Checkpoint != nil && after.Less(ev.Checkpoint.ResolvedTS) {
				t.Fatal("expected to see write which occurred before the checkpoint")
			}

			if ev.Val != nil && ev.Val.Key.Equal(junkDescriptorKey) {
				var gotProto sqlbase.Descriptor
				require.NoError(t, ev.Val.Value.GetProto(&gotProto))
				require.EqualValues(t, junkDescriptor.DescriptorProto(), &gotProto)
				break
			}
		}
		cancel()
		// There are several cases that seems like they can happen due
		// to closed connections. Instead we just expect an error.
		// The main point is we get an error in a timely manner.
		require.Error(t, <-rangefeedErrChan)
	})
	t.Run("does not work on user ranges", func(t *testing.T) {
		k := tc.ScratchRange(t)
		require.NoError(t, tc.WaitForSplitAndInitialization(k))
		startTS := db.Clock().Now()
		scratchSpan := roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
		evChan := make(chan *roachpb.RangeFeedEvent)
		require.Regexp(t, `rangefeeds require the kv\.rangefeed.enabled setting`,
			ds.RangeFeed(ctx, scratchSpan, startTS, false /* withDiff */, evChan))
	})
}

// TestMergeOfRangeEventTableWhileRunningRangefeed ensures that it is safe
// for a range merge transaction which has laid down intents on the RHS of the
// merge commit while a rangefeed is running on the RHS. At the time of writing,
// the only such range that this can happen to is the RangeEventTable.
func TestMergeOfRangeEventTableWhileRunningRangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		// Using ReplicationManual will disable the merge queue.
		ReplicationMode: base.ReplicationManual,
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	// Set a short closed timestamp interval so that we don't need to wait long
	// for resolved events off of the rangefeed later.
	_, err := tc.ServerConn(0).Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'")
	require.NoError(t, err)

	// Find the range containing the range event table and then find the range
	// to its left.
	rangeEventTableStart := keys.SystemSQLCodec.TablePrefix(keys.RangeEventTableID)
	require.NoError(t, tc.WaitForSplitAndInitialization(rangeEventTableStart))
	store, _ := getFirstStoreReplica(t, tc.Server(0), rangeEventTableStart)
	var lhsRepl *kvserver.Replica
	store.VisitReplicas(func(repl *kvserver.Replica) (wantMore bool) {
		if repl.Desc().EndKey.AsRawKey().Equal(rangeEventTableStart) {
			lhsRepl = repl
			return false
		}
		return true
	})
	require.NotNil(t, lhsRepl)

	// Set up a rangefeed for the lhs.
	db := tc.Server(0).DB()
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)

	rangefeedCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rangefeedErrChan := make(chan error, 1)
	// Make the buffer large so we don't risk blocking.
	eventCh := make(chan *roachpb.RangeFeedEvent, 1000)
	start := db.Clock().Now()
	go func() {
		rangefeedErrChan <- ds.RangeFeed(rangefeedCtx,
			lhsRepl.Desc().RSpan().AsRawSpanWithNoLocals(),
			start,
			false, /* withDiff */
			eventCh)
	}()

	// Wait for an event on the rangefeed to let us know that we're connected.
	<-eventCh

	// Merge the range event table range with its lhs neighbor.
	require.NoError(t, db.AdminMerge(ctx, lhsRepl.Desc().StartKey.AsRawKey()))

	// Ensure that we get a checkpoint after the merge.
	afterMerge := db.Clock().Now()
	for ev := range eventCh {
		if ev.Checkpoint == nil {
			continue
		}
		if afterMerge.Less(ev.Checkpoint.ResolvedTS) {
			break
		}
	}

	// Cancel the rangefeed and ensure we get the right error.
	cancel()
	require.Regexp(t, context.Canceled.Error(), <-rangefeedErrChan)
}
