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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// TestRangefeedWorksOnSystemRangesUnconditionally ensures that a rangefeed will
// not return an error when operating on a system span even if the setting is
// disabled. The test also ensures that an error is received if a rangefeed is
// run on a user table.
func TestRangefeedWorksOnSystemRangesUnconditionally(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Make sure the rangefeed setting really is disabled.
	_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = false")
	require.NoError(t, err)

	db := tc.Server(0).DB()
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)
	tc.Server(0)

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
			rangefeedErrChan <- ds.RangeFeed(ctxToCancel, []roachpb.Span{descTableSpan}, startTS, false /* withDiff */, evChan)
		}()

		// Note: 42 is a system descriptor.
		const junkDescriptorID = 42
		junkDescriptorKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, junkDescriptorID)
		junkDescriptor := dbdesc.NewInitial(
			junkDescriptorID, "junk", security.AdminRoleName())
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return txn.Put(ctx, junkDescriptorKey, junkDescriptor.DescriptorProto())
		}))
		after := db.Clock().Now()
		for {
			ev := <-evChan
			if ev.Checkpoint != nil && after.Less(ev.Checkpoint.ResolvedTS) {
				t.Fatal("expected to see write which occurred before the checkpoint")
			}

			if ev.Val != nil && ev.Val.Key.Equal(junkDescriptorKey) {
				var gotProto descpb.Descriptor
				require.NoError(t, ev.Val.Value.GetProto(&gotProto))
				require.EqualValues(t, junkDescriptor.DescriptorProto(), &gotProto)
				break
			}
		}
		cancel()
		// There are several cases that seems like they can happen due
		// to closed connections. Instead we just expect an error.
		// The main point is we get an error in a timely manner.
		select {
		case <-time.After(30 * time.Second):
			t.Fatal("timed out")
		case err := <-rangefeedErrChan:
			require.Error(t, err)
		}
	})
	t.Run("does not work on user ranges", func(t *testing.T) {
		k := tc.ScratchRange(t)
		require.NoError(t, tc.WaitForSplitAndInitialization(k))
		startTS := db.Clock().Now()
		scratchSpan := roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
		testutils.SucceedsSoon(t, func() error {
			for i := 0; i < tc.NumServers(); i++ {
				repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(k))
				if repl == nil {
					return fmt.Errorf("replica not found on n%d", i)
				}
				if repl.SpanConfig().RangefeedEnabled {
					return errors.New("waiting for span configs")
				}
			}
			return nil
		})
		evChan := make(chan *roachpb.RangeFeedEvent)
		require.Regexp(t, `rangefeeds require the kv\.rangefeed.enabled setting`,
			ds.RangeFeed(ctx, []roachpb.Span{scratchSpan}, startTS, false /* withDiff */, evChan))
	})
}

// TestMergeOfRangeEventTableWhileRunningRangefeed ensures that it is safe
// for a range merge transaction which has laid down intents on the RHS of the
// merge commit while a rangefeed is running on the RHS. At the time of writing,
// the only such range that this can happen to is the RangeEventTable.
func TestMergeOfRangeEventTableWhileRunningRangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		// Using ReplicationManual will disable the merge queue.
		ReplicationMode: base.ReplicationManual,
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	// Set a short closed timestamp interval so that we don't need to wait long
	// for resolved events off of the rangefeed later.
	_, err := tc.ServerConn(0).Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
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
			[]roachpb.Span{lhsRepl.Desc().RSpan().AsRawSpanWithNoLocals()},
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

func TestRangefeedIsRoutedToNonVoter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := aggressiveResolvedTimestampClusterArgs
	// We want to manually add a non-voter to a range in this test, so disable
	// the replicateQueue to prevent it from disrupting the test.
	clusterArgs.ReplicationMode = base.ReplicationManual
	// NB: setupClusterForClosedTSTesting sets a low closed timestamp target
	// duration.
	tc, _, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, clusterArgs, "cttest", "kv")
	defer tc.Stopper().Stop(ctx)
	tc.AddNonVotersOrFatal(t, desc.StartKey.AsRawKey(), tc.Target(1))

	db := tc.Server(1).DB()
	ds := tc.Server(1).DistSenderI().(*kvcoord.DistSender)
	_, err := tc.ServerConn(1).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)

	startTS := db.Clock().Now()
	rangefeedCtx, rangefeedCancel := context.WithCancel(ctx)
	rangefeedCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(rangefeedCtx,
		tc.Server(1).TracerI().(*tracing.Tracer),
		"rangefeed over non-voter")
	defer getRecAndFinish()

	// Do a read on the range to make sure that the dist sender learns about the
	// latest state of the range (with the new non-voter).
	_, err = db.Get(ctx, desc.StartKey.AsRawKey())
	require.NoError(t, err)

	rangefeedErrChan := make(chan error, 1)
	eventCh := make(chan *roachpb.RangeFeedEvent, 1000)
	go func() {
		rangefeedErrChan <- ds.RangeFeed(
			rangefeedCtx,
			[]roachpb.Span{desc.RSpan().AsRawSpanWithNoLocals()},
			startTS,
			false, /* withDiff */
			eventCh,
		)
	}()

	// Wait for an event to ensure that the rangefeed is set up.
	select {
	case <-eventCh:
	case err := <-rangefeedErrChan:
		t.Fatalf("rangefeed failed with %s", err)
	case <-time.After(60 * time.Second):
		t.Fatalf("rangefeed initialization took too long")
	}
	rangefeedCancel()
	require.Regexp(t, "context canceled", <-rangefeedErrChan)
	require.Regexp(t, "attempting to create a RangeFeed over replica.*2NON_VOTER", getRecAndFinish().String())
}
