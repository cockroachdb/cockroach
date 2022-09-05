// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package kvcoord_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestRestartsStuckRangeFeeds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testKey struct{}

	ctx := context.Background()

	var canceled int32 // atomic

	var doneErr = errors.New("gracefully terminating test")

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingRangefeedFilter: func(args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer) *roachpb.Error {
						md, ok := metadata.FromIncomingContext(stream.Context())
						if (!ok || len(md[t.Name()]) == 0) && stream.Context().Value(testKey{}) == nil {
							return nil
						}
						if atomic.LoadInt32(&canceled) != 0 {
							return roachpb.NewError(doneErr)
						}

						t.Logf("intercepting %s", args)
						// Send a first response to "arm" the stuck detector in DistSender.
						if assert.NoError(t, stream.Send(&roachpb.RangeFeedEvent{Checkpoint: &roachpb.RangeFeedCheckpoint{
							Span:       args.Span,
							ResolvedTS: hlc.Timestamp{Logical: 1},
						}})) {
							t.Log("sent first event, now blocking")
						}
						select {
						case <-time.After(testutils.DefaultSucceedsSoonDuration):
							return roachpb.NewErrorf("timed out waiting for stuck rangefeed's ctx cancellation")
						case <-stream.Context().Done():
							t.Log("server side rangefeed canceled (as expected)")
							atomic.StoreInt32(&canceled, 1)
						}
						return nil
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	startTime := ts.Clock().Now()

	for _, stmt := range []string{
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`,
		`SET CLUSTER SETTING kv.rangefeed.range_stuck_threshold='1s'`,
	} {
		sqlDB.Exec(t, stmt)
	}

	span := func() roachpb.Span {
		desc := tc.LookupRangeOrFatal(t, tc.ScratchRange(t))
		t.Logf("r%d", desc.RangeID)
		return desc.RSpan().AsRawSpanWithNoLocals()
	}()

	ds := ts.DistSenderI().(*kvcoord.DistSender)

	// Use both gRPC metadata and a local ctx key to tag the context for the
	// outgoing rangefeed. At time of writing, we're bypassing gRPC due to
	// the local optimization, but it's not worth special casing on that.
	ctx = metadata.AppendToOutgoingContext(ctx, t.Name(), "please block me")
	cancel, wait := rangeFeed(t, context.WithValue(ctx, testKey{}, testKey{}), ds, span, startTime)
	defer time.AfterFunc(testutils.DefaultSucceedsSoonDuration, cancel).Stop()
	{
		err := wait()
		require.True(t, errors.Is(err, doneErr), "%+v", err)
	}

	require.EqualValues(t, 1, atomic.LoadInt32(&canceled))
	// NB: We  really expect exactly 1 but with a 1s timeout, it's not inconceivable that
	// on a particularly slow CI machine some unrelated rangefeed could also catch the occasional
	// retry.
	require.NotZero(t, ds.Metrics().RangefeedRestartStuck.Count())
}

// rangeFeed is a helper to execute rangefeed.  We are not using rangefeed library
// here because of circular dependencies.
func rangeFeed(
	t *testing.T,
	ctx context.Context,
	ds *kvcoord.DistSender,
	sp roachpb.Span,
	startFrom hlc.Timestamp,
) (_cancel func(), _wait func() error) {
	events := make(chan *roachpb.RangeFeedEvent)
	ctx, cancel := context.WithCancel(ctx)
	{
		origCancel := cancel
		cancel = func() {
			t.Helper()
			t.Log("cancel invoked")
			origCancel()
		}
	}

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer close(events)
		err := ds.RangeFeed(ctx, []roachpb.Span{sp}, startFrom, false, events)
		t.Logf("from RangeFeed: %v", err)
		return err
	})
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil // expected
			case ev := <-events:
				t.Logf("from consumer: %+v", ev)
			case <-time.After(testutils.DefaultSucceedsSoonDuration):
				return errors.New("timed out waiting to consume events")
			}
		}
	})

	return cancel, g.Wait
}
