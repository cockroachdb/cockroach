// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type testRangefeedClient struct {
	roachpb.InternalClient
	muxRangeFeedEnabled bool
	count               func()
}

func (c *testRangefeedClient) RangeFeed(
	ctx context.Context, args *roachpb.RangeFeedRequest, opts ...grpc.CallOption,
) (roachpb.Internal_RangeFeedClient, error) {
	defer c.count()

	if c.muxRangeFeedEnabled {
		panic(errors.AssertionFailedf("unexpected call to RangeFeed"))
	}
	return c.InternalClient.RangeFeed(ctx, args, opts...)
}

func (c *testRangefeedClient) RangeFeedStream(
	ctx context.Context, opts ...grpc.CallOption,
) (roachpb.Internal_MuxRangeFeedClient, error) {
	defer c.count()

	if !c.muxRangeFeedEnabled {
		panic(errors.AssertionFailedf("unexpected call to RangeFeedStream"))
	}
	return c.InternalClient.MuxRangeFeed(ctx, opts...)
}

type internalClientCounts struct {
	syncutil.Mutex
	counts map[roachpb.InternalClient]int
}

func (c *internalClientCounts) Inc(ic roachpb.InternalClient) {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.counts[ic]++
}

type countConnectionsTransport struct {
	wrapped         kvcoord.Transport
	counts          *internalClientCounts
	rfStreamEnabled bool
}

func (c *countConnectionsTransport) IsExhausted() bool {
	return c.wrapped.IsExhausted()
}

func (c *countConnectionsTransport) SendNext(
	ctx context.Context, request roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	return c.wrapped.SendNext(ctx, request)
}

type testFeedCtxKey struct{}

func (c *countConnectionsTransport) NextInternalClient(
	ctx context.Context,
) (context.Context, roachpb.InternalClient, error) {
	ctx, client, err := c.wrapped.NextInternalClient(ctx)
	if err != nil {
		return ctx, client, err
	}

	// Count rangefeed calls but only for feeds started by this test.
	countFn := func() {}
	if ctx.Value(testFeedCtxKey{}) != nil {
		countFn = func() {
			c.counts.Inc(client)
		}
	}

	tc := &testRangefeedClient{
		InternalClient:      client,
		muxRangeFeedEnabled: c.rfStreamEnabled,
		count:               countFn,
	}
	return ctx, tc, nil
}

func (c *countConnectionsTransport) NextReplica() roachpb.ReplicaDescriptor {
	return c.wrapped.NextReplica()
}

func (c *countConnectionsTransport) SkipReplica() {
	c.wrapped.SkipReplica()
}

func (c *countConnectionsTransport) MoveToFront(descriptor roachpb.ReplicaDescriptor) {
	c.wrapped.MoveToFront(descriptor)
}

func (c *countConnectionsTransport) Release() {
	c.wrapped.Release()
}

var _ kvcoord.Transport = (*countConnectionsTransport)(nil)

func makeTransportFactory(
	rfStreamEnabled bool, counts *internalClientCounts,
) kvcoord.TransportFactory {
	return func(
		options kvcoord.SendOptions,
		dialer *nodedialer.Dialer,
		slice kvcoord.ReplicaSlice,
	) (kvcoord.Transport, error) {
		transport, err := kvcoord.GRPCTransportFactory(options, dialer, slice)
		if err != nil {
			return nil, err
		}
		countingTransport := &countConnectionsTransport{
			wrapped:         transport,
			rfStreamEnabled: rfStreamEnabled,
			counts:          counts,
		}
		return countingTransport, nil
	}
}

// rangeFeed is a helper to execute rangefeed.  We are not using rangefeed library
// here because of circular dependencies.
func rangeFeed(
	dsI interface{},
	sp roachpb.Span,
	startFrom hlc.Timestamp,
	onValue func(event *roachpb.RangeFeedEvent),
) func() {
	ds := dsI.(*kvcoord.DistSender)
	events := make(chan *roachpb.RangeFeedEvent)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), testFeedCtxKey{}, struct{}{}))

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) (err error) {
		return ds.RangeFeed(ctx, []roachpb.Span{sp}, startFrom, false, events)
	})
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ev := <-events:
				onValue(ev)
			}
		}
	})

	return func() {
		cancel()
		_ = g.Wait()
	}
}

// observeNValues returns on value handler which expects to see N rangefeed values,
// along with the channel which gets closed when requisite count of events has been seen.
func observeNValues(n int) (chan struct{}, func(ev *roachpb.RangeFeedEvent)) {
	var count = struct {
		syncutil.Mutex
		c int
	}{}
	allSeen := make(chan struct{})
	return allSeen, func(ev *roachpb.RangeFeedEvent) {
		if ev.Val != nil {
			count.Lock()
			count.c++
			if count.c == n {
				close(allSeen)
			}
			count.Unlock()
		}
	}
}

func TestBiDirectionalRangefeedNotUsedUntilUpgradeFinalilzed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	startServerAtVer := func(ver roachpb.Version) (*testcluster.TestCluster, func()) {
		st := cluster.MakeTestingClusterSettingsWithVersions(ver, ver, true)
		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual, // Turn off replication queues.

			ServerArgs: base.TestServerArgs{
				Settings: st,
				Knobs: base.TestingKnobs{
					KVClient: &kvcoord.ClientTestingKnobs{
						TransportFactory: makeTransportFactory(false, nil),
					},

					Server: &server.TestingKnobs{
						DisableAutomaticVersionUpgrade: 1,
						BinaryVersionOverride:          ver,
					},
				},
			},
		})
		return tc, func() { tc.Stopper().Stop(ctx) }
	}

	// Create a small table; run rangefeed.  The transport factory we injected above verifies
	// that we use the old rangefeed implementation.
	runRangeFeed := func(tc *testcluster.TestCluster) {
		ts := tc.Server(0)

		sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		startTime := ts.Clock().Now()

		sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
		sqlDB.Exec(t, `CREATE TABLE foo (key INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`)

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			ts.DB(), keys.SystemSQLCodec, "defaultdb", "foo")
		fooSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

		allSeen, onValue := observeNValues(1000)
		closeFeed := rangeFeed(ts.DistSenderI(), fooSpan, startTime, onValue)
		<-allSeen
		closeFeed()
	}

	t.Run("rangefeed-stream-disabled-prior-to-version-upgrade", func(t *testing.T) {
		noRfStreamVer := clusterversion.ByKey(clusterversion.RangefeedUseOneStreamPerNode - 1)
		tc, cleanup := startServerAtVer(noRfStreamVer)
		defer cleanup()
		runRangeFeed(tc)
	})

	t.Run("rangefeed-stream-disabled-via-environment", func(t *testing.T) {
		defer kvcoord.TestinSetEnableMuxRangeFeed(false)()
		// Even though we could use rangefeed stream, it's disable via kill switch.
		rfStreamVer := clusterversion.ByKey(clusterversion.RangefeedUseOneStreamPerNode)
		tc, cleanup := startServerAtVer(rfStreamVer)
		defer cleanup()
		runRangeFeed(tc)
	})
}

func TestMuxRangeFeedConnectsToNodeOnce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	connCounts := &internalClientCounts{counts: make(map[roachpb.InternalClient]int)}
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // Turn off replication queues.

		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				KVClient: &kvcoord.ClientTestingKnobs{
					TransportFactory: makeTransportFactory(true, connCounts),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	startTime := ts.Clock().Now()

	// Create a table, and split it so that we have multiple ranges, distributed across
	// test cluster nodes.
	sqlDB.Exec(t, `
SET CLUSTER SETTING kv.rangefeed.enabled = true;
ALTER DATABASE defaultdb  CONFIGURE ZONE USING num_replicas = 1;
CREATE TABLE foo (key INT PRIMARY KEY);
INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000);
ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(100, 900, 100));
`)

	for i := 100; i <= 900; i += 100 {
		storeID := 1 + i%3
		rowID := i
		sqlDB.Exec(t, "ALTER TABLE foo EXPERIMENTAL_RELOCATE VALUES (ARRAY[$1], $2)", storeID, rowID)
	}

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(
		ts.DB(), keys.SystemSQLCodec, "defaultdb", "foo")
	fooSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

	allSeen, onValue := observeNValues(1000)
	closeFeed := rangeFeed(ts.DistSenderI(), fooSpan, startTime, onValue)
	<-allSeen
	closeFeed()

	// Verify we connected to each node once.
	connCounts.Lock()
	defer connCounts.Unlock()
	for _, c := range connCounts.counts {
		require.Equal(t, 1, c)
	}
}
