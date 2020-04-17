// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	gosql "database/sql"
	"net"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSQLServer starts up a semi-dedicated SQL server and runs some smoke test
// queries. The SQL server shares some components, notably Gossip, with a test
// server serving as a KV backend.
//
// TODO(tbg): start narrowing down and enumerating the unwanted dependencies. In
// the end, the SQL server in this test should not depend on a Gossip instance
// and must not rely on having a NodeID/NodeDescriptor/NodeLiveness/...
//
// In short, it should not rely on the test server through anything other than a
// `*kv.DB` and a small number of whitelisted RPCs.
func TestSQLServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := serverutils.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	n1 := tc.Server(0).(*TestServer)
	args := testSQLServerArgs(n1)

	const nodeID = 9999
	args.nodeIDContainer.Set(context.Background(), nodeID)

	s, err := newSQLServer(ctx, args)
	require.NoError(t, err)

	s.execCfg.DistSQLPlanner.SetNodeDesc(roachpb.NodeDescriptor{
		NodeID: args.nodeIDContainer.Get(),
	})

	connManager := netutil.MakeServer(
		args.stopper,
		// The SQL server only uses connManager.ServeWith. The both below
		// are unused.
		nil, // tlsConfig
		nil, // handler
	)

	pgL, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = pgL.Close() }()

	const (
		socketFile = "" // no unix socket
	)
	orphanedLeasesTimeThresholdNanos := args.clock.Now().WallTime

	require.NoError(t, s.start(ctx,
		args.stopper,
		args.Config.TestingKnobs,
		connManager,
		pgL,
		socketFile,
		orphanedLeasesTimeThresholdNanos,
	))

	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, pgL.Addr().String(), t.Name() /* prefix */, url.User(security.RootUser))

	db, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	tc.Stopper().AddCloser(
		stop.CloserFn(func() {
			_ = db.Close()
			cleanupGoDB()
		}))

	r := sqlutils.MakeSQLRunner(db)
	r.QueryStr(t, `SELECT 1`)
	r.Exec(t, `CREATE DATABASE foo`)
	r.Exec(t, `CREATE TABLE foo.kv (k STRING PRIMARY KEY, v STRING)`)
	r.Exec(t, `INSERT INTO foo.kv VALUES('foo', 'bar')`)
	t.Log(sqlutils.MatrixToStr(r.QueryStr(t, `SET distsql=off; SELECT * FROM foo.kv`)))
	t.Log(sqlutils.MatrixToStr(r.QueryStr(t, `SET distsql=auto; SELECT * FROM foo.kv`)))
}

func testSQLServerArgs(ts *TestServer) sqlServerArgs {
	st := cluster.MakeTestingClusterSettings()
	stopper := ts.Stopper()

	cfg := makeTestConfig(st)

	clock := hlc.NewClock(hlc.UnixNano, 1)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)

	// Dummy. This thing needs the world and then some.
	statusServer := &statusServer{
		sessionRegistry: sql.NewSessionRegistry(),
	}
	nl := allErrorsFakeLiveness{}

	ds := ts.DistSender()

	// Protected timestamps won't be available (at first) in multi-tenant
	// clusters. TODO(tbg): fail with an error instead of a crash when it's
	// used. I believe IMPORT INTO is the only use that needs it for correct-
	// ness, everywhere else we can just not protect the timestamp and continue.
	var protectedTSProvider protectedts.Provider

	registry := metric.NewRegistry()

	// If we used a dummy gossip, DistSQL and random other things won't work.
	// Just use the test server's for now.
	// g := gossip.NewTest(nodeID, nil, nil, stopper, registry, nil)
	g := ts.Gossip()

	nd := nodedialer.New(rpcContext, gossip.AddressResolver(ts.Gossip()))

	dummyRecorder := &status.MetricsRecorder{}

	var nodeIDContainer base.NodeIDContainer

	// We don't need this for anything except some services that want a gRPC
	// server to register against (but they'll never get RPCs at the time of
	// writing): the blob service and DistSQL.
	dummyRPCServer := rpc.NewServer(rpcContext)

	return sqlServerArgs{
		Config:              &cfg,
		stopper:             stopper,
		clock:               clock,
		rpcContext:          rpcContext,
		distSender:          ds,
		status:              statusServer,
		nodeLiveness:        nl,
		protectedtsProvider: protectedTSProvider,
		gossip:              g,
		nodeDialer:          nd,
		grpcServer:          dummyRPCServer,
		recorder:            dummyRecorder,
		isMeta1Leaseholder: func(timestamp hlc.Timestamp) (bool, error) {
			return false, errors.New("fake isMeta1Leaseholder")
		},
		runtime:                  &status.RuntimeStatSampler{}, // dummy
		db:                       ts.DB(),
		registry:                 registry,
		circularInternalExecutor: &sql.InternalExecutor{},
		nodeIDContainer:          &nodeIDContainer,
		externalStorage: func(ctx context.Context, dest roachpb.ExternalStorage) (cloud.ExternalStorage, error) {
			return nil, errors.New("fake external storage")
		},
		externalStorageFromURI: func(ctx context.Context, uri string) (cloud.ExternalStorage, error) {
			return nil, errors.New("fake external uri storage")
		},
		jobRegistry: &jobs.Registry{},
	}
}

type allErrorsFakeLiveness struct{}

var _ jobs.NodeLiveness = (*allErrorsFakeLiveness)(nil)

func (allErrorsFakeLiveness) Self() (storagepb.Liveness, error) {
	return storagepb.Liveness{}, errors.New("fake liveness")

}
func (allErrorsFakeLiveness) GetLivenesses() []storagepb.Liveness {
	return nil
}

func (allErrorsFakeLiveness) IsLive(roachpb.NodeID) (bool, error) {
	return false, errors.New("fake liveness")
}
