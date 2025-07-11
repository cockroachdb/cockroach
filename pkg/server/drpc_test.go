// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
	"storj.io/drpc/drpcclient"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmigrate"
)

// TestDRPCBatchServer verifies that CRDB nodes can host a drpc server that
// serves BatchRequest. It doesn't verify that nodes use drpc to communiate with
// each other.
func TestDRPCBatchServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	const numNodes = 1

	testutils.RunTrueAndFalse(t, "insecure", func(t *testing.T, insecure bool) {
		args := base.TestClusterArgs{}
		args.ServerArgs.Insecure = insecure
		args.ReplicationMode = base.ReplicationManual
		args.ServerArgs.Settings = cluster.MakeClusterSettings()
		rpc.ExperimentalDRPCEnabled.Override(ctx, &args.ServerArgs.Settings.SV, true)
		c := testcluster.StartTestCluster(t, numNodes, args)
		defer c.Stopper().Stop(ctx)

		require.Equal(t, insecure, c.Server(0).RPCContext().Insecure)

		rpcAddr := c.Server(0).RPCAddr()

		// Dial the drpc server with the drpc connection header.
		rawconn, err := drpcmigrate.DialWithHeader(ctx, "tcp", rpcAddr, drpcmigrate.DRPCHeader)
		require.NoError(t, err)

		var conn *drpcconn.Conn
		if !insecure {
			cm, err := c.Server(0).RPCContext().GetCertificateManager()
			require.NoError(t, err)
			tlsCfg, err := cm.GetNodeClientTLSConfig()
			require.NoError(t, err)
			tlsCfg = tlsCfg.Clone()
			tlsCfg.ServerName = "*.local"
			tlsConn := tls.Client(rawconn, tlsCfg)
			conn = drpcconn.New(tlsConn)
		} else {
			conn = drpcconn.New(rawconn)
		}
		defer func() { require.NoError(t, conn.Close()) }()

		desc := c.LookupRangeOrFatal(t, c.ScratchRange(t))

		client := kvpb.NewDRPCKVBatchClient(conn)
		ba := &kvpb.BatchRequest{}
		ba.RangeID = desc.RangeID
		var ok bool
		ba.Replica, ok = desc.GetReplicaDescriptor(1)
		require.True(t, ok)
		req := &kvpb.LeaseInfoRequest{}
		req.Key = desc.StartKey.AsRawKey()
		ba.Add(req)
		_, err = client.Batch(ctx, ba)
		require.NoError(t, err)
	})
}

func TestStreamContextCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numNodes = 1
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeClusterSettings(),
		},
	}

	ctx := context.Background()
	rpc.ExperimentalDRPCEnabled.Override(ctx, &args.ServerArgs.Settings.SV, true)
	c := testcluster.StartTestCluster(t, numNodes, args)
	defer c.Stopper().Stop(ctx)

	rpcAddr := c.Server(0).RPCAddr()

	// Dial the drpc server with the drpc connection header.
	rawconn, err := drpcmigrate.DialWithHeader(ctx, "tcp", rpcAddr, drpcmigrate.DRPCHeader)
	require.NoError(t, err)

	cm, err := c.Server(0).RPCContext().GetCertificateManager()
	require.NoError(t, err)
	tlsCfg, err := cm.GetNodeClientTLSConfig()
	require.NoError(t, err)
	tlsCfg = tlsCfg.Clone()
	tlsCfg.ServerName = "*.local"
	tlsConn := tls.Client(rawconn, tlsCfg)
	conn := drpcconn.NewWithOptions(tlsConn, drpcconn.Options{
		Manager: drpcmanager.Options{
			SoftCancel: true, // don't close the transport when stream context is canceled
		},
	})
	defer func() {
		require.NoError(t, conn.Close())
	}()

	desc := c.LookupRangeOrFatal(t, c.ScratchRange(t))
	client := kvpb.NewDRPCKVBatchClient(conn)

	singleRequest := func() {
		streamCtx, streamCtxCancel := context.WithCancel(ctx)
		defer streamCtxCancel()

		s, err := client.BatchStream(streamCtx)
		require.NoError(t, err)

		ba := &kvpb.BatchRequest{}
		ba.RangeID = desc.RangeID

		var ok bool
		ba.Replica, ok = desc.GetReplicaDescriptor(1)
		require.True(t, ok)

		req := &kvpb.LeaseInfoRequest{}
		req.Key = desc.StartKey.AsRawKey()
		ba.Add(req)

		err = s.Send(ba)
		require.NoError(t, err)

		_, err = s.Recv()
		require.NoError(t, err)
	}

	// Make two consecutive stream requests using the same connection.
	for i := 0; i < 2; i++ {
		select {
		case <-conn.Closed():
			t.Fatal("connection closed unexpectedly")
		default:
		}

		singleRequest()
	}
}

func TestDefaultDRPCOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "drpc-enabled", func(t *testing.T, option bool) {
		opt := base.TestDRPCDisabled
		if option {
			opt = base.TestDRPCEnabled
		}
		s := serverutils.StartServerOnly(t, base.TestServerArgs{DefaultDRPCOption: opt})
		defer s.Stopper().Stop(context.Background())

		var enabled bool
		sqlutils.MakeSQLRunner(s.SQLConn(t)).QueryRow(t, "SHOW CLUSTER SETTING rpc.experimental_drpc.enabled").Scan(&enabled)
		require.Equal(t, option, enabled)
	})
}

func TestDialDRPC_InterceptorsAreSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var unaryInterceptorCalled bool
	var streamInterceptorCalled bool

	// dummy unary interceptor for testing.
	mockUnaryInterceptor := func(
		ctx context.Context,
		rpc string,
		enc drpc.Encoding,
		in, out drpc.Message,
		cc *drpcclient.ClientConn,
		invoker drpcclient.UnaryInvoker,
	) error {
		unaryInterceptorCalled = true
		return invoker(ctx, rpc, enc, in, out, cc)
	}

	// dummy stream interceptor for testing.
	mockStreamInterceptor := func(
		ctx context.Context,
		rpc string,
		enc drpc.Encoding,
		cc *drpcclient.ClientConn,
		streamer drpcclient.Streamer,
	) (drpc.Stream, error) {
		streamInterceptorCalled = true
		return streamer(ctx, rpc, enc, cc)
	}

	ctx := context.Background()
	const numNodes = 1
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeClusterSettings(),
			Insecure: true,
		},
	}

	rpc.ExperimentalDRPCEnabled.Override(ctx, &args.ServerArgs.Settings.SV, true)
	c := testcluster.StartTestCluster(t, numNodes, args)
	defer c.Stopper().Stop(ctx)
	require.True(t, c.Server(0).RPCContext().Insecure)

	rpcAddr := c.Server(0).RPCAddr()

	// Client setup
	// Setup a minimal rpcCtx with interceptors
	rpcContextOptions := rpc.DefaultContextOptions()
	rpcContextOptions.Stopper = c.Stopper()
	rpcContextOptions.Settings = c.Server(0).ClusterSettings()
	rpcCtx := rpc.NewContext(ctx, rpcContextOptions)
	rpcCtx.ContextOptions = rpc.ContextOptions{Insecure: true}
	// Adding test interceptors
	rpcCtx.Knobs = rpc.ContextTestingKnobs{
		UnaryClientInterceptorDRPC: func(target string, class rpcbase.ConnectionClass) drpcclient.UnaryClientInterceptor {
			return mockUnaryInterceptor
		},
		StreamClientInterceptorDRPC: func(target string, class rpcbase.ConnectionClass) drpcclient.StreamClientInterceptor {
			return mockStreamInterceptor
		},
	}
	getConn := rpc.DialDRPC(rpcCtx)
	conn, err := getConn(ctx, rpcAddr, rpcbase.DefaultClass)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	desc := c.LookupRangeOrFatal(t, c.ScratchRange(t))

	// reset flag values
	unaryInterceptorCalled = false
	streamInterceptorCalled = false

	client := kvpb.NewDRPCKVBatchClient(conn)
	ba := &kvpb.BatchRequest{}
	ba.RangeID = desc.RangeID
	var ok bool
	ba.Replica, ok = desc.GetReplicaDescriptor(1)
	require.True(t, ok)
	req := &kvpb.LeaseInfoRequest{}
	req.Key = desc.StartKey.AsRawKey()
	ba.Add(req)
	_, err = client.Batch(ctx, ba)
	require.NoError(t, err)
	s, err := client.BatchStream(ctx)
	require.NoError(t, err)
	err = s.Send(ba)
	require.NoError(t, err)
	_, err = s.Recv()
	require.NoError(t, err)
	// assert that the interceptors were called
	require.True(t, unaryInterceptorCalled)
	require.True(t, streamInterceptorCalled)
}
