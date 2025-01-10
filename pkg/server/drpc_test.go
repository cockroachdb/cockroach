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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"storj.io/drpc/drpcconn"
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

		client := kvpb.NewDRPCBatchClient(conn)
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
