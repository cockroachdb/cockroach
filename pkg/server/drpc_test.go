// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmigrate"
)

func TestTestClusterDRPC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	const numNodes = 3

	testutils.RunTrueAndFalse(t, "insecure", func(t *testing.T, insecure bool) {

		args := base.TestClusterArgs{}
		args.ServerArgs.Insecure = insecure
		args.ReplicationMode = base.ReplicationManual
		tc := testcluster.StartTestCluster(t, numNodes, args)
		defer tc.Stopper().Stop(ctx)

		require.Equal(t, insecure, tc.Server(0).RPCContext().Insecure)

		rpcAddr := tc.Server(0).RPCAddr()

		// dial the drpc server with the drpc connection header
		rawconn, err := drpcmigrate.DialWithHeader(ctx, "tcp", rpcAddr, drpcmigrate.DRPCHeader)
		require.NoError(t, err)

		var conn *drpcconn.Conn
		if !insecure {
			cm, err := tc.Server(0).RPCContext().GetCertificateManager()
			require.NoError(t, err)
			tlsCfg, err := cm.GetNodeClientTLSConfig()

			// manager closed: tls: either ServerName or InsecureSkipVerify must be
			// specified in the tls.Config
			// storj.io/drpc/drpcmanager.(*Manager).manageReader:234
			tlsCfg.InsecureSkipVerify = true
			tlsConn := tls.Client(rawconn, tlsCfg)
			conn = drpcconn.New(tlsConn)
		} else {
			conn = drpcconn.New(rawconn)
		}
		defer func() { require.NoError(t, conn.Close()) }()

		desc := tc.LookupRangeOrFatal(t, tc.ScratchRange(t))

		client := kvpb.NewDRPCDRPCBatchServiceClient(conn)
		ba := &kvpb.BatchRequest{}
		ba.RangeID = desc.RangeID
		var ok bool
		ba.Replica, ok = desc.GetReplicaDescriptor(1)
		require.True(t, ok)
		req := &kvpb.LeaseInfoRequest{}
		req.Key = desc.StartKey.AsRawKey()
		ba.Add(req)
		resp, err := client.Batch(ctx, ba)
		require.NoError(t, err)
		t.Logf("%+v", resp)
	})
}
