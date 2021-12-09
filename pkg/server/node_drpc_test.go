// Copyright 2021 The Cockroach Authors.
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
	"net"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"storj.io/drpc/drpcconn"
)

func TestDRPC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var args base.TestClusterArgs
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)

	h, p, err := net.SplitHostPort(tc.Server(0).RPCAddr())
	require.NoError(t, err)
	pi, err := strconv.Atoi(p)
	require.NoError(t, err)
	pi += 100
	addr := net.JoinHostPort(h, strconv.Itoa(pi))

	rawconn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	// convert the net.Conn to a drpc.Conn
	conn := drpcconn.New(rawconn)
	defer conn.Close()

	c := roachpb.NewDRPCInternalClient(conn)
	br, err := c.Batch(ctx, &roachpb.BatchRequest{})
	t.Log(br)
	t.Log(err)
}
