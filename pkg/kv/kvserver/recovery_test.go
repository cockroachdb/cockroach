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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRecoverRangeFromReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 2, args)
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	desc, err := tc.AddReplicas(k, tc.Target(1))
	require.NoError(t, err)
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(1)))

	s := tc.Server(0)
	require.NoError(t, s.DB().Put(ctx, k, "bar"))

	tc.StopServer(1)

	cCtx, cancel := context.WithTimeout(ctx, 10000*time.Millisecond)
	defer cancel()
	require.Error(t, s.DB().Put(cCtx, k, "baz"))
	require.Equal(t, context.DeadlineExceeded, cCtx.Err())

	for i := 0; i < 5; i++ {
		_, err := s.Node().(*server.Node).UnsafeHealRange(ctx, &roachpb.UnsafeHealRangeRequest{RangeID: desc.RangeID})
		require.NoError(t, err)
	}

	require.NoError(t, s.DB().CPut(ctx, k, "bay", roachpb.MakeValueFromString("bar").TagAndDataBytes()))
}
