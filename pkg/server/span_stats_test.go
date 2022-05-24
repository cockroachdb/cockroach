// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestXXX(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	spanStatsServer := s.SpanStatsServer().(serverpb.SpanStatsServer)
	//serverpb.NewSpanStatsClient() // XXX: Figure out where to get a grpcconn from in tests.
	resp, err := spanStatsServer.GetSpanStatistics(ctx, &serverpb.GetSpanStatisticsRequest{})
	require.NoError(t, err)
	_ = resp

	t.Log("sup")
}
