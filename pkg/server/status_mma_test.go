// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestStatusMMAState verifies that the /_status/mma_state
// endpoint returns a populated snapshot for the local node.
func TestStatusMMAState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	cc := s.GetStatusClient(t)
	for _, nodeID := range []string{"local", "1"} {
		resp, err := cc.MMAState(ctx, &serverpb.MMAStateRequest{
			NodeId: nodeID,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Snapshot)
		// The single test node has at least one store registered with the
		// allocator via the gossip callback in pkg/server/server.go.
		require.NotEmpty(t, resp.Snapshot.Stores)
	}
}
