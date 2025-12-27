// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestProblemRangesResponse tests that the ProblemRanges endpoint returns
// a valid response. In a healthy single-node test cluster, we expect no
// problem ranges.
func TestProblemRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	ts := srv.SystemLayer()
	client := ts.GetStatusClient(t)
	nodeID := srv.NodeID()

	t.Run("returns valid response", func(t *testing.T) {
		resp, err := client.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
		require.NoError(t, err)

		// In a healthy single-node cluster, we should have a response for node 1.
		require.Contains(t, resp.ProblemsByNodeID, nodeID)

		nodeProblems := resp.ProblemsByNodeID[nodeID]
		// No error should be reported for the local node.
		require.Empty(t, nodeProblems.ErrorMessage)

		// In a healthy cluster, we expect no problem ranges.
		require.Empty(t, nodeProblems.UnavailableRangeIDs, "expected no unavailable ranges")
		require.Empty(t, nodeProblems.UnderreplicatedRangeIDs, "expected no underreplicated ranges")
		require.Empty(t, nodeProblems.OverreplicatedRangeIDs, "expected no overreplicated ranges")
		require.Empty(t, nodeProblems.NoRaftLeaderRangeIDs, "expected no ranges without raft leader")
		require.Empty(t, nodeProblems.NoLeaseRangeIDs, "expected no ranges without lease")
		require.Empty(t, nodeProblems.CircuitBreakerErrorRangeIDs, "expected no ranges with circuit breaker errors")
	})

	t.Run("returns valid response for specific node", func(t *testing.T) {
		resp, err := client.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{
			NodeID: "local",
		})
		require.NoError(t, err)

		// Should only have one node in the response.
		require.Len(t, resp.ProblemsByNodeID, 1)
		require.Contains(t, resp.ProblemsByNodeID, nodeID)
	})
}

// TestProblemRangesLocalResponse tests that the ProblemRangesLocal endpoint
// returns a valid response for the local node only.
func TestProblemRangesLocalResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	ts := srv.SystemLayer()
	client := ts.GetStatusClient(t)

	resp, err := client.ProblemRangesLocal(ctx, &serverpb.ProblemRangesLocalRequest{})
	require.NoError(t, err)

	// No error should be reported.
	require.Empty(t, resp.ErrorMessage)

	// In a healthy cluster, we expect no problem ranges.
	require.Empty(t, resp.UnavailableRangeIDs, "expected no unavailable ranges")
	require.Empty(t, resp.UnderreplicatedRangeIDs, "expected no underreplicated ranges")
	require.Empty(t, resp.OverreplicatedRangeIDs, "expected no overreplicated ranges")
	require.Empty(t, resp.NoRaftLeaderRangeIDs, "expected no ranges without raft leader")
	require.Empty(t, resp.NoLeaseRangeIDs, "expected no ranges without lease")
	require.Empty(t, resp.CircuitBreakerErrorRangeIDs, "expected no ranges with circuit breaker errors")
}

// TestProblemRangesWithUnderreplicatedRanges tests that the ProblemRanges
// endpoint correctly reports underreplicated ranges when a node is stopped.
func TestProblemRangesWithUnderreplicatedRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // multi-node cluster is slow under race

	ctx := context.Background()

	// Start a 3-node cluster.
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Create a table and wait for it to replicate.
	db := tc.ServerConn(0)
	_, err := db.Exec("CREATE TABLE test_table (id INT PRIMARY KEY)")
	require.NoError(t, err)

	// Configure the table to have 3 replicas.
	_, err = db.Exec("ALTER TABLE test_table CONFIGURE ZONE USING num_replicas = 3")
	require.NoError(t, err)

	// Wait for the table's range to be fully replicated.
	testutils.SucceedsSoon(t, func() error {
		var count int
		err := db.QueryRow(
			"SELECT count(*) FROM [SHOW RANGES FROM TABLE test_table] WHERE array_length(replicas, 1) = 3",
		).Scan(&count)
		if err != nil {
			return err
		}
		if count == 0 {
			return errors.New("waiting for table range to have 3 replicas")
		}
		return nil
	})

	// Stop node 2 to create underreplicated ranges.
	tc.StopServer(2)

	// Get the status client from a running node.
	client := tc.Server(0).SystemLayer().GetStatusClient(t)

	// Wait for the underreplicated ranges to be detected.
	var underreplicatedRangeIDs []roachpb.RangeID
	testutils.SucceedsSoon(t, func() error {
		resp, err := client.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
		if err != nil {
			return err
		}

		// Collect underreplicated ranges from all nodes.
		underreplicatedRangeIDs = nil
		for _, nodeProblems := range resp.ProblemsByNodeID {
			underreplicatedRangeIDs = append(underreplicatedRangeIDs, nodeProblems.UnderreplicatedRangeIDs...)
		}

		if len(underreplicatedRangeIDs) == 0 {
			return errors.New("waiting for underreplicated ranges to be detected")
		}
		return nil
	})

	require.NotEmpty(t, underreplicatedRangeIDs, "expected underreplicated ranges after stopping a node")
	t.Logf("found %d underreplicated range(s)", len(underreplicatedRangeIDs))
}

// TestProblemRangesLocalWithUnderreplicatedRanges tests that the
// ProblemRangesLocal endpoint correctly reports underreplicated ranges.
func TestProblemRangesLocalWithUnderreplicatedRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // multi-node cluster is slow under race

	ctx := context.Background()

	// Start a 3-node cluster.
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Create a table and wait for it to replicate.
	db := tc.ServerConn(0)
	_, err := db.Exec("CREATE TABLE test_table (id INT PRIMARY KEY)")
	require.NoError(t, err)

	// Configure the table to have 3 replicas.
	_, err = db.Exec("ALTER TABLE test_table CONFIGURE ZONE USING num_replicas = 3")
	require.NoError(t, err)

	// Wait for the table's range to be fully replicated.
	testutils.SucceedsSoon(t, func() error {
		var count int
		err := db.QueryRow(
			"SELECT count(*) FROM [SHOW RANGES FROM TABLE test_table] WHERE array_length(replicas, 1) = 3",
		).Scan(&count)
		if err != nil {
			return err
		}
		if count == 0 {
			return errors.New("waiting for table range to have 3 replicas")
		}
		return nil
	})

	// Stop node 2 to create underreplicated ranges.
	tc.StopServer(2)

	// Get the status client from a running node.
	client := tc.Server(0).SystemLayer().GetStatusClient(t)

	// Wait for the underreplicated ranges to be detected via ProblemRangesLocal.
	testutils.SucceedsSoon(t, func() error {
		resp, err := client.ProblemRangesLocal(ctx, &serverpb.ProblemRangesLocalRequest{})
		if err != nil {
			return err
		}

		if len(resp.UnderreplicatedRangeIDs) == 0 {
			return errors.New("waiting for underreplicated ranges to be detected")
		}
		return nil
	})

	// Final verification.
	resp, err := client.ProblemRangesLocal(ctx, &serverpb.ProblemRangesLocalRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, resp.UnderreplicatedRangeIDs, "expected underreplicated ranges after stopping a node")
	t.Logf("found %d underreplicated range(s) via ProblemRangesLocal", len(resp.UnderreplicatedRangeIDs))
}
