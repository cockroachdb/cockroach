// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

// TestAddNewStoresToExistingNodes tests database behavior with
// multiple stores per node, in particular when new stores are
// added while nodes are shut down. This test starts a cluster with
// three nodes, shuts down all nodes and adds a store to each node,
// and ensures nodes start back up successfully. See #39415.
func TestAddNewStoresToExistingNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Nine stores is a lot of goroutines.
	skip.UnderStress(t, "too many new stores and nodes for stress")
	skip.UnderRace(t, "too many new stores and nodes for race")
	skip.UnderDeadlock(t, "too many new stores and nodes for deadlock")

	ctx := context.Background()

	ser := fs.NewStickyRegistry()

	const (
		numNodes                     = 3
		numStoresPerNodeInitially    = 1
		numStoresPerNodeAfterRestart = 3
	)

	mkClusterArgs := func(numNodes, numStoresPerNode int) base.TestClusterArgs {
		tcArgs := base.TestClusterArgs{
			// NB: it's important that this test wait for full replication. Otherwise,
			// with only a single voter on the range that allocates store IDs, it can
			// pass erroneously. StartTestCluster already calls it, but we call it
			// again explicitly.
			ReplicationMode:   base.ReplicationAuto,
			ServerArgsPerNode: map[int]base.TestServerArgs{},
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TODOTestTenantDisabled,
			},
		}
		for srvIdx := 0; srvIdx < numNodes; srvIdx++ {
			serverArgs := base.TestServerArgs{}
			serverArgs.Knobs.Server = &server.TestingKnobs{StickyVFSRegistry: ser}
			for storeIdx := 0; storeIdx < numStoresPerNode; storeIdx++ {
				id := fmt.Sprintf("s%d.%d", srvIdx+1, storeIdx+1)
				serverArgs.StoreSpecs = append(
					serverArgs.StoreSpecs,
					base.StoreSpec{InMemory: true, StickyVFSID: id},
				)
			}
			tcArgs.ServerArgsPerNode[srvIdx] = serverArgs
		}
		return tcArgs
	}

	tc := testcluster.StartTestCluster(t, numNodes, mkClusterArgs(numNodes, numStoresPerNodeInitially))
	clusterID := tc.Server(0).StorageClusterID()
	tc.Stopper().Stop(ctx)

	tcArgs := mkClusterArgs(numNodes, numStoresPerNodeAfterRestart)
	tcArgs.ReplicationMode = base.ReplicationManual // saves time, ok now
	// We need ParallelStart since this is an existing cluster. If
	// we started sequentially, then the first node would hang forever
	// waiting for the KV layer to become available, but that only
	// happens when the second node also starts.
	tcArgs.ParallelStart = true

	// Start all nodes with additional stores.
	tc = testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(ctx)

	// Sanity check that we're testing what we wanted to test and didn't accidentally
	// bootstrap three single-node clusters (who knows).
	for _, srv := range tc.Servers {
		require.Equal(t, clusterID, srv.StorageClusterID())
	}

	// Ensure all nodes have all stores available, and each store has a unique
	// store ID.
	testutils.SucceedsSoon(t, func() error {
		var storeIDs []roachpb.StoreID
		for _, server := range tc.Servers {
			var storeCount = 0
			if err := server.GetStores().(*kvserver.Stores).VisitStores(
				func(s *kvserver.Store) error {
					storeCount++
					storeIDs = append(storeIDs, s.StoreID())
					return nil
				},
			); err != nil {
				return errors.Wrap(err, "failed to visit all nodes")
			}

			if storeCount != 3 {
				return errors.Errorf("expected 3 stores to be available on n%s, got %d stores instead", server.NodeID(), storeCount)
			}
		}

		sort.Slice(storeIDs, func(i, j int) bool {
			return storeIDs[i] < storeIDs[j]
		})
		for i := range storeIDs {
			expStoreID := roachpb.StoreID(i + 1)
			if storeIDs[i] != expStoreID {
				t.Fatalf("expected the %s store to have storeID s%s, found s%s", humanize.Ordinal(i+1), expStoreID, storeIDs[i])
			}
		}

		return nil
	})
}

// TestMultiStoreIDAlloc validates that we don't accidentally re-use or
// skip-over allocated store IDs in multi-store setups.
func TestMultiStoreIDAlloc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "too many new stores and nodes for stress")

	ctx := context.Background()
	numNodes := 3
	numStoresPerNode := 3
	var storeSpecs []base.StoreSpec
	for i := 0; i < numStoresPerNode; i++ {
		storeSpecs = append(storeSpecs, base.StoreSpec{InMemory: true})
	}
	tcArgs := base.TestClusterArgs{
		ParallelStart:   true,
		ReplicationMode: base.ReplicationManual, // saves time
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {StoreSpecs: storeSpecs},
			1: {StoreSpecs: storeSpecs},
			2: {StoreSpecs: storeSpecs},
		},
	}

	tc := testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(ctx)

	// Sanity check that we're testing what we wanted to test and didn't accidentally
	// bootstrap three single-node clusters (who knows).
	clusterID := tc.Server(0).StorageClusterID()
	for _, srv := range tc.Servers {
		require.Equal(t, clusterID, srv.StorageClusterID())
	}

	// Ensure all nodes have all stores available, and each store has a unique
	// store ID.
	testutils.SucceedsSoon(t, func() error {
		var storeIDs []roachpb.StoreID
		for _, server := range tc.Servers {
			var storeCount = 0
			if err := server.GetStores().(*kvserver.Stores).VisitStores(
				func(s *kvserver.Store) error {
					storeCount++
					storeIDs = append(storeIDs, s.StoreID())
					return nil
				},
			); err != nil {
				return errors.Wrap(err, "failed to visit all nodes")
			}

			if storeCount != numStoresPerNode {
				return errors.Errorf("expected %d stores to be available on n%s, got %d stores instead",
					numStoresPerNode, server.NodeID(), storeCount)
			}
		}

		sort.Slice(storeIDs, func(i, j int) bool {
			return storeIDs[i] < storeIDs[j]
		})
		for i := range storeIDs {
			expStoreID := roachpb.StoreID(i + 1)
			if storeIDs[i] != expStoreID {
				t.Fatalf("expected the %s store to have storeID s%s, found s%s", humanize.Ordinal(i+1), expStoreID, storeIDs[i])
			}
		}

		return nil
	})
}
