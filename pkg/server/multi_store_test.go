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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

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
	clusterID := tc.Server(0).ClusterID()
	for _, srv := range tc.Servers {
		require.Equal(t, clusterID, srv.ClusterID())
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
				return errors.Errorf("failed to visit all nodes, got %v", err)
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
