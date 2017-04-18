// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis

package storage_test

import (
	"math"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestReplicateQueueRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#12943")

	if testing.Short() {
		t.Skip("short flag")
	}

	// Set the gossip stores interval lower to speed up rebalancing. With the
	// default of 5s we have to wait ~5s for the rebalancing to start.
	defer func(v time.Duration) {
		gossip.GossipStoresInterval = v
	}(gossip.GossipStoresInterval)
	gossip.GossipStoresInterval = 100 * time.Millisecond

	const numNodes = 5
	tc := testcluster.StartTestCluster(t, numNodes,
		base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
	)
	defer tc.Stopper().Stop(context.TODO())

	const newRanges = 5
	for i := 0; i < newRanges; i++ {
		tableID := keys.MaxReservedDescID + i + 1
		splitKey := keys.MakeRowSentinelKey(keys.MakeTablePrefix(uint32(tableID)))
		if _, _, err := tc.SplitRange(splitKey); err != nil {
			t.Fatal(err)
		}
	}

	countReplicas := func() []int {
		counts := make([]int, len(tc.Servers))
		for _, s := range tc.Servers {
			err := s.Stores().VisitStores(func(s *storage.Store) error {
				counts[s.StoreID()-1] += s.ReplicaCount()
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		return counts
	}

	initialRanges, err := server.ExpectedInitialRangeCount(tc.Servers[0].DB())
	if err != nil {
		t.Fatal(err)
	}
	numRanges := newRanges + initialRanges
	numReplicas := numRanges * 3
	const minThreshold = 0.9
	minReplicas := int(math.Floor(minThreshold * (float64(numReplicas) / numNodes)))

	testutils.SucceedsSoon(t, func() error {
		counts := countReplicas()
		for _, c := range counts {
			if c < minReplicas {
				err := errors.Errorf("not balanced: %d", counts)
				log.Info(context.Background(), err)
				return err
			}
		}
		return nil
	})
}

// TestReplicateQueueDownReplicate verifies that the replication queue will notice
// over-replicated ranges and remove replicas from them.
func TestReplicateQueueDownReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#12943")

	const replicaCount = 3

	tc := testcluster.StartTestCluster(t, replicaCount+2,
		base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
	)
	defer tc.Stopper().Stop(context.TODO())

	// Split off a range from the initial range for testing; there are
	// complications if the metadata ranges are moved.
	testKey := roachpb.Key("m")
	if _, _, err := tc.SplitRange(testKey); err != nil {
		t.Fatal(err)
	}

	desc, err := tc.LookupRange(testKey)
	if err != nil {
		t.Fatal(err)
	}
	rangeID := desc.RangeID

	countReplicas := func() int {
		count := 0
		for _, s := range tc.Servers {
			if err := s.Stores().VisitStores(func(store *storage.Store) error {
				if _, err := store.GetReplica(rangeID); err == nil {
					count++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		return count
	}

	// Up-replicate the new range to all servers to create redundant replicas.
	// Add replicas to all of the nodes. Only 2 of these calls will succeed
	// because the range is already replicated to the other 3 nodes.
	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			_, err := tc.AddReplicas(testKey, tc.Target(i))
			if err != nil {
				if testutils.IsError(err, "unable to add replica .* which is already present") {
					continue
				}
				return err
			}
		}
		if c := countReplicas(); c != tc.NumServers() {
			return errors.Errorf("replica count = %d", c)
		}
		return nil
	})

	// Ensure that the replicas for the new range down replicate.
	testutils.SucceedsSoon(t, func() error {
		if c := countReplicas(); c != replicaCount {
			return errors.Errorf("replica count = %d", c)
		}
		return nil
	})
}
