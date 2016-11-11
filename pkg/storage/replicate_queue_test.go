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
	"os"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func TestReplicateQueueRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set the gossip stores interval lower to speed up rebalancing. With the
	// default of 5s we have to wait ~5s for the rebalancing to start.
	if err := os.Setenv("COCKROACH_GOSSIP_STORES_INTERVAL", "100ms"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Unsetenv("COCKROACH_GOSSIP_STORES_INTERVAL"); err != nil {
			t.Fatal(err)
		}
	}()

	// TODO(peter): Bump this to 10 nodes. Doing so is flaky until we have lease
	// rebalancing because store 1 can hold on to too many replicas. Consider:
	//
	//   [15 4 2 3 3 5 5 0 5 5]
	//
	// Store 1 is holding all of the leases so we can't rebalance away from
	// it. Every other store has within the ceil(average-replicas) threshold. So
	// there are no rebalancing opportunities for store 8.
	tc := testcluster.StartTestCluster(t, 5,
		base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
	)
	defer tc.Stopper().Stop()

	// Create a handful of ranges. Along with the initial ranges in the cluster,
	// this will result in 15 total ranges and 45 total replicas. Spread across
	// the 10 nodes in the cluster the average is 4.5 replicas per node. Note
	// that we don't expect to achieve that perfect balance as rebalancing
	// targets a threshold around the average.
	for i := 0; i < 10; i++ {
		tableID := keys.MaxReservedDescID + i + 1
		splitKey := keys.MakeRowSentinelKey(keys.MakeTablePrefix(uint32(tableID)))
		for {
			if _, _, err := tc.SplitRange(splitKey); err != nil {
				if testutils.IsError(err, "split at key .* failed: conflict updating range descriptors") ||
					testutils.IsError(err, "range is already split at key") {
					continue
				}
				t.Fatal(err)
			}
			break
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

	util.SucceedsSoon(t, func() error {
		counts := countReplicas()
		for _, c := range counts {
			// TODO(peter): This is a weak check for rebalancing. When lease
			// rebalancing is in place we can make this somewhat more robust.
			if c == 0 {
				err := errors.Errorf("not balanced: %d", counts)
				log.Info(context.Background(), err)
				return err
			}
		}
		return nil
	})
}

// TestReplicateQueueDownReplicateverifies that the replication queue will notice
// over-replicated ranges and remove replicas from them.
func TestReplicateQueueDownReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const replicaCount = 3

	tc := testcluster.StartTestCluster(t, replicaCount+2,
		base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
	)
	defer tc.Stopper().Stop()

	// Split off a range from the initial range for testing; there are
	// complications if the metadata ranges are up-replicated.
	testKey := roachpb.Key("m")
	for {
		if _, _, err := tc.SplitRange(testKey); err != nil {
			if testutils.IsError(err, "split at key .* failed: conflict updating range descriptors") {
				continue
			}
			t.Fatal(err)
		}
		break
	}

	desc, err := tc.LookupRange(testKey)
	if err != nil {
		t.Fatal(err)
	}
	rangeID := desc.RangeID

	countReplicas := func() int {
		count := 0
		for _, s := range tc.Servers {
			if err := s.Stores().VisitStores(func(s *storage.Store) error {
				return storage.IterateRangeDescriptors(context.TODO(), s.Engine(), func(desc roachpb.RangeDescriptor) (bool, error) {
					if desc.RangeID == rangeID {
						count++
					}
					return false, nil
				})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		return count
	}

	// Ensure that the new range is fully replicated.
	util.SucceedsSoon(t, func() error {
		if c := countReplicas(); c != replicaCount {
			return errors.Errorf("replica count = %d", c)
		}
		return nil
	})

	// Up-replicate the new range to all servers to create redundant replicas.
	util.SucceedsSoon(t, func() error {
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
	util.SucceedsSoon(t, func() error {
		if c := countReplicas(); c != replicaCount {
			return errors.Errorf("replica count = %d", c)
		}
		return nil
	})
}
