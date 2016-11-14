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
	"os"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
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

	const numNodes = 5
	tc := testcluster.StartTestCluster(t, numNodes,
		base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
	)
	defer tc.Stopper().Stop()

	const newRanges = 5
	for i := 0; i < newRanges; i++ {
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

	numRanges := newRanges + server.ExpectedInitialRangeCount()
	numReplicas := numRanges * 3
	const minThreshold = 0.9
	minReplicas := int(math.Floor(minThreshold * (float64(numReplicas) / numNodes)))

	util.SucceedsSoon(t, func() error {
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
