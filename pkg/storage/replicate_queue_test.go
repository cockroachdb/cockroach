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

package storage_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"

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

	for _, server := range tc.Servers {
		st := server.ClusterSettings()
		st.Manual.Store(true)
		storage.EnableStatsBasedRebalancing.Override(&st.SV, false)
	}

	const newRanges = 5
	for i := 0; i < newRanges; i++ {
		tableID := keys.MaxReservedDescID + i + 1
		splitKey := keys.MakeTablePrefix(uint32(tableID))
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

// Test that up-replication only proceeds if there are a good number of
// candidates to up-replicate to. Specifically, we won't up-replicate to an
// even number of replicas unless there is an additional candidate that will
// allow a subsequent up-replication to an odd number.
func TestReplicateQueueUpReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const replicaCount = 3

	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
	)
	defer tc.Stopper().Stop(context.Background())

	testKey := keys.MetaMin
	desc, err := tc.LookupRange(testKey)
	if err != nil {
		t.Fatal(err)
	}

	if len(desc.Replicas) != 1 {
		t.Fatalf("replica count, want 1, current %d", len(desc.Replicas))
	}

	tc.AddServer(t, base.TestServerArgs{})

	testutils.SucceedsSoon(t, func() error {
		// After the initial splits have been performed, all of the resulting ranges
		// should be present in replicate queue purgatory (because we only have a
		// single store in the test and thus replication cannot succeed).
		expected, err := tc.Servers[0].ExpectedInitialRangeCount()
		if err != nil {
			return err
		}

		var store *storage.Store
		_ = tc.Servers[0].Stores().VisitStores(func(s *storage.Store) error {
			store = s
			return nil
		})

		if n := store.ReplicateQueuePurgatoryLength(); expected != n {
			return errors.Errorf("expected %d replicas in purgatory, but found %d", expected, n)
		}
		return nil
	})

	tc.AddServer(t, base.TestServerArgs{})

	// Now wait until the replicas have been up-replicated to the
	// desired number.
	testutils.SucceedsSoon(t, func() error {
		desc, err := tc.LookupRange(testKey)
		if err != nil {
			t.Fatal(err)
		}
		if len(desc.Replicas) != replicaCount {
			return errors.Errorf("replica count, want %d, current %d", replicaCount, len(desc.Replicas))
		}
		return nil
	})

	if err := verifyRangeLog(
		tc.Conns[0], storage.RangeLogEventType_add, storage.ReasonRangeUnderReplicated,
	); err != nil {
		t.Fatal(err)
	}
}

// TestReplicateQueueDownReplicate verifies that the replication queue will
// notice over-replicated ranges and remove replicas from them.
func TestReplicateQueueDownReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const replicaCount = 3

	// The goal of this test is to ensure that down replication occurs correctly
	// using the replicate queue, and to ensure that's the case, the test
	// cluster needs to be kept in auto replication mode.
	tc := testcluster.StartTestCluster(t, replicaCount+2,
		base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
	)
	defer tc.Stopper().Stop(context.Background())

	// Split off a range from the initial range for testing; there are
	// complications if the metadata ranges are moved.
	testKey := roachpb.Key("m")
	if _, _, err := tc.SplitRange(testKey); err != nil {
		t.Fatal(err)
	}

	allowedErrs := strings.Join([]string{
		// If a node is already present, we expect this error.
		"unable to add replica .* which is already present",
		// If a replica for this range was previously present on this store and
		// it has already been removed but has not yet been GCed, this error
		// is expected.
		storage.IntersectingSnapshotMsg,
	}, "|")

	// Up-replicate the new range to all nodes to create redundant replicas.
	// Every time a new replica is added, there's a very good chance that
	// another one is removed. So all the replicas can't be added at once and
	// instead need to be added one at a time ensuring that the replica did
	// indeed make it to the desired target.
	for _, server := range tc.Servers {
		nodeID := server.NodeID()
		// If this is not wrapped in a SucceedsSoon, then other temporary
		// failures unlike the ones listed below, such as rejected reservations
		// can cause the test to fail. When encountering those failures, a
		// retry is in order.
		testutils.SucceedsSoon(t, func() error {
			_, err := tc.AddReplicas(testKey, roachpb.ReplicationTarget{
				NodeID:  nodeID,
				StoreID: server.GetFirstStoreID(),
			})
			if testutils.IsError(err, allowedErrs) {
				return nil
			}
			return err
		})
	}

	// Now wait until the replicas have been down-replicated back to the
	// desired number.
	testutils.SucceedsSoon(t, func() error {
		desc, err := tc.LookupRange(testKey)
		if err != nil {
			t.Fatal(err)
		}
		if len(desc.Replicas) != replicaCount {
			return errors.Errorf("replica count, want %d, current %d", replicaCount, len(desc.Replicas))
		}
		return nil
	})

	if err := verifyRangeLog(
		tc.Conns[0], storage.RangeLogEventType_remove, storage.ReasonRangeOverReplicated,
	); err != nil {
		t.Fatal(err)
	}
}

func verifyRangeLog(
	conn *gosql.DB, eventType storage.RangeLogEventType, reason storage.RangeLogEventReason,
) error {
	rows, err := conn.Query(
		`SELECT info FROM system.public.rangelog WHERE "eventType" = $1;`, eventType.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	var numEntries int
	for rows.Next() {
		numEntries++
		var infoStr string
		if err := rows.Scan(&infoStr); err != nil {
			return err
		}
		var info storage.RangeLogEvent_Info
		if err := json.Unmarshal([]byte(infoStr), &info); err != nil {
			return errors.Errorf("error unmarshalling info string %q: %s", infoStr, err)
		}
		if a, e := info.Reason, reason; a != e {
			return errors.Errorf("expected range log event reason %s, got %s from info %v", e, a, info)
		}
		if info.Details == "" {
			return errors.Errorf("got empty range log event details: %v", info)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if numEntries == 0 {
		return errors.New("no range log entries found for up-replication events")
	}
	return nil
}
