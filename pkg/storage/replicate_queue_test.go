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
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
)

func TestReplicateQueueRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag")
	}

	const numNodes = 5
	tc := testcluster.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				ScanMinIdleTime: time.Millisecond,
				ScanMaxIdleTime: time.Millisecond,
				Knobs: base.TestingKnobs{
					Store: &storage.StoreTestingKnobs{
						// Prevent the merge queue from immediately discarding our splits.
						DisableMergeQueue: true,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(context.TODO())

	for _, server := range tc.Servers {
		st := server.ClusterSettings()
		st.Manual.Store(true)
		storage.LoadBasedRebalancingMode.Override(&st.SV, int64(storage.LBRebalancingOff))
	}

	const newRanges = 5
	for i := 0; i < newRanges; i++ {
		tableID := keys.MinUserDescID + i
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
				err := errors.Errorf(
					"not balanced (want at least %d replicas on all stores): %d", minReplicas, counts)
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

	if err := tc.Servers[0].Stores().VisitStores(func(s *storage.Store) error {
		return s.ForceReplicationScanAndProcess()
	}); err != nil {
		t.Fatal(err)
	}
	// After the initial splits have been performed, all of the resulting ranges
	// should be present in replicate queue purgatory (because we only have a
	// single store in the test and thus replication cannot succeed).
	expected, err := tc.Servers[0].ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}

	var store *storage.Store
	_ = tc.Servers[0].Stores().VisitStores(func(s *storage.Store) error {
		store = s
		return nil
	})

	if n := store.ReplicateQueuePurgatoryLength(); expected != n {
		t.Fatalf("expected %d replicas in purgatory, but found %d", expected, n)
	}

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
		tc.Conns[0], storagepb.RangeLogEventType_add, storagepb.ReasonRangeUnderReplicated,
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
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				ScanMinIdleTime: 10 * time.Millisecond,
				ScanMaxIdleTime: 10 * time.Millisecond,
				Knobs: base.TestingKnobs{
					Store: &storage.StoreTestingKnobs{
						// Prevent the merge queue from immediately discarding our splits.
						DisableMergeQueue: true,
					},
				},
			},
		},
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
		tc.Conns[0], storagepb.RangeLogEventType_remove, storagepb.ReasonRangeOverReplicated,
	); err != nil {
		t.Fatal(err)
	}
}

func verifyRangeLog(
	conn *gosql.DB, eventType storagepb.RangeLogEventType, reason storagepb.RangeLogEventReason,
) error {
	rows, err := conn.Query(
		"SELECT info FROM system.rangelog WHERE \"eventType\" = $1;", eventType.String())
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
		var info storagepb.RangeLogEvent_Info
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

type delayingRaftMessageHandler struct {
	storage.RaftMessageHandler
	leaseHolderNodeID uint64
	rangeID           roachpb.RangeID
}

const (
	queryInterval = 10 * time.Millisecond
	raftDelay     = 175 * time.Millisecond
)

func (h delayingRaftMessageHandler) HandleRaftRequest(
	ctx context.Context,
	req *storage.RaftMessageRequest,
	respStream storage.RaftMessageResponseStream,
) *roachpb.Error {
	if h.rangeID != req.RangeID {
		return h.RaftMessageHandler.HandleRaftRequest(ctx, req, respStream)
	}
	go func() {
		time.Sleep(raftDelay)
		err := h.RaftMessageHandler.HandleRaftRequest(ctx, req, respStream)
		if err != nil {
			log.Infof(ctx, "HandleRaftRequest returned err %s", err)
		}
	}()

	return nil
}

func TestTransferLeaseToLaggingNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				ScanMaxIdleTime: time.Millisecond,
				StoreSpecs: []base.StoreSpec{{
					InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{"n1"}},
				}},
			},
			1: {
				ScanMaxIdleTime: time.Millisecond,
				StoreSpecs: []base.StoreSpec{{
					InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{"n2"}},
				}},
			},
			2: {
				ScanMaxIdleTime: time.Millisecond,
				StoreSpecs: []base.StoreSpec{{
					InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{"n3"}},
				}},
			},
		},
	}

	tc := testcluster.StartTestCluster(t,
		len(clusterArgs.ServerArgsPerNode), clusterArgs)
	defer tc.Stopper().Stop(ctx)

	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}

	// Get the system.comments' range and lease holder
	var rangeID roachpb.RangeID
	var leaseHolderNodeID uint64
	s := sqlutils.MakeSQLRunner(tc.Conns[0])
	s.Exec(t, "insert into system.comments values(0,0,0,'abc')")
	s.QueryRow(t,
		"select range_id, lease_holder from "+
			"[show experimental_ranges from table system.comments] limit 1",
	).Scan(&rangeID, &leaseHolderNodeID)
	remoteNodeID := uint64(1)
	if leaseHolderNodeID == 1 {
		remoteNodeID = 2
	}
	log.Infof(ctx, "RangeID %d, RemoteNodeID %d, LeaseHolderNodeID %d",
		rangeID, remoteNodeID, leaseHolderNodeID)
	leaseHolderSrv := tc.Servers[leaseHolderNodeID-1]
	leaseHolderStoreID := leaseHolderSrv.GetFirstStoreID()
	leaseHolderStore, err := leaseHolderSrv.Stores().GetStore(leaseHolderStoreID)
	if err != nil {
		t.Fatal(err)
	}

	// Start delaying Raft messages to the remote node
	remoteSrv := tc.Servers[remoteNodeID-1]
	remoteStoreID := remoteSrv.GetFirstStoreID()
	remoteStore, err := remoteSrv.Stores().GetStore(remoteStoreID)
	if err != nil {
		t.Fatal(err)
	}
	remoteStore.Transport().Listen(
		remoteStoreID,
		delayingRaftMessageHandler{remoteStore, leaseHolderNodeID, rangeID},
	)

	workerReady := make(chan bool)
	// Create persistent range load.
	tc.Stopper().RunWorker(ctx, func(ctx context.Context) {
		s = sqlutils.MakeSQLRunner(tc.Conns[remoteNodeID-1])
		workerReady <- true
		for {
			s.Exec(t, fmt.Sprintf("update system.comments set comment='abc' "+
				"where type=0 and object_id=0 and sub_id=0"))

			select {
			case <-ctx.Done():
				return
			case <-tc.Stopper().ShouldQuiesce():
				return
			case <-time.After(queryInterval):
			}
		}
	})
	<-workerReady
	// Wait until we see remote making progress
	leaseHolderRepl, err := leaseHolderStore.GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}

	var remoteRepl *storage.Replica
	testutils.SucceedsSoon(t, func() error {
		remoteRepl, err = remoteStore.GetReplica(rangeID)
		return err
	})
	testutils.SucceedsSoon(t, func() error {
		status := leaseHolderRepl.RaftStatus()
		progress := status.Progress[uint64(remoteRepl.ReplicaID())]
		if progress.Match > 0 {
			return nil
		}
		return errors.Errorf(
			"remote is not making progress: %+v", progress.Match,
		)
	})

	// Wait until we see the remote replica lagging behind
	for {
		// Ensure that the replica on the remote node is lagging.
		status := leaseHolderRepl.RaftStatus()
		progress := status.Progress[uint64(remoteRepl.ReplicaID())]
		if progress.State == raft.ProgressStateReplicate &&
			(status.Commit-progress.Match) > 0 {
			break
		}
		time.Sleep(13 * time.Millisecond)
	}

	// Set the zone preference for the replica to show that it has to be moved
	// to the remote node.
	desc, zone := leaseHolderRepl.DescAndZone()
	newZone := *zone
	newZone.LeasePreferences = []config.LeasePreference{
		{
			Constraints: []config.Constraint{
				{
					Type:  config.Constraint_REQUIRED,
					Value: fmt.Sprintf("n%d", remoteNodeID),
				},
			},
		},
	}

	// By now the lease holder may have changed.
	testutils.SucceedsSoon(t, func() error {
		leaseBefore, _ := leaseHolderRepl.GetLease()
		log.Infof(ctx, "Lease before transfer %+v\n", leaseBefore)

		if uint64(leaseBefore.Replica.NodeID) == remoteNodeID {
			log.Infof(
				ctx,
				"Lease successfully transferred to desired node %d\n",
				remoteNodeID,
			)
			return nil
		}
		currentSrv := tc.Servers[leaseBefore.Replica.NodeID-1]
		leaseStore, err := currentSrv.Stores().GetStore(currentSrv.GetFirstStoreID())
		if err != nil {
			return err
		}
		leaseRepl, err := leaseStore.GetReplica(rangeID)
		if err != nil {
			return err
		}
		transferred, err := leaseStore.FindTargetAndTransferLease(
			ctx, leaseRepl, desc, &newZone)
		if err != nil {
			return err
		}
		if !transferred {
			return errors.Errorf("unable to transfer")
		}
		return errors.Errorf("Repeat check for correct leaseholder")
	})
}
