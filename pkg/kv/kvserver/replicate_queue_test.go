// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

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
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/tracker"
)

func TestReplicateQueueRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// This test was seen taking north of 20m under race.
		t.Skip("too heavyweight for race")
	}

	testutils.RunTrueAndFalse(t, "atomic", func(t *testing.T, atomic bool) {
		testReplicateQueueRebalanceInner(t, atomic)
	})
}

func testReplicateQueueRebalanceInner(t *testing.T, atomic bool) {
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
			},
		},
	)
	defer tc.Stopper().Stop(context.Background())

	for _, server := range tc.Servers {
		st := server.ClusterSettings()
		st.Manual.Store(true)
		kvserver.LoadBasedRebalancingMode.Override(&st.SV, int64(kvserver.LBRebalancingOff))
		// NB: usually it's preferred to set the cluster settings, but this is less
		// boilerplate than setting it and then waiting for all nodes to have it.
		kvserver.UseAtomicReplicationChanges.Override(&st.SV, atomic)
	}

	const newRanges = 10
	trackedRanges := map[roachpb.RangeID]struct{}{}
	for i := 0; i < newRanges; i++ {
		tableID := keys.MinUserDescID + i
		splitKey := keys.SystemSQLCodec.TablePrefix(uint32(tableID))
		// Retry the splits on descriptor errors which are likely as the replicate
		// queue is already hard at work.
		testutils.SucceedsSoon(t, func() error {
			desc := tc.LookupRangeOrFatal(t, splitKey)
			if i > 0 && len(desc.Replicas().Voters()) > 3 {
				// Some system ranges have five replicas but user ranges only three,
				// so we'll see downreplications early in the startup process which
				// we want to ignore. Delay the splits so that we don't create
				// more over-replicated ranges.
				// We don't do this for i=0 since that range stays at five replicas.
				return errors.Errorf("still downreplicating: %s", &desc)
			}
			_, rightDesc, err := tc.SplitRange(splitKey)
			if err != nil {
				return err
			}
			t.Logf("split off %s", &rightDesc)
			if i > 0 {
				trackedRanges[rightDesc.RangeID] = struct{}{}
			}
			return nil
		})
	}

	countReplicas := func() []int {
		counts := make([]int, len(tc.Servers))
		for _, s := range tc.Servers {
			err := s.Stores().VisitStores(func(s *kvserver.Store) error {
				counts[s.StoreID()-1] += s.ReplicaCount()
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		return counts
	}

	initialRanges, err := server.ExpectedInitialRangeCount(tc.Servers[0].DB(), zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
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
				log.Infof(context.Background(), "%v", err)
				return err
			}
		}
		return nil
	})

	// Query the range log to see if anything unexpected happened. Concretely,
	// we'll make sure that our tracked ranges never had >3 replicas.
	infos, err := queryRangeLog(tc.Conns[0], `SELECT info FROM system.rangelog ORDER BY timestamp DESC`)
	require.NoError(t, err)
	for _, info := range infos {
		if _, ok := trackedRanges[info.UpdatedDesc.RangeID]; !ok || len(info.UpdatedDesc.Replicas().Voters()) <= 3 {
			continue
		}
		// If we have atomic changes enabled, we expect to never see four replicas
		// on our tracked ranges. If we don't have atomic changes, we can't avoid
		// it.
		if atomic {
			t.Error(info)
		} else {
			t.Log(info)
		}
	}
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

	if len(desc.InternalReplicas) != 1 {
		t.Fatalf("replica count, want 1, current %d", len(desc.InternalReplicas))
	}

	tc.AddServer(t, base.TestServerArgs{})

	if err := tc.Servers[0].Stores().VisitStores(func(s *kvserver.Store) error {
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

	var store *kvserver.Store
	_ = tc.Servers[0].Stores().VisitStores(func(s *kvserver.Store) error {
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
		if len(desc.InternalReplicas) != replicaCount {
			return errors.Errorf("replica count, want %d, current %d", replicaCount, len(desc.InternalReplicas))
		}
		return nil
	})

	infos, err := filterRangeLog(
		tc.Conns[0], kvserverpb.RangeLogEventType_add, kvserverpb.ReasonRangeUnderReplicated,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) < 1 {
		t.Fatalf("found no upreplication due to underreplication in the range logs")
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
		kvserver.IntersectingSnapshotMsg,
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
		if len(desc.InternalReplicas) != replicaCount {
			return errors.Errorf("replica count, want %d, current %d", replicaCount, len(desc.InternalReplicas))
		}
		return nil
	})

	infos, err := filterRangeLog(
		tc.Conns[0], kvserverpb.RangeLogEventType_remove, kvserverpb.ReasonRangeOverReplicated,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) < 1 {
		t.Fatalf("found no downreplication due to over-replication in the range logs")
	}
}

// queryRangeLog queries the range log. The query must be of type:
// `SELECT info from system.rangelog ...`.
func queryRangeLog(
	conn *gosql.DB, query string, args ...interface{},
) ([]kvserverpb.RangeLogEvent_Info, error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, err
	}

	var sl []kvserverpb.RangeLogEvent_Info
	defer rows.Close()
	var numEntries int
	for rows.Next() {
		numEntries++
		var infoStr string
		if err := rows.Scan(&infoStr); err != nil {
			return nil, err
		}
		var info kvserverpb.RangeLogEvent_Info
		if err := json.Unmarshal([]byte(infoStr), &info); err != nil {
			return nil, errors.Errorf("error unmarshaling info string %q: %s", infoStr, err)
		}
		sl = append(sl, info)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sl, nil
}

func filterRangeLog(
	conn *gosql.DB, eventType kvserverpb.RangeLogEventType, reason kvserverpb.RangeLogEventReason,
) ([]kvserverpb.RangeLogEvent_Info, error) {
	return queryRangeLog(conn, `SELECT info FROM system.rangelog WHERE "eventType" = $1 AND info LIKE concat('%', $2, '%');`, eventType.String(), reason)
}

func toggleReplicationQueues(tc *testcluster.TestCluster, active bool) {
	for _, s := range tc.Servers {
		_ = s.Stores().VisitStores(func(store *kvserver.Store) error {
			store.SetReplicateQueueActive(active)
			return nil
		})
	}
}

func toggleSplitQueues(tc *testcluster.TestCluster, active bool) {
	for _, s := range tc.Servers {
		_ = s.Stores().VisitStores(func(store *kvserver.Store) error {
			store.SetSplitQueueActive(active)
			return nil
		})
	}
}

// Test that ranges larger than range_max_bytes that can't be split can still be
// processed by the replication queue (in particular, up-replicated).
func TestLargeUnsplittableRangeReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() || testutils.NightlyStress() || util.RaceEnabled {
		t.Skip("https://github.com/cockroachdb/cockroach/issues/38565")
	}
	ctx := context.Background()

	// Create a cluster with really small ranges.
	const rangeMaxSize = base.MinRangeMaxBytes
	zcfg := zonepb.DefaultZoneConfig()
	zcfg.RangeMinBytes = proto.Int64(rangeMaxSize / 2)
	zcfg.RangeMaxBytes = proto.Int64(rangeMaxSize)
	tc := testcluster.StartTestCluster(t, 5,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				ScanMinIdleTime: time.Millisecond,
				ScanMaxIdleTime: time.Millisecond,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DefaultZoneConfigOverride: &zcfg,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)

	// We're going to create a table with a big row and a small row. We'll split
	// the table in between the rows, to produce a large range and a small one.
	// Then we'll increase the replication factor to 5 and check that both ranges
	// behave the same - i.e. they both get up-replicated. For the purposes of
	// this test we're only worried about the large one up-replicating, but we
	// test the small one as a control so that we don't fool ourselves.

	// Disable the queues so they don't mess with our manual relocation. We'll
	// re-enable them later.
	toggleReplicationQueues(tc, false /* active */)
	toggleSplitQueues(tc, false /* active */)

	db := tc.Conns[0]
	_, err := db.Exec("create table t (i int primary key, s string)")
	require.NoError(t, err)

	_, err = db.Exec(`ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE t SPLIT AT VALUES (2)`)
	require.NoError(t, err)

	toggleReplicationQueues(tc, true /* active */)
	toggleSplitQueues(tc, true /* active */)

	// We're going to create a row that's larger than range_max_bytes, but not
	// large enough that write back-pressuring kicks in and refuses it.
	var sb strings.Builder
	for i := 0; i < 1.5*rangeMaxSize; i++ {
		sb.WriteRune('a')
	}
	_, err = db.Exec("insert into t(i,s) values (1, $1)", sb.String())
	require.NoError(t, err)
	_, err = db.Exec("insert into t(i,s) values (2, 'b')")
	require.NoError(t, err)

	// Now ask everybody to up-replicate.
	_, err = db.Exec("alter table t configure zone using num_replicas = 5")
	require.NoError(t, err)

	forceProcess := func() {
		// Speed up the queue processing.
		for _, s := range tc.Servers {
			err := s.Stores().VisitStores(func(store *kvserver.Store) error {
				return store.ForceReplicationScanAndProcess()
			})
			require.NoError(t, err)
		}
	}

	// Wait until the smaller range (the 2nd) has up-replicated.
	testutils.SucceedsSoon(t, func() error {
		forceProcess()
		r := db.QueryRow(
			"select replicas from [show ranges from table t] where start_key='/2'")
		var repl string
		if err := r.Scan(&repl); err != nil {
			return err
		}
		if repl != "{1,2,3,4,5}" {
			return fmt.Errorf("not up-replicated yet. replicas: %s", repl)
		}
		return nil
	})

	// Now check that the large range also gets up-replicated.
	testutils.SucceedsSoon(t, func() error {
		forceProcess()
		r := db.QueryRow(
			"select replicas from [show ranges from table t] where start_key is null")
		var repl string
		if err := r.Scan(&repl); err != nil {
			return err
		}
		if repl != "{1,2,3,4,5}" {
			return fmt.Errorf("not up-replicated yet")
		}
		return nil
	})
}

type delayingRaftMessageHandler struct {
	kvserver.RaftMessageHandler
	leaseHolderNodeID uint64
	rangeID           roachpb.RangeID
}

const (
	queryInterval = 10 * time.Millisecond
	raftDelay     = 175 * time.Millisecond
)

func (h delayingRaftMessageHandler) HandleRaftRequest(
	ctx context.Context,
	req *kvserver.RaftMessageRequest,
	respStream kvserver.RaftMessageResponseStream,
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
			"[show ranges from table system.comments] limit 1",
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

	var remoteRepl *kvserver.Replica
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
		if progress.State == tracker.StateReplicate &&
			(status.Commit-progress.Match) > 0 {
			break
		}
		time.Sleep(13 * time.Millisecond)
	}

	// Set the zone preference for the replica to show that it has to be moved
	// to the remote node.
	desc, zone := leaseHolderRepl.DescAndZone()
	newZone := *zone
	newZone.LeasePreferences = []zonepb.LeasePreference{
		{
			Constraints: []zonepb.Constraint{
				{
					Type:  zonepb.Constraint_REQUIRED,
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
