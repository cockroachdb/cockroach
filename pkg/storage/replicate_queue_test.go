// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
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

	initialRanges, err := server.ExpectedInitialRangeCount(tc.Servers[0].DB(), config.DefaultZoneConfigRef(), config.DefaultSystemZoneConfigRef())
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

	if len(desc.InternalReplicas) != 1 {
		t.Fatalf("replica count, want 1, current %d", len(desc.InternalReplicas))
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
		if len(desc.InternalReplicas) != replicaCount {
			return errors.Errorf("replica count, want %d, current %d", replicaCount, len(desc.InternalReplicas))
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
		if len(desc.InternalReplicas) != replicaCount {
			return errors.Errorf("replica count, want %d, current %d", replicaCount, len(desc.InternalReplicas))
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

func toggleReplicationQueues(tc *testcluster.TestCluster, active bool) {
	for _, s := range tc.Servers {
		_ = s.Stores().VisitStores(func(store *storage.Store) error {
			store.SetReplicateQueueActive(active)
			return nil
		})
	}
}

func toggleSplitQueues(tc *testcluster.TestCluster, active bool) {
	for _, s := range tc.Servers {
		_ = s.Stores().VisitStores(func(store *storage.Store) error {
			store.SetSplitQueueActive(active)
			return nil
		})
	}
}

// Test that ranges larger than range_max_bytes that can't be split can still be
// processed by the replication queue (in particular, up-replicated).
func TestLargeUnsplittableRangeReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag - #38565")
	}
	ctx := context.Background()

	// Create a cluster with really small ranges.
	const rangeMaxSize = base.MinRangeMaxBytes
	zcfg := config.DefaultZoneConfig()
	zcfg.RangeMinBytes = proto.Int64(rangeMaxSize / 2)
	zcfg.RangeMaxBytes = proto.Int64(rangeMaxSize)
	tc := testcluster.StartTestCluster(t, 5,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
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
			err := s.Stores().VisitStores(func(store *storage.Store) error {
				return store.ForceReplicationScanAndProcess()
			})
			require.NoError(t, err)
		}
	}

	// Wait until the smaller range (the 2nd) has up-replicated.
	testutils.SucceedsSoon(t, func() error {
		forceProcess()
		r := db.QueryRow(
			"select replicas from [show experimental_ranges from table t] where start_key='/2'")
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
			"select replicas from [show experimental_ranges from table t] where start_key is null")
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
