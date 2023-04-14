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
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/tracker"
)

func TestReplicateQueueRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test was seen taking north of 20m under race.
	skip.UnderRace(t)
	skip.UnderShort(t)

	const numNodes = 5

	ctx := context.Background()
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
		kvserver.LoadBasedRebalancingMode.Override(ctx, &st.SV, int64(kvserver.LBRebalancingOff))
	}

	const newRanges = 10
	trackedRanges := map[roachpb.RangeID]struct{}{}
	for i := uint32(0); i < newRanges; i++ {
		tableID := bootstrap.TestingUserDescID(i)
		splitKey := keys.SystemSQLCodec.TablePrefix(tableID)
		// Retry the splits on descriptor errors which are likely as the replicate
		// queue is already hard at work.
		testutils.SucceedsSoon(t, func() error {
			desc := tc.LookupRangeOrFatal(t, splitKey)
			if i > 0 && len(desc.Replicas().VoterDescriptors()) > 3 {
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

	initialRanges, err := server.ExpectedInitialRangeCount(
		keys.SystemSQLCodec,
		zonepb.DefaultZoneConfigRef(),
		zonepb.DefaultSystemZoneConfigRef(),
	)
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
				log.Infof(ctx, "%v", err)
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
		if _, ok := trackedRanges[info.UpdatedDesc.RangeID]; !ok || len(info.UpdatedDesc.Replicas().VoterDescriptors()) <= 3 {
			continue
		}
		// If we have atomic changes enabled, we expect to never see four replicas
		// on our tracked ranges. If we don't have atomic changes, we can't avoid
		// it.
		t.Error(info)
	}
}

// TestReplicateQueueRebalanceMultiStore creates a test cluster with and without
// multiple stores, splits some ranges, and then waits until the replicate queue
// rebalances the replicas and leases.
func TestReplicateQueueRebalanceMultiStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)
	skip.UnderShort(t)

	testCases := []struct {
		name          string
		nodes         int
		storesPerNode int
	}{
		{"simple", 5, 1},
		{"multi-store", 4, 2},
	}

	// Speed up the test.
	allocatorimpl.MinLeaseTransferStatsDuration = 1 * time.Millisecond
	allocatorimpl.LeaseRebalanceThreshold = 0.01
	allocatorimpl.LeaseRebalanceThresholdMin = 0.0

	const useDisk = false // for debugging purposes
	spec := func(node int, store int) base.StoreSpec {
		return base.DefaultTestStoreSpec
	}
	if useDisk {
		td, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		t.Logf("store dirs in %s", td)
		spec = func(node int, store int) base.StoreSpec {
			return base.StoreSpec{
				Path: filepath.Join(td, fmt.Sprintf("n%ds%d", node, store)),
				Size: base.SizeSpec{},
			}
		}
		t.Cleanup(func() {
			if t.Failed() {
				return
			}
			_ = os.RemoveAll(td)
		})
	}
	for _, testCase := range testCases {

		t.Run(testCase.name, func(t *testing.T) {
			// Set up a test cluster with multiple stores per node if needed.
			args := base.TestClusterArgs{
				ReplicationMode:   base.ReplicationAuto,
				ServerArgsPerNode: map[int]base.TestServerArgs{},
			}
			for i := 0; i < testCase.nodes; i++ {
				perNode := base.TestServerArgs{
					ScanMinIdleTime: time.Millisecond,
					ScanMaxIdleTime: time.Millisecond,
				}
				perNode.StoreSpecs = make([]base.StoreSpec, testCase.storesPerNode)
				for idx := range perNode.StoreSpecs {
					perNode.StoreSpecs[idx] = spec(i+1, idx+1)
				}
				args.ServerArgsPerNode[i] = perNode
			}
			tc := testcluster.StartTestCluster(t, testCase.nodes,
				args)
			defer tc.Stopper().Stop(context.Background())
			ctx := context.Background()
			for _, server := range tc.Servers {
				st := server.ClusterSettings()
				st.Manual.Store(true)
			}

			// Add a few ranges per store.
			numStores := testCase.nodes * testCase.storesPerNode
			newRanges := numStores * 2
			trackedRanges := map[roachpb.RangeID]struct{}{}
			for i := 0; i < newRanges; i++ {
				tableID := bootstrap.TestingUserDescID(uint32(i))
				splitKey := keys.SystemSQLCodec.TablePrefix(tableID)
				// Retry the splits on descriptor errors which are likely as the replicate
				// queue is already hard at work.
				testutils.SucceedsSoon(t, func() error {
					desc := tc.LookupRangeOrFatal(t, splitKey)
					if i > 0 && len(desc.Replicas().VoterDescriptors()) > 3 {
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

			countReplicas := func() (total int, perStore []int) {
				perStore = make([]int, numStores)
				for _, s := range tc.Servers {
					err := s.Stores().VisitStores(func(s *kvserver.Store) error {
						require.Zero(t, perStore[s.StoreID()-1])
						perStore[s.StoreID()-1] = s.ReplicaCount()
						total += s.ReplicaCount()
						return nil
					})
					require.NoError(t, err)
				}
				return total, perStore
			}
			countLeases := func() (total int, perStore []int) {
				perStore = make([]int, numStores)
				for _, s := range tc.Servers {
					err := s.Stores().VisitStores(func(s *kvserver.Store) error {
						c, err := s.Capacity(ctx, false)
						require.NoError(t, err)
						leases := int(c.LeaseCount)
						require.Zero(t, perStore[s.StoreID()-1])
						perStore[s.StoreID()-1] = leases
						total += leases
						return nil
					})
					require.NoError(t, err)
				}
				return total, perStore
			}

			// The requirement for minimum leases is low because of the following: in
			// the case of 8 stores we create 8*2=16 ranges, we also have another 52
			// ranges in the cluster which brings us up to 68 leases (ranges) total.
			// Each store should have around 68/8=8.5 leases. With a leasesThreshold
			// of 70% the test expects floor(8.5*0.7)=5 leases per store. The
			// allocator should allow up to ceil(8.5)=9 leases per store, meaning,
			// that in the worst case 7 stores can have up to 9 leases each, which
			// leaves us with 68-7*9=5 leases on the 8th store, which is exactly what
			// the test expects (and therefore should not be flaky). Note that without
			// setting LeaseRebalanceThreshold and LeaseRebalanceThresholdMin above we
			// would need more than 100 ranges per store, which will make this test
			// significantly slower (5-10 minutes). Currently this test should succeed
			// within a minute normally.
			const replicasThreshold = 0.9
			const leasesThreshold = 0.7
			testutils.SucceedsWithin(t, func() error {
				totalReplicas, replicasPerStore := countReplicas()
				minReplicas := int(math.Floor(replicasThreshold * (float64(totalReplicas) / float64(numStores))))
				t.Logf("current replica state (want at least %d replicas on all stores): %d", minReplicas, replicasPerStore)
				for _, c := range replicasPerStore {
					if c < minReplicas {
						err := errors.Errorf(
							"not balanced (want at least %d replicas on all stores): %d", minReplicas, replicasPerStore)
						log.Infof(ctx, "%v", err)
						return err
					}
				}
				totalLeases, leasesPerStore := countLeases()
				minLeases := int(math.Floor(leasesThreshold * (float64(totalLeases) / float64(numStores))))
				t.Logf("current lease state (want at least %d leases on all stores): %d", minLeases, leasesPerStore)
				for _, c := range leasesPerStore {
					if c < minLeases {
						err := errors.Errorf(
							"not balanced (want at least %d leases on all stores): %d", minLeases, leasesPerStore)
						log.Infof(ctx, "%v", err)
						return err
					}
				}
				return nil
			}, 4*time.Minute)

			// Query the range log to see if anything unexpected happened. Concretely,
			// we'll make sure that our tracked ranges never had >3 replicas.
			infos, err := queryRangeLog(tc.Conns[0], `SELECT info FROM system.rangelog ORDER BY timestamp DESC`)
			require.NoError(t, err)
			for _, info := range infos {
				if _, ok := trackedRanges[info.UpdatedDesc.RangeID]; !ok || len(info.UpdatedDesc.Replicas().VoterDescriptors()) <= 3 {
					continue
				}
				// If we have atomic changes enabled, we expect to never see four replicas
				// on our tracked ranges. If we don't have atomic changes, we can't avoid
				// it.
				t.Error(info)
			}
		})
	}
}

// TestReplicateQueueUpReplicateOddVoters tests that up-replication only
// proceeds if there are a good number of candidates to up-replicate to.
// Specifically, we won't up-replicate to an even number of replicas unless
// there is an additional candidate that will allow a subsequent up-replication
// to an odd number.
func TestReplicateQueueUpReplicateOddVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRaceWithIssue(t, 57144, "flaky under race")
	defer log.Scope(t).Close(t)
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

	tc.AddAndStartServer(t, base.TestServerArgs{})

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

	tc.AddAndStartServer(t, base.TestServerArgs{})

	// Now wait until the replicas have been up-replicated to the
	// desired number.
	testutils.SucceedsSoon(t, func() error {
		descriptor, err := tc.LookupRange(testKey)
		if err != nil {
			t.Fatal(err)
		}
		if len(descriptor.InternalReplicas) != replicaCount {
			return errors.Errorf("replica count, want %d, current %d", replicaCount, len(desc.InternalReplicas))
		}
		return nil
	})

	infos, err := filterRangeLog(
		tc.Conns[0], desc.RangeID, kvserverpb.RangeLogEventType_add_voter, kvserverpb.ReasonRangeUnderReplicated,
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
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1min under race")

	ctx := context.Background()
	// The goal of this test is to ensure that down replication occurs
	// correctly using the replicate queue, and to ensure that's the case,
	// the test cluster needs to be kept in auto replication mode.
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				ScanMinIdleTime: 10 * time.Millisecond,
				ScanMaxIdleTime: 10 * time.Millisecond,
				Knobs: base.TestingKnobs{
					SpanConfig: &spanconfig.TestingKnobs{
						ConfigureScratchRange: true,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)

	testKey := tc.ScratchRange(t)
	testutils.SucceedsSoon(t, func() error {
		desc := tc.LookupRangeOrFatal(t, testKey)
		if got := len(desc.Replicas().Descriptors()); got != 3 {
			return errors.Newf("expected 3 replicas for scratch range, found %d", got)
		}
		return nil
	})

	_, err := tc.ServerConn(0).Exec(
		`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 1`,
	)
	require.NoError(t, err)

	for _, s := range tc.Servers {
		require.NoError(t, s.Stores().VisitStores(func(s *kvserver.Store) error {
			require.NoError(t, s.ForceReplicationScanAndProcess())
			return nil
		}))
	}

	// Now wait until the replicas have been down-replicated back to the
	// desired number.
	testutils.SucceedsSoon(t, func() error {
		desc := tc.LookupRangeOrFatal(t, testKey)
		if got := len(desc.Replicas().Descriptors()); got != 1 {
			return errors.Errorf("expected 1 replica, found %d", got)
		}
		return nil
	})

	desc := tc.LookupRangeOrFatal(t, testKey)
	infos, err := filterRangeLog(
		tc.Conns[0], desc.RangeID, kvserverpb.RangeLogEventType_remove_voter, kvserverpb.ReasonRangeOverReplicated,
	)
	require.NoError(t, err)
	require.Truef(t, len(infos) >= 1, "found no down replication due to over-replication in the range logs")
}

func scanAndGetNumNonVoters(
	t *testing.T, tc *testcluster.TestCluster, scratchKey roachpb.Key,
) (numNonVoters int) {
	for _, s := range tc.Servers {
		// Nudge internal queues to up/down-replicate our scratch range.
		require.NoError(t, s.Stores().VisitStores(func(s *kvserver.Store) error {
			require.NoError(t, s.ForceSplitScanAndProcess())
			require.NoError(t, s.ForceReplicationScanAndProcess())
			require.NoError(t, s.ForceRaftSnapshotQueueProcess())
			return nil
		}))
	}
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
	row := tc.ServerConn(0).QueryRow(
		`SELECT coalesce(max(array_length(non_voting_replicas, 1)),0) FROM crdb_internal.ranges_no_leases WHERE range_id=$1`,
		scratchRange.GetRangeID())
	require.NoError(t, row.Scan(&numNonVoters))
	return numNonVoters
}

// TestReplicateQueueUpAndDownReplicateNonVoters is an end-to-end test ensuring
// that the replicateQueue will add or remove non-voter(s) to a range based on
// updates to its zone configuration.
func TestReplicateQueueUpAndDownReplicateNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t)
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				// Test fails with the default tenant. Disabling and
				// tracking with #76378.
				DisableDefaultTestTenant: true,
				Knobs: base.TestingKnobs{
					SpanConfig: &spanconfig.TestingKnobs{
						ConfigureScratchRange: true,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(context.Background())

	scratchKey := tc.ScratchRange(t)
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)

	// Since we started the TestCluster with 1 node, that first node should have
	// 1 voting replica.
	require.Len(t, scratchRange.Replicas().VoterDescriptors(), 1)
	// Set up the default zone configs such that every range should have 1 voting
	// replica and 2 non-voting replicas.
	_, err := tc.ServerConn(0).Exec(
		`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 3, num_voters = 1`,
	)
	require.NoError(t, err)

	// Add two new servers and expect that 2 non-voters are added to the range.
	tc.AddAndStartServer(t, base.TestServerArgs{})
	tc.AddAndStartServer(t, base.TestServerArgs{})

	var expectedNonVoterCount = 2
	testutils.SucceedsSoon(t, func() error {
		if found := scanAndGetNumNonVoters(t, tc, scratchKey); found != expectedNonVoterCount {
			return errors.Errorf("expected upreplication to %d non-voters; found %d",
				expectedNonVoterCount, found)
		}
		return nil
	})

	// Now remove all non-voting replicas and expect that the range will
	// down-replicate to having just 1 voting replica.
	_, err = tc.ServerConn(0).Exec(`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 1`)
	require.NoError(t, err)
	expectedNonVoterCount = 0
	testutils.SucceedsSoon(t, func() error {
		if found := scanAndGetNumNonVoters(t, tc, scratchKey); found != expectedNonVoterCount {
			return errors.Errorf("expected downreplication to %d non-voters; found %d",
				expectedNonVoterCount, found)
		}
		return nil
	})
}

func checkReplicaCount(
	ctx context.Context,
	tc *testcluster.TestCluster,
	rangeDesc *roachpb.RangeDescriptor,
	voterCount, nonVoterCount int,
) (bool, error) {
	err := forceScanOnAllReplicationQueues(tc)
	if err != nil {
		log.Infof(ctx, "store.ForceReplicationScanAndProcess() failed with: %s", err)
		return false, err
	}
	*rangeDesc, err = tc.LookupRange(rangeDesc.StartKey.AsRawKey())
	if err != nil {
		return false, err
	}
	if len(rangeDesc.Replicas().VoterDescriptors()) != voterCount {
		return false, nil
	}
	if len(rangeDesc.Replicas().NonVoterDescriptors()) != nonVoterCount {
		return false, nil
	}
	return true, nil
}

// TestReplicateQueueDecommissioningNonVoters is an end-to-end test ensuring
// that the replicateQueue will replace or remove non-voter(s) on
// decommissioning nodes.
func TestReplicateQueueDecommissioningNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes a long time or times out under race")
	skip.UnderDeadlockWithIssue(t, 94383)
	skip.UnderMetamorphicWithIssue(t, 99207)

	ctx := context.Background()

	// Setup a scratch range on a test cluster with 2 non-voters and 1 voter.
	setupFn := func(t *testing.T) (*testcluster.TestCluster, roachpb.RangeDescriptor) {
		tc := testcluster.StartTestCluster(t, 5,
			base.TestClusterArgs{
				ReplicationMode: base.ReplicationAuto,
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							DisableReplicaRebalancing: true,
						},
						SpanConfig: &spanconfig.TestingKnobs{
							ConfigureScratchRange: true,
						},
					},
				},
			},
		)
		_, err := tc.ServerConn(0).Exec(
			`SET CLUSTER SETTING server.failed_reservation_timeout='1ms'`)
		require.NoError(t, err)

		scratchKey := tc.ScratchRange(t)
		scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
		_, err = tc.ServerConn(0).Exec(
			`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 3, num_voters = 1`,
		)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, &scratchRange, 1 /* voterCount */, 2 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			return ok
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
		return tc, scratchRange
	}

	// Check that non-voters on decommissioning nodes are replaced by
	// upreplicating elsewhere. This test is supposed to tickle the
	// `AllocatorReplaceDecommissioningNonVoter` code path.
	t.Run("replace", func(t *testing.T) {
		tc, scratchRange := setupFn(t)
		defer tc.Stopper().Stop(ctx)
		// Do a fresh look up on the range descriptor.
		scratchRange = tc.LookupRangeOrFatal(t, scratchRange.StartKey.AsRawKey())
		beforeNodeIDs := getNonVoterNodeIDs(scratchRange)
		store, err := getLeaseholderStore(tc, scratchRange)
		if err != nil {
			t.Fatal(err)
		}
		// Check the value of metrics prior to replacement.
		previousAddCount := store.ReplicateQueueMetrics().AddNonVoterReplicaCount.Count()
		previousRemovalCount := store.ReplicateQueueMetrics().RemoveNonVoterReplicaCount.Count()
		previousDecommRemovals :=
			store.ReplicateQueueMetrics().RemoveDecommissioningNonVoterReplicaCount.Count()
		previousDecommReplacementSuccesses :=
			store.ReplicateQueueMetrics().ReplaceDecommissioningReplicaSuccessCount.Count()

		// Decommission each of the two nodes that have the non-voters and make sure
		// that those non-voters are upreplicated elsewhere.
		require.NoError(t,
			tc.Server(0).Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONING, beforeNodeIDs))

		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, &scratchRange, 1 /* voterCount */, 2 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			if !ok {
				return false
			}
			// Ensure that the non-voters have actually been removed from the dead
			// nodes and moved to others.
			scratchRange = tc.LookupRangeOrFatal(t, scratchRange.StartKey.AsRawKey())
			afterNodeIDs := getNonVoterNodeIDs(scratchRange)
			for _, before := range beforeNodeIDs {
				for _, after := range afterNodeIDs {
					if after == before {
						return false
					}
				}
			}
			return true
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

		// replica replacements update the addition/removal metrics as replicas
		// are being removed on two decommissioning stores added to other stores.
		currentAddCount := store.ReplicateQueueMetrics().AddNonVoterReplicaCount.Count()
		currentRemoveCount := store.ReplicateQueueMetrics().RemoveNonVoterReplicaCount.Count()
		currentDecommRemovals :=
			store.ReplicateQueueMetrics().RemoveDecommissioningNonVoterReplicaCount.Count()
		currentDecommReplacementSuccesses :=
			store.ReplicateQueueMetrics().ReplaceDecommissioningReplicaSuccessCount.Count()

		require.GreaterOrEqualf(
			t, currentAddCount, previousAddCount+2,
			"expected replica additions to increase by at least 2",
		)
		require.GreaterOrEqualf(
			t, currentRemoveCount, previousRemovalCount+2,
			"expected total replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(
			t, currentDecommRemovals, previousDecommRemovals+2,
			"expected decommissioning replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(t, currentDecommReplacementSuccesses, previousDecommReplacementSuccesses+2,
			"expected decommissioning replica replacement successes to increase by at least 2",
		)
	})

	// Check that when we have more non-voters than needed and some of those
	// non-voters are on decommissioning nodes, that we simply remove those
	// non-voters. This test is supposed to tickle the
	// `AllocatorRemoveDecommissioningNonVoter` code path.
	t.Run("remove", func(t *testing.T) {
		tc, scratchRange := setupFn(t)
		defer tc.Stopper().Stop(ctx)

		// Turn off the replicateQueue and update the zone configs to remove all
		// non-voters. At the same time, also mark all the nodes that have
		// non-voters as decommissioning.
		tc.ToggleReplicateQueues(false)
		_, err := tc.ServerConn(0).Exec(
			`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 1`,
		)
		require.NoError(t, err)

		// Do a fresh look up on the range descriptor.
		scratchRange = tc.LookupRangeOrFatal(t, scratchRange.StartKey.AsRawKey())
		var nonVoterNodeIDs []roachpb.NodeID
		for _, repl := range scratchRange.Replicas().NonVoterDescriptors() {
			nonVoterNodeIDs = append(nonVoterNodeIDs, repl.NodeID)
		}
		// Check metrics of leaseholder store prior to removal.
		store, err := getLeaseholderStore(tc, scratchRange)
		if err != nil {
			t.Fatal(err)
		}

		// Ensure leaseholder has updated span config with 0 non-voters.
		require.Eventually(t, func() bool {
			repl, err := store.GetReplica(scratchRange.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			_, conf := repl.DescAndSpanConfig()
			return conf.GetNumNonVoters() == 0
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

		previousRemovalCount := store.ReplicateQueueMetrics().RemoveNonVoterReplicaCount.Count()
		previousDecommRemovals :=
			store.ReplicateQueueMetrics().RemoveDecommissioningNonVoterReplicaCount.Count()
		previousDecommRemovalSuccesses :=
			store.ReplicateQueueMetrics().RemoveDecommissioningReplicaSuccessCount.Count()

		require.NoError(t,
			tc.Server(0).Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONING, nonVoterNodeIDs))

		// At this point, we know that we have an over-replicated range with
		// non-voters on nodes that are marked as decommissioning. So turn the
		// replicateQueue on and ensure that these redundant non-voters are removed.
		tc.ToggleReplicateQueues(true)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, &scratchRange, 1 /* voterCount */, 0 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			return ok
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

		currentRemoveCount := store.ReplicateQueueMetrics().RemoveNonVoterReplicaCount.Count()
		currentDecommRemovals :=
			store.ReplicateQueueMetrics().RemoveDecommissioningNonVoterReplicaCount.Count()
		currentDecommRemovalSuccesses :=
			store.ReplicateQueueMetrics().RemoveDecommissioningReplicaSuccessCount.Count()
		require.GreaterOrEqualf(
			t, currentRemoveCount, previousRemovalCount+2,
			"expected replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(
			t, currentDecommRemovals, previousDecommRemovals+2,
			"expected replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(t, currentDecommRemovalSuccesses, previousDecommRemovalSuccesses+2,
			"expected decommissioning replica removal successes to increase by at least 2",
		)
	})
}

// TestReplicateQueueTracingOnError tests that an error or slowdown in
// processing a replica results in traces being logged.
func TestReplicateQueueTracingOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	_ = log.SetVModule("replicate_queue=2")
	defer s.Close(t)

	// NB: This test injects a fake failure during replica rebalancing, and we use
	// this `rejectSnapshots` variable as a flag to activate or deactivate that
	// injected failure.
	var rejectSnapshots int64
	ctx := context.Background()
	tc := testcluster.StartTestCluster(
		t, 4, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
				ReceiveSnapshot: func(_ *kvserverpb.SnapshotRequest_Header) error {
					if atomic.LoadInt64(&rejectSnapshots) == 1 {
						return errors.Newf("boom")
					}
					return nil
				},
			}}},
		},
	)
	defer tc.Stopper().Stop(ctx)

	// Add a replica to the second and third nodes, and then decommission the
	// second node. Since there are only 4 nodes in the cluster, the
	// decommissioning replica must be rebalanced to the fourth node.
	const decomNodeIdx = 1
	const decomNodeID = 2
	scratchKey := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, scratchKey, tc.Target(decomNodeIdx))
	tc.AddVotersOrFatal(t, scratchKey, tc.Target(decomNodeIdx+1))
	adminSrv := tc.Server(decomNodeIdx)
	conn, err := adminSrv.RPCContext().GRPCDialNode(adminSrv.RPCAddr(), adminSrv.NodeID(), rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	adminClient := serverpb.NewAdminClient(conn)
	_, err = adminClient.Decommission(
		ctx, &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{decomNodeID},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
		},
	)
	require.NoError(t, err)

	// Activate the above testing knob to start rejecting future rebalances and
	// then attempt to rebalance the decommissioning replica away. We expect a
	// purgatory error to be returned here.
	atomic.StoreInt64(&rejectSnapshots, 1)
	store := tc.GetFirstStoreFromServer(t, 0)
	repl, err := store.GetReplica(tc.LookupRangeOrFatal(t, scratchKey).RangeID)
	require.NoError(t, err)

	testStartTs := timeutil.Now()
	recording, processErr, enqueueErr := tc.GetFirstStoreFromServer(t, 0).Enqueue(
		ctx, "replicate", repl, true /* skipShouldQueue */, false, /* async */
	)
	require.NoError(t, enqueueErr)
	require.Error(t, processErr, "expected processing error")

	// Flush logs and get log messages from replicate_queue.go since just
	// before calling store.Enqueue(..).
	log.FlushFileSinks()
	entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
		math.MaxInt64, 100, regexp.MustCompile(`replicate_queue\.go`), log.WithMarkedSensitiveData)
	require.NoError(t, err)

	opName := "process replica"
	errRegexp, err := regexp.Compile(`error processing replica:.*boom`)
	require.NoError(t, err)
	traceRegexp, err := regexp.Compile(`trace:.*`)
	require.NoError(t, err)
	opRegexp, err := regexp.Compile(fmt.Sprintf(`operation:%s`, opName))
	require.NoError(t, err)

	// Validate that the error is logged, so that we can use the log entry to
	// validate the trace output.
	foundEntry := false
	var entry logpb.Entry
	for _, entry = range entries {
		if errRegexp.MatchString(entry.Message) {
			foundEntry = true
			break
		}
	}
	require.True(t, foundEntry)

	// Validate that the trace is included in the log message.
	require.Regexp(t, traceRegexp, entry.Message)
	require.Regexp(t, opRegexp, entry.Message)

	// Validate that the logged trace filtered out the verbose execChangeReplicasTxn
	// child span, as well as the verbose child spans tracing txn operations.
	require.NotRegexp(t, `operation:change-replica-update-desc`, entry.Message)
	require.NotRegexp(t, `operation:txn coordinator send`, entry.Message)
	require.NotRegexp(t, `operation:log-range-event`, entry.Message)

	// Validate that the logged trace includes the changes to the descriptor.
	require.Regexp(t, `change replicas \(add.*remove.*\): existing descriptor`, entry.Message)

	// Validate that the trace was logged with the correct tags for the replica.
	require.Regexp(t, fmt.Sprintf("n%d", repl.NodeID()), entry.Tags)
	require.Regexp(t, fmt.Sprintf("s%d", repl.StoreID()), entry.Tags)
	require.Regexp(t, fmt.Sprintf("r%d/%d", repl.GetRangeID(), repl.ReplicaID()), entry.Tags)
	require.Regexp(t, `replicate`, entry.Tags)

	// Validate that the returned tracing span includes the operation, but also
	// that the stringified trace was not logged to the span or its parent.
	processRecSpan, foundSpan := recording.FindSpan(opName)
	require.True(t, foundSpan)

	foundParent := false
	var parentRecSpan tracingpb.RecordedSpan
	for _, parentRecSpan = range recording {
		if parentRecSpan.SpanID == processRecSpan.ParentSpanID {
			foundParent = true
			break
		}
	}
	require.True(t, foundParent)
	spans := tracingpb.Recording{parentRecSpan, processRecSpan}
	stringifiedSpans := spans.String()
	require.NotRegexp(t, errRegexp, stringifiedSpans)
	require.NotRegexp(t, traceRegexp, stringifiedSpans)
}

// TestReplicateQueueDecommissionPurgatoryError tests that failure to move a
// decommissioning replica puts it in the replicate queue purgatory.
func TestReplicateQueueDecommissionPurgatoryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// NB: This test injects a fake failure during replica rebalancing, and we use
	// this `rejectSnapshots` variable as a flag to activate or deactivate that
	// injected failure.
	var rejectSnapshots int64
	ctx := context.Background()
	tc := testcluster.StartTestCluster(
		t, 4, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
				ReceiveSnapshot: func(_ *kvserverpb.SnapshotRequest_Header) error {
					if atomic.LoadInt64(&rejectSnapshots) == 1 {
						return errors.Newf("boom")
					}
					return nil
				},
			}}},
		},
	)
	defer tc.Stopper().Stop(ctx)

	// Add a replica to the second and third nodes, and then decommission the
	// second node. Since there are only 4 nodes in the cluster, the
	// decommissioning replica must be rebalanced to the fourth node.
	const decomNodeIdx = 1
	const decomNodeID = 2
	scratchKey := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, scratchKey, tc.Target(decomNodeIdx))
	tc.AddVotersOrFatal(t, scratchKey, tc.Target(decomNodeIdx+1))
	adminSrv := tc.Server(decomNodeIdx)
	conn, err := adminSrv.RPCContext().GRPCDialNode(adminSrv.RPCAddr(), adminSrv.NodeID(), rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	adminClient := serverpb.NewAdminClient(conn)
	_, err = adminClient.Decommission(
		ctx, &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{decomNodeID},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
		},
	)
	require.NoError(t, err)

	// Activate the above testing knob to start rejecting future rebalances and
	// then attempt to rebalance the decommissioning replica away. We expect a
	// purgatory error to be returned here.
	atomic.StoreInt64(&rejectSnapshots, 1)
	store := tc.GetFirstStoreFromServer(t, 0)
	repl, err := store.GetReplica(tc.LookupRangeOrFatal(t, scratchKey).RangeID)
	require.NoError(t, err)
	_, processErr, enqueueErr := tc.GetFirstStoreFromServer(t, 0).Enqueue(
		ctx, "replicate", repl, true /* skipShouldQueue */, false, /* async */
	)
	require.NoError(t, enqueueErr)
	_, isPurgErr := kvserver.IsPurgatoryError(processErr)
	if !isPurgErr {
		t.Fatalf("expected to receive a purgatory error, got %v", processErr)
	}
}

// getLeaseholderStore returns the leaseholder store for the given scratchRange.
func getLeaseholderStore(
	tc *testcluster.TestCluster, scratchRange roachpb.RangeDescriptor,
) (*kvserver.Store, error) {
	leaseHolder, err := tc.FindRangeLeaseHolder(scratchRange, nil)
	if err != nil {
		return nil, err
	}
	leaseHolderSrv := tc.Servers[leaseHolder.NodeID-1]
	store, err := leaseHolderSrv.Stores().GetStore(leaseHolder.StoreID)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// TestReplicateQueueDeadNonVoters is an end to end test ensuring that
// non-voting replicas on dead nodes are replaced or removed.
func TestReplicateQueueDeadNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 76996)
	skip.UnderRace(t, "takes a long time or times out under race")

	ctx := context.Background()

	var livenessTrap atomic.Value
	setupFn := func(t *testing.T) (*testcluster.TestCluster, roachpb.RangeDescriptor) {
		tc := testcluster.StartTestCluster(t, 5,
			base.TestClusterArgs{
				ReplicationMode: base.ReplicationAuto,
				ServerArgs: base.TestServerArgs{
					ScanMinIdleTime: time.Millisecond,
					ScanMaxIdleTime: time.Millisecond,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							DisableReplicaRebalancing: true,
						},
						SpanConfig: &spanconfig.TestingKnobs{
							ConfigureScratchRange: true,
						},
						NodeLiveness: kvserver.NodeLivenessTestingKnobs{
							StorePoolNodeLivenessFn: func(
								id roachpb.NodeID, now time.Time, duration time.Duration,
							) livenesspb.NodeLivenessStatus {
								val := livenessTrap.Load()
								if val == nil {
									return livenesspb.NodeLivenessStatus_LIVE
								}
								return val.(func(nodeID roachpb.NodeID) livenesspb.NodeLivenessStatus)(id)
							},
						},
					},
				},
			},
		)
		_, err := tc.ServerConn(0).Exec(
			`SET CLUSTER SETTING server.failed_reservation_timeout='1ms'`)
		require.NoError(t, err)

		// Setup a scratch range on a test cluster with 2 non-voters and 1 voter.
		scratchKey := tc.ScratchRange(t)
		scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
		_, err = tc.ServerConn(0).Exec(
			`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 3, num_voters = 1`,
		)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, &scratchRange, 1 /* voterCount */, 2 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			return ok
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
		return tc, scratchRange
	}

	markDead := func(nodeIDs []roachpb.NodeID) {
		livenessTrap.Store(func(id roachpb.NodeID) livenesspb.NodeLivenessStatus {
			for _, dead := range nodeIDs {
				if dead == id {
					return livenesspb.NodeLivenessStatus_DEAD
				}
			}
			return livenesspb.NodeLivenessStatus_LIVE
		})
	}

	// This subtest checks that non-voters on dead nodes are replaced by
	// upreplicating elsewhere. This test is supposed to tickle the
	// `AllocatorReplaceDeadNonVoter` code path. It does the following:
	//
	// 1. On a 5 node cluster, instantiate a range with 1 voter and 2 non-voters.
	// 2. Kill the 2 nodes that have the non-voters.
	// 3. Check that those non-voters are replaced.
	t.Run("replace", func(t *testing.T) {
		tc, scratchRange := setupFn(t)
		defer tc.Stopper().Stop(ctx)

		// Check the value of non-voter metrics from leaseholder store prior to removals.
		store, err := getLeaseholderStore(tc, scratchRange)
		if err != nil {
			t.Fatal(err)
		}

		prevAdditions := store.ReplicateQueueMetrics().AddNonVoterReplicaCount.Count()
		prevRemovals := store.ReplicateQueueMetrics().RemoveNonVoterReplicaCount.Count()
		prevDeadRemovals := store.ReplicateQueueMetrics().RemoveDeadNonVoterReplicaCount.Count()
		prevDeadReplacementSuccesses := store.ReplicateQueueMetrics().ReplaceDeadReplicaSuccessCount.Count()

		beforeNodeIDs := getNonVoterNodeIDs(scratchRange)
		markDead(beforeNodeIDs)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, &scratchRange, 1 /* voterCount */, 2 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			if !ok {
				return false
			}
			// Ensure that the non-voters have actually been removed from the dead
			// nodes and moved to others.
			scratchRange = tc.LookupRangeOrFatal(t, scratchRange.StartKey.AsRawKey())
			afterNodeIDs := getNonVoterNodeIDs(scratchRange)
			for _, before := range beforeNodeIDs {
				for _, after := range afterNodeIDs {
					if after == before {
						return false
					}
				}
			}
			return true
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

		addCount := store.ReplicateQueueMetrics().AddNonVoterReplicaCount.Count()
		removeNonVoterCount := store.ReplicateQueueMetrics().RemoveNonVoterReplicaCount.Count()
		removeDeadNonVoterCount := store.ReplicateQueueMetrics().RemoveDeadNonVoterReplicaCount.Count()
		replaceDeadSuccesses := store.ReplicateQueueMetrics().ReplaceDeadReplicaSuccessCount.Count()

		require.GreaterOrEqualf(
			t, addCount, prevAdditions+2,
			"expected replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(
			t, removeNonVoterCount, prevRemovals+2,
			"expected replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(
			t, removeDeadNonVoterCount, prevDeadRemovals+2,
			"expected replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(
			t, replaceDeadSuccesses, prevDeadReplacementSuccesses+2,
			"expected dead replica replacement successes to increase by at least 2",
		)
	})

	// This subtest checks that when we have more non-voters than needed and some
	// existing non-voters are on dead nodes, we will simply remove these
	// non-voters. This test is supposed to tickle the
	// AllocatorRemoveDeadNonVoter` code path. The test does the following:
	//
	// 1. Instantiate a range with 1 voter and 2 non-voters on a 5-node cluster.
	// 2. Turn off the replicateQueue
	// 3. Change the zone configs such that there should be no non-voters --
	// the two existing non-voters should now be considered "over-replicated"
	// by the system.
	// 4. Kill the nodes that have non-voters.
	// 5. Turn on the replicateQueue
	// 6. Make sure that the non-voters are downreplicated from the dead nodes.
	t.Run("remove", func(t *testing.T) {
		tc, scratchRange := setupFn(t)
		defer tc.Stopper().Stop(ctx)

		toggleReplicationQueues(tc, false)
		_, err := tc.ServerConn(0).Exec(
			// Remove all non-voters.
			"ALTER RANGE default CONFIGURE ZONE USING num_replicas = 1",
		)
		require.NoError(t, err)

		// Check the value of non-voter metrics from leaseholder store prior to removals.
		store, err := getLeaseholderStore(tc, scratchRange)
		if err != nil {
			t.Fatal(err)
		}

		// Ensure leaseholder has updated span config with 0 non-voters.
		require.Eventually(t, func() bool {
			repl, err := store.GetReplica(scratchRange.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			_, conf := repl.DescAndSpanConfig()
			return conf.GetNumNonVoters() == 0
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

		prevRemovals := store.ReplicateQueueMetrics().RemoveReplicaCount.Count()
		prevNonVoterRemovals := store.ReplicateQueueMetrics().RemoveNonVoterReplicaCount.Count()
		prevDeadRemovals := store.ReplicateQueueMetrics().RemoveDeadNonVoterReplicaCount.Count()
		prevDeadRemovalSuccesses := store.ReplicateQueueMetrics().RemoveDeadReplicaSuccessCount.Count()

		beforeNodeIDs := getNonVoterNodeIDs(scratchRange)
		markDead(beforeNodeIDs)

		toggleReplicationQueues(tc, true)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, &scratchRange, 1 /* voterCount */, 0 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			return ok
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

		removeCount := store.ReplicateQueueMetrics().RemoveReplicaCount.Count()
		removeNonVoterCount := store.ReplicateQueueMetrics().RemoveNonVoterReplicaCount.Count()
		removeDeadNonVoterCount := store.ReplicateQueueMetrics().RemoveDeadNonVoterReplicaCount.Count()
		removeDeadSuccesses := store.ReplicateQueueMetrics().RemoveDeadReplicaSuccessCount.Count()
		require.GreaterOrEqualf(
			t, removeCount, prevRemovals+2,
			"expected replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(
			t, removeNonVoterCount, prevNonVoterRemovals+2,
			"expected replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(
			t, removeDeadNonVoterCount, prevDeadRemovals+2,
			"expected replica removals to increase by at least 2",
		)
		require.GreaterOrEqualf(
			t, removeDeadSuccesses, prevDeadRemovalSuccesses+2,
			"expected dead replica removal successes to increase by at least 2",
		)
	})
}

// TestReplicateQueueMetrics is an end-to-end test ensuring the replicateQueue
// voter replica metrics will be updated correctly during upreplication and downreplication.
func TestReplicateQueueMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes a long time or times out under race")

	ctx := context.Background()
	var clusterArgs = base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableReplicaRebalancing: true,
				},
			},
		},
	}
	dbName := "testdb"
	tableName := "kv"
	numNodes := 3
	tc, scratchRange := setupTestClusterWithDummyRange(t, clusterArgs, dbName, tableName, numNodes)
	defer tc.Stopper().Stop(ctx)

	// Check that the cluster is initialized correctly with 3 voters.
	require.Eventually(t, func() bool {
		ok, err := checkReplicaCount(
			ctx, tc.(*testcluster.TestCluster),
			&scratchRange, 3 /* voterCount */, 0, /* nonVoterCount */
		)
		if err != nil {
			log.Errorf(ctx, "error checking replica count: %s", err)
			return false
		}
		return ok
	}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

	// Get a map of voter replica store locations before the zone configuration change.
	voterStores := getVoterStores(t, tc.(*testcluster.TestCluster), &scratchRange)
	// Check the aggregated voter removal metrics across voter stores.
	previousRemoveCount, previousRemoveVoterCount := getAggregateMetricCounts(
		ctx,
		tc.(*testcluster.TestCluster),
		voterStores,
		false, /* add */
	)

	_, err := tc.ServerConn(0).Exec(
		`ALTER TABLE testdb.kv CONFIGURE ZONE USING num_replicas = 1`,
	)
	require.NoError(t, err)
	require.Eventually(
		t, func() bool {
			ok, err := checkReplicaCount(
				ctx, tc.(*testcluster.TestCluster), &scratchRange, 1, 0,
			)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			return ok
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond,
	)

	// Expect the new aggregated voter removal metrics across stores which had
	// voters removed increase by at least 2.
	currentRemoveCount, currentRemoveVoterCount := getAggregateMetricCounts(
		ctx,
		tc.(*testcluster.TestCluster),
		voterStores,
		false, /* add */
	)
	require.GreaterOrEqualf(
		t,
		currentRemoveCount,
		previousRemoveCount+2,
		"expected replica removals to increase by at least 2",
	)
	require.GreaterOrEqualf(
		t,
		currentRemoveVoterCount,
		previousRemoveVoterCount+2,
		"expected replica removals to increase by at least 2",
	)

	scratchRange = tc.LookupRangeOrFatal(t, scratchRange.StartKey.AsRawKey())
	store, err := getLeaseholderStore(tc.(*testcluster.TestCluster), scratchRange)
	if err != nil {
		t.Fatal(err)
	}
	// Track add counts on leaseholder before upreplication.
	previousAddCount := store.ReplicateQueueMetrics().AddReplicaCount.Count()
	previousAddVoterCount := store.ReplicateQueueMetrics().AddVoterReplicaCount.Count()

	_, err = tc.ServerConn(0).Exec(
		`ALTER TABLE testdb.kv CONFIGURE ZONE USING num_replicas = 3`,
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ok, err := checkReplicaCount(
			ctx, tc.(*testcluster.TestCluster), &scratchRange, 3, 0,
		)
		if err != nil {
			log.Errorf(ctx, "error checking replica count: %s", err)
			return false
		}
		return ok
	}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

	// Expect the aggregated voter add metrics across voter stores increase by at least 2.
	voterMap := getVoterStores(t, tc.(*testcluster.TestCluster), &scratchRange)
	currentAddCount, currentAddVoterCount := getAggregateMetricCounts(
		ctx,
		tc.(*testcluster.TestCluster),
		voterMap,
		true, /* add */
	)
	require.GreaterOrEqualf(
		t, currentAddCount, previousAddCount+2,
		"expected replica additions to increase by at least 2",
	)
	require.GreaterOrEqualf(
		t, currentAddVoterCount, previousAddVoterCount+2,
		"expected voter additions to increase by at least 2",
	)
}

// getVoterStores returns a mapping of voter nodeIDs to storeIDs.
func getVoterStores(
	t *testing.T, tc *testcluster.TestCluster, rangeDesc *roachpb.RangeDescriptor,
) (storeMap map[roachpb.NodeID]roachpb.StoreID) {
	*rangeDesc = tc.LookupRangeOrFatal(t, rangeDesc.StartKey.AsRawKey())
	voters := rangeDesc.Replicas().VoterDescriptors()
	storeMap = make(map[roachpb.NodeID]roachpb.StoreID)
	for i := 0; i < len(voters); i++ {
		storeMap[voters[i].NodeID] = voters[i].StoreID
	}
	return storeMap
}

// getAggregateMetricCounts adds metric counts from all stores in a given map.
// and returns the totals.
func getAggregateMetricCounts(
	ctx context.Context,
	tc *testcluster.TestCluster,
	voterMap map[roachpb.NodeID]roachpb.StoreID,
	add bool,
) (currentCount int64, currentVoterCount int64) {
	for _, s := range tc.Servers {
		if storeId, exists := voterMap[s.NodeID()]; exists {
			store, err := s.Stores().GetStore(storeId)
			if err != nil {
				log.Errorf(ctx, "error finding store: %s", err)
				continue
			}
			if add {
				currentCount += store.ReplicateQueueMetrics().AddReplicaCount.Count()
				currentVoterCount += store.ReplicateQueueMetrics().AddVoterReplicaCount.Count()
			} else {
				currentCount += store.ReplicateQueueMetrics().RemoveReplicaCount.Count()
				currentVoterCount += store.ReplicateQueueMetrics().RemoveVoterReplicaCount.Count()
			}
		}
	}
	return currentCount, currentVoterCount
}
func getNonVoterNodeIDs(rangeDesc roachpb.RangeDescriptor) (result []roachpb.NodeID) {
	for _, repl := range rangeDesc.Replicas().NonVoterDescriptors() {
		result = append(result, repl.NodeID)
	}
	return result
}

// TestReplicateQueueSwapVoterWithNonVoters tests that voting replicas can
// rebalance to stores that already have a non-voter by "swapping" with them.
// "Swapping" in this context means simply changing the `ReplicaType` on the
// receiving store from non-voter to voter and changing it on the other side
// from voter to non-voter.
func TestReplicateQueueSwapVotersWithNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes a long time or times out under race")

	ctx := context.Background()
	serverArgs := make(map[int]base.TestServerArgs)
	// Assign each store a rack number so we can constrain individual voting and
	// non-voting replicas to them.
	for i := 1; i <= 5; i++ {
		serverArgs[i-1] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key: "rack", Value: strconv.Itoa(i),
					},
				},
			},
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
		}
	}
	clusterArgs := base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: serverArgs,
	}

	synthesizeRandomConstraints := func() (
		constraints string, voterStores, nonVoterStores []roachpb.StoreID,
	) {
		storeList := []roachpb.StoreID{1, 2, 3, 4, 5}
		// Shuffle the list of stores and designate the first 3 as voters and the
		// rest as non-voters.
		rand.Shuffle(5, func(i, j int) {
			storeList[i], storeList[j] = storeList[j], storeList[i]
		})
		voterStores = storeList[:3]
		nonVoterStores = storeList[3:5]

		var overallConstraints, voterConstraints []string
		for _, store := range nonVoterStores {
			overallConstraints = append(overallConstraints, fmt.Sprintf(`"+rack=%d": 1`, store))
		}
		for _, store := range voterStores {
			voterConstraints = append(voterConstraints, fmt.Sprintf(`"+rack=%d": 1`, store))
		}
		return fmt.Sprintf(
			"ALTER RANGE default CONFIGURE ZONE USING num_replicas = 5, num_voters = 3,"+
				" constraints = '{%s}', voter_constraints = '{%s}'",
			strings.Join(overallConstraints, ","), strings.Join(voterConstraints, ","),
		), voterStores, nonVoterStores
	}

	tc := testcluster.StartTestCluster(t, 5, clusterArgs)
	defer tc.Stopper().Stop(context.Background())

	scratchKey := tc.ScratchRange(t)
	// Start with 1 voter and 4 non-voters. This ensures that we also exercise the
	// swapping behavior during voting replica allocation when we upreplicate to 3
	// voters after calling `synthesizeRandomConstraints` below. See comment
	// inside `allocateTargetFromList`.
	_, err := tc.ServerConn(0).Exec("ALTER RANGE default CONFIGURE ZONE USING" +
		" num_replicas=5, num_voters=1")
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		if err := forceScanOnAllReplicationQueues(tc); err != nil {
			return err
		}
		scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
		if voters := scratchRange.Replicas().VoterDescriptors(); len(voters) != 1 {
			return errors.Newf("expected 1 voter; got %v", voters)
		}
		if nonVoters := scratchRange.Replicas().NonVoterDescriptors(); len(nonVoters) != 4 {
			return errors.Newf("expected 4 non-voters; got %v", nonVoters)
		}
		return nil
	})

	checkRelocated := func(t *testing.T, voterStores, nonVoterStores []roachpb.StoreID) {
		testutils.SucceedsSoon(t, func() error {
			if err := forceScanOnAllReplicationQueues(tc); err != nil {
				return err
			}
			scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
			if n := len(scratchRange.Replicas().VoterDescriptors()); n != 3 {
				return errors.Newf("number of voters %d does not match expectation", n)
			}
			if n := len(scratchRange.Replicas().NonVoterDescriptors()); n != 2 {
				return errors.Newf("number of non-voters %d does not match expectation", n)
			}

			// Check that each replica set is present on the stores designated by
			// synthesizeRandomConstraints.
			for _, store := range voterStores {
				replDesc, ok := scratchRange.GetReplicaDescriptor(store)
				if !ok {
					return errors.Newf("no replica found on store %d", store)
				}
				if typ := replDesc.Type; typ != roachpb.VOTER_FULL {
					return errors.Newf("replica on store %d does not match expectation;"+
						" expected VOTER_FULL, got %s", typ)
				}
			}
			for _, store := range nonVoterStores {
				replDesc, ok := scratchRange.GetReplicaDescriptor(store)
				if !ok {
					return errors.Newf("no replica found on store %d", store)
				}
				if typ := replDesc.Type; typ != roachpb.NON_VOTER {
					return errors.Newf("replica on store %d does not match expectation;"+
						" expected NON_VOTER, got %s", typ)
				}
			}
			return nil
		})
	}

	var numIterations = 10
	if util.RaceEnabled {
		numIterations = 1
	}
	for i := 0; i < numIterations; i++ {
		// Generate random (but valid) constraints for the 3 voters and 2 non_voters
		// and check that the replicate queue achieves conformance.
		//
		// NB: `synthesizeRandomConstraints` sets up the default zone configs such
		// that every range should have 3 voting replica and 2 non-voting replicas.
		// The crucial thing to note here is that we have 5 stores and 5 replicas,
		// and since we never allow a single store to have >1 replica for a range at
		// any given point, any change in the configuration of these 5 replicas
		// _must_ go through atomic non-voter promotions and voter demotions.
		alterStatement, voterStores, nonVoterStores := synthesizeRandomConstraints()
		log.Infof(ctx, "applying: %s", alterStatement)
		_, err := tc.ServerConn(0).Exec(alterStatement)
		require.NoError(t, err)
		checkRelocated(t, voterStores, nonVoterStores)
	}
}

// TestReplicateQueueShouldQueueNonVoter tests that, in situations where the
// voting replicas don't need to be rebalanced but the non-voting replicas do,
// that the replicate queue correctly accepts the replica into the queue.
func TestReplicateQueueShouldQueueNonVoter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serverArgs := make(map[int]base.TestServerArgs)
	// Assign each store a rack number so we can constrain individual voting and
	// non-voting replicas to them.
	for i := 1; i <= 3; i++ {
		serverArgs[i-1] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key: "rack", Value: strconv.Itoa(i),
					},
				},
			},
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	_, err := tc.ServerConn(0).Exec("ALTER RANGE default CONFIGURE ZONE USING" +
		" num_replicas = 2, num_voters = 1," +
		" constraints='{\"+rack=2\": 1}', voter_constraints='{\"+rack=1\": 1}'")
	require.NoError(t, err)

	// Make sure that the range has conformed to the constraints we just set
	// above.
	require.Eventually(t, func() bool {
		if err := forceScanOnAllReplicationQueues(tc); err != nil {
			log.Warningf(ctx, "received error while forcing a replicateQueue scan: %s", err)
			return false
		}
		scratchRange := tc.LookupRangeOrFatal(t, scratchStartKey)
		if len(scratchRange.Replicas().VoterDescriptors()) != 1 {
			return false
		}
		if len(scratchRange.Replicas().NonVoterDescriptors()) != 1 {
			return false
		}
		// Ensure that the voter is on rack 1 and the non-voter is on rack 2.
		if scratchRange.Replicas().VoterDescriptors()[0].NodeID != tc.Server(0).NodeID() {
			return false
		}
		if scratchRange.Replicas().NonVoterDescriptors()[0].NodeID != tc.Server(1).NodeID() {
			return false
		}
		return true
	}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

	// Turn off the replicateQueues to prevent them from taking action on
	// `scratchRange`. We will manually enqueue the leaseholder for `scratchRange`
	// below.
	toggleReplicationQueues(tc, false)
	// We change the default zone configuration to dictate that the existing
	// voter doesn't need to be rebalanced but non-voter should be rebalanced to
	// rack 3 instead.
	_, err = tc.ServerConn(0).Exec("ALTER RANGE default CONFIGURE ZONE USING" +
		" constraints='{\"+rack=3\": 1}', voter_constraints='{\"+rack=1\": 1}'")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		// NB: Manually enqueuing the replica on server 0 (i.e. rack 1) is copacetic
		// because we know that it is the leaseholder (since it is the only voting
		// replica).
		store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
		recording, processErr, err := store.Enqueue(
			ctx, "replicate", repl, false /* skipShouldQueue */, false, /* async */
		)
		if err != nil {
			log.Errorf(ctx, "err: %s", err.Error())
			return false
		}
		if processErr != nil {
			log.Errorf(ctx, "processErr: %s", processErr.Error())
			return false
		}
		if matched, err := regexp.Match("rebalance target found for non-voter, enqueuing",
			[]byte(recording.String())); !matched {
			require.NoError(t, err)
			return false
		}
		return true
	}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
}

// queryRangeLog queries the range log. The query must be of type:
// `SELECT info from system.rangelog ...`.
func queryRangeLog(
	conn *gosql.DB, query string, args ...interface{},
) ([]kvserverpb.RangeLogEvent_Info, error) {

	// The range log can get large and sees unpredictable writes, so run this in a
	// proper txn to avoid spurious retries.
	var events []kvserverpb.RangeLogEvent_Info
	err := crdb.ExecuteTx(context.Background(), conn, nil, func(conn *gosql.Tx) error {
		events = nil // reset in case of a retry

		rows, err := conn.Query(query, args...)
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
			var info kvserverpb.RangeLogEvent_Info
			if err := json.Unmarshal([]byte(infoStr), &info); err != nil {
				return errors.Wrapf(err, "error unmarshaling info string %q", infoStr)
			}
			events = append(events, info)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil
	})
	return events, err

}

func filterRangeLog(
	conn *gosql.DB,
	rangeID roachpb.RangeID,
	eventType kvserverpb.RangeLogEventType,
	reason kvserverpb.RangeLogEventReason,
) ([]kvserverpb.RangeLogEvent_Info, error) {
	return queryRangeLog(conn, `SELECT info FROM system.rangelog WHERE "rangeID" = $1 AND "eventType" = $2 AND info LIKE concat('%', $3, '%') ORDER BY timestamp ASC;`, rangeID, eventType.String(), reason)
}

func toggleReplicationQueues(tc *testcluster.TestCluster, active bool) {
	for _, s := range tc.Servers {
		_ = s.Stores().VisitStores(func(store *kvserver.Store) error {
			store.SetReplicateQueueActive(active)
			return nil
		})
	}
}

func forceScanOnAllReplicationQueues(tc *testcluster.TestCluster) (err error) {
	for _, s := range tc.Servers {
		err = s.Stores().VisitStores(func(store *kvserver.Store) error {
			return store.ForceReplicationScanAndProcess()
		})
	}
	return err
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
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, 38565)
	skip.UnderRaceWithIssue(t, 38565)
	skip.UnderShort(t, 38565)
	skip.UnderDeadlockWithIssue(t, 38565)
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
	_, err = db.Exec("INSERT INTO t(i,s) VALUES (1, $1)", sb.String())
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t(i,s) VALUES (2, 'b')")
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
			"SELECT replicas FROM [SHOW RANGES FROM TABLE t] WHERE start_key LIKE '%/2'")
		var repl string
		if err := r.Scan(&repl); err != nil {
			return err
		}
		t.Logf("replicas: %v", repl)
		if repl != "{1,2,3,4,5}" {
			return fmt.Errorf("not up-replicated yet. replicas: %s", repl)
		}
		return nil
	})

	// Now check that the large range also gets up-replicated.
	testutils.SucceedsSoon(t, func() error {
		forceProcess()
		r := db.QueryRow(
			"SELECT replicas FROM [SHOW RANGES FROM TABLE t] WHERE start_key LIKE '%TableMin%'")
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
	req *kvserverpb.RaftMessageRequest,
	respStream kvserver.RaftMessageResponseStream,
) *kvpb.Error {
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
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1min under race")

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
	s.Exec(t, "INSERT INTO system.comments VALUES(0,0,0,'abc')")
	s.QueryRow(t,
		"SELECT range_id, lease_holder FROM "+
			"[SHOW RANGES FROM TABLE system.comments WITH DETAILS] LIMIT 1",
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
	require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "load", func(ctx context.Context) {
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
	}))
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
	desc, conf := leaseHolderRepl.DescAndSpanConfig()
	newConf := conf
	newConf.LeasePreferences = []roachpb.LeasePreference{
		{
			Constraints: []roachpb.Constraint{
				{
					Type:  roachpb.Constraint_REQUIRED,
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
			ctx, leaseRepl, desc, newConf)
		if err != nil {
			return err
		}
		if !transferred {
			return errors.Errorf("unable to transfer")
		}
		return errors.Errorf("Repeat check for correct leaseholder")
	})
}

// TestReplicateQueueAcquiresInvalidLeases asserts that following a restart,
// leases are invalidated and that the replicate queue acquires invalid leases
// when enabled.
func TestReplicateQueueAcquiresInvalidLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism

	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	zcfg := zonepb.DefaultZoneConfig()
	zcfg.NumReplicas = proto.Int32(1)
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			// Disable the replication queue initially, to assert on the lease
			// statuses pre and post enabling the replicate queue.
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings:                 st,
				DisableDefaultTestTenant: true,
				ScanMinIdleTime:          time.Millisecond,
				ScanMaxIdleTime:          time.Millisecond,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyEngineRegistry:      stickyEngineRegistry,
						DefaultZoneConfigOverride: &zcfg,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)
	db := tc.Conns[0]
	// Disable consistency checker and sql stats collection that may acquire a
	// lease by querying a range.
	_, err := db.Exec("set cluster setting server.consistency_check.interval = '0s'")
	require.NoError(t, err)
	_, err = db.Exec("set cluster setting sql.stats.automatic_collection.enabled = false")
	require.NoError(t, err)

	// Create ranges to assert on their lease status post restart and after
	// replicate queue processing.
	ranges := 30
	scratchRangeKeys := make([]roachpb.Key, ranges)
	splitKey := tc.ScratchRange(t)
	for i := range scratchRangeKeys {
		_, _ = tc.SplitRangeOrFatal(t, splitKey)
		scratchRangeKeys[i] = splitKey
		splitKey = splitKey.Next()
	}

	invalidLeases := func() []kvserverpb.LeaseStatus {
		invalid := []kvserverpb.LeaseStatus{}
		for _, key := range scratchRangeKeys {
			// Assert that the lease is invalid after restart.
			repl := tc.GetRaftLeader(t, roachpb.RKey(key))
			if leaseStatus := repl.CurrentLeaseStatus(ctx); !leaseStatus.IsValid() {
				invalid = append(invalid, leaseStatus)
			}
		}
		return invalid
	}

	// Assert that the leases are valid initially.
	require.Len(t, invalidLeases(), 0)

	// Restart the servers to invalidate the leases.
	for i := range tc.Servers {
		tc.StopServer(i)
		err = tc.RestartServerWithInspect(i, nil)
		require.NoError(t, err)
	}

	forceProcess := func() {
		// Speed up the queue processing.
		for _, s := range tc.Servers {
			err := s.Stores().VisitStores(func(store *kvserver.Store) error {
				return store.ForceReplicationScanAndProcess()
			})
			require.NoError(t, err)
		}
	}

	// NB: The cosnsistency checker and sql stats collector both will attempt a
	// lease acquisition when processing a range, if it the lease is currently
	// invalid. They are disabled in this test. We do not assert on the number
	// of invalid leases prior to enabling the replicate queue here to avoid
	// test flakiness if this changes in the future or for some other reason.
	// Instead, we are only concerned that no invalid leases remain.
	toggleReplicationQueues(tc, true /* active */)
	testutils.SucceedsSoon(t, func() error {
		forceProcess()
		// Assert that there are now no invalid leases.
		invalid := invalidLeases()
		if len(invalid) > 0 {
			return errors.Newf("The number of invalid leases are greater than 0, %+v", invalid)
		}
		return nil
	})
}

func iterateOverAllStores(
	t *testing.T, tc *testcluster.TestCluster, f func(*kvserver.Store) error,
) {
	for _, server := range tc.Servers {
		require.NoError(t, server.Stores().VisitStores(f))
	}
}

// TestPromoteNonVoterInAddVoter tests the prioritization of promoting
// non-voters when switching from ZONE to REGION survival i.e.
//
// ZONE survival configuration:
// Region 1: Voter, Voter, Voter
// Region 2: Non-Voter
// Region 3: Non-Voter
// to REGION survival configuration:
// Region 1: Voter, Voter
// Region 2: Voter, Voter
// Region 3: Voter
//
// Here we have 7 stores: 3 in Region 1, 2 in Region 2, and 2 in Region 3.
//
// The expected behaviour is that there should not be any add voter events in
// the range log where the added replica type is a LEARNER.
func TestPromoteNonVoterInAddVoter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is slow under stress and can time out when upreplicating /
	// rebalancing to ensure all stores have the same range count initially, due
	// to slow heartbeats.
	skip.UnderStress(t)

	ctx := context.Background()

	// Create 7 stores: 3 in Region 1, 2 in Region 2, and 2 in Region 3.
	const numNodes = 7
	serverArgs := make(map[int]base.TestServerArgs)
	regions := [numNodes]int{1, 1, 1, 2, 2, 3, 3}
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key: "region", Value: strconv.Itoa(regions[i]),
					},
				},
			},
		}
	}

	// Start test cluster.
	clusterArgs := base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: serverArgs,
	}
	tc := testcluster.StartTestCluster(t, numNodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)

	setConstraintFn := func(object string, numReplicas, numVoters int, additionalConstraints string) {
		_, err := db.Exec(
			fmt.Sprintf("ALTER %s CONFIGURE ZONE USING num_replicas = %d, num_voters = %d%s",
				object, numReplicas, numVoters, additionalConstraints))
		require.NoError(t, err)
	}

	// Ensure all stores have the same range count initially, to allow for more
	// predictable behaviour when the allocator ranks stores using balance score.
	setConstraintFn("DATABASE system", 7, 7, "")
	setConstraintFn("RANGE system", 7, 7, "")
	setConstraintFn("RANGE liveness", 7, 7, "")
	setConstraintFn("RANGE meta", 7, 7, "")
	setConstraintFn("RANGE default", 7, 7, "")
	testutils.SucceedsSoon(t, func() error {
		if err := forceScanOnAllReplicationQueues(tc); err != nil {
			return err
		}
		rangeCount := -1
		allEqualRangeCount := true
		iterateOverAllStores(t, tc, func(s *kvserver.Store) error {
			if rangeCount == -1 {
				rangeCount = s.ReplicaCount()
			} else if rangeCount != s.ReplicaCount() {
				allEqualRangeCount = false
			}
			return nil
		})
		if !allEqualRangeCount {
			return errors.New("Range counts are not all equal")
		}
		return nil
	})

	// Create a new range to simulate switching from ZONE to REGION survival.
	_, err := db.Exec("CREATE TABLE t (i INT PRIMARY KEY, s STRING)")
	require.NoError(t, err)

	// ZONE survival configuration.
	setConstraintFn("TABLE t", 5, 3,
		", constraints = '{\"+region=2\": 1, \"+region=3\": 1}', voter_constraints = '{\"+region=1\": 3}'")

	// computeNumberOfReplicas is used to find the number of voters and
	// non-voters to check if we are meeting our zone configuration.
	computeNumberOfReplicas := func(
		t *testing.T,
		tc *testcluster.TestCluster,
		db *gosql.DB,
	) (numVoters, numNonVoters int, err error) {
		if err := forceScanOnAllReplicationQueues(tc); err != nil {
			return 0, 0, err
		}

		var rangeID roachpb.RangeID
		if err := db.QueryRow("SELECT range_id FROM [SHOW RANGES FROM TABLE t] LIMIT 1").Scan(&rangeID); err != nil {
			return 0, 0, err
		}
		iterateOverAllStores(t, tc, func(s *kvserver.Store) error {
			if replica, err := s.GetReplica(rangeID); err == nil && replica.OwnsValidLease(ctx, replica.Clock().NowAsClockTimestamp()) {
				desc := replica.Desc()
				numVoters = len(desc.Replicas().VoterDescriptors())
				numNonVoters = len(desc.Replicas().NonVoterDescriptors())
			}
			return nil
		})
		return numVoters, numNonVoters, nil
	}

	// Ensure we are meeting our ZONE survival configuration.
	testutils.SucceedsSoon(t, func() error {
		numVoters, numNonVoters, err := computeNumberOfReplicas(t, tc, db)
		require.NoError(t, err)
		if numVoters != 3 {
			return errors.Newf("expected 3 voters; got %d", numVoters)
		}
		if numNonVoters != 2 {
			return errors.Newf("expected 2 non-voters; got %v", numNonVoters)
		}
		return nil
	})

	// REGION survival configuration.
	setConstraintFn("TABLE t", 5, 5,
		", constraints = '{}', voter_constraints = '{\"+region=1\": 2, \"+region=2\": 2, \"+region=3\": 1}'")
	require.NoError(t, err)

	// Ensure we are meeting our REGION survival configuration.
	testutils.SucceedsSoon(t, func() error {
		numVoters, numNonVoters, err := computeNumberOfReplicas(t, tc, db)
		require.NoError(t, err)
		if numVoters != 5 {
			return errors.Newf("expected 5 voters; got %d", numVoters)
		}
		if numNonVoters != 0 {
			return errors.Newf("expected 0 non-voters; got %v", numNonVoters)
		}
		return nil
	})

	// Retrieve the add voter events from the range log.
	var rangeID roachpb.RangeID
	err = db.QueryRow("SELECT range_id FROM [SHOW RANGES FROM TABLE t] LIMIT 1").Scan(&rangeID)
	require.NoError(t, err)
	addVoterEvents, err := filterRangeLog(tc.Conns[0], rangeID, kvserverpb.RangeLogEventType_add_voter, kvserverpb.ReasonRangeUnderReplicated)
	require.NoError(t, err)

	// Check if an add voter event has an added replica of type LEARNER, and if
	// it does, it shows that we are adding a new voter rather than promoting an
	// existing non-voter, which is unexpected.
	for _, addVoterEvent := range addVoterEvents {
		switch addVoterEvent.AddedReplica.Type {
		case roachpb.LEARNER:
			require.Failf(
				t,
				"Expected to promote non-voter, instead added voter",
				"Added voter store ID: %v\nAdd voter events: %v",
				addVoterEvent.AddedReplica.StoreID, addVoterEvents)
		case roachpb.VOTER_FULL:
		default:
			require.Failf(
				t,
				"Unexpected added replica type",
				"Replica type: %v\nAdd voter events: %v",
				addVoterEvent.AddedReplica.Type, addVoterEvents)
		}
	}
}

// TestReplicateQueueExpirationLeasesOnly tests that changing
// kv.expiration_leases_only.enabled switches all leases to the correct kind.
func TestReplicateQueueExpirationLeasesOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t) // too slow under stressrace
	skip.UnderDeadlock(t)
	skip.UnderShort(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: st,
			// Speed up the replicate queue, which switches the lease type.
			ScanMinIdleTime: time.Millisecond,
			ScanMaxIdleTime: time.Millisecond,
		},
	})
	defer tc.Stopper().Stop(ctx)

	require.NoError(t, tc.WaitForFullReplication())

	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)

	// Split off a few ranges so we have something to work with.
	scratchKey := tc.ScratchRange(t)
	for i := 0; i <= 255; i++ {
		splitKey := append(scratchKey.Clone(), byte(i))
		require.NoError(t, db.AdminSplit(ctx, splitKey, hlc.MaxTimestamp))
	}

	countLeases := func() (epoch int64, expiration int64) {
		for i := 0; i < tc.NumServers(); i++ {
			require.NoError(t, tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				require.NoError(t, s.ComputeMetrics(ctx))
				expiration += s.Metrics().LeaseExpirationCount.Value()
				epoch += s.Metrics().LeaseEpochCount.Value()
				return nil
			}))
		}
		return
	}

	// We expect to have both expiration and epoch leases at the start, since the
	// meta and liveness ranges require expiration leases. However, it's possible
	// that there are a few other stray expiration leases too, since lease
	// transfers use expiration leases as well.
	epochLeases, expLeases := countLeases()
	require.NotZero(t, epochLeases)
	require.NotZero(t, expLeases)
	initialExpLeases := expLeases
	t.Logf("initial: epochLeases=%d expLeases=%d", epochLeases, expLeases)

	// Switch to expiration leases and wait for them to change.
	_, err := sqlDB.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = true`)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		epochLeases, expLeases = countLeases()
		t.Logf("enabling: epochLeases=%d expLeases=%d", epochLeases, expLeases)
		return epochLeases == 0 && expLeases > 0
	}, 30*time.Second, 500*time.Millisecond) // accomodate stress/deadlock builds

	// Run a scan across the ranges, just to make sure they work.
	scanCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = db.Scan(scanCtx, scratchKey, scratchKey.PrefixEnd(), 1)
	require.NoError(t, err)

	// Switch back to epoch leases and wait for them to change. We still expect to
	// have some required expiration leases, but they should be at or below the
	// number of expiration leases we had at the start (primarily the meta and
	// liveness ranges, but possibly a few more since lease transfers also use
	// expiration leases).
	_, err = sqlDB.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = false`)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		epochLeases, expLeases = countLeases()
		t.Logf("disabling: epochLeases=%d expLeases=%d", epochLeases, expLeases)
		return epochLeases > 0 && expLeases > 0 && expLeases <= initialExpLeases
	}, 30*time.Second, 500*time.Millisecond)
}
