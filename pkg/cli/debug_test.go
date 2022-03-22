// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

func createStore(t *testing.T, path string) {
	t.Helper()
	db, err := storage.Open(
		context.Background(),
		storage.Filesystem(path),
		storage.CacheSize(server.DefaultCacheSize))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestOpenExistingStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	dirExists := filepath.Join(baseDir, "exists")
	dirMissing := filepath.Join(baseDir, "missing")
	createStore(t, dirExists)

	for _, test := range []struct {
		dir    string
		expErr string
	}{
		{
			dir:    dirExists,
			expErr: "",
		},
		{
			dir:    dirMissing,
			expErr: `does not exist|no such file or directory`,
		},
	} {
		t.Run(fmt.Sprintf("dir=%s", test.dir), func(t *testing.T) {
			_, err := OpenExistingStore(test.dir, stopper, false /* readOnly */, false /* disableAutomaticCompactions */)
			if !testutils.IsError(err, test.expErr) {
				t.Errorf("wanted %s but got %v", test.expErr, err)
			}
		})
	}
}

func TestOpenReadOnlyStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	storePath := filepath.Join(baseDir, "store")
	createStore(t, storePath)

	for _, test := range []struct {
		readOnly bool
		expErr   string
	}{
		{
			readOnly: false,
			expErr:   "",
		},
		{
			readOnly: true,
			expErr:   `Not supported operation in read only mode|pebble: read-only`,
		},
	} {
		t.Run(fmt.Sprintf("readOnly=%t", test.readOnly), func(t *testing.T) {
			db, err := OpenExistingStore(storePath, stopper, test.readOnly, false /* disableAutomaticCompactions */)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			key := roachpb.Key("key")
			val := []byte("value")
			err = db.PutUnversioned(key, val)
			if !testutils.IsError(err, test.expErr) {
				t.Fatalf("wanted %s but got %v", test.expErr, err)
			}
		})
	}
}

func TestRemoveDeadReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is pretty slow under race (200+ cpu-seconds) because it
	// uses multiple real disk-backed stores and goes through multiple
	// cycles of rereplicating all ranges.
	skip.UnderRace(t)

	ctx := context.Background()

	testCases := []struct {
		survivingNodes, totalNodes, replicationFactor int
	}{
		{1, 3, 3},
		{2, 4, 4},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%d/%d/r=%d", testCase.survivingNodes, testCase.totalNodes,
			testCase.replicationFactor),
			func(t *testing.T) {
				baseDir, dirCleanupFn := testutils.TempDir(t)
				defer dirCleanupFn()

				// The surviving nodes get a real store, others are just in memory.
				var storePaths []string
				clusterArgs := base.TestClusterArgs{
					ServerArgsPerNode: map[int]base.TestServerArgs{},
				}
				deadReplicas := map[roachpb.StoreID]struct{}{}

				for i := 0; i < testCase.totalNodes; i++ {
					args := base.TestServerArgs{}
					args.ScanMaxIdleTime = time.Millisecond
					storeID := roachpb.StoreID(i + 1)
					if i < testCase.survivingNodes {
						path := filepath.Join(baseDir, fmt.Sprintf("store%d", storeID))
						storePaths = append(storePaths, path)
						// ServerArgsPerNode uses 0-based index, not node ID.
						args.StoreSpecs = []base.StoreSpec{{Path: path}}
					} else {
						deadReplicas[storeID] = struct{}{}
					}
					clusterArgs.ServerArgsPerNode[i] = args
				}

				// Start the cluster, let it replicate, then stop it. Since the
				// non-surviving nodes use in-memory stores, this automatically
				// causes the cluster to lose its quorum.
				//
				// While it's running, start a transaction and write an intent to
				// one of the range descriptors (without committing or aborting the
				// transaction). This exercises a special case in removeDeadReplicas.
				func() {
					tc := testcluster.StartTestCluster(t, testCase.totalNodes, clusterArgs)
					defer tc.Stopper().Stop(ctx)

					s := sqlutils.MakeSQLRunner(tc.Conns[0])

					// Set the replication factor on all zones.
					func() {
						rows := s.Query(t, "show all zone configurations")
						defer rows.Close()
						re := regexp.MustCompile(`num_replicas = \d+`)
						var sqls []string
						for rows.Next() {
							var name, configSQL string
							if err := rows.Scan(&name, &configSQL); err != nil {
								t.Fatal(err)
							}
							sqls = append(sqls, configSQL)
						}
						for _, sql := range sqls {
							if re.FindString(sql) != "" {
								sql = re.ReplaceAllString(sql,
									fmt.Sprintf("num_replicas = %d", testCase.replicationFactor))
							} else {
								sql = fmt.Sprintf("%s, num_replicas = %d", sql, testCase.replicationFactor)
							}
							s.Exec(t, sql)
						}
					}()
					if err := tc.WaitForFullReplication(); err != nil {
						t.Fatal(err)
					}

					// Perform a write, to ensure that pre-crash data is preserved.
					// Creating a table causes extra friction in the test harness when
					// we restart the cluster, so just write a setting.
					s.Exec(t, "set cluster setting cluster.organization='remove dead replicas test'")

					txn := kv.NewTxn(ctx, tc.Servers[0].DB(), 1)
					var desc roachpb.RangeDescriptor
					// Pick one of the predefined split points.
					rdKey := keys.RangeDescriptorKey(roachpb.RKey(keys.TimeseriesPrefix))
					if err := txn.GetProto(ctx, rdKey, &desc); err != nil {
						t.Fatal(err)
					}
					desc.NextReplicaID++
					if err := txn.Put(ctx, rdKey, &desc); err != nil {
						t.Fatal(err)
					}

					// At this point the intent has been written to Pebble but this
					// write was not synced (only the raft log append was synced). We
					// need to force another sync, but we're far from the storage
					// layer here so the easiest thing to do is simply perform a
					// second write. This will force the first write to be persisted
					// to disk (the second write may or may not make it to disk due to
					// timing).
					desc.NextReplicaID++
					if err := txn.Put(ctx, rdKey, &desc); err != nil {
						t.Fatal(err)
					}

					// We deliberately do not close this transaction so the intent is
					// left behind.
				}()

				// Open the surviving stores directly to repair them.
				repairStore := func(idx int) error {
					stopper := stop.NewStopper()
					defer stopper.Stop(ctx)

					db, err := OpenExistingStore(storePaths[idx], stopper, false /* readOnly */, false /* disableAutomaticCompactions */)
					if err != nil {
						return err
					}

					batch, err := removeDeadReplicas(db, deadReplicas)
					if err != nil {
						return err
					}
					if batch != nil {
						if err := batch.Commit(true); err != nil {
							return err
						}
						batch.Close()
					}

					// The repair process is idempotent and should give a nil batch the second time
					batch, err = removeDeadReplicas(db, deadReplicas)
					if err != nil {
						return err
					}
					if batch != nil {
						batch.Close()
						return errors.New("expected nil batch on second attempt")
					}
					return nil
				}
				for i := 0; i < testCase.survivingNodes; i++ {
					err := repairStore(i)
					if err != nil {
						t.Fatal(err)
					}
				}

				// Now that the data is salvaged, we can restart the cluster.
				// The nodes with the in-memory stores will be assigned new,
				// higher node IDs (e.g. in the 1/3 case, the restarted nodes
				// will be 4 and 5).
				//
				// This can activate the adaptive zone config feature (using 5x
				// replication for clusters of 5 nodes or more), so we must
				// decommission the dead nodes for WaitForFullReplication to
				// complete. (note that the cluster is working even when it
				// doesn't consider itself fully replicated - that's what allows
				// the decommissioning to succeed. We're just waiting for full
				// replication so that we can validate that ranges were moved
				// from e.g. {1,2,3} to {1,4,5}).
				//
				// Set replication mode to manual so that TestCluster doesn't call
				// WaitForFullReplication before we've decommissioned the nodes.
				clusterArgs.ReplicationMode = base.ReplicationManual
				clusterArgs.ParallelStart = true

				// Sleep before restarting to allow all previous leases to expire.
				// Without this sleep, the 2/4 case is flaky because sometimes
				// node 1 will be left with an unexpired lease, which tricks
				// some heuristics on node 2 into never campaigning even though it
				// is the only surviving replica.
				//
				// The reason it never campaigns is incompletely understood.
				// The result of shouldCampaignOnWake will correctly
				// transition from false to true when the old lease expires,
				// but nothing triggers a "wake" event after this point.
				//
				// TODO(bdarnell): This is really a bug in Replica.leaseStatus.
				// It should not return VALID for a lease held by a node that
				// has been removed from the configuration.
				time.Sleep(10 * time.Second)

				tc := testcluster.StartTestCluster(t, testCase.totalNodes, clusterArgs)
				defer tc.Stopper().Stop(ctx)

				grpcConn, err := tc.Server(0).RPCContext().GRPCDialNode(
					tc.Server(0).ServingRPCAddr(),
					tc.Server(0).NodeID(),
					rpc.DefaultClass,
				).Connect(ctx)
				if err != nil {
					t.Fatal(err)
				}
				adminClient := serverpb.NewAdminClient(grpcConn)

				deadNodes := []roachpb.NodeID{}
				for i := testCase.survivingNodes; i < testCase.totalNodes; i++ {
					deadNodes = append(deadNodes, roachpb.NodeID(i+1))
				}

				if err := runDecommissionNodeImpl(
					ctx, adminClient, nodeDecommissionWaitNone, deadNodes, tc.Server(0).NodeID(),
				); err != nil {
					t.Fatal(err)
				}

				for i := 0; i < len(tc.Servers); i++ {
					err = tc.Servers[i].Stores().VisitStores(func(store *kvserver.Store) error {
						store.SetReplicateQueueActive(true)
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := tc.WaitForFullReplication(); err != nil {
					t.Fatal(err)
				}

				for i := 0; i < len(tc.Servers); i++ {
					err = tc.Servers[i].Stores().VisitStores(func(store *kvserver.Store) error {
						return store.ForceConsistencyQueueProcess()
					})
					if err != nil {
						t.Fatal(err)
					}
				}

				expectedReplicas := []string{}
				for i := 0; i < testCase.totalNodes; i++ {
					var storeID int
					if i < testCase.survivingNodes {
						// If the replica survived, just adjust from zero-based to one-based.
						storeID = i + 1
					} else {
						storeID = i + 1 + testCase.totalNodes - testCase.survivingNodes
					}
					expectedReplicas = append(expectedReplicas, fmt.Sprintf("%d", storeID))
				}
				expectedReplicaStr := fmt.Sprintf("{%s}", strings.Join(expectedReplicas, ","))

				s := sqlutils.MakeSQLRunner(tc.Conns[0])
				row := s.QueryRow(t, "select replicas from [show ranges from table system.namespace] limit 1")
				var replicaStr string
				row.Scan(&replicaStr)
				if replicaStr != expectedReplicaStr {
					t.Fatalf("expected replicas on %s but got %s", expectedReplicaStr, replicaStr)
				}

				row = s.QueryRow(t, "show cluster setting cluster.organization")
				var org string
				row.Scan(&org)

				// If there was only one surviving node, we know we
				// recovered the write we did at the start of the test. But
				// if there were multiples, we might have picked a lagging
				// replica that didn't have it. See comments around
				// maxLivePeer in debug.go.
				//
				// TODO(bdarnell): It doesn't look to me like this is
				// guaranteed even in the single-survivor case, but it's
				// only flaky when there are multiple survivors.
				if testCase.survivingNodes == 1 {
					if org != "remove dead replicas test" {
						t.Fatalf("expected old setting to be present, got %s instead", org)
					}
				}
			})
	}
}

func TestParseGossipValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	var gossipInfo gossip.InfoStatus
	if err := serverutils.GetJSONProto(tc.Server(0), "/_status/gossip/1", &gossipInfo); err != nil {
		t.Fatal(err)
	}

	debugOutput, err := parseGossipValues(&gossipInfo)
	if err != nil {
		t.Fatal(err)
	}
	debugLines := strings.Split(debugOutput, "\n")
	if len(debugLines) != len(gossipInfo.Infos) {
		var gossipInfoKeys []string
		for key := range gossipInfo.Infos {
			gossipInfoKeys = append(gossipInfoKeys, key)
		}
		sort.Strings(gossipInfoKeys)
		t.Errorf("`debug gossip-values` output contains %d entries, but the source gossip contains %d:\ndebug output:\n%v\n\ngossipInfos:\n%v",
			len(debugLines), len(gossipInfo.Infos), debugOutput, strings.Join(gossipInfoKeys, "\n"))
	}
}

func TestParsePositiveDuration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	duration, _ := parsePositiveDuration("1h")
	if duration != time.Hour {
		t.Errorf("Expected %v, got %v", time.Hour, duration)
	}
	_, err := parsePositiveDuration("-5m")
	if err == nil {
		t.Errorf("Expected to fail parsing negative duration -5m")
	}
}
