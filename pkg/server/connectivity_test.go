// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// TestClusterConnectivity sets up an uninitialized cluster with custom join
// flags (individual nodes point to specific others, instead of all pointing to
// n1), and tests that the cluster/node IDs are distributed correctly
// throughout.
func TestClusterConnectivity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(irfansharif): Teach TestServer to accept a list of join addresses
	// instead of just one.

	var testConfigurations = []struct {
		// bootstrapNode controls which node is `cockroach init`-ialized.
		// Everything is 0-indexed.
		bootstrapNode int

		// joinConfig[i] returns the node the i-th node is pointing to through
		// its join flags. Everything is 0-indexed.
		joinConfig []int
	}{
		// 0. Every node points to the first, including the first.
		{0, []int{0, 0, 0, 0, 0}},

		// 1. Every node points to the previous, except the first, which points to
		// itself.
		//
		// 0 <-- 1 <-- 2 <-- 3 <-- 4
		{0, []int{0, 0, 1, 2, 3}},

		// 2. Same as previous, but a few links switched around.
		//
		// 0 <-- 2 <-- 1 <-- 3 <-- 4
		{0, []int{0, 2, 0, 1, 3}},

		// 3. Introduce a bidirectional link.
		//
		// 1 <-> 2 <-- 0 <-- 3
		// 1 <-- 4
		{1, []int{2, 2, 1, 0, 1}},

		// 4. Same as above, but bootstrap the other node in the bidirectional
		// link.
		//
		// 1 <-> 2 <-- 0 <-- 3
		// 1 <-- 4
		{2, []int{2, 2, 1, 0, 1}},

		// 5. Another topology centered around node 1, which itself is pointed
		// to node 0.
		//
		// 0 <-> 1 <-- 2
		//       1 <-- 3
		{0, []int{1, 0, 1, 1}},

		// 6. Same as above, but bootstrapping the centered node directly.
		//
		// 0 <-> 1 <-- 2
		//       1 <-- 3
		{1, []int{1, 0, 1, 1}},

		// TODO(irfansharif): We would really like to be able to set up test
		// clusters that are only partially connected, and assert that only
		// nodes that are supposed to find out about bootstrap, actually do.
		// Something like:
		//
		// 		0 <-> 1 <-- 2
		// 		5 <-- 4 <-- 3 <-- 5
		//
		// A version of this was originally prototyped in #52526 but the changes
		// required in Test{Cluster,Server} were too invasive to justify at the
		// time.
	}

	// getListener is a short hand to allocate a listener to an unbounded port.
	getListener := func() net.Listener {
		t.Helper()

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return listener
	}
	baseServerArgs := base.TestServerArgs{
		// We're going to manually control initialization in this test.
		NoAutoInitializeCluster: true,
		StoreSpecs:              []base.StoreSpec{{InMemory: true}},
	}

	for i, test := range testConfigurations {
		t.Run(fmt.Sprintf("topology=%d", i), func(t *testing.T) {
			numNodes := len(test.joinConfig)
			var serverArgsPerNode = make(map[int]base.TestServerArgs)

			// We start off with installing a listener for each server. We
			// pre-bind a listener so the kernel can go ahead and assign an
			// address for us. We'll later use this address to populate join
			// flags for neighboring nodes.
			var listeners = make([]net.Listener, numNodes)
			for i := 0; i < numNodes; i++ {
				listener := getListener()

				serverArg := baseServerArgs
				serverArg.Listener = listener
				serverArg.Addr = listener.Addr().String()
				serverArgsPerNode[i] = serverArg
				listeners[i] = listener
			}

			// We'll annotate the server args with the right join flags.
			for i := 0; i < numNodes; i++ {
				joinNode := test.joinConfig[i]
				joinAddr := listeners[joinNode].Addr().String()

				serverArg := serverArgsPerNode[i]
				serverArg.JoinAddr = joinAddr
				serverArgsPerNode[i] = serverArg
			}

			tcArgs := base.TestClusterArgs{
				// Saves time in this test.
				ReplicationMode:   base.ReplicationManual,
				ServerArgsPerNode: serverArgsPerNode,

				// We have to start servers in parallel because we're looking to
				// bootstrap the cluster manually in a separate thread. Each
				// individual Server.Start is a blocking call (it waits for
				// init). We want to start all of them in parallel to simulate a
				// bunch of servers each waiting for init.
				ParallelStart: true,
			}

			// The test structure here is a bit convoluted, but necessary given
			// the current implementation of TestCluster. TestCluster.Start
			// wants to wait for all the nodes in the test cluster to be fully
			// initialized before returning. Given we're testing initialization
			// behavior, we do all the real work in a separate thread and keep
			// the main thread limited to simply starting and stopping the test
			// cluster.
			//
			// NB: That aside, TestCluster very much wants to live on the main
			// goroutine running the test. That's mostly to do with its internal
			// error handling and the limitations imposed by
			// https://golang.org/pkg/testing/#T.FailNow (which sits underneath
			// t.Fatal).

			tc := testcluster.NewTestCluster(t, numNodes, tcArgs)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Attempt to bootstrap the cluster through the configured node.
				bootstrapNode := test.bootstrapNode
				testutils.SucceedsSoon(t, func() (e error) {
					ctx := context.Background()
					serv := tc.Server(bootstrapNode)

					dialOpts, err := tc.Server(bootstrapNode).RPCContext().GRPCDialOptions()
					if err != nil {
						return err
					}

					conn, err := grpc.DialContext(ctx, serv.ServingRPCAddr(), dialOpts...)
					if err != nil {
						return err
					}
					defer func() {
						_ = conn.Close() // nolint:grpcconnclose
					}()

					client := serverpb.NewInitClient(conn)
					_, err = client.Bootstrap(context.Background(), &serverpb.BootstrapRequest{})
					return err
				})

				// Wait to get a real cluster ID (doesn't always get populated
				// right after bootstrap).
				testutils.SucceedsSoon(t, func() error {
					clusterID := tc.Server(bootstrapNode).ClusterID()
					if clusterID.Equal(uuid.UUID{}) {
						return errors.New("cluster ID still not recorded")
					}
					return nil
				})

				clusterID := tc.Server(bootstrapNode).ClusterID()
				testutils.SucceedsSoon(t, func() error {
					var nodeIDs []roachpb.NodeID
					var storeIDs []roachpb.StoreID

					// Sanity check that all the nodes we expect to join this
					// network actually do (by checking they discover the right
					// cluster ID). Also collect node/store IDs for below.
					for i := 0; i < numNodes; i++ {
						if got := tc.Server(i).ClusterID(); got != clusterID {
							return errors.Newf("mismatched cluster IDs; %s (for node %d) != %s (for node %d)",
								clusterID.String(), bootstrapNode, got.String(), i)
						}

						nodeIDs = append(nodeIDs, tc.Server(i).NodeID())
						storeIDs = append(storeIDs, tc.Server(i).GetFirstStoreID())
					}

					sort.Slice(nodeIDs, func(i, j int) bool {
						return nodeIDs[i] < nodeIDs[j]
					})
					sort.Slice(storeIDs, func(i, j int) bool {
						return storeIDs[i] < storeIDs[j]
					})

					// Double check that we have the full set of node/store IDs
					// we expect.
					for i := 1; i <= len(nodeIDs); i++ {
						expNodeID := roachpb.NodeID(i)
						if got := nodeIDs[i-1]; got != expNodeID {
							return errors.Newf("unexpected node ID; expected %s, got %s", expNodeID.String(), got.String())
						}

						expStoreID := roachpb.StoreID(i)
						if got := storeIDs[i-1]; got != expStoreID {
							return errors.Newf("unexpected store ID; expected %s, got %s", expStoreID.String(), got.String())
						}
					}

					return nil
				})
			}()

			// Start the test cluster. This is a blocking call, and expects the
			// configured number of servers in the cluster to be fully
			// initialized before it returns. Given that the initialization
			// happens in the other thread, we'll only get past it after having
			// bootstrapped the test cluster in the thread above.
			tc.Start(t)
			defer tc.Stopper().Stop(context.Background())

			wg.Wait()
		})
	}
}

// TestJoinVersionGate checks to see that improperly versioned cockroach nodes
// are not able to join a cluster.
func TestJoinVersionGate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	commonArg := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			{InMemory: true},
		},
	}

	numNodes := 3
	tcArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // Saves time in this test.
		ServerArgs:      commonArg,
		ParallelStart:   true,
	}

	tc := testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(context.Background())

	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < numNodes; i++ {
			clusterID := tc.Server(0).ClusterID()
			got := tc.Server(i).ClusterID()

			if got != clusterID {
				return errors.Newf("mismatched cluster IDs; %s (for node %d) != %s (for node %d)", clusterID.String(), 0, got.String(), i)
			}
		}
		return nil
	})

	var newVersion = clusterversion.TestingBinaryVersion
	var oldVersion = prev(newVersion)

	knobs := base.TestingKnobs{
		Server: &server.TestingKnobs{
			BinaryVersionOverride: oldVersion,
		},
	}

	oldVersionServerArgs := commonArg
	oldVersionServerArgs.Knobs = knobs
	oldVersionServerArgs.JoinAddr = tc.Servers[0].ServingRPCAddr()

	serv, err := tc.AddServer(oldVersionServerArgs)
	if err != nil {
		t.Fatal(err)
	}
	defer serv.Stop()

	ctx := context.Background()
	if err := serv.Start(ctx); !errors.Is(errors.Cause(err), server.ErrIncompatibleBinaryVersion) {
		t.Fatalf("expected error %s, got %v", server.ErrIncompatibleBinaryVersion.Error(), err.Error())
	}
}

func TestDecommissionedNodeCannotConnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	numNodes := 3
	tcArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
	}

	tc := testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
	require.Len(t, scratchRange.InternalReplicas, 1)
	require.Equal(t, tc.Server(0).NodeID(), scratchRange.InternalReplicas[0].NodeID)

	decomSrv := tc.Server(2)
	for _, status := range []livenesspb.MembershipStatus{
		livenesspb.MembershipStatus_DECOMMISSIONING, livenesspb.MembershipStatus_DECOMMISSIONED,
	} {
		require.NoError(t, tc.Servers[0].Decommission(ctx, status, []roachpb.NodeID{decomSrv.NodeID()}))
	}

	testutils.SucceedsSoon(t, func() error {
		for _, idx := range []int{0, 1} {
			clusterSrv := tc.Server(idx)

			// Within a short period of time, the cluster (n1, n2) will refuse to reach out to n3.
			_, err := clusterSrv.RPCContext().GRPCDialNode(
				decomSrv.RPCAddr(), decomSrv.NodeID(), rpc.DefaultClass,
			).Connect(ctx)
			s, ok := grpcstatus.FromError(errors.UnwrapAll(err))
			if !ok || s.Code() != codes.FailedPrecondition {
				return errors.Errorf("expected failed precondition for n%d->n%d, got %v", clusterSrv.NodeID(), decomSrv.NodeID(), err)
			}

			// And similarly, n3 will be refused by n1, n2.
			_, err = decomSrv.RPCContext().GRPCDialNode(
				clusterSrv.RPCAddr(), clusterSrv.NodeID(), rpc.DefaultClass,
			).Connect(ctx)
			s, ok = grpcstatus.FromError(errors.UnwrapAll(err))
			if !ok || s.Code() != codes.PermissionDenied {
				return errors.Errorf("expected permission denied for n%d->n%d, got %v", decomSrv.NodeID(), clusterSrv.NodeID(), err)
			}
		}

		// Trying to scan the scratch range from the decommissioned node should
		// now result in a permission denied error.
		_, err := decomSrv.DB().Scan(ctx, scratchKey, keys.MaxKey, 1)
		s, ok := grpcstatus.FromError(errors.UnwrapAll(err))
		if !ok || s.Code() != codes.PermissionDenied {
			return errors.Errorf("expected permission denied for scan, got %v", err)
		}
		return nil
	})
}
