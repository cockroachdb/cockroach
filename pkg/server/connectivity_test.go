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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
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
		bootstrapNode int

		// joinConfig[i] returns the node the i-th node is pointing to through
		// its join flags.
		joinConfig []int

		// expectedConnectivity[i] is whether or not the i-th node is expected
		// to learn about the cluster ID when the cluster is constructed using
		// the join flags specified through joinConfig.
		//
		// TODO(irfansharif): This is not really being used today because
		// TestCluster.Start cannot be pre-empted via context cancellation. It
		// expects all of the servers started through it to have been fully
		// initialized, which is at odds with what this test wants to do.
		//
		expectedConnectivity []bool
	}{
		// 0. Every node points to the first, including the first.
		{0, []int{0, 0, 0, 0, 0}, []bool{true, true, true, true, true}},

		// 1. Every node points to the previous, except the first, which points to
		// itself.
		//
		// 0 <-- 1 <-- 2 <-- 3 <-- 4
		{0, []int{0, 0, 1, 2, 3}, []bool{true, true, true, true, true}},

		// 2. Same as previous, but a few links switched around.
		//
		// 0 <-- 2 <-- 1 <-- 3 <-- 4
		{0, []int{0, 2, 0, 1, 3}, []bool{true, true, true, true, true}},

		// 3. Introduce a bidirectional link.
		//
		// 1 <-> 2 <-- 0 <-- 3
		// 1 <-- 4
		{1, []int{2, 2, 1, 0, 1}, []bool{true, true, true, true, true}},

		// 4. Same as above, but bootstrap the other node in the bidirectional
		// link.
		//
		// 1 <-> 2 <-- 0 <-- 3
		// 1 <-- 4
		{2, []int{2, 2, 1, 0, 1}, []bool{true, true, true, true, true}},

		// 5. Another topology centered around node 1, which itself is pointed
		// to node 0.
		//
		// 0 <-> 1 <-- 2
		//       1 <-- 3
		{0, []int{1, 0, 1, 1}, []bool{true, true, true, true, true}},

		// 6. Same as above, but bootstrapping the centered node directly.
		//
		// 0 <-> 1 <-- 2
		//       1 <-- 3
		{1, []int{1, 0, 1, 1}, []bool{true, true, true, true, true}},

		// 7. Each node points to itself.
		{2, []int{0, 1, 2}, []bool{false, false, true}},

		// 8. A subset of the cluster is connected, and bootstrap info correctly
		// propagates.
		//
		// 0 <-> 1 <-- 2
		// 5 <-- 4 <-- 3 <-- 5
		{1, []int{1, 0, 1, 4, 5, 3}, []bool{true, true, true, false, false, false}},

		// 9. Same as the above, but we're bootstrapping the other connected
		// component.
		//
		// 0 <-> 1 <-- 2
		// 5 <-- 4 <-- 3 <-- 5
		{3, []int{1, 0, 1, 4, 5, 3}, []bool{false, false, false, true, true, true}},
	}

	getListener := func() net.Listener {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return listener
	}
	commonArg := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			{InMemory: true},
		},
		// We're going to manually control initialization.
		NoAutoInitializeCluster: true,
	}

	for i, test := range testConfigurations {
		t.Run(fmt.Sprintf("topology=%d", i), func(t *testing.T) {
			numNodes := len(test.joinConfig)
			var serverArgsPerNode = make(map[int]base.TestServerArgs)

			// We start off with creating a listener for each server. We
			// pre-bind a listener so the kernel can go ahead and assign an
			// address for us. We'll later use this address to populate join
			// flags for neighboring nodes.
			var listeners = make([]net.Listener, numNodes)
			for i := 0; i < numNodes; i++ {
				serverArg := commonArg

				listener := getListener()
				serverArg.Listener = listener
				serverArg.Addr = listener.Addr().String()

				serverArgsPerNode[i] = serverArg
				listeners[i] = listener
			}

			// We'll annotate the server args with the right join flags.
			for i := 0; i < numNodes; i++ {
				serverArg := serverArgsPerNode[i]
				joinNode := test.joinConfig[i]
				joinAddr := listeners[joinNode].Addr().String()
				serverArg.JoinAddr = joinAddr
				serverArgsPerNode[i] = serverArg
			}

			tcArgs := base.TestClusterArgs{
				ReplicationMode:   base.ReplicationManual, // Saves time in this test.
				ServerArgsPerNode: serverArgsPerNode,
				ParallelStart:     true,
			}

			tc := testcluster.NewTestCluster(t, numNodes, tcArgs)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				// We need to start this in a separate thread because the
				// cluster is not initialized yet (it will be below), and is
				// this a blocking call.
				tc.Start(t, tcArgs)
				wg.Done()
			}()
			defer tc.Stopper().Stop(context.Background())
			defer wg.Wait()

			bootstrapNode := test.bootstrapNode
			testutils.SucceedsSoon(t, func() (e error) {
				ctx := context.Background()
				server := tc.Server(bootstrapNode)

				dialOpts, err := tc.Server(bootstrapNode).RPCContext().GRPCDialOptions()
				if err != nil {
					return err
				}

				conn, err := grpc.DialContext(ctx, server.ServingRPCAddr(), dialOpts...)
				if err != nil {
					return err
				}
				defer func() {
					_ = conn.Close()
				}()

				client := serverpb.NewInitClient(conn)
				_, err = client.Bootstrap(context.Background(), &serverpb.BootstrapRequest{})
				if err != nil {
					return err
				}

				clusterID := tc.Server(bootstrapNode).ClusterID()
				if clusterID.Equal(uuid.UUID{}) {
					return errors.New("cluster ID still not recorded")
				}
				return nil
			})

			clusterID := tc.Server(bootstrapNode).ClusterID()
			if clusterID.Equal(uuid.UUID{}) {
				t.Fatal("empty cluster ID found")
			}

			testutils.SucceedsSoon(t, func() error {
				var nodeIDs []roachpb.NodeID
				var storeIDs []roachpb.StoreID

				for i := 0; i < numNodes; i++ {
					got := tc.Server(i).ClusterID()
					if !test.expectedConnectivity[i] {
						if !got.Equal(uuid.UUID{}) {
							return errors.Newf("mismatched cluster IDs; expected empty UUID, got %s (for node %d)", got.String(), i)
						}
						continue
					}
					if got != clusterID {
						return errors.Newf("mismatched cluster IDs; %s (for node %d) != %s (for node %d)", clusterID.String(), bootstrapNode, got.String(), i)
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

			// We stop all the servers manually because certain subtests end up
			// creating disconnected networks where the server process is not
			// expected to start up completely.
			for i := 0; i < tc.NumServers(); i++ {
				tc.Server(i).Stopper().Stop(context.Background())
			}
		})
	}
}
