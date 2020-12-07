// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"google.golang.org/grpc"
)

func TestHelperEveryNodeUntilClusterStable(t *testing.T) {
	defer leaktest.AfterTest(t)

	cv := clusterversion.ClusterVersion{}
	ctx := context.Background()
	var mu syncutil.Mutex
	const numNodes = 3

	t.Run("with-node-addition", func(t *testing.T) {
		// Add a node mid-way through execution. We expect EveryNode to start
		// over from scratch and include the newly added node.
		tc := TestingNewCluster(numNodes)
		h := newHelper(tc, cv)
		opCount := 0
		err := h.UntilClusterStable(ctx, func() error {
			return h.ForEveryNode(ctx, "dummy-op", func(context.Context, serverpb.MigrationClient) error {
				mu.Lock()
				defer mu.Unlock()

				opCount++
				if opCount == numNodes {
					tc.addNode()
				}

				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}

		if exp := numNodes*2 + 1; exp != opCount {
			t.Fatalf("expected closure to be invoked %d times, got %d", exp, opCount)
		}
	})

	t.Run("with-node-restart", func(t *testing.T) {
		// Restart a node mid-way through execution. We expect EveryNode to
		// start over from scratch and include the restarted node.
		tc := TestingNewCluster(numNodes)
		h := newHelper(tc, cv)
		opCount := 0
		err := h.UntilClusterStable(ctx, func() error {
			return h.ForEveryNode(ctx, "dummy-op", func(context.Context, serverpb.MigrationClient) error {
				mu.Lock()
				defer mu.Unlock()

				opCount++
				if opCount == numNodes {
					tc.restartNode(2)
				}

				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}

		if exp := numNodes * 2; exp != opCount {
			t.Fatalf("expected closure to be invoked %d times, got %d", exp, opCount)
		}
	})

	t.Run("with-node-downNode", func(t *testing.T) {
		// Down a node mid-way through execution. We expect EveryNode to error
		// out.
		const downedNode = 2
		tc := TestingNewCluster(numNodes)
		expRe := fmt.Sprintf("n%d required, but unavailable", downedNode)
		h := newHelper(tc, cv)
		opCount := 0
		if err := h.UntilClusterStable(ctx, func() error {
			return h.ForEveryNode(ctx, "dummy-op", func(context.Context, serverpb.MigrationClient) error {
				mu.Lock()
				defer mu.Unlock()

				opCount++
				if opCount == 1 {
					tc.downNode(downedNode)
				}
				return nil
			})
		}); !testutils.IsError(err, expRe) {
			t.Fatalf("expected error %q, got %q", expRe, err)
		}

		tc.restartNode(downedNode)
		if err := h.UntilClusterStable(ctx, func() error {
			return h.ForEveryNode(ctx, "dummy-op", func(context.Context, serverpb.MigrationClient) error {
				return nil
			})
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestClusterNodes(t *testing.T) {
	defer leaktest.AfterTest(t)

	ctx := context.Background()
	const numNodes = 3

	t.Run("retrieves-all", func(t *testing.T) {
		nl := newTestNodeLiveness(numNodes)
		c := clusterImpl{nl: nl}

		ns, err := c.nodes(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if got := len(ns); got != numNodes {
			t.Fatalf("expected %d nodes, got %d", numNodes, got)
		}

		for i := range ns {
			if exp := roachpb.NodeID(i + 1); exp != ns[i].id {
				t.Fatalf("expected to find node ID %s, got %s", exp, ns[i].id)
			}
			if ns[i].epoch != 1 {
				t.Fatalf("expected to find epoch=1, got %d", ns[i].epoch)
			}
		}
	})

	t.Run("ignores-decommissioned", func(t *testing.T) {
		nl := newTestNodeLiveness(numNodes)
		c := clusterImpl{nl: nl}
		const decommissionedNode = 3
		nl.decommission(decommissionedNode)

		ns, err := c.nodes(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if got := len(ns); got != numNodes-1 {
			t.Fatalf("expected %d nodes, got %d", numNodes-1, got)
		}

		for i := range ns {
			if exp := roachpb.NodeID(i + 1); exp != ns[i].id {
				t.Fatalf("expected to find node ID %s, got %s", exp, ns[i].id)
			}
			if ns[i].epoch != 1 {
				t.Fatalf("expected to find epoch=1, got %d", ns[i].epoch)
			}
		}
	})

	t.Run("errors-if-down", func(t *testing.T) {
		nl := newTestNodeLiveness(numNodes)
		c := clusterImpl{nl: nl}
		const downedNode = 3
		nl.downNode(downedNode)

		_, err := c.nodes(ctx)
		expRe := fmt.Sprintf("n%d required, but unavailable", downedNode)
		if !testutils.IsError(err, expRe) {
			t.Fatalf("expected error %q, got %q", expRe, err)
		}
	})
}

// mockClusterImpl is a testing only implementation of the cluster interface. It
// lets callers mock out adding, killing, and restarting nodes in the cluster.
type mockClusterImpl struct {
	nl *mockNodeLivenessImpl
	*clusterImpl
}

var _ cluster = &mockClusterImpl{}

// TestingNewCluster is an exported a constructor for a test-only implementation
// of the cluster interface.
func TestingNewCluster(numNodes int, options ...func(*mockClusterImpl)) *mockClusterImpl {
	nl := newTestNodeLiveness(numNodes)
	tc := &mockClusterImpl{
		nl:          nl,
		clusterImpl: newCluster(nl, nil, nil, nil),
	}
	for _, option := range options {
		option(tc)
	}
	return tc
}

// TestingWithKV facilitates the creation of a test cluster backed by the given
// KV instance.
func TestingWithKV(db *kv.DB) func(*mockClusterImpl) {
	return func(impl *mockClusterImpl) {
		impl.clusterImpl.kvDB = db
	}
}

// dial is part of the cluster interface. We override it here as tests don't
// expect to make any outbound requests.
func (t *mockClusterImpl) dial(context.Context, roachpb.NodeID) (*grpc.ClientConn, error) {
	return nil, nil
}

func (t *mockClusterImpl) addNode() {
	t.nl.addNode(roachpb.NodeID(len(t.nl.ls) + 1))
}

func (t *mockClusterImpl) downNode(id roachpb.NodeID) {
	t.nl.downNode(id)
}

func (t *mockClusterImpl) restartNode(id roachpb.NodeID) {
	t.nl.restartNode(id)
}

// mockNodeLivenessImpl is a testing-only implementation of the nodeLiveness. It
// lets tests mock out restarting, killing, decommissioning and adding nodes to
// the cluster.
type mockNodeLivenessImpl struct {
	ls   []livenesspb.Liveness
	dead map[roachpb.NodeID]struct{}
}

var _ nodeLiveness = &mockNodeLivenessImpl{}

func newTestNodeLiveness(numNodes int) *mockNodeLivenessImpl {
	nl := &mockNodeLivenessImpl{
		ls:   make([]livenesspb.Liveness, numNodes),
		dead: make(map[roachpb.NodeID]struct{}),
	}
	for i := 0; i < numNodes; i++ {
		nl.ls[i] = livenesspb.Liveness{
			NodeID: roachpb.NodeID(i + 1), Epoch: 1,
			Membership: livenesspb.MembershipStatus_ACTIVE,
		}
	}
	return nl
}

// GetLivenessesFromKV implements the nodeLiveness interface.
func (t *mockNodeLivenessImpl) GetLivenessesFromKV(context.Context) ([]livenesspb.Liveness, error) {
	return t.ls, nil
}

// IsLive implements the nodeLiveness interface.
func (t *mockNodeLivenessImpl) IsLive(id roachpb.NodeID) (bool, error) {
	_, dead := t.dead[id]
	return !dead, nil
}

func (t *mockNodeLivenessImpl) decommission(id roachpb.NodeID) {
	for i := range t.ls {
		if t.ls[i].NodeID == id {
			t.ls[i].Membership = livenesspb.MembershipStatus_DECOMMISSIONED
			break
		}
	}
}

func (t *mockNodeLivenessImpl) addNode(id roachpb.NodeID) {
	t.ls = append(t.ls, livenesspb.Liveness{
		NodeID:     id,
		Epoch:      1,
		Membership: livenesspb.MembershipStatus_ACTIVE,
	})
}

func (t *mockNodeLivenessImpl) downNode(id roachpb.NodeID) {
	t.dead[id] = struct{}{}
}

func (t *mockNodeLivenessImpl) restartNode(id roachpb.NodeID) {
	for i := range t.ls {
		if t.ls[i].NodeID == id {
			t.ls[i].Epoch++
			break
		}
	}

	delete(t.dead, id)
}

// TestingNewHelper is an exported a constructor for Helper for testing
// purposes.
func TestingNewHelper(c cluster, cv clusterversion.ClusterVersion) *Helper {
	return &Helper{c: c, cv: cv}
}
