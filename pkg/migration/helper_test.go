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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

func TestHelperEveryNode(t *testing.T) {
	defer leaktest.AfterTest(t)

	cv := clusterversion.ClusterVersion{}
	ctx := context.Background()
	var mu syncutil.Mutex
	const numNodes = 3

	t.Run("with-node-addition", func(t *testing.T) {
		// Add a node mid-way through execution. We expect EveryNode to start
		// over from scratch and include the newly added node.
		tc := TestingNewCluster(numNodes, nil, nil)
		h := newHelper(tc, cv)
		opCount := 0
		err := h.EveryNode(ctx, "dummy-op", func(context.Context, serverpb.MigrationClient) error {
			mu.Lock()
			defer mu.Unlock()

			opCount += 1
			if opCount == numNodes {
				tc.addNode()
			}

			return nil
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
		tc := TestingNewCluster(numNodes, nil, nil)
		h := newHelper(tc, cv)
		opCount := 0
		err := h.EveryNode(ctx, "dummy-op", func(context.Context, serverpb.MigrationClient) error {
			mu.Lock()
			defer mu.Unlock()

			opCount += 1
			if opCount == numNodes {
				tc.restartNode(2)
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if exp := numNodes * 2; exp != opCount {
			t.Fatalf("expected closure to be invoked %d times, got %d", exp, opCount)
		}
	})

	t.Run("with-node-down", func(t *testing.T) {
		// Down a node mid-way through execution. We expect EveryNode to error
		// out.
		const downedNode = 2
		tc := TestingNewCluster(numNodes, nil, nil)
		tc.downNode(downedNode)
		expRe := fmt.Sprintf("n%d required, but unavailable", downedNode)
		h := newHelper(tc, cv)
		if err := h.EveryNode(ctx, "dummy-op", func(context.Context, serverpb.MigrationClient) error {
			return nil
		}); !testutils.IsError(err, expRe) {
			t.Fatalf("expected error %q, got %q", expRe, err)
		}

		tc.healCluster()
		if err := h.EveryNode(ctx, "dummy-op", func(context.Context, serverpb.MigrationClient) error {
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestClusterNodes(t *testing.T) {
	defer leaktest.AfterTest(t)

	ctx := context.Background()
	const numNodes = 3

	t.Run("retrieve-all", func(t *testing.T) {
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

	t.Run("ignore-decommissioned", func(t *testing.T) {
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

	t.Run("error-if-down", func(t *testing.T) {
		nl := newTestNodeLiveness(numNodes)
		c := clusterImpl{nl: nl}
		const downedNode = 3
		nl.down(downedNode)

		_, err := c.nodes(ctx)
		expRe := fmt.Sprintf("n%d required, but unavailable", downedNode)
		if !testutils.IsError(err, expRe) {
			t.Fatalf("expected error %q, got %q", expRe, err)
		}
	})
}

type testClusterImpl struct {
	ns   nodes
	err  error
	kvDB *kv.DB
	exec sqlutil.InternalExecutor
}

var _ cluster = &testClusterImpl{}

func TestingNewCluster(numNodes int, db *kv.DB, exec sqlutil.InternalExecutor) *testClusterImpl {
	tc := &testClusterImpl{
		kvDB: db,
		exec: exec,
	}
	for i := 0; i < numNodes; i++ {
		tc.addNode()
	}

	return tc
}

func (t *testClusterImpl) nodes(context.Context) (nodes, error) {
	ns := make([]node, len(t.ns))
	for i, n := range t.ns {
		ns[i] = n
	}
	return ns, t.err
}

func (t *testClusterImpl) dial(context.Context, roachpb.NodeID) (*grpc.ClientConn, error) {
	return nil, nil
}

func (t *testClusterImpl) db() *kv.DB {
	return t.kvDB
}

func (t *testClusterImpl) executor() sqlutil.InternalExecutor {
	return t.exec
}

func (t *testClusterImpl) addNode() {
	t.ns = append(t.ns, node{id: roachpb.NodeID(len(t.ns) + 1), epoch: 1})
}

func (t *testClusterImpl) restartNode(id int) {
	for i := range t.ns {
		if t.ns[i].id == roachpb.NodeID(id) {
			t.ns[i].epoch++
			break
		}
	}
}

func (t *testClusterImpl) downNode(id int) {
	t.err = errors.Newf("n%d required, but unavailable", id)
}

func (t *testClusterImpl) healCluster() {
	t.err = nil
}

type testNodeLivenessImpl struct {
	ls   []livenesspb.Liveness
	dead map[roachpb.NodeID]struct{}
}

var _ nodeLiveness = &testNodeLivenessImpl{}

func newTestNodeLiveness(numNodes int) *testNodeLivenessImpl {
	nl := &testNodeLivenessImpl{
		ls:   make([]livenesspb.Liveness, numNodes),
		dead: make(map[roachpb.NodeID]struct{}),
	}
	for i := 0; i < numNodes; i++ {
		nl.ls[i] = livenesspb.Liveness{
			NodeID:     roachpb.NodeID(i + 1),
			Epoch:      1,
			Membership: livenesspb.MembershipStatus_ACTIVE,
		}
	}
	return nl
}

func (t *testNodeLivenessImpl) GetLivenessesFromKV(
	ctx context.Context,
) ([]livenesspb.Liveness, error) {
	return t.ls, nil
}

func (t *testNodeLivenessImpl) IsLive(id roachpb.NodeID) (bool, error) {
	_, dead := t.dead[id]
	return !dead, nil
}

func (t *testNodeLivenessImpl) decommission(id roachpb.NodeID) {
	for i := range t.ls {
		if t.ls[i].NodeID == id {
			t.ls[i].Membership = livenesspb.MembershipStatus_DECOMMISSIONED
			break
		}
	}
}

func (t *testNodeLivenessImpl) down(id roachpb.NodeID) {
	t.dead[id] = struct{}{}
}

func TestingNewHelper(c cluster, cv clusterversion.ClusterVersion) *Helper {
	return &Helper{c: c, cv: cv}
}

func (h *Helper) TestingInsertMigrationRecord(ctx context.Context, desc string) error {
	return h.insertMigrationRecord(ctx, desc)
}

func (h *Helper) TestingUpdateStatus(ctx context.Context, status Status) error {
	return h.updateStatus(ctx, status)
}
