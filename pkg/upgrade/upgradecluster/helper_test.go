// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgradecluster

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"google.golang.org/grpc"
)

type NoopDialer struct{}

func (n NoopDialer) Dial(
	ctx context.Context, id roachpb.NodeID, class rpc.ConnectionClass,
) (*grpc.ClientConn, error) {
	return nil, nil
}

var _ NodeDialer = NoopDialer{}

func TestHelperEveryNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var mu syncutil.Mutex
	const numNodes = 3

	t.Run("with-node-addition", func(t *testing.T) {
		// Add a node mid-way through execution. We expect EveryNode to start
		// over from scratch and include the newly added node.
		tc := livenesspb.TestCreateNodeVitality(1, 2, 3)
		h := New(ClusterConfig{
			NodeLiveness: tc,
			Dialer:       NoopDialer{},
		})
		opCount := 0
		err := h.UntilClusterStable(ctx, retry.Options{
			// Speed up testing, run for at most 10 retries over a second.
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     100 * time.Millisecond,
			Multiplier:     1.0,
			MaxRetries:     10,
		}, func() error {
			return h.ForEveryNodeOrServer(ctx, "dummy-op", func(
				context.Context, serverpb.MigrationClient,
			) error {
				mu.Lock()
				defer mu.Unlock()

				opCount++
				if opCount == numNodes {
					tc.AddNextNode()
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
		tc := livenesspb.TestCreateNodeVitality(1, 2, 3)
		h := New(ClusterConfig{
			NodeLiveness: tc,
			Dialer:       NoopDialer{},
		})
		opCount := 0
		err := h.UntilClusterStable(ctx, retry.Options{
			// Speed up testing, run for at most 10 retries over a second.
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     100 * time.Millisecond,
			Multiplier:     1.0,
			MaxRetries:     10,
		}, func() error {
			return h.ForEveryNodeOrServer(ctx, "dummy-op", func(
				context.Context, serverpb.MigrationClient,
			) error {
				mu.Lock()
				defer mu.Unlock()

				opCount++
				if opCount == numNodes {
					tc.RestartNode(2)
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
		tc := livenesspb.TestCreateNodeVitality(1, 2, 3)
		h := New(ClusterConfig{
			NodeLiveness: tc,
			Dialer:       NoopDialer{},
		})
		expRe := "cluster not stable, nodes: n\\{1,2,3\\}, unavailable: n\\{2\\}"
		opCount := 0

		if err := h.UntilClusterStable(ctx, retry.Options{
			// Speed up testing, run for at most 10 retries over a second.
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     100 * time.Millisecond,
			Multiplier:     1.0,
			MaxRetries:     10,
		}, func() error {
			return h.ForEveryNodeOrServer(ctx, "dummy-op", func(
				context.Context, serverpb.MigrationClient,
			) error {
				mu.Lock()
				defer mu.Unlock()

				opCount++
				if opCount == 1 {
					tc.DownNode(downedNode)
				}
				return nil
			})
		}); !testutils.IsError(err, expRe) {
			t.Fatalf("expected error %q, got %q", expRe, err)
		}

		tc.RestartNode(downedNode)
		if err := h.UntilClusterStable(ctx, retry.Options{
			// Speed up testing, run for at most 10 retries over a second.
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     100 * time.Millisecond,
			Multiplier:     1.0,
			MaxRetries:     10,
		}, func() error {
			return h.ForEveryNodeOrServer(ctx, "dummy-op", func(
				context.Context, serverpb.MigrationClient,
			) error {
				return nil
			})
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestClusterNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	const numNodes = 3

	t.Run("retrieves-all", func(t *testing.T) {
		nl := livenesspb.TestCreateNodeVitality(1, 2, 3)
		ns, unavailable, err := NodesFromNodeLiveness(ctx, nl)
		if err != nil {
			t.Fatal(err)
		}

		if len(unavailable) > 0 {
			t.Fatalf("expected no unavailable nodes, found %d", len(unavailable))
		}

		if got := len(ns); got != numNodes {
			t.Fatalf("expected %d Nodes, got %d", numNodes, got)
		}

		for i := range ns {
			if exp := roachpb.NodeID(i + 1); exp != ns[i].ID {
				t.Fatalf("expected to find node ID %s, got %s", exp, ns[i].ID)
			}
			if ns[i].Epoch != 1 {
				t.Fatalf("expected to find Epoch=1, got %d", ns[i].Epoch)
			}
		}
	})

	t.Run("ignores-decommissioned", func(t *testing.T) {
		nl := livenesspb.TestCreateNodeVitality(1, 2, 3)

		const decommissionedNode = 3
		nl.Decommissioned(decommissionedNode, false)

		ns, _, err := NodesFromNodeLiveness(ctx, nl)
		if err != nil {
			t.Fatal(err)
		}

		if got := len(ns); got != numNodes-1 {
			t.Fatalf("expected %d Nodes, got %d", numNodes-1, got)
		}

		for i := range ns {
			if exp := roachpb.NodeID(i + 1); exp != ns[i].ID {
				t.Fatalf("expected to find node ID %s, got %s", exp, ns[i].ID)
			}
			if ns[i].Epoch != 1 {
				t.Fatalf("expected to find Epoch=1, got %d", ns[i].Epoch)
			}
		}
	})

	t.Run("errors-if-down", func(t *testing.T) {
		nl := livenesspb.TestCreateNodeVitality(1, 2, 3)
		const downedNode = 3
		nl.DownNode(downedNode)

		_, unavailable, err := NodesFromNodeLiveness(ctx, nl)
		if len(unavailable) != 1 {
			t.Fatalf("expected 1 unavailable node, found %d", len(unavailable))
		}
		if err != nil {
			t.Fatal(err)
		}
	})
}
