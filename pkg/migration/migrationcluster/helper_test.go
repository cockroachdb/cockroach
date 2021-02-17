// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationcluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/migration/nodelivenesstest"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
		tc := nodelivenesstest.New(numNodes)
		h := New(ClusterConfig{
			NodeLiveness: tc,
			Dialer:       NoopDialer{},
		})
		opCount := 0
		err := h.UntilClusterStable(ctx, func() error {
			return h.ForEveryNode(ctx, "dummy-op", func(
				context.Context, serverpb.MigrationClient,
			) error {
				mu.Lock()
				defer mu.Unlock()

				opCount++
				if opCount == numNodes {
					tc.AddNewNode()
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
		tc := nodelivenesstest.New(numNodes)
		h := New(ClusterConfig{
			NodeLiveness: tc,
			Dialer:       NoopDialer{},
		})
		opCount := 0
		err := h.UntilClusterStable(ctx, func() error {
			return h.ForEveryNode(ctx, "dummy-op", func(
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
		tc := nodelivenesstest.New(numNodes)
		h := New(ClusterConfig{
			NodeLiveness: tc,
			Dialer:       NoopDialer{},
		})
		expRe := fmt.Sprintf("n%d required, but unavailable", downedNode)
		opCount := 0
		if err := h.UntilClusterStable(ctx, func() error {
			return h.ForEveryNode(ctx, "dummy-op", func(
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
		if err := h.UntilClusterStable(ctx, func() error {
			return h.ForEveryNode(ctx, "dummy-op", func(
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
		nl := nodelivenesstest.New(numNodes)
		ns, err := NodesFromNodeLiveness(ctx, nl)
		if err != nil {
			t.Fatal(err)
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
		nl := nodelivenesstest.New(numNodes)

		const decommissionedNode = 3
		nl.Decommission(decommissionedNode)

		ns, err := NodesFromNodeLiveness(ctx, nl)
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
		nl := nodelivenesstest.New(numNodes)
		const downedNode = 3
		nl.DownNode(downedNode)

		_, err := NodesFromNodeLiveness(ctx, nl)
		expRe := fmt.Sprintf("n%d required, but unavailable", downedNode)
		if !testutils.IsError(err, expRe) {
			t.Fatalf("expected error %q, got %q", expRe, err)
		}
	})
}
