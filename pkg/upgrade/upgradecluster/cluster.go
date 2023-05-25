// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package upgradecluster provides implementations of upgrade.Cluster.
package upgradecluster

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc"
)

// Cluster mediates interacting with a cockroach cluster.
type Cluster struct {
	c ClusterConfig
}

// ClusterConfig configures a Cluster.
type ClusterConfig struct {

	// NodeLiveness is used to determine the set of nodes in the cluster.
	NodeLiveness NodeLiveness

	// Dialer constructs connections to other nodes.
	Dialer NodeDialer

	// RangeDescScanner paginates through all range descriptors.
	RangeDescScanner rangedesc.Scanner

	// DB provides access the kv.DB instance backing the cluster.
	//
	// TODO(irfansharif): We could hide the kv.DB instance behind an interface
	// to expose only relevant, vetted bits of kv.DB. It'll make our tests less
	// "integration-ey".
	DB *kv.DB
}

// NodeDialer abstracts connecting to other nodes in the cluster.
type NodeDialer interface {
	// Dial returns a grpc connection to the given node.
	Dial(context.Context, roachpb.NodeID, rpc.ConnectionClass) (*grpc.ClientConn, error)
}

// NodeLiveness is the subset of the interface satisfied by CRDB's node liveness
// component that the upgrade manager relies upon.
type NodeLiveness interface {
	ScanNodeVitalityFromKV(context.Context) (livenesspb.NodeVitalityMap, error)
}

// New constructs a new Cluster with the provided dependencies.
func New(cfg ClusterConfig) *Cluster {
	return &Cluster{c: cfg}
}

// UntilClusterStable is part of the upgrade.Cluster interface.
func (c *Cluster) UntilClusterStable(ctx context.Context, fn func() error) error {
	ns, err := NodesFromNodeLiveness(ctx, c.c.NodeLiveness)
	if err != nil {
		return err
	}

	for {
		if err := fn(); err != nil {
			return err
		}
		curNodes, err := NodesFromNodeLiveness(ctx, c.c.NodeLiveness)
		if err != nil {
			return err
		}

		if ok, diffs := ns.Identical(curNodes); !ok {
			log.Infof(ctx, "%s, retrying", diffs)
			ns = curNodes
			continue
		}
		break
	}
	return nil
}

// NumNodesOrTenantPods is part of the upgrade.Cluster interface.
func (c *Cluster) NumNodesOrServers(ctx context.Context) (int, error) {
	ns, err := NodesFromNodeLiveness(ctx, c.c.NodeLiveness)
	if err != nil {
		return 0, err
	}
	return len(ns), nil
}

// ForEveryNodeOrTenantPod is part of the upgrade.Cluster interface.
func (c *Cluster) ForEveryNodeOrServer(
	ctx context.Context, op string, fn func(context.Context, serverpb.MigrationClient) error,
) error {

	ns, err := NodesFromNodeLiveness(ctx, c.c.NodeLiveness)
	if err != nil {
		return err
	}

	// We'll want to rate limit outgoing RPCs (limit pulled out of thin air).
	qp := quotapool.NewIntPool("every-node", 25)
	log.Infof(ctx, "executing %s on nodes %s", redact.Safe(op), ns)
	grp := ctxgroup.WithContext(ctx)

	for _, node := range ns {
		id := node.ID // copy out of the loop variable
		alloc, err := qp.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		grp.GoCtx(func(ctx context.Context) error {
			defer alloc.Release()

			conn, err := c.c.Dialer.Dial(ctx, id, rpc.DefaultClass)
			if err != nil {
				return err
			}
			client := serverpb.NewMigrationClient(conn)
			return fn(ctx, client)
		})
	}
	return grp.Wait()
}

// IterateRangeDescriptors is part of the upgrade.Cluster interface.
func (c *Cluster) IterateRangeDescriptors(
	ctx context.Context, blockSize int, init func(), fn func(...roachpb.RangeDescriptor) error,
) error {
	return c.c.RangeDescScanner.Scan(ctx, blockSize, init, keys.EverythingSpan, fn)
}

// ValidateAfterUpdateSystemVersion is part of the upgrade.Cluster interface.
func (c *Cluster) ValidateAfterUpdateSystemVersion(_ context.Context) error {
	return nil
}
