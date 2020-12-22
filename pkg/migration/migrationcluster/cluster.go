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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/redact"
	"github.com/pkg/errors"
)

// Cluster mediates interacting with a cockroach cluster.
type Cluster struct {
	c ClusterConfig
}

// New constructs a new Cluster with the provided dependencies.
func New(cfg ClusterConfig) *Cluster {
	return &Cluster{c: cfg}
}

// EveryNode is part of the migration.Cluster interface.
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

// ForEveryNode is part of the migration.Cluster interface.
func (c *Cluster) ForEveryNode(
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

// IterateRangeDescriptors is part of the migration.Cluster interface.
func (c *Cluster) IterateRangeDescriptors(
	ctx context.Context, blockSize int, init func(), fn func(...roachpb.RangeDescriptor) error,
) error {
	if err := c.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Inform the caller that we're starting a fresh attempt to page in
		// range descriptors.
		init()

		// Iterate through meta2 to pull out all the range descriptors.
		return txn.Iterate(ctx, keys.Meta2Prefix, keys.MetaMax, blockSize,
			func(rows []kv.KeyValue) error {
				descriptors := make([]roachpb.RangeDescriptor, len(rows))
				for i, row := range rows {
					if err := row.ValueProto(&descriptors[i]); err != nil {
						return errors.Wrapf(err,
							"unable to unmarshal range descriptor from %s",
							row.Key,
						)
					}
				}

				// Invoke fn with the current chunk (of size ~blockSize) of
				// range descriptors.
				if err := fn(descriptors...); err != nil {
					return err
				}

				return nil
			})
	}); err != nil {
		return err
	}

	return nil
}

// DB provides exposes the underlying *kv.DB instance.
func (c *Cluster) DB() *kv.DB {
	return c.c.DB
}
