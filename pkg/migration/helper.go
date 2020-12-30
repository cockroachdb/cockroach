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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc"
)

// Helper captures all the primitives required to fully specify a migration.
type Helper struct {
	c  cluster
	cv clusterversion.ClusterVersion
}

// cluster mediates access to the crdb cluster.
type cluster interface {
	// nodes returns the IDs and epochs for all nodes that are currently part of
	// the cluster (i.e. they haven't been decommissioned away). Migrations have
	// the pre-requisite that all nodes are up and running so that we're able to
	// execute all relevant node-level operations on them. If any of the nodes
	// are found to be unavailable, an error is returned.
	//
	// It's important to note that this makes no guarantees about new nodes
	// being added to the cluster. It's entirely possible for that to happen
	// concurrently with the retrieval of the current set of nodes. Appropriate
	// usage of this entails wrapping it under a stabilizing loop, like we do in
	// EveryNode.
	nodes(ctx context.Context) (nodes, error)

	// dial returns a grpc connection to the given node.
	dial(context.Context, roachpb.NodeID) (*grpc.ClientConn, error)

	// db provides access the kv.DB instance backing the cluster.
	//
	// TODO(irfansharif): We could hide the kv.DB instance behind an interface
	// to expose only relevant, vetted bits of kv.DB. It'll make our tests less
	// "integration-ey".
	db() *kv.DB

	// executor provides access to an internal executor instance to run
	// arbitrary SQL statements.
	executor() sqlutil.InternalExecutor
}

func newHelper(c cluster, cv clusterversion.ClusterVersion) *Helper {
	return &Helper{c: c, cv: cv}
}

// ForEveryNode is a short hand to execute the given closure (named by the
// informational parameter op) against every node in the cluster at a given
// point in time. Given it's possible for nodes to join or leave the cluster
// during (we don't make any guarantees for the ordering of cluster membership
// events), we only expect this to be used in conjunction with
// UntilClusterStable (see the comment there for how these two primitives can be
// put together).
func (h *Helper) ForEveryNode(
	ctx context.Context, op string, fn func(context.Context, serverpb.MigrationClient) error,
) error {
	ns, err := h.c.nodes(ctx)
	if err != nil {
		return err
	}

	// We'll want to rate limit outgoing RPCs (limit pulled out of thin air).
	qp := quotapool.NewIntPool("every-node", 25)
	log.Infof(ctx, "executing %s on nodes %s", redact.Safe(op), ns)
	grp := ctxgroup.WithContext(ctx)

	for _, node := range ns {
		id := node.id // copy out of the loop variable
		alloc, err := qp.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		grp.GoCtx(func(ctx context.Context) error {
			defer alloc.Release()

			conn, err := h.c.dial(ctx, id)
			if err != nil {
				return err
			}
			client := serverpb.NewMigrationClient(conn)
			return fn(ctx, client)
		})
	}
	return grp.Wait()
}

// UntilClusterStable invokes the given closure until the cluster membership is
// stable, i.e once the set of nodes in the cluster before and after the closure
// are identical, and no nodes have restarted in the interim, we can return to
// the caller[*].
//
// The mechanism for doing so, while accounting for the possibility of new nodes
// being added to the cluster in the interim, is provided by the following
// structure:
//   (a) We'll retrieve the list of node IDs for all nodes in the system
//   (b) We'll invoke the closure
//   (c) We'll retrieve the list of node IDs again to account for the
//       possibility of a new node being added during (b), or a node
//       restarting
//   (d) If there any discrepancies between the list retrieved in (a)
//       and (c), we'll invoke the closure again
//   (e) We'll continue to loop around until the node ID list stabilizes
//
// [*]: We can be a bit more precise here. What UntilClusterStable gives us is a
// strict causal happened-before relation between running the given closure and
// the next node that joins the cluster. Put another way: using
// UntilClusterStable callers will have managed to run something without a new
// node joining half-way through (which could have allowed it to pick up some
// state off one of the existing nodes that hadn't heard from us yet).
//
// To consider an example of how this primitive is used, let's consider our use
// of it to bump the cluster version. We use in conjunction with ForEveryNode,
// where after we return, we can rely on the guarantee that all nodes in the
// cluster will have their cluster versions bumped. This then implies that
// future node additions will observe the latest version (through the join RPC).
// That in turn lets us author migrations that can assume that a certain version
// gate has been enabled on all nodes in the cluster, and will always be enabled
// for any new nodes in the system.
//
// Given that it'll always be possible for new nodes to join after an
// UntilClusterStable round, it means that some migrations may have to be split
// up into two version bumps: one that phases out the old version (i.e. stops
// creation of stale data or behavior) and a clean-up version, which removes any
// vestiges of the stale data/behavior, and which, when active, ensures that the
// old data has vanished from the system. This is similar in spirit to how
// schema changes are split up into multiple smaller steps that are carried out
// sequentially.
func (h *Helper) UntilClusterStable(ctx context.Context, fn func() error) error {
	ns, err := h.c.nodes(ctx)
	if err != nil {
		return err
	}

	for {
		if err := fn(); err != nil {
			return err
		}

		curNodes, err := h.c.nodes(ctx)
		if err != nil {
			return err
		}

		if ok, diffs := ns.identical(curNodes); !ok {
			log.Infof(ctx, "%s, retrying", diffs)
			ns = curNodes
			continue
		}

		break
	}

	return nil
}

// IterateRangeDescriptors provides a handle on every range descriptor in the
// system, which callers can then use to send out arbitrary KV requests to in
// order to run arbitrary KV-level migrations. These requests will typically
// just be the `Migrate` request, with code added within [1] to do the specific
// things intended for the specified version.
//
// It's important to note that the closure is being executed in the context of a
// distributed transaction that may be automatically retried. So something like
// the following is an anti-pattern:
//
//     processed := 0
//     _ = h.IterateRangeDescriptors(...,
//         func(descriptors ...roachpb.RangeDescriptor) error {
//             processed += len(descriptors) // we'll over count if retried
//             log.Infof(ctx, "processed %d ranges", processed)
//         },
//     )
//
// Instead we allow callers to pass in a callback to signal on every attempt
// (including the first). This lets us salvage the example above:
//
//     var processed int
//     init := func() { processed = 0 }
//     _ = h.IterateRangeDescriptors(..., init,
//         func(descriptors ...roachpb.RangeDescriptor) error {
//             processed += len(descriptors)
//             log.Infof(ctx, "processed %d ranges", processed)
//         },
//     )
//
// [1]: pkg/kv/kvserver/batch_eval/cmd_migrate.go
func (h *Helper) IterateRangeDescriptors(
	ctx context.Context, blockSize int, init func(), fn func(...roachpb.RangeDescriptor) error,
) error {
	if err := h.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
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
func (h *Helper) DB() *kv.DB {
	return h.c.db()
}

// ClusterVersion exposes the cluster version associated with the ongoing
// migration.
func (h *Helper) ClusterVersion() clusterversion.ClusterVersion {
	return h.cv
}

type clusterImpl struct {
	nl     nodeLiveness
	exec   sqlutil.InternalExecutor
	dialer *nodedialer.Dialer
	kvDB   *kv.DB
}

var _ cluster = &clusterImpl{}

func newCluster(
	nl nodeLiveness, dialer *nodedialer.Dialer, executor *sql.InternalExecutor, db *kv.DB,
) *clusterImpl {
	return &clusterImpl{nl: nl, dialer: dialer, exec: executor, kvDB: db}
}

// nodes implements the cluster interface.
func (c *clusterImpl) nodes(ctx context.Context) (nodes, error) {
	var ns []node
	ls, err := c.nl.GetLivenessesFromKV(ctx)
	if err != nil {
		return nil, err
	}
	for _, l := range ls {
		if l.Membership.Decommissioned() {
			continue
		}
		live, err := c.nl.IsLive(l.NodeID)
		if err != nil {
			return nil, err
		}
		if !live {
			return nil, errors.Newf("n%d required, but unavailable", l.NodeID)
		}
		ns = append(ns, node{id: l.NodeID, epoch: l.Epoch})
	}
	return ns, nil
}

// dial implements the cluster interface.
func (c *clusterImpl) dial(ctx context.Context, id roachpb.NodeID) (*grpc.ClientConn, error) {
	return c.dialer.Dial(ctx, id, rpc.DefaultClass)
}

// db implements the cluster interface.
func (c *clusterImpl) db() *kv.DB {
	return c.kvDB
}

// executor implements the cluster interface.
func (c *clusterImpl) executor() sqlutil.InternalExecutor {
	return c.exec
}
