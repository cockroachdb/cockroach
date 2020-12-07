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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Helper captures all the primitives required to fully specify a migration.
type Helper struct {
	*Manager
}

// RequiredNodeIDs returns the node IDs for all nodes that are currently part of
// the cluster (i.e. they haven't been decommissioned away). Migrations have the
// pre-requisite that all required nodes are up and running so that we're able
// to execute all relevant node-level operations on them. If any of the nodes
// are found to be unavailable, an error is returned.
func (h *Helper) RequiredNodeIDs(ctx context.Context) ([]roachpb.NodeID, error) {
	var nodeIDs []roachpb.NodeID
	ls, err := h.nl.GetLivenessesFromKV(ctx)
	if err != nil {
		return nil, err
	}
	for _, l := range ls {
		if l.Membership.Decommissioned() {
			continue
		}
		live, err := h.nl.IsLive(l.NodeID)
		if err != nil {
			return nil, err
		}
		if !live {
			return nil, errors.Newf("n%d required, but unavailable", l.NodeID)
		}
		nodeIDs = append(nodeIDs, l.NodeID)
	}
	return nodeIDs, nil
}

// EveryNode invokes the given closure (named by the informational parameter op)
// across every node in the cluster[*]. The mechanism for ensuring that we've
// done so, while accounting for the possibility of new nodes being added to the
// cluster in the interim, is provided by the following structure:
//   (a) We'll retrieve the list of node IDs for all nodes in the system
//   (b) For each node, we'll invoke the closure
//   (c) We'll retrieve the list of node IDs again to account for the
//       possibility of a new node being added during (b)
//   (d) If there any discrepancies between the list retrieved in (a)
//       and (c), we'll invoke the closure each node again
//   (e) We'll continue to loop around until the node ID list stabilizes
//
// [*]: We can be a bit more precise here. What EveryNode gives us is a strict
// causal happened-before relation between running the given closure against
// every node that's currently a member of the cluster, and the next node that
// joins the cluster. Put another way: using EveryNode callers will have managed
// to run something against all nodes without a new node joining half-way
// through (which could have allowed it to pick up some state off one of the
// existing nodes that hadn't heard from us yet).
//
// To consider one example of how this primitive is used, let's consider our use
// of it to bump the cluster version. After we return, given all nodes in the
// cluster will have their cluster versions bumped, and future node additions
// will observe the latest version (through the join RPC). This lets us author
// migrations that can assume that a certain version gate has been enabled on
// all nodes in the cluster, and will always be enabled for any new nodes in the
// system.
//
// Given that it'll always be possible for new nodes to join after an EveryNode
// round, it means that some migrations may have to be split up into two version
// bumps: one that phases out the old version (i.e. stops creation of stale data
// or behavior) and a clean-up version, which removes any vestiges of the stale
// data/behavior, and which, when active, ensures that the old data has vanished
// from the system. This is similar in spirit to how schema changes are split up
// into multiple smaller steps that are carried out sequentially.
func (h *Helper) EveryNode(
	ctx context.Context, op string, fn func(context.Context, serverpb.MigrationClient) error,
) error {
	nodeIDs, err := h.RequiredNodeIDs(ctx)
	if err != nil {
		return err
	}

	for {
		// TODO(irfansharif): We can/should send out these RPCs in parallel.
		log.Infof(ctx, "executing op=%s on nodes=%s", op, nodeIDs)
		for _, nodeID := range nodeIDs {
			conn, err := h.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
			if err != nil {
				return err
			}
			client := serverpb.NewMigrationClient(conn)
			if err := fn(ctx, client); err != nil {
				return err
			}
		}

		curNodeIDs, err := h.RequiredNodeIDs(ctx)
		if err != nil {
			return err
		}

		if !identical(nodeIDs, curNodeIDs) {
			nodeIDs = curNodeIDs
			continue
		}

		break
	}

	return nil
}
