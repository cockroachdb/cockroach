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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// Cluster mediates access to the crdb cluster.
type ClusterConfig struct {

	// NodeLiveness is used to determine the set of nodes in the cluster.
	NodeLiveness NodeLiveness

	// Dialer constructs connections to other nodes.
	Dialer NodeDialer

	// DB provides access the kv.DB instance backing the cluster.
	//
	// TODO(irfansharif): We could hide the kv.DB instance behind an interface
	// to expose only relevant, vetted bits of kv.DB. It'll make our tests less
	// "integration-ey".
	DB *kv.DB
}

// NodeDialier abstracts connecting to other nodes in the cluster.
type NodeDialer interface {
	// Dial returns a grpc connection to the given node.
	Dial(context.Context, roachpb.NodeID, rpc.ConnectionClass) (*grpc.ClientConn, error)
}

// NodeLiveness is the subset of the interface satisfied by CRDB's node liveness
// component that the migration manager relies upon.
type NodeLiveness interface {
	GetLivenessesFromKV(context.Context) ([]livenesspb.Liveness, error)
	IsLive(roachpb.NodeID) (bool, error)
}

// NodesFromNodeLiveness returns the IDs and epochs for all nodes that are
// currently part of the cluster (i.e. they haven't been decommissioned away).
// Migrations have the pre-requisite that all nodes are up and running so that
// we're able to execute all relevant node-level operations on them. If any of
// the nodes are found to be unavailable, an error is returned.
//
// It's important to note that this makes no guarantees about new nodes
// being added to the cluster. It's entirely possible for that to happen
// concurrently with the retrieval of the current set of nodes. Appropriate
// usage of this entails wrapping it under a stabilizing loop, like we do in
// EveryNode.
func NodesFromNodeLiveness(ctx context.Context, nl NodeLiveness) (Nodes, error) {
	var ns []Node
	ls, err := nl.GetLivenessesFromKV(ctx)
	if err != nil {
		return nil, err
	}
	for _, l := range ls {
		if l.Membership.Decommissioned() {
			continue
		}
		live, err := nl.IsLive(l.NodeID)
		if err != nil {
			return nil, err
		}
		if !live {
			return nil, errors.Newf("n%d required, but unavailable", l.NodeID)
		}
		ns = append(ns, Node{ID: l.NodeID, Epoch: l.Epoch})
	}
	return ns, nil
}
