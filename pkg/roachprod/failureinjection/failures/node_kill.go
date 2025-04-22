// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"golang.org/x/sys/unix"
)

type NodeKillArgs struct {
	Nodes install.Nodes
	// GracefulShutdown will allow the cockroach process to drain before exiting.
	GracefulShutdown bool
	// The replication factor to wait for in WaitForFailureToRecover. Defaults to 3 if empty.
	ReplicationFactor int
}

const NodeKillFailureName = "node-kill"

func registerNodeKillFailure(r *FailureRegistry) {
	r.add(NodeKillFailureName, NodeKillArgs{}, MakeNodeKillFailure)
}

func MakeNodeKillFailure(
	clusterName string, l *logger.Logger, connectionInfo ConnectionInfo,
) (FailureMode, error) {
	genericFailure, err := makeGenericFailure(clusterName, l, connectionInfo, NodeKillFailureName)
	if err != nil {
		return nil, err
	}

	return &NodeKillFailure{GenericFailure: *genericFailure}, nil
}

type NodeKillFailure struct {
	GenericFailure
}

func (f *NodeKillFailure) Description() string {
	return NodeKillFailureName
}

func (f *NodeKillFailure) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return nil
}

func (f *NodeKillFailure) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	nodes := args.(NodeKillArgs).Nodes
	signal := unix.SIGKILL
	if args.(NodeKillArgs).GracefulShutdown {
		signal = unix.SIGTERM
	}
	l.Printf("Shutting down n%d with signal %d", nodes, signal)
	return f.Run(ctx, l, nodes, "pkill", fmt.Sprintf("-%d", signal), "-f", "cockroach\\ start")
}

func (f *NodeKillFailure) Recover(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	nodes := args.(NodeKillArgs).Nodes
	l.Printf("Restarting cockroach process on nodes: %v", nodes)
	return f.StartNodes(ctx, l, nodes)
}

func (f *NodeKillFailure) Cleanup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	defer f.CloseConnections()
	return nil
}

func (f *NodeKillFailure) WaitForFailureToPropagate(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	nodes := args.(NodeKillArgs).Nodes
	l.Printf("Waiting for node kill to propagate on nodes: %v", nodes)

	return forEachNode(nodes, func(n install.Nodes) error {
		return f.WaitForProcessDeath(ctx, l, n, time.Minute)
	})
}

func (f *NodeKillFailure) WaitForFailureToRecover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	nodeKillArgs := args.(NodeKillArgs)
	nodes := nodeKillArgs.Nodes
	l.Printf("Waiting for cockroach process to recover on nodes: %v", nodes)
	return f.WaitForRestartedNodesToStabilize(ctx, l, nodes, nodeKillArgs.ReplicationFactor)
}
