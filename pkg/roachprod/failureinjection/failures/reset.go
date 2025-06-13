// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type (
	ResetVMArgs struct {
		Nodes install.Nodes
	}
	resetVMFailure struct {
		GenericFailure
		Processes map[install.Node][]install.MonitorProcessRunning
	}
)

var _ FailureMode = &resetVMFailure{}

const ResetVMFailureName = "reset-vm"

func registerResetVM(r *FailureRegistry) {
	r.add(ResetVMFailureName, ResetVMArgs{}, MakeResetVMFailure)
}

func MakeResetVMFailure(
	clusterName string, l *logger.Logger, clusterOpts ClusterOptions,
) (FailureMode, error) {
	c, err := roachprod.GetClusterFromCache(l, clusterName, install.SecureOption(clusterOpts.secure))
	if err != nil {
		return nil, err
	}

	return &resetVMFailure{
		GenericFailure: GenericFailure{
			c: c,
		},
	}, nil
}

// Description implements FailureMode.
func (r *resetVMFailure) Description() string {
	return ResetVMFailureName
}

// Setup implements FailureMode.
func (r *resetVMFailure) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return nil
}

// Inject implements FailureMode.
func (r *resetVMFailure) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	// Capture the processes running on the nodes.
	nodes := args.(ResetVMArgs).Nodes
	monitorChan := r.c.WithNodes(nodes).Monitor(l, ctx, install.MonitorOpts{OneShot: true})
	r.Processes = make(map[install.Node][]install.MonitorProcessRunning, 0)
	for e := range monitorChan {
		if p, ok := e.Event.(install.MonitorProcessRunning); ok {
			r.Processes[e.Node] = append(r.Processes[e.Node], p)
		}
	}

	return r.c.WithNodes(nodes).Reset(l)
}

// Cleanup implements FailureMode.
func (r *resetVMFailure) Cleanup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return nil
}

// Recover implements FailureMode.
func (r *resetVMFailure) Recover(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	// Restart the processes.
	for node, processes := range r.Processes {
		for _, p := range processes {
			l.Printf("Starting process %s on node %s", p.PID, p.VirtualClusterName)
			err := r.c.WithNodes([]install.Node{node}).Start(ctx, l, install.StartOpts{
				VirtualClusterName: p.VirtualClusterName,
				SQLInstance:        p.SQLInstance,
				IsRestart:          true,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// WaitForFailureToPropagate implements FailureMode.
func (r *resetVMFailure) WaitForFailureToPropagate(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	nodes := args.(ResetVMArgs).Nodes
	l.Printf("Waiting for nodes to become unavailable: %v", nodes)

	// Some providers take a while to stop VMs (>10 minutes).
	return forEachNode(nodes, func(n install.Nodes) error {
		return r.WaitForSQLUnavailable(ctx, l, n, 15*time.Minute)
	})
}

// WaitForFailureToRecover implements FailureMode.
func (r *resetVMFailure) WaitForFailureToRecover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	nodes := args.(ResetVMArgs).Nodes
	l.Printf("Waiting for nodes to become available: %v", nodes)

	// Some providers take a while to start VMs (>10 minutes).
	return forEachNode(nodes, func(n install.Nodes) error {
		return r.WaitForSQLReady(ctx, l, n, 15*time.Minute)
	})
}
