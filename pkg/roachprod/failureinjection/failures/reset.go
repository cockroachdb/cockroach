// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
)

type (
	ResetVMArgs struct {
		Nodes         install.Nodes
		StopProcesses bool
	}
	resetVMFailure struct {
		GenericFailure
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
	genericFailure, err := makeGenericFailure(clusterName, l, clusterOpts, ResetVMFailureName)
	if err != nil {
		return nil, err
	}

	return &resetVMFailure{GenericFailure: *genericFailure}, nil
}

// SupportedDeploymentMode implements FailureMode.
func (r *resetVMFailure) SupportedDeploymentMode(_ roachprodutil.DeploymentMode) bool {
	return true
}

// Setup implements FailureMode.
func (r *resetVMFailure) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return nil
}

// Inject implements FailureMode.
func (r *resetVMFailure) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	// Capture the processes running on the nodes.
	nodes := args.(ResetVMArgs).Nodes
	r.CaptureProcesses(ctx, l, nodes)

	// Optionally stop the processes.
	if args.(ResetVMArgs).StopProcesses {
		if err := r.StopProcesses(ctx, l); err != nil {
			return err
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
	if err := r.RestartProcesses(ctx, l); err != nil {
		return err
	}

	// Restarting the VM seems to cause cgroups controllers to be removed, readd them.
	if err := r.Run(ctx, l, r.c.Nodes, "sudo", "/bin/bash", "-c",
		`'echo "+cpuset +cpu +io +memory +pids" > /sys/fs/cgroup/cgroup.subtree_control'`); err != nil {
		return err
	}

	return r.Run(ctx, l, r.c.Nodes, "sudo", "/bin/bash", "-c",
		`'echo "+cpuset +cpu +io +memory +pids" > /sys/fs/cgroup/system.slice/cgroup.subtree_control'`)
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
	return r.WaitForRestartedNodesToStabilize(ctx, l, nodes, 30*time.Minute)
}
