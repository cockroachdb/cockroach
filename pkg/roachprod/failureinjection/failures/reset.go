// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type (
	ResetVMArgs struct {
		Nodes         install.Nodes
		StopProcesses bool
	}
	resetVMFailure struct {
		GenericFailure
		processes processMap
	}
	// nodeSet is a set of nodes.
	nodeSet map[install.Node]struct{}
	// instanceMap is a map of SQL instances to a map of nodes that are running
	// that instance.
	instanceMap map[int]nodeSet
	// processMap is a map of virtual cluster names to instance maps. It's a
	// convenience type that allows grouping the processes that should be
	// started and stopped together.
	processMap map[string]instanceMap
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

func (m *processMap) add(virtualClusterName string, instance int, node install.Node) {
	if virtualClusterName == "" {
		virtualClusterName = install.SystemInterfaceName
	}
	if _, ok := (*m)[virtualClusterName]; !ok {
		(*m)[virtualClusterName] = make(map[int]nodeSet, 0)
	}
	if _, ok := (*m)[virtualClusterName][instance]; !ok {
		(*m)[virtualClusterName][instance] = make(nodeSet, 0)
	}
	(*m)[virtualClusterName][instance][node] = struct{}{}
}

// getStartOrder returns the order in which the processes should be started. It
// ensures that the System interface is started first.
func (m *processMap) getStartOrder() []string {
	var order []string
	// If the System interface is present, it should be the first to start.
	if _, ok := (*m)[install.SystemInterfaceName]; ok {
		order = append(order, install.SystemInterfaceName)
	}
	for virtualClusterName := range *m {
		if virtualClusterName != install.SystemInterfaceName {
			order = append(order, virtualClusterName)
		}
	}
	return order
}

// getStopOrder returns the order in which the processes should be stopped.
func (m *processMap) getStopOrder() []string {
	order := m.getStartOrder()
	rand.Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})
	return order
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
	r.processes = make(processMap, 0)
	for e := range monitorChan {
		if p, ok := e.Event.(install.MonitorProcessRunning); ok {
			r.processes.add(p.VirtualClusterName, p.SQLInstance, e.Node)
		}
	}

	// Optionally stop the processes.
	if args.(ResetVMArgs).StopProcesses {
		for _, virtualClusterName := range r.processes.getStopOrder() {
			instanceMap := r.processes[virtualClusterName]
			for _, nodeMap := range instanceMap {
				var stopNodes install.Nodes
				for node := range nodeMap {
					stopNodes = append(nodes, node)
				}
				l.Printf("Stopping process %s on nodes %v", virtualClusterName, stopNodes)
				stopOpts := roachprod.DefaultStopOpts()
				err := r.c.WithNodes(stopNodes).Stop(ctx, l, stopOpts.Sig, stopOpts.Wait, stopOpts.GracePeriod, virtualClusterName)
				if err != nil {
					return err
				}
			}
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
	for _, virtualClusterName := range r.processes.getStartOrder() {
		instanceMap := r.processes[virtualClusterName]
		for instance, nodeMap := range instanceMap {
			var nodes install.Nodes
			for node := range nodeMap {
				nodes = append(nodes, node)
			}
			l.Printf("Starting process %s on nodes %v", virtualClusterName, nodes)
			err := r.c.WithNodes(nodes).Start(ctx, l, install.StartOpts{
				VirtualClusterName: virtualClusterName,
				SQLInstance:        instance,
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
	return r.WaitForRestartedNodesToStabilize(ctx, l, nodes, 30*time.Minute)
}
