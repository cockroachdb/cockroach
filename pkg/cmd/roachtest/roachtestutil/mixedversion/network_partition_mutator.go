// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
)

// PartitionStrategy defines how network partitions are created during mixed-version tests.
type PartitionStrategy interface {
	String() string
	selectProtectedNodes(rng *rand.Rand, nodes option.NodeListOption) (option.NodeListOption, error)
	selectPartitions(rng *rand.Rand, nodes option.NodeListOption) ([]failures.NetworkPartition, option.NodeListOption, error)
}

type partitionStrategy = PartitionStrategy

// singlePartitionStrategy creates a single node-to-node partition, ensuring we never
// partition a leaseholder/leader from the rest of its replicas (assuming RF=3).
type singlePartitionStrategy struct{}

func (s singlePartitionStrategy) String() string {
	return "single-partition"
}

func (s singlePartitionStrategy) selectProtectedNodes(rng *rand.Rand, nodes option.NodeListOption) (option.NodeListOption, error) {
	// Since we at most inject one partition, we don't need to protect any nodes.
	return nil, nil
}

func (s singlePartitionStrategy) selectPartitions(rng *rand.Rand, nodes option.NodeListOption) ([]failures.NetworkPartition, option.NodeListOption, error) {
	if len(nodes) < 3 {
		return nil, nil, errors.New("at least three nodes are required to create a partition")
	}
	rng.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	src := install.Node(nodes[0])
	dst := install.Node(nodes[1])

	// We don't need to mark any nodes as unavailable since a single partition with
	// leader leases should always be recoverable from.
	return []failures.NetworkPartition{{
		Source:      install.Nodes{src},
		Destination: install.Nodes{dst},
		Type:        failures.AllPartitionTypes[rng.Intn(len(failures.AllPartitionTypes))],
	}}, nil /* unavailableNodes */, nil
}

// protectedPartitionStrategy designates a random quorum of nodes as protected. These nodes
// will never be partitioned from any other node in the cluster, and will always have pinned
// voter replicas.
type protectedPartitionStrategy struct {
	protectedNodes map[int]bool
	quorum         int
}

func (p protectedPartitionStrategy) String() string {
	return "protected-nodes"
}

func (p protectedPartitionStrategy) selectProtectedNodes(
	rng *rand.Rand, nodes option.NodeListOption,
) (option.NodeListOption, error) {
	if len(nodes) < 4 {
		return nil, errors.New("at least four nodes are required for a protected partition")
	}

	rng.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	for _, node := range nodes[:p.quorum] {
		p.protectedNodes[node] = true
	}
	return nodes[:p.quorum], nil
}

func (p protectedPartitionStrategy) selectPartitions(
	rng *rand.Rand, nodes option.NodeListOption,
) ([]failures.NetworkPartition, option.NodeListOption, error) {
	if len(nodes)-len(p.protectedNodes) < 2 {
		return nil, nil, errors.New("at least two non protected nodes are required to create a partition")
	}

	// Build every possible connection between nodes, excluding ones between
	// two protected nodes.
	var connections [][2]int
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			if p.protectedNodes[nodes[i]] && p.protectedNodes[nodes[j]] {
				continue
			}
			connections = append(connections, [2]int{nodes[i], nodes[j]})
		}
	}
	rng.Shuffle(len(connections), func(i, j int) {
		connections[i], connections[j] = connections[j], connections[i]
	})

	nodeToPartition := make(map[int]*failures.NetworkPartition)
	unvailableNodes := make(option.NodeListOption, 0)
	for _, conn := range connections[:rng.Intn(len(connections))+1] {
		if _, ok := nodeToPartition[conn[0]]; !ok {
			nodeToPartition[conn[0]] = &failures.NetworkPartition{
				Source:      install.Nodes{install.Node(conn[0])},
				Destination: install.Nodes{install.Node(conn[1])},
				Type:        failures.AllPartitionTypes[rng.Intn(len(failures.AllPartitionTypes))],
			}
		} else {
			nodeToPartition[conn[0]].Destination = append(nodeToPartition[conn[0]].Destination, install.Node(conn[1]))
		}
		// For simplicity, we say any node that has a partition touching it is unavailable,
		// except the protected nodes, since those will always maintain quorum.
		if !p.protectedNodes[conn[0]] {
			unvailableNodes = unvailableNodes.Merge(option.NodeListOption{conn[0]})
		}
		if !p.protectedNodes[conn[1]] {
			unvailableNodes = unvailableNodes.Merge(option.NodeListOption{conn[1]})
		}
	}

	partitions := make([]failures.NetworkPartition, 0, len(nodeToPartition))
	for _, partition := range nodeToPartition {
		partitions = append(partitions, *partition)
	}

	return partitions, unvailableNodes /* unavailableNodes */, nil
}

type networkPartitionMutator struct {
	strategy partitionStrategy
}

func (m networkPartitionMutator) Name() string {
	return fmt.Sprintf("network-partition-mutator-%s", m.strategy)
}

func (m networkPartitionMutator) Init(p *testPlanner) bool {
	m.strategy = singlePartitionStrategy{}
	if p.options.partitionStrategy != nil {
		m.strategy = p.options.partitionStrategy
	}
	// Disable network partitions for separate process deployments. Network partitions
	// currently only support partitions between VMs, forcing us to unconditionally partition
	// all processes on a VM. This can cause separate process tenants to shut down.
	return shouldEnableFailureInjection(p) && p.deploymentMode != SeparateProcessDeployment
}

func (m networkPartitionMutator) Probability() float64 {
	return 0.3
}

func (m networkPartitionMutator) Generate(
	rng *rand.Rand, plan *TestPlan, planner *testPlanner,
) ([]mutation, error) {
	fmt.Printf("partition strategy: %s\n", m.strategy)
	f, err := GetFailer(planner, failures.IPTablesNetworkPartitionName)
	if err != nil {
		return nil, fmt.Errorf("failed to get failer for %s: %w", failures.IPTablesNetworkPartitionName, err)
	}

	var mutations []mutation
	upgrades := randomUpgrades(rng, plan)
	idx := newStepIndex(plan)

	for i, upgrade := range upgrades {
		// Force a network partition if it's the last upgrade as it's the most important one
		// to test. Otherwise, we inject a partition with 50% probability.
		if i != len(upgrades)-1 && rng.Float64() < 0.5 {
			continue
		}

		mut, err := m.generatePartition(rng, upgrade, idx, planner, f)
		if err != nil {
			planner.logger.Printf("failed to generate valid partition: %v", err)
		} else {
			mutations = append(mutations, mut...)
		}
	}

	return mutations, nil
}

func (m networkPartitionMutator) generatePartition(
	rng *rand.Rand, steps stepSelector, idx stepIndex, planner *testPlanner, f *failures.Failer,
) ([]mutation, error) {
	const maxAttempts = 500

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Randomly select a step to inject before and a step to recover after.
		// This ensures that we skip "uninteresting partition windows", i.e. immediately
		// recovering after injecting.
		injectIdx := rng.Intn(len(steps))
		recoverIdx := rng.Intn(len(steps))

		if m.isValidPartitionWindow(injectIdx, recoverIdx, steps, idx, planner) {
			return m.createPartitionMutations(rng, injectIdx, recoverIdx, steps, f, planner)
		}
	}

	return nil, errors.Newf("failed to find valid partition window after %d attempts", maxAttempts)
}

// A partition window is valid if:
//  1. The inject and recovery steps are non-concurrent.
//  2. The partition does not cause a loss of quorum, e.g. it doesn't run with other failure injection.
//  3. The window does not contain any incompatible steps.
func (m networkPartitionMutator) isValidPartitionWindow(
	injectIdx, recoverIdx int, steps stepSelector, idx stepIndex, planner *testPlanner,
) bool {
	if recoverIdx < injectIdx {
		return false
	}

	injectStep := steps[injectIdx]
	windowSteps := steps[injectIdx : recoverIdx+1]

	// Don't concurrently run our inject step, as we don't want to cause unavailable nodes
	// while other steps may be connecting to them.
	if idx.IsConcurrent(injectStep) {
		return false
	}
	// Don't inject a partition before we are done running setup steps.
	if injectStep.context.System.Stage < InitUpgradeStage {
		return false
	}

	// All steps in the window must be compatible with network partition
	for _, s := range windowSteps {
		if !m.isCompatibleWithPartition(s, planner) {
			return false
		}
	}

	return true
}

func (m networkPartitionMutator) isCompatibleWithPartition(
	s *singleStep, planner *testPlanner,
) bool {
	// Avoid restarting nodes while we have a partition injected, as it could
	// lead to a loss of quorum.
	// e.g. In a 3 node cluster, if we partition node 1 from node 2, then restarting
	// node 3 will cause a loss of quorum.
	//
	// TODO: Loosen this restriction. In the above example, we can't restart node
	// 3, but we can restart nodes 1 or 2 without causing a loss of quorum. Similarly
	// if we are running under a protectedPartitionStrategy, we can restart any node
	// that is not protected.
	_, restartSystem := s.impl.(restartWithNewBinaryStep)
	_, restartTenant := s.impl.(restartVirtualClusterStep)

	// Many hook steps hardcode a connection to a specific node, or attempt to connect
	// to a random node in the cluster without checking for availability.
	// Unless enabled, we avoid running hooks while a partition is injected.
	_, runHook := s.impl.(runHookStep)

	// Waiting for the cluster version to finalize requires all nodes to be able to
	// communicate with each other.
	_, waitForStable := s.impl.(waitForStableClusterVersionStep)

	if restartSystem || restartTenant || waitForStable {
		return false
	}

	if runHook && !planner.options.hooksSupportFailureInjection {
		return false
	}

	// Similar to avoiding node restarts, avoid injecting a partition if a node
	// is already unavailable.
	if s.context.System.hasUnavailableNodes {
		return false
	}
	if s.context.Tenant != nil && s.context.Tenant.hasUnavailableNodes {
		return false
	}

	return true
}

func (m networkPartitionMutator) createPartitionMutations(
	rng *rand.Rand,
	injectIdx int,
	recoverIdx int,
	steps stepSelector,
	f *failures.Failer,
	planner *testPlanner,
) ([]mutation, error) {
	partitions, unavailableNodes, err := m.strategy.selectPartitions(rng, planner.currentContext.System.Descriptor.Nodes)
	if err != nil {
		return nil, err
	}

	injectStep := stepSelector{steps[injectIdx]}
	recoverStep := stepSelector{steps[recoverIdx]}

	addPartitionStep := injectStep.InsertBefore(networkPartitionInjectStep{
		f:                f,
		partitions:       partitions,
		unavailableNodes: unavailableNodes,
	})

	addRecoveryStep := recoverStep.InsertAfter(networkPartitionRecoveryStep{
		f:                f,
		partitions:       partitions,
		unavailableNodes: unavailableNodes,
	})

	// Mark all steps in the failure window as having unavailable nodes, so
	// other mutators don't attempt to also take down nodes in the same window.
	failureContextSteps := steps[injectIdx : recoverIdx+1]
	failureContextSteps.MarkNodesUnavailable(true, true)
	addPartitionStep[0].hasUnavailableSystemNodes = true
	addPartitionStep[0].hasUnavailableTenantNodes = true
	addRecoveryStep[0].hasUnavailableSystemNodes = true
	addRecoveryStep[0].hasUnavailableTenantNodes = true

	return append(addPartitionStep, addRecoveryStep...), nil
}
