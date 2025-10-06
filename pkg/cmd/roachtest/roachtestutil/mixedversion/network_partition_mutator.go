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
	selectPartitions(rng *rand.Rand, protectedNodes, nodes option.NodeListOption) ([]failures.NetworkPartition, option.NodeListOption, error)
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

func (s singlePartitionStrategy) selectPartitions(rng *rand.Rand, protectedNodes, nodes option.NodeListOption) ([]failures.NetworkPartition, option.NodeListOption, error) {
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
	quorum int
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
	return nodes[:p.quorum], nil
}

func (p protectedPartitionStrategy) selectPartitions(
	rng *rand.Rand, protectedNodes, nodes option.NodeListOption,
) ([]failures.NetworkPartition, option.NodeListOption, error) {
	protectedNodesMap := make(map[int]bool)
	for _, node := range nodes[:p.quorum] {
		protectedNodesMap[node] = true
	}

	if len(nodes)-len(protectedNodesMap) < 2 {
		return nil, nil, errors.New("at least two non protected nodes are required to create a partition")
	}

	// Build every possible connection between nodes, excluding ones between
	// two protected nodes.
	var connections [][2]int
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			if protectedNodesMap[nodes[i]] && protectedNodesMap[nodes[j]] {
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
		if !protectedNodesMap[conn[0]] {
			unvailableNodes = unvailableNodes.Merge(option.NodeListOption{conn[0]})
		}
		if !protectedNodesMap[conn[1]] {
			unvailableNodes = unvailableNodes.Merge(option.NodeListOption{conn[1]})
		}
	}

	partitions := make([]failures.NetworkPartition, 0, len(nodeToPartition))
	for _, partition := range nodeToPartition {
		partitions = append(partitions, *partition)
	}

	return partitions, unvailableNodes /* unavailableNodes */, nil
}

var (
	singleNetworkPartitionMutator = networkPartitionMutator{
		strategy:    singlePartitionStrategy{},
		probability: 0.3,
	}

	protectedNetworkPartitionMutator = networkPartitionMutator{
		strategy: protectedPartitionStrategy{
			quorum: 2,
		},
		probability: 0.0,
	}
)

type networkPartitionMutator struct {
	strategy    partitionStrategy
	probability float64
}

func (m networkPartitionMutator) Name() string {
	return fmt.Sprintf("network-partition-mutator-%s", m.strategy)
}

func (m networkPartitionMutator) IsCompatible(p *testPlanner) bool {
	// Disable network partitions for separate process deployments. Network partitions
	// currently only support partitions between VMs, forcing us to unconditionally partition
	// all processes on a VM. This can cause separate process tenants to shut down.
	return shouldEnableFailureInjection(p) && p.deploymentMode != SeparateProcessDeployment
}

func (m networkPartitionMutator) Probability() float64 {
	return m.probability
}

func (m networkPartitionMutator) Generate(
	rng *rand.Rand, plan *TestPlan, planner *testPlanner,
) ([]mutation, error) {
	var mutations []mutation
	protectedNodes, err := m.strategy.selectProtectedNodes(rng, planner.currentContext.System.Descriptor.Nodes)
	if err != nil {
		return nil, err
	}
	if len(protectedNodes) > 0 {
		protectedNodesMut, err := m.createProtectedNodesMutation(plan.newStepSelector(), protectedNodes)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, protectedNodesMut...)
	}

	f, err := GetFailer(planner, failures.IPTablesNetworkPartitionName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get failer for %s", failures.IPTablesNetworkPartitionName)
	}

	upgrades := randomUpgrades(rng, plan)
	idx := newStepIndex(plan)

	for i, upgrade := range upgrades {
		// Force a network partition if it's the last upgrade as it's the most important one
		// to test. Otherwise, we inject a partition with 50% probability.
		if i != len(upgrades)-1 && rng.Float64() < 0.5 {
			continue
		}

		mut, err := m.generatePartition(rng, upgrade, idx, planner, protectedNodes, f)
		if err != nil {
			return nil, err
		}
		if mut != nil {
			mutations = append(mutations, mut...)
		}
	}

	return mutations, nil
}

func (m networkPartitionMutator) createProtectedNodesMutation(
	steps stepSelector, protectedNodes option.NodeListOption,
) ([]mutation, error) {
	// Find the last setup step. We want to set the zone constraints
	// after the cluster is setup, but before any meaningful work is done.
	setupSteps := steps.Filter(func(step *singleStep) bool {
		return step.context.System.Stage < OnStartupStage
	})

	if len(setupSteps) == 0 {
		return nil, errors.New("no setup steps found")
	}

	lastSetupStep := setupSteps[len(setupSteps)-1]
	return stepSelector{lastSetupStep}.InsertAfter(
		pinVoterReplicasStep{protectedNodes: protectedNodes},
	), nil
}

func (m networkPartitionMutator) generatePartition(
	rng *rand.Rand, steps stepSelector, idx stepIndex, planner *testPlanner, protectedNodes option.NodeListOption, f *failures.Failer,
) ([]mutation, error) {
	const maxAttempts = 500
	for range maxAttempts {
		// Randomly select a step to inject before and a step to recover after.
		// This ensures that we skip "uninteresting partition windows", i.e. immediately
		// recovering after injecting.
		injectIdx := rng.Intn(len(steps))
		recoverIdx := rng.Intn(len(steps))

		if m.isValidPartitionWindow(injectIdx, recoverIdx, steps, idx, planner) {
			return m.createPartitionMutations(rng, injectIdx, recoverIdx, steps, f, protectedNodes, planner)
		}
	}
	// We should always have at least one valid step (i.e. the preserve downgrade step) that
	// supports a partition window. However, this step may have already been selected by another
	// failure injection mutator (e.g. node panic). In this case, we just skip injecting a partition.
	// TODO(darryl): ideally failure injection mutators could avoid having to coordinate within
	// the mutators themselves, e.g. we could enforce that each upgrade only has at most one failure
	// injection mutator.
	return nil, nil
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
	protectedNodes option.NodeListOption,
	planner *testPlanner,
) ([]mutation, error) {
	partitions, unavailableNodes, err := m.strategy.selectPartitions(rng, protectedNodes, planner.currentContext.System.Descriptor.Nodes)
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
