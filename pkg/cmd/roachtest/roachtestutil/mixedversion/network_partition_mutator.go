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

func (m networkPartitionMutator) Probability() float64 {
	return 0.3
}

func (m networkPartitionMutator) Generate(
	rng *rand.Rand, plan *TestPlan, planner *testPlanner,
) ([]mutation, error) {
	var mutations []mutation
	upgrades := randomUpgrades(rng, plan)
	idx := newStepIndex(plan)
	nodeList := planner.currentContext.System.Descriptor.Nodes

	f, err := GetFailer(planner, failures.IPTablesNetworkPartitionName)
	if err != nil {
		return nil, fmt.Errorf("failed to get failer for %s: %w", failures.IPTablesNetworkPartitionName, err)
	}

	for _, upgrade := range upgrades {
		possiblePointsInTime := upgrade.
			Filter(func(s *singleStep) bool {
				// We don't want to set up a partition concurrently with other steps, and inserting
				// before a concurrent step causes the step to run concurrently with that step, so
				// we filter out any concurrent steps. We don't want to set up a partition while
				// nodes are unavailable, as that could cause the cluster to lose quorum,
				//	so we filter out steps with unavailable nodes.
				var unavailableNodes bool
				if planner.isMultitenant() {
					unavailableNodes = s.context.Tenant.hasUnavailableNodes || s.context.System.hasUnavailableNodes
				} else {
					unavailableNodes = s.context.System.hasUnavailableNodes
				}
				return s.context.System.Stage >= InitUpgradeStage && !idx.IsConcurrent(s) && !unavailableNodes
			})

		stepToPartition := possiblePointsInTime.RandomStep(rng)
		hasInvalidConcurrentStep := false
		var firstStepInConcurrentBlock *singleStep

		isInvalidRecoverStep := func(s *singleStep) bool {
			// Restarting a node in the middle of a network partition has a chance of
			// loss of quorum, so we do should recover the network partition before this
			// if the restarted node is not the node being partitioned.
			// e.g. In a 4-node cluster, if node 1 is partitioned from nodes 2, 3, and
			// 4, then restarting node 2 would cause a loss of quorum since 3 and 4
			// cannot talk to 1.

			// TODO: The partitioned node should be able to restart safely, provided
			// the necessary steps are altered to allow it.

			_, restartSystem := s.impl.(restartWithNewBinaryStep)
			_, restartTenant := s.impl.(restartVirtualClusterStep)
			// Many hook steps require communication between specific nodes, so we
			// should recover the network partition before running any incompatible
			// hook steps.
			_, runHook := s.impl.(runHookStep)
			// Waiting for stable cluster version requires communication between
			// all nodes in the cluster, so we should recover the network partition
			// before running it.
			_, waitForStable := s.impl.(waitForStableClusterVersionStep)

			if idx.IsConcurrent(s) {
				if firstStepInConcurrentBlock == nil {
					firstStepInConcurrentBlock = s
				}
				hasInvalidConcurrentStep = true
			} else {
				hasInvalidConcurrentStep = false
				firstStepInConcurrentBlock = nil
			}

			var unavailableNodes bool
			if planner.isMultitenant() {
				unavailableNodes = s.context.Tenant.hasUnavailableNodes || s.context.System.hasUnavailableNodes
			} else {
				unavailableNodes = s.context.System.hasUnavailableNodes
			}
			return unavailableNodes || restartTenant || restartSystem || (runHook && !planner.options.hooksSupportFailureInjection) || waitForStable
		}

		_, validStartStep := upgrade.CutAfter(func(s *singleStep) bool {
			return s == stepToPartition[0]
		})

		validEndStep, _, cutStep := validStartStep.Cut(func(s *singleStep) bool {
			return isInvalidRecoverStep(s)
		})

		// Inserting before a concurrent step will cause the step to run concurrently with that step,
		// so we remove the concurrent steps from the list of possible insertions if they contain
		// any invalid steps.
		if hasInvalidConcurrentStep {
			validEndStep, _ = validEndStep.CutAfter(func(s *singleStep) bool {
				return s == firstStepInConcurrentBlock
			})
		}

		partitions, unavailableNodes, err := m.strategy.selectPartitions(rng, nodeList)
		if err != nil {
			return nil, err
		}

		addPartition := stepToPartition.
			InsertBefore(networkPartitionInjectStep{
				f:                f,
				partitions:       partitions,
				unavailableNodes: unavailableNodes,
			})
		var addRecoveryStep []mutation
		var recoveryStep stepSelector
		// If validEndStep is nil, it means that there are no steps after the partition step that are
		// compatible with a network partition, so we immediately restart the node after the partition.
		if validEndStep == nil {
			recoveryStep = cutStep
			addRecoveryStep = cutStep.InsertBefore(networkPartitionRecoveryStep{
				f:                f,
				partitions:       partitions,
				unavailableNodes: unavailableNodes,
			})
		} else {
			recoveryStep = validEndStep.RandomStep(rng)
			addRecoveryStep = recoveryStep.
				Insert(rng, networkPartitionRecoveryStep{
					f:                f,
					partitions:       partitions,
					unavailableNodes: unavailableNodes,
				})
		}

		failureContextSteps, _ := validStartStep.CutBefore(func(s *singleStep) bool {
			return s == recoveryStep[0]
		})

		failureContextSteps.MarkNodesUnavailable(true, true)
		addPartition[0].hasUnavailableSystemNodes = true
		addPartition[0].hasUnavailableTenantNodes = true
		addRecoveryStep[0].hasUnavailableSystemNodes = true
		addRecoveryStep[0].hasUnavailableTenantNodes = true

		mutations = append(mutations, addPartition...)
		mutations = append(mutations, addRecoveryStep...)
	}

	return mutations, nil
}
