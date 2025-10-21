// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
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

// singleNetworkPartitionMutator creates a single node-to-node partition.
// With leader leases, this ensures that the cluster can always recover
// from the partition and experiences no unavailability.
type singlePartitionStrategy struct{}

func (s singlePartitionStrategy) String() string {
	return SingleNetworkPartition
}

func (s singlePartitionStrategy) selectProtectedNodes(
	rng *rand.Rand, nodes option.NodeListOption,
) (option.NodeListOption, error) {
	// Since we at most inject one partition, we don't need to protect any nodes.
	return nil, nil
}

func (s singlePartitionStrategy) selectPartitions(
	rng *rand.Rand, protectedNodes, nodes option.NodeListOption,
) ([]failures.NetworkPartition, option.NodeListOption, error) {
	if len(nodes) < 3 {
		return nil, nil, errors.New("at least three nodes are required to create a partition")
	}
	rng.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	src := install.Node(nodes[0])
	dst := install.Node(nodes[1])
	unavailableNodes := nodes[:2]

	return []failures.NetworkPartition{{
		Source:      install.Nodes{src},
		Destination: install.Nodes{dst},
		Type:        failures.AllPartitionTypes[rng.Intn(len(failures.AllPartitionTypes))],
	}}, unavailableNodes, nil
}

// protectedNetworkPartitionMutator delegates a quorum of nodes as protected.
// These protected nodes have voter replicas for all ranges pinned to them,
// and will never be partitioned from each other. This ensures that as long as
// we connect to a protected node, we should always have availability.
type protectedPartitionStrategy struct {
	// TODO(darryl): consider making replication factor configurable in the
	// mixed version framework itself.
	quorum int
}

func (p protectedPartitionStrategy) String() string {
	return ProtectedNodeNetworkPartition
}

func (p protectedPartitionStrategy) selectProtectedNodes(
	rng *rand.Rand, nodes option.NodeListOption,
) (option.NodeListOption, error) {
	if len(nodes) < 3 {
		return nil, errors.New("at least three nodes are required for a protected partition")
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
	for _, node := range protectedNodes {
		protectedNodesMap[node] = true
	}

	if len(nodes)-len(protectedNodesMap) < 1 {
		return nil, nil, errors.New("at least one non protected node is required to create a partition")
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
	unavailableNodes := make(option.NodeListOption, 0)
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
			unavailableNodes = unavailableNodes.Merge(option.NodeListOption{conn[0]})
		}
		if !protectedNodesMap[conn[1]] {
			unavailableNodes = unavailableNodes.Merge(option.NodeListOption{conn[1]})
		}
	}

	partitions := make([]failures.NetworkPartition, 0, len(nodeToPartition))
	for _, partition := range nodeToPartition {
		partitions = append(partitions, *partition)
	}

	return partitions, unavailableNodes, nil
}

// Both mutators are disabled by default, as we are still testing
// their stability in select tests.
// TODO(darryl): once we are confident in their stability, we should
// enable them by default.
var (
	singleNetworkPartitionMutator = networkPartitionMutator{
		strategy:    singlePartitionStrategy{},
		probability: 0.0,
	}
	protectedNetworkPartitionMutator = networkPartitionMutator{
		strategy: protectedPartitionStrategy{
			quorum: 2,
		},
		probability: 0.0,
	}
)

type networkPartitionMutator struct {
	strategy    PartitionStrategy
	probability float64
}

func (m networkPartitionMutator) Name() string {
	return m.strategy.String()
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
		protectedNodesMut, err := m.createProtectedNodes(plan.newStepSelector(), protectedNodes)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, protectedNodesMut...)
	}

	f, err := GetFailer(planner, failures.IPTablesNetworkPartitionName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get failer for %s", failures.IPTablesNetworkPartitionName)
	}

	// N.B. we iterate over all upgrades in order (over using randomUpgrades),
	// as we want to enable leader leases as soon as possible. We take care of
	// random selection later.
	upgrades := allUpgrades(plan)
	idx := newStepIndex(plan)

	// We only want to inject partitions if leader leases are enabled. Epoch
	// leases suffer from a known limitation where a leaseholder that is partitioned
	// from its followers but still heart beating to node liveness may cause indefinite
	// unavailability.
	leaderLeasesEnabled := false
	for _, upgrade := range upgrades {
		// If leader leases are not yet enabled, check if we can enable them now.
		if !leaderLeasesEnabled {
			var leaderLeasesMut []mutation
			// Attempt to enable leader leases if we are able to do so.
			upgrade, leaderLeasesMut, err = m.maybeEnableLeaderLeases(upgrade)
			if err != nil {
				return nil, err
			}

			if leaderLeasesMut != nil {
				mutations = append(mutations, leaderLeasesMut...)
				leaderLeasesEnabled = true
			} else {
				// If the mutations are nil, that means we can't yet enable leader leases
				// and we should skip attempting to add a partition for this upgrade.
				continue
			}
		}

		// For each upgrade, we inject a partition with 50% probability,
		// except for the final upgrade. This is the most important one to
		// test so we always inject one.
		//
		// TODO(darryl): for now these mutators are opt in only so we temporarily
		// inject partitions with 100% probability per upgrade to increase signal.
		//if i != len(upgrades)-1 && rng.Float64() < 0.5 {
		//	continue
		//}

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

func (m networkPartitionMutator) createProtectedNodes(
	steps stepSelector, protectedNodes option.NodeListOption,
) ([]mutation, error) {
	// Find the last system setup step. We want to set the zone constraints
	// after the cluster is setup, but before any meaningful work is done.
	setupSteps := steps.Filter(func(step *singleStep) bool {
		return step.context.System.Stage == SystemSetupStage
	})

	if len(setupSteps) == 0 {
		return nil, errors.New("no setup steps found")
	}

	lastSetupStep := setupSteps[len(setupSteps)-1]
	return stepSelector{lastSetupStep}.InsertAfter(
		pinVoterReplicasStep{protectedNodes: protectedNodes},
	), nil
}

// leaderLeasesMinVersion is the version in which leader leases were introduced.
var leaderLeasesMinVersion = clusterupgrade.MustParseVersion("v25.1.0")

// maybeEnableLeaderLeases checks if leader leases can be enabled within the given steps.
// If so, it returns mutations to do so as soon as possible in the plan, as well as a
// new stepSelector where all steps have leader leases enabled.
func (m networkPartitionMutator) maybeEnableLeaderLeases(
	steps stepSelector,
) (stepSelector, []mutation, error) {
	var stepsUnderLeaderLeases stepSelector
	fromVersion := steps[0].context.System.FromVersion
	toVersion := steps[0].context.System.ToVersion

	// If we are not upgrading to >= 25.1, then we can't enable leader leases.
	if !toVersion.AtLeast(leaderLeasesMinVersion) {
		return nil, nil, nil
	}

	var firstStep stepSelector

	// If we are upgrading from >= 25.1, then leader leases can be enabled immediately.
	if fromVersion.AtLeast(leaderLeasesMinVersion) {
		firstStepAfterClusterStart := steps.Filter(func(s *singleStep) bool {
			return s.context.System.Stage >= OnStartupStage
		})
		if len(firstStepAfterClusterStart) == 0 {
			return nil, nil, nil
		}

		firstStep = stepSelector{firstStepAfterClusterStart[0]}
		stepsUnderLeaderLeases = firstStepAfterClusterStart[1:]
	} else {
		// If we are upgrading to 25.1, then we need to find the first step after
		// at least one node has been restarted with the new version.
		firstStepAfterRestart := steps.Filter(func(s *singleStep) bool {
			_, isRestart := s.impl.(restartWithNewBinaryStep)
			return isRestart
		})
		if len(firstStepAfterRestart) == 0 {
			return nil, nil, nil
		}

		firstStep = stepSelector{firstStepAfterRestart[0]}
		stepsUnderLeaderLeases = firstStepAfterRestart[1:]
	}

	var mutations []mutation
	disableExpirationLeasesOnlyMutation := firstStep.InsertAfter(
		setClusterSettingStep{
			minVersion:         leaderLeasesMinVersion,
			name:               "kv.expiration_leases_only.enabled",
			value:              false,
			virtualClusterName: install.SystemInterfaceName,
		},
	)
	mutations = append(mutations, disableExpirationLeasesOnlyMutation...)

	enableLeaderFortificationMutation := firstStep.InsertAfter(
		setClusterSettingStep{
			minVersion:         leaderLeasesMinVersion,
			name:               "kv.raft.leader_fortification.fraction_enabled",
			value:              1.0,
			virtualClusterName: install.SystemInterfaceName,
		},
	)
	mutations = append(mutations, enableLeaderFortificationMutation...)

	return stepsUnderLeaderLeases, mutations, nil
}

func (m networkPartitionMutator) generatePartition(
	rng *rand.Rand,
	steps stepSelector,
	idx stepIndex,
	planner *testPlanner,
	protectedNodes option.NodeListOption,
	f *failures.Failer,
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

	// All steps in the window must be compatible with a network partition.
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
		protectedNodes:   protectedNodes,
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
