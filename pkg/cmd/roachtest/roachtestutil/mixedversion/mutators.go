// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mixedversion

import "math/rand"

const (
	// PreserveDowngradeOptionRandomizer is a mutator that changes the
	// timing in which the `preserve_downgrade_option` cluster setting
	// is reset during the upgrade test. Typically (when this mutator is
	// not enabled), this happens at the end of the `LastUpgrade`
	// stage, when all nodes have been restarted and are running the
	// target binary version. However, we have observed bugs in the past
	// that only manifested when this setting was reset at different
	// times in the test (e.g., #111610); this mutator exists to catch
	// regressions of this type.
	PreserveDowngradeOptionRandomizer = "preserve_downgrade_option_randomizer"
)

type preserveDowngradeOptionRandomizerMutator struct{}

func (m preserveDowngradeOptionRandomizerMutator) Name() string {
	return PreserveDowngradeOptionRandomizer
}

// Most runs will have this mutator disabled, as the base upgrade
// plan's approach of resetting the cluster setting when all nodes are
// upgraded is the most sensible / common.
func (m preserveDowngradeOptionRandomizerMutator) Probability() float64 {
	return 0.3
}

// Generate returns mutations to remove the existing step to reset the
// `preserve_downgrade_option` cluster setting, and reinserts it back
// in some other point in the test, before all nodes are upgraded. Not
// every upgrade in the test plan is affected, but the upgrade to the
// current version is always mutated.
func (m preserveDowngradeOptionRandomizerMutator) Generate(
	rng *rand.Rand, plan *TestPlan,
) []mutation {
	var mutations []mutation
	for _, upgradeSelector := range randomUpgrades(rng, plan) {
		removeExistingStep := upgradeSelector.
			Filter(func(s *singleStep) bool {
				_, ok := s.impl.(allowUpgradeStep)
				return ok
			}).
			Remove()

		addRandomly := upgradeSelector.
			Filter(func(s *singleStep) bool {
				// It is valid to reset the cluster setting when we are
				// performing a rollback (as we know the next upgrade will be
				// the final one); or during the final upgrade itself.
				return (s.context.Stage == LastUpgradeStage || s.context.Stage == RollbackUpgradeStage) &&
					// We also don't want all nodes to be running the latest
					// binary, as that would be equivalent to the test plan
					// without this mutator.
					len(s.context.NodesInNextVersion()) < len(s.context.CockroachNodes)
			}).
			RandomStep(rng).
			// Note that we don't attempt a concurrent insert because the
			// selected step could be one that restarts a cockroach node,
			// and `allowUpgradeStep` could fail in that situation.
			InsertBefore(allowUpgradeStep{})

		// Finally, we update the context associated with every step where
		// all nodes are running the next verison to indicate they are in
		// fact in `Finalizing` state. Previously, this would only be set
		// after `allowUpgradeStep` but, when this mutator is enabled,
		// `Finalizing` should be `true` as soon as all nodes are on the
		// next version.
		for _, step := range upgradeSelector.
			Filter(func(s *singleStep) bool {
				return s.context.Stage == LastUpgradeStage &&
					len(s.context.NodesInNextVersion()) == len(s.context.CockroachNodes)
			}) {
			step.context.Finalizing = true
		}

		mutations = append(mutations, removeExistingStep...)
		mutations = append(mutations, addRandomly...)
	}

	return mutations
}

// randomUpgrades returns selectors for the steps of a random subset
// of upgrades in the plan. The last upgrade is always returned, as
// that is the most critical upgrade being tested.
func randomUpgrades(rng *rand.Rand, plan *TestPlan) []stepSelector {
	allUpgrades := plan.allUpgrades()
	numChanges := rng.Intn(len(allUpgrades)) // other than last upgrade
	allExceptLastUpgrade := append([]*upgradePlan{}, allUpgrades[:len(allUpgrades)-1]...)

	rng.Shuffle(len(allExceptLastUpgrade), func(i, j int) {
		allExceptLastUpgrade[i], allExceptLastUpgrade[j] = allExceptLastUpgrade[j], allExceptLastUpgrade[i]
	})

	byUpgrade := func(upgrade *upgradePlan) func(*singleStep) bool {
		return func(s *singleStep) bool {
			return s.context.FromVersion.Equal(upgrade.from)
		}
	}

	// By default, include the last upgrade.
	selectors := []stepSelector{
		plan.newStepSelector().Filter(byUpgrade(allUpgrades[len(allUpgrades)-1])),
	}
	for _, upgrade := range allExceptLastUpgrade[:numChanges] {
		selectors = append(selectors, plan.newStepSelector().Filter(byUpgrade(upgrade)))
	}

	return selectors
}
