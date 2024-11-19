// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"golang.org/x/exp/maps"
)

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
	//
	// Note that this mutator only applies to the system tenant. For
	// other virtual clusters, the upgrade performs an explicit `SET` on
	// the cluster version since the auto upgrades feature has been
	// broken for tenants in several published releases (see #121858).
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
// current version is always mutated. The length of the returned
// mutations is always even.
func (m preserveDowngradeOptionRandomizerMutator) Generate(
	rng *rand.Rand, plan *TestPlan,
) []mutation {
	var mutations []mutation
	for _, upgradeSelector := range randomUpgrades(rng, plan) {
		removeExistingStep := upgradeSelector.
			Filter(func(s *singleStep) bool {
				step, ok := s.impl.(allowUpgradeStep)
				return ok && step.virtualClusterName == install.SystemInterfaceName
			}).
			Remove()

		addRandomly := upgradeSelector.
			Filter(func(s *singleStep) bool {
				// It is valid to reset the cluster setting when we are
				// performing a rollback (as we know the next upgrade will be
				// the final one); or during the final upgrade itself.
				return (s.context.System.Stage == LastUpgradeStage || s.context.System.Stage == RollbackUpgradeStage) &&
					// We also don't want all nodes to be running the latest
					// binary, as that would be equivalent to the test plan
					// without this mutator.
					len(s.context.System.NodesInNextVersion()) < len(s.context.System.Descriptor.Nodes)
			}).
			RandomStep(rng).
			// Note that we don't attempt a concurrent insert because the
			// selected step could be one that restarts a cockroach node,
			// and `allowUpgradeStep` could fail in that situation.
			InsertBefore(allowUpgradeStep{virtualClusterName: install.SystemInterfaceName})

		// Finally, we update the context associated with every step where
		// all nodes are running the next version to indicate they are in
		// fact in `Finalizing` state. Previously, this would only be set
		// after `allowUpgradeStep` but, when this mutator is enabled,
		// `Finalizing` should be `true` as soon as all nodes are on the
		// next version.
		for _, step := range upgradeSelector.
			Filter(func(s *singleStep) bool {
				return s.context.System.Stage == LastUpgradeStage &&
					len(s.context.System.NodesInNextVersion()) == len(s.context.System.Descriptor.Nodes)
			}) {
			step.context.System.Finalizing = true
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
			return s.context.System.FromVersion.Equal(upgrade.from)
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

// ClusterSettingMutator returns the name of the mutator associated
// with the given cluster setting name. Callers can disable a specific
// cluster setting mutator with:
//
//	mixedversion.DisableMutators(mixedversion.ClusterSettingMutator("my_setting"))
func ClusterSettingMutator(name string) string {
	return fmt.Sprintf("cluster_setting[%s]", name)
}

// clusterSettingMutator implements a mutator that randomly sets (or
// resets) a cluster setting during a mixed-version test.
//
// TODO(renato): currently this can only be used for changing settings
// on the system tenant; support for non-system virtual clusters will
// be added in the future.
type clusterSettingMutator struct {
	// The name of the cluster setting.
	name string
	// The probability that the mutator will be applied to a test.
	probability float64
	// The list of possible values we may set the setting to.
	possibleValues []interface{}
	// The version the cluster setting was introduced.
	minVersion *clusterupgrade.Version
	// The maximum number of changes (set or reset) we will perform.
	maxChanges int
}

// clusterSettingMutatorOption is the signature of functions passed to
// `newClusterSettingMutator` that allow callers to customize
// parameters of the mutator.
type clusterSettingMutatorOption func(*clusterSettingMutator)

//lint:ignore U1000 currently unused // TODO(renato): remove when used.
func clusterSettingProbability(p float64) clusterSettingMutatorOption {
	return func(csm *clusterSettingMutator) {
		csm.probability = p
	}
}

func clusterSettingMinimumVersion(v string) clusterSettingMutatorOption {
	return func(csm *clusterSettingMutator) {
		csm.minVersion = clusterupgrade.MustParseVersion(v)
	}
}

//lint:ignore U1000 currently unused // TODO(renato): remove when used.
func clusterSettingMaxChanges(n int) clusterSettingMutatorOption {
	return func(csm *clusterSettingMutator) {
		csm.maxChanges = n
	}
}

// newClusterSettingMutator creates a new `clusterSettingMutator` for
// the given cluster setting. The list of `values` are the list of
// values that the cluster setting can be set to.
func newClusterSettingMutator[T any](
	name string, values []T, opts ...clusterSettingMutatorOption,
) clusterSettingMutator {
	possibleValues := make([]interface{}, 0, len(values))
	for _, v := range values {
		possibleValues = append(possibleValues, v)
	}

	csm := clusterSettingMutator{
		name:           name,
		probability:    0.3,
		possibleValues: possibleValues,
		maxChanges:     3,
	}

	for _, opt := range opts {
		opt(&csm)
	}

	return csm
}

func (m clusterSettingMutator) Name() string {
	return ClusterSettingMutator(m.name)
}

func (m clusterSettingMutator) Probability() float64 {
	return m.probability
}

// Generate returns a list of mutations to be performed on the
// original test plan. Up to `maxChanges` steps will be added to the
// plan. Changes may be concurrent with user-provided steps and may
// happen any time after cluster setup.
func (m clusterSettingMutator) Generate(rng *rand.Rand, plan *TestPlan) []mutation {
	var mutations []mutation

	// possiblePointsInTime is the list of steps in the plan that are
	// valid points in time during the mixedversion test where applying
	// a cluster setting change is acceptable.
	possiblePointsInTime := plan.
		newStepSelector().
		Filter(func(s *singleStep) bool {
			if m.minVersion != nil {
				// If we have a minimum version set, we need to make sure we
				// are upgrading to a supported version.
				if !s.context.System.ToVersion.AtLeast(m.minVersion) {
					return false
				}

				// If we are upgrading from a version that is older than the
				// minimum supported version, then only upgraded nodes are
				// able to service the cluster setting change request. In that
				// case, we ensure there is at least one such node.
				if !s.context.System.FromVersion.AtLeast(m.minVersion) && len(s.context.System.NodesInNextVersion()) == 0 {
					return false
				}
			}

			// We skip restart steps as we might insert the cluster setting
			// change step concurrently with the selected step.
			_, isRestartSystem := s.impl.(restartWithNewBinaryStep)
			_, isRestartTenant := s.impl.(restartVirtualClusterStep)
			isRestart := isRestartSystem || isRestartTenant
			return s.context.System.Stage >= OnStartupStage && !isRestart
		})

	for _, changeStep := range m.changeSteps(rng, len(possiblePointsInTime)) {
		var currentSlot int
		applyChange := possiblePointsInTime.
			Filter(func(_ *singleStep) bool {
				currentSlot++
				return currentSlot == changeStep.slot
			}).
			Insert(rng, changeStep.impl)

		mutations = append(mutations, applyChange...)
	}

	return mutations
}

// clusterSettingChangeStep encapsulates the information necessary to
// insert a cluster setting change step into a test plan. The `impl`
// field contains the implementation of the step itself, while `slot`
// indicates the position, relative to the possible list of points in
// time where changes can happen, where we will carry out the change.
type clusterSettingChangeStep struct {
	impl singleStepProtocol
	slot int
}

// changeSteps returns a list of `clusterSettingChangeStep`s that
// describe what steps to perform and where to insert them. The
// location (`slot`) is the 1-indexed position relative to the number
// of possible steps where they *can* happen.
//
// The changes are chosen based on a very simple state-machine: when
// the cluster setting is currently set to some value, we
// non-deterministically choose to either reset it or set it to a
// different value (if there is any); if the setting is currently
// reset, we choose a random value to set it to.
func (m clusterSettingMutator) changeSteps(
	rng *rand.Rand, numPossibleSteps int,
) []clusterSettingChangeStep {
	numChanges := 1 + rng.Intn(m.maxChanges)
	numChanges = min(numChanges, numPossibleSteps)
	chosenSlots := make(map[int]struct{})
	for len(chosenSlots) != numChanges {
		chosenSlots[1+rng.Intn(numPossibleSteps)] = struct{}{}
	}

	slots := maps.Keys(chosenSlots)
	sort.Ints(slots)

	nextSlot := func() int {
		n := slots[0]
		slots = slots[1:]
		return n
	}

	// setToValue indicates that the cluster setting is currently set to
	// the `value` field.
	type setToValue struct {
		value interface{}
	}

	// reset indicates the cluster setting is currently reset.
	type reset struct{}

	// When the test starts, the cluster setting is `reset`.
	var currentState interface{} = reset{}
	var steps []clusterSettingChangeStep

	// setClusterSettingTransition adds a new step to the return value,
	// encoding that we are changing the cluster setting to one of the
	// `possibleValues`. It also updates `currentState` accordingly.
	setClusterSettingTransition := func(possibleValues []interface{}) {
		newValue := possibleValues[rng.Intn(len(possibleValues))]
		steps = append(steps, clusterSettingChangeStep{
			impl: setClusterSettingStep{
				minVersion:         m.minVersion,
				name:               m.name,
				value:              newValue,
				virtualClusterName: install.SystemInterfaceName,
			},
			slot: nextSlot(),
		})

		currentState = setToValue{newValue}
	}

	// resetClusterSettingTransition adds a reset step to the return
	// value, and moves our `currentState` accordingly.
	resetClusterSettingTransition := func() {
		steps = append(steps, clusterSettingChangeStep{
			impl: resetClusterSettingStep{
				minVersion:         m.minVersion,
				name:               m.name,
				virtualClusterName: install.SystemInterfaceName,
			},
			slot: nextSlot(),
		})

		currentState = reset{}
	}

	for j := 0; j < numChanges; j++ {
		switch s := currentState.(type) {
		case setToValue:
			var possibleOtherValues []interface{}
			for _, v := range m.possibleValues {
				if v != s.value {
					possibleOtherValues = append(possibleOtherValues, v)
				}
			}

			// If the cluster setting is currently set to some value, we
			// reset it if there are no other values to set it to, or with a
			// 50% chance.
			performReset := len(possibleOtherValues) == 0 || rng.Float64() < 0.5
			if performReset {
				resetClusterSettingTransition()
			} else {
				setClusterSettingTransition(possibleOtherValues)
			}

		case reset:
			// If the cluster setting is currently reset, we choose a
			// possible value for the cluster setting, and update it.
			setClusterSettingTransition(m.possibleValues)
		}
	}

	return steps
}
