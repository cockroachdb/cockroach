// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mixedversion

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

type (
	// TestPlan is the output of planning a mixed-version test. The
	// initialVersion and finalVersion fields are just for presentation
	// purposes, as the plan is defined by the sequence of steps that it
	// contains.
	TestPlan struct {
		initialVersion string
		finalVersion   string
		steps          []testStep
	}

	// testPlanner wraps the state and the logic involved in generating
	// a test plan from the given rng and user-provided hooks.
	testPlanner struct {
		stepCount      int
		initialVersion string
		crdbNodes      option.NodeListOption
		rt             test.Test
		hooks          *testHooks
		prng           *rand.Rand
	}
)

const (
	branchString       = "├──"
	nestedBranchString = "│   "
	lastBranchString   = "└──"
	lastBranchPadding  = "   "
)

// Plan returns the TestPlan generated using the `prng` in the
// testPlanner field. Currently, the test will always follow the
// following high level outline:
//
//   - start all nodes in the cluster from the predecessor version,
//     using fixtures.
//   - set `preserve_downgrade_option`.
//   - run startup hooks.
//   - upgrade all nodes to the current cockroach version (running
//     mixed-version hooks at times determined by the planner).
//   - downgrade all nodes back to the predecessor version (running
//     mixed-version hooks again).
//   - upgrade all nodes back to the current cockroach version one
//     more time (running mixed-version hooks).
//   - finally, reset `preserve_downgrade_option`, allowing the
//     cluster to upgrade. Mixed-version hooks may be executed while
//     this is happening.
//   - run after-test hooks.
//
// TODO(renato): further opportunities for random exploration:
// - going back multiple releases instead of just one
// - picking a patch release randomly instead of just the latest release
// - inserting arbitrary delays (`sleep` calls) during the test.
func (p *testPlanner) Plan() *TestPlan {
	var steps []testStep
	addSteps := func(ss []testStep) { steps = append(steps, ss...) }

	addSteps(p.initSteps())
	addSteps(p.hooks.BackgroundSteps(p.nextID, p.initialContext()))

	// previous -> current
	addSteps(p.upgradeSteps(p.initialVersion, clusterupgrade.MainVersion))
	// current -> previous (rollback)
	addSteps(p.downgradeSteps(clusterupgrade.MainVersion, p.initialVersion))
	// previous -> current
	addSteps(p.upgradeSteps(p.initialVersion, clusterupgrade.MainVersion))
	// finalize
	addSteps(p.finalizeUpgradeSteps())

	addSteps(p.finalSteps())
	return &TestPlan{
		initialVersion: p.initialVersion,
		finalVersion:   versionMsg(clusterupgrade.MainVersion),
		steps:          steps,
	}
}

func (p *testPlanner) initialContext() Context {
	return Context{
		FromVersion:      p.initialVersion,
		ToVersion:        clusterupgrade.MainVersion,
		FromVersionNodes: p.crdbNodes,
	}
}

func (p *testPlanner) finalContext(finalizing bool) Context {
	return Context{
		FromVersion:    p.initialVersion,
		ToVersion:      clusterupgrade.MainVersion,
		ToVersionNodes: p.crdbNodes,
		Finalizing:     finalizing,
	}
}

// initSteps returns the sequence of steps that should be executed
// before we start changing binaries on nodes in the process of
// upgrading/downgrading. It will also run any startup hooks the user
// may have provided.
func (p *testPlanner) initSteps() []testStep {
	return append([]testStep{
		startFromCheckpointStep{id: p.nextID(), version: p.initialVersion, rt: p.rt, crdbNodes: p.crdbNodes},
		uploadCurrentVersionStep{id: p.nextID(), rt: p.rt, crdbNodes: p.crdbNodes, dest: CurrentCockroachPath},
		waitForStableClusterVersionStep{id: p.nextID(), nodes: p.crdbNodes},
		preserveDowngradeOptionStep{id: p.nextID(), prng: p.newRNG(), crdbNodes: p.crdbNodes},
	}, p.hooks.StartupSteps(p.nextID, p.initialContext())...)
}

// finalSteps are the steps to be run once the nodes have been
// upgraded/downgraded. It will wait for the cluster version on all
// nodes to be the same and then run any after-finalization hooks the
// user may have provided.
func (p *testPlanner) finalSteps() []testStep {
	return append([]testStep{
		waitForStableClusterVersionStep{id: p.nextID(), nodes: p.crdbNodes},
	}, p.hooks.AfterUpgradeFinalizedSteps(p.nextID, p.finalContext(false /* finalizing */))...)
}

func (p *testPlanner) upgradeSteps(from, to string) []testStep {
	msg := fmt.Sprintf("upgrade nodes %v from %q to %q", p.crdbNodes, versionMsg(from), versionMsg(to))
	return p.changeVersionSteps(from, to, msg)
}

func (p *testPlanner) downgradeSteps(from, to string) []testStep {
	msg := fmt.Sprintf("downgrade nodes %v from %q to %q", p.crdbNodes, versionMsg(from), versionMsg(to))
	return p.changeVersionSteps(from, to, msg)
}

// changeVersionSteps returns the sequence of steps to be performed
// when we are changing the version of cockroach in the nodes of a
// cluster (either upgrading to a new binary, or downgrading to a
// predecessor version).
func (p *testPlanner) changeVersionSteps(from, to, label string) []testStep {
	// copy `crdbNodes` here so that shuffling won't mutate that array
	oldVersionNodes := append(option.NodeListOption{}, p.crdbNodes...)
	newVersionNodes := option.NodeListOption{}

	// change the binary version on the nodes in random order
	p.prng.Shuffle(len(oldVersionNodes), func(i, j int) {
		oldVersionNodes[i], oldVersionNodes[j] = oldVersionNodes[j], oldVersionNodes[i]
	})

	var steps []testStep
	nodeOrder := append(option.NodeListOption{}, oldVersionNodes...)
	for _, node := range nodeOrder {
		steps = append(steps, restartWithNewBinaryStep{id: p.nextID(), version: to, node: node, rt: p.rt})
		oldVersionNodes = oldVersionNodes[1:]
		newVersionNodes = append(newVersionNodes, node)

		testContext := Context{
			FromVersion:      from,
			FromVersionNodes: oldVersionNodes,
			ToVersion:        to,
			ToVersionNodes:   newVersionNodes,
		}
		steps = append(steps, p.hooks.MixedVersionSteps(testContext, p.nextID)...)
	}

	return []testStep{sequentialRunStep{label: label, steps: steps}}
}

// finalizeUpgradeSteps finalizes the upgrade by resetting the
// `preserve_downgrade_option` and potentially running mixed-version
// hooks while the cluster version is changing.
func (p *testPlanner) finalizeUpgradeSteps() []testStep {
	return append([]testStep{
		finalizeUpgradeStep{id: p.nextID(), prng: p.newRNG(), crdbNodes: p.crdbNodes},
	}, p.hooks.MixedVersionSteps(p.finalContext(true /* finalizing */), p.nextID)...)
}

func (p *testPlanner) nextID() int {
	p.stepCount++
	return p.stepCount
}

func (p *testPlanner) newRNG() *rand.Rand {
	return rngFromRNG(p.prng)
}

// PrettyPrint displays a tree-like view of the mixed-version test
// plan, useful when debugging mixed-version test failures. Each step
// is assigned an ID, making it easy to correlate the step that
// failed with the point in the test where the failure occurred. See
// test file for an example of the output of this function.
func (plan *TestPlan) PrettyPrint() string {
	var out strings.Builder
	for i, step := range plan.steps {
		plan.prettyPrintStep(&out, step, treeBranchString(i, len(plan.steps)))
	}

	return fmt.Sprintf(
		"mixed-version test plan for upgrading from %s to %s:\n%s",
		plan.initialVersion, plan.finalVersion, out.String(),
	)
}

func (plan *TestPlan) prettyPrintStep(out *strings.Builder, step testStep, prefix string) {
	writeNested := func(label string, steps []testStep) {
		out.WriteString(fmt.Sprintf("%s %s\n", prefix, label))
		for i, subStep := range steps {
			nestedPrefix := strings.ReplaceAll(prefix, branchString, nestedBranchString)
			nestedPrefix = strings.ReplaceAll(nestedPrefix, lastBranchString, lastBranchPadding)
			subPrefix := fmt.Sprintf("%s%s", nestedPrefix, treeBranchString(i, len(steps)))
			plan.prettyPrintStep(out, subStep, subPrefix)
		}
	}

	// writeRunnable is the function that generates the description for
	// a singleStep. It can include extra information, such as whether
	// there's a delay associated with the step (in the case of
	// concurrent execution), and what database node the step is
	// connecting to.
	writeSingle := func(rs singleStep, extraContext ...string) {
		var extras string
		if contextStr := strings.Join(extraContext, ", "); contextStr != "" {
			extras = ", " + contextStr
		}
		out.WriteString(fmt.Sprintf(
			"%s %s%s (%d)\n", prefix, rs.Description(), extras, rs.ID(),
		))
	}

	switch s := step.(type) {
	case sequentialRunStep:
		writeNested(s.Description(), s.steps)
	case concurrentRunStep:
		writeNested(s.Description(), s.delayedSteps)
	case delayedStep:
		delayStr := fmt.Sprintf("after %s delay", s.delay)
		writeSingle(s.step.(singleStep), delayStr)
	default:
		writeSingle(s.(singleStep))
	}
}

func treeBranchString(idx, sliceLen int) string {
	if idx == sliceLen-1 {
		return lastBranchString
	}

	return branchString
}
