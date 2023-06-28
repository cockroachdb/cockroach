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
	// TestPlan is the output of planning a mixed-version test. The list
	// of `versions` are just for presentation purposes, as the plan is
	// defined by the sequence of steps that it contains.
	TestPlan struct {
		versions       []string
		startClusterID int // step ID after which the cluster should be ready to receive connections
		steps          []testStep
	}

	// testPlanner wraps the state and the logic involved in generating
	// a test plan from the given rng and user-provided hooks.
	testPlanner struct {
		stepCount      int
		startClusterID int
		versions       []string
		crdbNodes      option.NodeListOption
		rt             test.Test
		options        testOptions
		hooks          *testHooks
		prng           *rand.Rand
		bgChans        []shouldStop
	}
)

const (
	branchString       = "├──"
	nestedBranchString = "│   "
	lastBranchString   = "└──"
	lastBranchPadding  = "   "
)

// Plan returns the TestPlan used to upgrade the cluster from the
// first to the final version in the `versions` field. The test plan
// roughly translates to the sequence of steps below:
//
//  1. start all nodes in the cluster at the initial version, maybe
//     using fixtures.
//  2. run startup hooks.
//  3. for each cluster upgrade:
//     - set `preserve_downgrade_option`.
//     - upgrade all nodes to the next cockroach version (running
//     mixed-version hooks at times determined by the planner).
//     - maybe downgrade all nodes back to the previous version
//     (running mixed-version hooks again).
//     - if a rollback was performed, upgrade all nodes back to the
//     next version one more time (running mixed-version hooks).
//     - reset `preserve_downgrade_option`, allowing the cluster
//     to upgrade. Mixed-version hooks may be executed while
//     this is happening.
//     - run after-upgrade hooks.
func (p *testPlanner) Plan() *TestPlan {
	var steps []testStep

	steps = append(steps, p.testSetupSteps()...)
	steps = append(steps, p.hooks.BackgroundSteps(p.nextID, p.longRunningContext(), p.bgChans)...)

	for prevVersionIdx := 0; prevVersionIdx+1 < len(p.versions); prevVersionIdx++ {
		fromVersion := p.versions[prevVersionIdx]
		toVersion := p.versions[prevVersionIdx+1]

		upgradeStep := sequentialRunStep{
			label: fmt.Sprintf("upgrade cluster from %q to %q", versionMsg(fromVersion), versionMsg(toVersion)),
		}
		addUpgradeSteps := func(ss []testStep) {
			upgradeStep.steps = append(upgradeStep.steps, ss...)
		}

		addUpgradeSteps(p.initUpgradeSteps(fromVersion, toVersion))

		// previous -> next
		addUpgradeSteps(p.upgradeSteps(fromVersion, toVersion))
		if p.shouldRollback(toVersion) {
			// next -> previous (rollback)
			addUpgradeSteps(p.downgradeSteps(toVersion, fromVersion))

			// previous -> current
			addUpgradeSteps(p.upgradeSteps(fromVersion, toVersion))
		}

		// finalize
		addUpgradeSteps(p.finalizeUpgradeSteps(fromVersion, toVersion))

		// wait for upgrade to finalize
		addUpgradeSteps(p.finalUpgradeSteps(fromVersion, toVersion))

		steps = append(steps, upgradeStep)
	}

	return &TestPlan{
		versions:       p.versions,
		startClusterID: p.startClusterID,
		steps:          steps,
	}
}

func (p *testPlanner) finalContext(fromVersion, toVersion string, finalizing bool) Context {
	return Context{
		FromVersion:    fromVersion,
		ToVersion:      toVersion,
		ToVersionNodes: p.crdbNodes,
		Finalizing:     finalizing,
	}
}

// longRunningContext is the test context passed to long running tasks
// (background functions and the like). In these scenarios,
// `FromVersion` and `ToVersion` correspond to, respectively, the
// initial version the cluster is started at, and the final version
// once the test finishes.
func (p *testPlanner) longRunningContext() Context {
	return Context{
		FromVersion: p.versions[0],
		ToVersion:   p.versions[len(p.versions)-1],
		Finalizing:  false,
	}
}

func (p *testPlanner) testSetupSteps() []testStep {
	initialVersion := p.versions[0]

	var steps []testStep
	if p.prng.Float64() < p.options.useFixturesProbability {
		steps = []testStep{installFixturesStep{id: p.nextID(), version: initialVersion, crdbNodes: p.crdbNodes}}
	}

	p.startClusterID = p.nextID()
	steps = append(steps,
		startStep{id: p.startClusterID, version: initialVersion, rt: p.rt, crdbNodes: p.crdbNodes},
		waitForStableClusterVersionStep{id: p.nextID(), nodes: p.crdbNodes, timeout: p.options.upgradeTimeout},
	)

	return append(
		steps,
		p.hooks.StartupSteps(p.nextID, p.longRunningContext())...,
	)
}

// initUpgradeSteps returns the sequence of steps that should be
// executed before we start changing binaries on nodes in the process
// of upgrading/downgrading.
func (p *testPlanner) initUpgradeSteps(fromVersion, toVersion string) []testStep {
	return []testStep{
		preserveDowngradeOptionStep{id: p.nextID(), prng: p.newRNG(), crdbNodes: p.crdbNodes},
	}
}

// finalUpgradeSteps are the steps to be run once the nodes have been
// upgraded/downgraded. It will wait for the cluster version on all
// nodes to be the same and then run any after-finalization hooks the
// user may have provided.
func (p *testPlanner) finalUpgradeSteps(fromVersion, toVersion string) []testStep {
	return append([]testStep{
		waitForStableClusterVersionStep{id: p.nextID(), nodes: p.crdbNodes, timeout: p.options.upgradeTimeout},
	}, p.hooks.AfterUpgradeFinalizedSteps(p.nextID, p.finalContext(fromVersion, toVersion, false /* finalizing */))...)
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
func (p *testPlanner) finalizeUpgradeSteps(fromVersion, toVersion string) []testStep {
	return append([]testStep{
		finalizeUpgradeStep{id: p.nextID(), prng: p.newRNG(), crdbNodes: p.crdbNodes},
	}, p.hooks.MixedVersionSteps(p.finalContext(fromVersion, toVersion, true /* finalizing */), p.nextID)...)
}

// shouldRollback returns whether the test will attempt a rollback. If
// we are upgrading to the current version being tested, we always
// rollback, as we want to expose that upgrade to complex upgrade
// scenarios. If this is an intermediate upgrade in a multi-upgrade
// test, then rollback with a probability. This stops tests from
// having excessively long running times.
func (p *testPlanner) shouldRollback(toVersion string) bool {
	if toVersion == clusterupgrade.MainVersion {
		return true
	}

	return p.prng.Float64() < rollbackIntermediateUpgradesProbability
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

	formattedVersions := make([]string, 0, len(plan.versions))
	for _, v := range plan.versions {
		formattedVersions = append(formattedVersions, fmt.Sprintf("%q", versionMsg(v)))
	}

	return fmt.Sprintf(
		"mixed-version test plan for upgrading from %s:\n%s",
		strings.Join(formattedVersions, " to "), out.String(),
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

	// writeSingle is the function that generates the description for
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
