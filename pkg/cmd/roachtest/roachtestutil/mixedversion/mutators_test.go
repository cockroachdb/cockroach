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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPreserveDowngradeOptionRandomizerMutator tests basic behaviour
// of the mutator by directly inspecting the mutations it produces
// when `Generate` is called. This mutator is also tested as part of
// the planner test suite, with a datadriven test.
func TestPreserveDowngradeOptionRandomizerMutator(t *testing.T) {
	numUpgrades := 3
	mvt := newBasicUpgradeTest(NumUpgrades(numUpgrades))
	plan, err := mvt.plan()
	require.NoError(t, err)

	var mut preserveDowngradeOptionRandomizerMutator
	mutations := mut.Generate(newRand(), plan)
	require.NotEmpty(t, mutations)
	require.True(t, len(mutations)%2 == 0, "should produce even number of mutations") // one removal and one insertion per upgrade

	// First half of mutations should be the removals of the existing
	// `allowUpgradeStep`s.
	for j := 0; j < numUpgrades/2; j++ {
		require.Equal(t, mutationRemove, mutations[j].op)
		require.IsType(t, allowUpgradeStep{}, mutations[j].reference.impl)
	}

	// Second half of mutations should be insertions of new
	// `allowUpgradeStep`s.
	for j := numUpgrades / 2; j < len(mutations); j++ {
		require.Equal(t, mutationInsertBefore, mutations[j].op)
		require.IsType(t, allowUpgradeStep{}, mutations[j].impl)
	}
}
