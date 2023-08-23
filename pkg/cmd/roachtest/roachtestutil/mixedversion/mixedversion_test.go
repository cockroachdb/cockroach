// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/stretchr/testify/require"
)

func Test_assertValidTest(t *testing.T) {
	var fatalErr error
	fatalFunc := func() func(...interface{}) {
		fatalErr = nil
		return func(args ...interface{}) {
			require.Len(t, args, 1)
			err, isErr := args[0].(error)
			require.True(t, isErr)

			fatalErr = err
		}
	}

	// Validating that number of nodes matches what is encoded in the
	// fixtures if using them.
	notEnoughNodes := option.NodeListOption{1, 2, 3}
	tooManyNodes := option.NodeListOption{1, 2, 3, 5, 6}
	for _, crdbNodes := range []option.NodeListOption{notEnoughNodes, tooManyNodes} {
		mvt := newTest()
		mvt.crdbNodes = crdbNodes

		assertValidTest(mvt, fatalFunc())
		require.Error(t, fatalErr)
		require.Contains(t, fatalErr.Error(), "mixedversion.NewTest: invalid cluster: use of fixtures requires 4 cockroach nodes")

		mvt = newTest(NeverUseFixtures)
		mvt.crdbNodes = crdbNodes

		assertValidTest(mvt, fatalFunc())
		require.NoError(t, fatalErr)
	}

	// Validating number of upgrades specified by the test.
	mvt := newTest(MinUpgrades(10))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Contains(t, fatalErr.Error(), "mixedversion.NewTest: invalid test options: maxUpgrades (3) must be greater than minUpgrades (10)")

	mvt = newTest(MaxUpgrades(0))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Contains(t, fatalErr.Error(), "mixedversion.NewTest: invalid test options: maxUpgrades (0) must be greater than minUpgrades (1)")
}
