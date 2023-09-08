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
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/version"
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

func Test_choosePreviousReleases(t *testing.T) {
	testCases := []struct {
		name               string
		arch               vm.CPUArch
		predecessorHistory []string
		predecessorErr     error
		expectedReleases   []string
		expectedErr        string
	}{
		{
			name:           "errors from predecessorFunc are returned",
			predecessorErr: fmt.Errorf("something went wrong"),
			expectedErr:    "something went wrong",
		},
		{
			name:               "predecessor history is unmodified for non-ARM architectures",
			arch:               vm.ArchAMD64,
			predecessorHistory: []string{"22.1.3", "22.2.10", "23.1.3"},
			expectedReleases:   []string{"22.1.3", "22.2.10", "23.1.3"},
		},
		{
			name:               "supported predecessor history is unmodified for ARM architectures",
			arch:               vm.ArchARM64,
			predecessorHistory: []string{"22.2.0", "23.1.10", "23.2.3"}, // all supported
			expectedReleases:   []string{"22.2.0", "23.1.10", "23.2.3"},
		},
		{
			name:               "predecessor history is filtered for ARM architectures",
			arch:               vm.ArchARM64,
			predecessorHistory: []string{"21.2.12", "22.1.10", "22.2.3", "23.1.0"},
			expectedReleases:   []string{"22.2.3", "23.1.0"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mvt := newTest()
			mvt._arch = &tc.arch
			mvt.predecessorFunc = func(_ *rand.Rand, _ *version.Version, _ int) ([]string, error) {
				return tc.predecessorHistory, tc.predecessorErr
			}

			releases, err := mvt.choosePreviousReleases()
			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedReleases, releases)
			} else {
				require.Error(t, err)
				require.Equal(t, tc.expectedErr, err.Error())
			}
		})
	}
}
