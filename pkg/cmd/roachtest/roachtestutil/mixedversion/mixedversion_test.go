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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

// testPredecessorMapping is a test-only artificial mapping from
// release series to an arbitrary release in the previous series.
var testPredecessorMapping = map[string]*clusterupgrade.Version{
	"19.2": clusterupgrade.MustParseVersion("v19.1.8"),
	"21.1": clusterupgrade.MustParseVersion("v19.2.16"),
	"21.2": clusterupgrade.MustParseVersion("v21.1.16"),
	"22.1": clusterupgrade.MustParseVersion("v21.2.8"),
	"22.2": clusterupgrade.MustParseVersion("v22.1.11"),
	"23.1": clusterupgrade.MustParseVersion("v22.2.14"),
	"23.2": clusterupgrade.MustParseVersion("v23.1.17"),
	"24.1": clusterupgrade.MustParseVersion("v23.2.4"),
	"24.2": clusterupgrade.MustParseVersion("v24.1.1"),
	"24.3": clusterupgrade.MustParseVersion("v24.2.2"),
}

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
	require.Contains(t, fatalErr.Error(), "mixedversion.NewTest: invalid test options: maxUpgrades (4) must be greater than minUpgrades (10)")

	mvt = newTest(MaxUpgrades(0))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Contains(t, fatalErr.Error(), "mixedversion.NewTest: invalid test options: maxUpgrades (0) must be greater than minUpgrades (1)")

	// Validating minimum supported version.
	defer withTestBuildVersion("v23.1.2")()

	mvt = newTest(MinimumSupportedVersion("v24.1.0"))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Equal(t,
		"mixedversion.NewTest: invalid test options: minimum supported version (v24.1.0) should be from an older release series than current version (v23.1.2)",
		fatalErr.Error(),
	)

	// minimum supported version is older than current version, but
	// still in the same release series.
	mvt = newTest(MinimumSupportedVersion("v23.1.8"))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Equal(t,
		"mixedversion.NewTest: invalid test options: minimum supported version (v23.1.8) should be from an older release series than current version (v23.1.2)",
		fatalErr.Error(),
	)

	// no deployment mode is enabled for a test.
	mvt = newTest(EnabledDeploymentModes())
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Equal(t,
		"mixedversion.NewTest: invalid test options: no deployment modes enabled",
		fatalErr.Error(),
	)

	// an invalid deployment mode is chosen
	mvt = newTest(EnabledDeploymentModes(SystemOnlyDeployment, "my-deployment"))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Equal(t,
		`mixedversion.NewTest: invalid test options: unknown deployment mode "my-deployment"`,
		fatalErr.Error(),
	)

	mvt = newTest(MinimumSupportedVersion("v22.2.0"))
	assertValidTest(mvt, fatalFunc())
	require.NoError(t, fatalErr)
}

func Test_choosePreviousReleases(t *testing.T) {
	defer withTestBuildVersion("v24.3.0")()

	testCases := []struct {
		name              string
		arch              vm.CPUArch
		enableSkipVersion bool
		numUpgrades       int
		predecessorErr    error
		expectedReleases  []string
		expectedErr       string
	}{
		{
			name:           "errors from predecessorFunc are returned",
			predecessorErr: fmt.Errorf("something went wrong"),
			numUpgrades:    3,
			expectedErr:    "something went wrong",
		},
		{
			name:             "predecessor history is unmodified for non-ARM architectures",
			arch:             vm.ArchAMD64,
			numUpgrades:      3,
			expectedReleases: []string{"23.2.4", "24.1.1", "24.2.2"},
		},
		{
			name:             "supported predecessor history is unmodified for ARM architectures",
			arch:             vm.ArchARM64,
			numUpgrades:      3,
			expectedReleases: []string{"23.2.4", "24.1.1", "24.2.2"},
		},
		{
			name:             "predecessor history is filtered for ARM architectures",
			arch:             vm.ArchARM64,
			numUpgrades:      6,
			expectedReleases: []string{"22.2.14", "23.1.17", "23.2.4", "24.1.1", "24.2.2"},
		},
		{
			name:              "skip-version upgrades",
			arch:              vm.ArchAMD64,
			numUpgrades:       3,
			enableSkipVersion: true,
			expectedReleases:  []string{"23.1.17", "23.2.4", "24.1.1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := []CustomOption{NumUpgrades(tc.numUpgrades)}
			if tc.enableSkipVersion {
				opts = append(opts, withSkipVersionProbability(1))
			} else {
				opts = append(opts, DisableSkipVersionUpgrades)
			}

			mvt := newTest(opts...)
			mvt.predecessorFunc = func(_ *rand.Rand, v *clusterupgrade.Version) (*clusterupgrade.Version, error) {
				return testPredecessorMapping[v.Series()], tc.predecessorErr
			}
			mvt._arch = &tc.arch

			releases, err := mvt.choosePreviousReleases()
			if tc.expectedErr == "" {
				require.NoError(t, err)
				var expectedVersions []*clusterupgrade.Version
				for _, er := range tc.expectedReleases {
					expectedVersions = append(expectedVersions, clusterupgrade.MustParseVersion(er))
				}
				require.Equal(t, expectedVersions, releases)
			} else {
				require.Error(t, err)
				require.Equal(t, tc.expectedErr, err.Error())
			}
		})
	}
}

// withTestBuildVersion overwrites the `TestBuildVersion` variable in
// the `clusterupgrade` package, allowing tests to set a fixed
// "current version". Returns a function that resets that variable.
func withTestBuildVersion(v string) func() {
	testBuildVersion := version.MustParse(v)
	clusterupgrade.TestBuildVersion = testBuildVersion
	return func() { clusterupgrade.TestBuildVersion = nil }
}
