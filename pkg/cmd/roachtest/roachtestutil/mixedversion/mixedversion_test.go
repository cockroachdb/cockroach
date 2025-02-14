// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	_ "embed"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// testPredecessorMapping is a test-only artificial mapping from
// release series to an arbitrary release in the previous series.
//
// TODO(radu): we use this mapping starting with the current version, so we need
// to keep updating it. Ideally, all unit tests would use a fixed "current"
// version so this doesn't need to change.
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
	"25.2": clusterupgrade.MustParseVersion("v24.3.0"),
}

//go:embed testdata/test_releases.yaml
var rawTestReleaseData []byte

// testReleaseData contains a mapping like the one in
// `cockroach_releases.yaml`, but hardcoded in these tests so that we
// are able to test the `randomPredecessor` function.
var testReleaseData = func() map[string]release.Series {
	var result map[string]release.Series
	err := yaml.UnmarshalStrict(rawTestReleaseData, &result)
	if err != nil {
		panic(fmt.Errorf("invalid test_releases.yaml: %w", err))
	}

	return result
}()

func Test_validDeploymentModesForCloud(t *testing.T) {
	testCases := []struct {
		name          string
		cloud         spec.Cloud
		modes         []DeploymentMode
		expectedModes []DeploymentMode
	}{
		{
			name:          "locally, all modes are allowed",
			cloud:         spec.Local,
			modes:         allDeploymentModes,
			expectedModes: allDeploymentModes,
		},
		{
			name:          "on gce, all modes are allowed",
			cloud:         spec.GCE,
			modes:         allDeploymentModes,
			expectedModes: allDeploymentModes,
		},
		{
			name:          "on aws, we can't run separate process deployments",
			cloud:         spec.AWS,
			modes:         allDeploymentModes,
			expectedModes: []DeploymentMode{SystemOnlyDeployment, SharedProcessDeployment},
		},
		{
			name:          "on azure, we can't run separate process deployments",
			cloud:         spec.Azure,
			modes:         allDeploymentModes,
			expectedModes: []DeploymentMode{SystemOnlyDeployment, SharedProcessDeployment},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := validDeploymentModesForCloud(tc.cloud, tc.modes)
			require.Equal(t, tc.expectedModes, actual)
		})
	}
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

	// simulate a NewTest call with an actual `cluster` implementation
	mvt = newTest(EnabledDeploymentModes(SeparateProcessDeployment))
	mvt.options.enabledDeploymentModes = validDeploymentModesForCloud(spec.AWS, mvt.options.enabledDeploymentModes)
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Equal(t,
		`mixedversion.NewTest: invalid test options: no deployment modes enabled`,
		fatalErr.Error(),
	)

	// separate-process deployments requires cluster validation
	mvt = newTest(NeverUseFixtures, EnabledDeploymentModes(allDeploymentModes...))
	mvt.crdbNodes = option.NodeListOption{1}
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Equal(t,
		`mixedversion.NewTest: invalid test options: separate-process deployments require cluster with at least 3 nodes`,
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
				opts = append(opts, WithSkipVersionProbability(1))
			} else {
				opts = append(opts, DisableSkipVersionUpgrades)
			}

			mvt := newTest(opts...)
			mvt.options.predecessorFunc = func(_ *rand.Rand, v, _ *clusterupgrade.Version) (*clusterupgrade.Version, error) {
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

func TestTest_plan(t *testing.T) {
	// Assert that planning failures are owned by test-eng. At the time
	// of writing, planning can only return an error if we fail to find
	// a required predecessor of a certain release.
	mvt := newTest(NumUpgrades(100))
	_, err := mvt.plan()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no known predecessor for")

	var errWithOwnership registry.ErrorWithOwnership
	require.True(t,
		errors.As(err, &errWithOwnership),
		"error %q (%T) is not ErrorWithOwnership", err.Error(), err,
	)
	require.Equal(t, registry.OwnerTestEng, errWithOwnership.Owner)
}

func Test_randomPredecessor(t *testing.T) {
	testCases := []struct {
		name                string
		v                   string
		minSupported        string
		expectedPredecessor string
		expectedError       string
	}{
		{
			name:                "minSupported is from a different release series as predecessor",
			v:                   "v23.2.0",
			minSupported:        "v24.1.0",
			expectedPredecessor: "v23.1.15",
		},
		{
			name:                "minSupported is same release series as predecessor, but patch is 0",
			v:                   "v23.2.0",
			minSupported:        "v23.1.0",
			expectedPredecessor: "v23.1.15",
		},
		{
			name:                "minSupported is same release series as predecessor and patch is not 0",
			v:                   "v23.2.0",
			minSupported:        "v23.1.8",
			expectedPredecessor: "v23.1.23",
		},
		{
			name:                "latest predecessor is pre-release, but minimum supported is also the same version",
			v:                   "v24.3.0-alpha.00000000",
			minSupported:        "v24.2.0-beta.1",
			expectedPredecessor: "v24.2.0-beta.1",
		},
		{
			name:          "latest predecessor is pre-release and minimum supported is not release yet",
			v:             "v24.3.0-alpha.00000000",
			minSupported:  "v24.2.0",
			expectedError: "latest release for 24.2 (v24.2.0-beta.1) is not sufficient for minimum supported version (v24.2.0)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var pred *clusterupgrade.Version
			var err error
			_ = release.WithReleaseData(testReleaseData, func() error {
				pred, err = randomPredecessor(
					newRand(),
					clusterupgrade.MustParseVersion(tc.v),
					clusterupgrade.MustParseVersion(tc.minSupported),
				)

				return err
			})

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, clusterupgrade.MustParseVersion(tc.expectedPredecessor), pred)
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

func TestSupportsSkipUpgradeTo(t *testing.T) {
	expect := func(verStr string, expected bool) {
		t.Helper()
		v := clusterupgrade.MustParseVersion(verStr)
		if r := clusterversion.Latest.ReleaseSeries(); int(r.Major) == v.Major() && int(r.Minor) == v.Minor() {
			// We have to special case the current series, to allow for bumping the
			// min supported version separately from the current version.
			expected = len(clusterversion.SupportedPreviousReleases()) > 1
		}
		require.Equal(t, expected, supportsSkipUpgradeTo(v))
	}
	for _, v := range []string{"v24.3.0", "v24.3.0-beta.1", "v25.2.1", "v25.2.0-rc.1"} {
		expect(v, true)
	}

	for _, v := range []string{"v25.1.0", "v25.1.0-beta.1", "v25.3.1", "v25.3.0-rc.1"} {
		expect(v, false)
	}
}
