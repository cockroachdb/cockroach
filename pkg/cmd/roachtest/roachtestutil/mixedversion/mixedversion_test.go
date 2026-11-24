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

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/version"
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
	"25.1": clusterupgrade.MustParseVersion("v24.3.0"),
	"25.2": clusterupgrade.MustParseVersion("v25.1.3"),
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
	func() {
		// We need to bump test build version to allow for 5 upgrades wrt MinBootstrapSupportedVersion
		defer withTestBuildVersion("v25.1.0")()

		mvt := newTest(MinUpgrades(5))
		assertValidTest(mvt, fatalFunc())
		require.Error(t, fatalErr)
		require.Contains(t, fatalErr.Error(), "mixedversion.NewTest: invalid test options: maxUpgrades (4) must be >= minUpgrades (5)")
	}()

	mvt := newTest(MaxUpgrades(0))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Contains(t, fatalErr.Error(), "mixedversion.NewTest: invalid test options: maxUpgrades (0) must be >= minUpgrades (1)")

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

	// Test that if there are fewer upgrades possible between the minimum
	// bootstrap version and the current version than MaxUpgrades, maxUpgrades
	// is overridden to the former.
	mvt = newTest(MinimumBootstrapVersion("v21.2.0"), MaxUpgrades(10))
	assertValidTest(mvt, fatalFunc())
	require.NoError(t, fatalErr)
	require.Equal(t, 3, mvt.options.maxUpgrades)

	// Test that if there are fewer upgrades possible between the minimum
	// bootstrap version and the current version than MinUpgrades, the test
	// is invalid.
	mvt = newTest(MinimumBootstrapVersion("v21.2.0"), MinUpgrades(10))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Equal(t,
		`mixedversion.NewTest: invalid test options: minimum bootstrap version (v21.2.0) does not allow for min 10 upgrades to v23.1.2, max is 3`,
		fatalErr.Error(),
	)

	mvt = newTest(MinimumBootstrapVersion("v24.2.0"))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Equal(t,
		`mixedversion.NewTest: invalid test options: minimum bootstrap version (v24.2.0) should be from an older release series than current version (v23.1.2)`,
		fatalErr.Error(),
	)

	// Validating that AlwaysUseLatestPredecessors and same-series upgrades
	// cannot be used together.
	mvt = newTest(AlwaysUseLatestPredecessors, WithSameSeriesUpgradeProbability(0.5))
	assertValidTest(mvt, fatalFunc())
	require.Error(t, fatalErr)
	require.Contains(t, fatalErr.Error(), "same-series upgrades (probability=0.50) cannot be used with AlwaysUseLatestPredecessors")

	// Verify that using AlwaysUseLatestPredecessors alone is fine.
	mvt = newTest(AlwaysUseLatestPredecessors)
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
			numUpgrades:      5,
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
			mvt.options.predecessorFunc = func(_ *rand.Rand, v *clusterupgrade.Version) (*clusterupgrade.Version, error) {
				return testPredecessorMapping[v.Series()], tc.predecessorErr
			}
			mvt._arch = &tc.arch

			assertValidTest(mvt, t.Fatal)
			enableSameSeriesUpgrades := mvt.options.sameSeriesUpgradeProbability > 0 && mvt.prng.Float64() < mvt.options.sameSeriesUpgradeProbability
			majorUpgrades, sameSeriesUpgrades := mvt.numUpgrades(enableSameSeriesUpgrades)
			releases, err := mvt.chooseUpgradePath(majorUpgrades, sameSeriesUpgrades, mvt.prng.Float64() < mvt.options.skipVersionProbability)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				var expectedVersions []*clusterupgrade.Version
				for _, er := range tc.expectedReleases {
					expectedVersions = append(expectedVersions, clusterupgrade.MustParseVersion(er))
				}
				expectedVersions = append(expectedVersions, clusterupgrade.CurrentVersion())
				require.Equal(t, expectedVersions, releases)
			} else {
				require.Error(t, err)
				require.Equal(t, tc.expectedErr, err.Error())
			}
		})
	}
}

func Test_chooseSameSeriesUpgrades(t *testing.T) {
	defer withTestBuildVersion("v24.3.0")()

	t.Run("same-series upgrades disabled", func(t *testing.T) {
		_ = release.WithReleaseData(testReleaseData, func() error {
			mvt := newTest(DisableSameSeriesUpgrades, DisableSkipVersionUpgrades, NumUpgrades(2))
			rng, _ := randutil.NewTestRand()
			mvt.prng = rng
			mvt.options.predecessorFunc = func(_ *rand.Rand, v *clusterupgrade.Version) (*clusterupgrade.Version, error) {
				return testPredecessorMapping[v.Series()], nil
			}
			assertValidTest(mvt, t.Fatal)

			enableSameSeriesUpgrades := mvt.options.sameSeriesUpgradeProbability > 0 && mvt.prng.Float64() < mvt.options.sameSeriesUpgradeProbability
			majorUpgrades, sameSeriesUpgrades := mvt.numUpgrades(enableSameSeriesUpgrades)
			releases, err := mvt.chooseUpgradePath(majorUpgrades, sameSeriesUpgrades, false)
			require.NoError(t, err)

			// With same-series upgrades disabled, expect: [predecessor1, predecessor2, current]
			// No extra versions should be inserted.
			expectedLen := 3 // 2 upgrades + current version
			require.Equal(t, expectedLen, len(releases),
				"expected %d versions but got %d: %v", expectedLen, len(releases), releases)

			// Verify no same-series pairs exist (each version should be from a different series).
			for i := 0; i < len(releases)-1; i++ {
				require.NotEqual(t, releases[i].Series(), releases[i+1].Series(),
					"unexpected same-series pair: %s and %s", releases[i], releases[i+1])
			}
			return nil
		})
	})

	t.Run("same-series upgrades enabled", func(t *testing.T) {
		_ = release.WithReleaseData(testReleaseData, func() error {
			// With the two-value numUpgrades() design, majorUpgrades and
			// sameSeriesUpgrades are determined separately. We pass explicit
			// counts to chooseUpgradePath to test rejection sampling.
			const numMajorUpgrades = 2
			const numSameSeriesUpgrades = 2
			mvt := newTest(WithSameSeriesUpgradeProbability(1), DisableSkipVersionUpgrades)
			rng, _ := randutil.NewTestRand()
			mvt.prng = rng
			mvt.options.predecessorFunc = func(_ *rand.Rand, v *clusterupgrade.Version) (*clusterupgrade.Version, error) {
				return testPredecessorMapping[v.Series()], nil
			}
			assertValidTest(mvt, t.Fatal)

			releases, err := mvt.chooseUpgradePath(numMajorUpgrades, numSameSeriesUpgrades, false)
			require.NoError(t, err)

			// Verify at least one same-series upgrade exists in the path.
			hasSameSeriesPair := false
			for i := 0; i < len(releases)-1; i++ {
				if releases[i].Series() == releases[i+1].Series() {
					hasSameSeriesPair = true
					// Verify the first version in the pair is older (lower patch number).
					require.Less(t, releases[i].Patch(), releases[i+1].Patch(),
						"same-series pair should have older version first: %s -> %s",
						releases[i], releases[i+1])
				}
			}
			require.True(t, hasSameSeriesPair,
				"expected at least one same-series upgrade pair in path: %v", releases)

			// Verify the path is ordered correctly (each version < next version).
			for i := 0; i < len(releases)-1; i++ {
				require.True(t, releases[i].Version.LessThan(releases[i+1].Version),
					"version %s should be less than %s", releases[i], releases[i+1])
			}

			// Verify the last version is current.
			require.Equal(t, clusterupgrade.CurrentVersion().String(), releases[len(releases)-1].String())

			// Verify total upgrades does not exceed the requested number.
			actualUpgrades := len(releases) - 1 // upgrades = versions - 1
			require.LessOrEqual(t, actualUpgrades, numMajorUpgrades+numSameSeriesUpgrades,
				"total upgrades %d should not exceed requested %d", actualUpgrades, numMajorUpgrades+numSameSeriesUpgrades)

			return nil
		})
	})

	t.Run("same-series upgrades respect minimumBootstrapVersion", func(t *testing.T) {
		_ = release.WithReleaseData(testReleaseData, func() error {
			// Set mbv so fewer older patches are available.
			// Use 1 major upgrade and 1 same-series insertion to test mbv filtering.
			mvt := newTest(
				WithSameSeriesUpgradeProbability(1),
				DisableSkipVersionUpgrades,
				NumUpgrades(1),
				MinimumBootstrapVersion("v24.1.1"),
			)
			rng, _ := randutil.NewTestRand()
			mvt.prng = rng
			mvt.options.predecessorFunc = func(_ *rand.Rand, v *clusterupgrade.Version) (*clusterupgrade.Version, error) {
				return testPredecessorMapping[v.Series()], nil
			}
			assertValidTest(mvt, t.Fatal)

			releases, err := mvt.chooseUpgradePath(1, 1, false)
			require.NoError(t, err)

			// Verify all versions in the path are >= minimumBootstrapVersion.
			mbv := clusterupgrade.MustParseVersion("v24.1.1")
			for _, v := range releases {
				if v.Series() == mbv.Series() {
					require.GreaterOrEqual(t, v.Patch(), mbv.Patch(),
						"version %s should be >= minimumBootstrapVersion %s", v, mbv)
				}
			}

			return nil
		})
	})
}

func TestTest_plan(t *testing.T) {
	// Assert that planning failures are owned by test-eng.
	mvt := newTest(MaxNumPlanSteps(2))
	assertValidTest(mvt, t.Fatal)
	_, err := mvt.plan()
	// There is no feasible plan with a maximum number of 2 steps
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to generate a test plan")

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
		minBootstrap        string
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
			name:                "minSupported and minBootstrap are from a different release series as predecessor",
			v:                   "v23.2.0",
			minSupported:        "v24.1.0",
			minBootstrap:        "v23.2.0",
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
			expectedPredecessor: "v23.1.15",
		},
		{
			name:                "minSupported is same release series as predecessor, but patch is 0 and minBootstrap is older release series",
			v:                   "v23.2.0",
			minSupported:        "v23.1.0",
			minBootstrap:        "v22.1.1",
			expectedPredecessor: "v23.1.15",
		},
		{
			name:                "minSupported is same release series as predecessor and patch is not 0 and minBootstrap is older release series",
			v:                   "v23.2.0",
			minSupported:        "v23.1.8",
			minBootstrap:        "v22.1.1",
			expectedPredecessor: "v23.1.15",
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
		{
			name:                "minBootstrap is same release series as predecessor, but patch is 0",
			v:                   "v23.2.0",
			minSupported:        "v24.1.0",
			minBootstrap:        "v23.1.0",
			expectedPredecessor: "v23.1.15",
		},
		{
			name:                "minBootstrap is same release series as predecessor and patch is not 0",
			v:                   "v23.2.0",
			minSupported:        "v24.1.0",
			minBootstrap:        "v23.1.8",
			expectedPredecessor: "v23.1.15",
		},
		{
			name:                "minBootstrap and minSupported are same release series as predecessor and patch is not 0",
			v:                   "v23.2.0",
			minSupported:        "v23.1.15",
			minBootstrap:        "v23.1.0",
			expectedPredecessor: "v23.1.15",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var pred *clusterupgrade.Version
			var err error
			_ = release.WithReleaseData(testReleaseData, func() error {
				var minBootstrap *clusterupgrade.Version
				if tc.minBootstrap != "" {
					minBootstrap = clusterupgrade.MustParseVersion(tc.minBootstrap)
				}
				v := clusterupgrade.MustParseVersion(tc.v)
				// N.B. `expectedPredecessor` is dependent on the seed, hence we must reuse rng in `maybeClampMsbMsv`.
				// Otherwise, the patch versions may differ from the expected.
				rng := newRand()
				pred, err = randomPredecessor(rng, v)
				if err != nil {
					return err
				}
				pred, err = maybeClampMsbMsv(rng, pred, minBootstrap, clusterupgrade.MustParseVersion(tc.minSupported))

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
	clusterupgrade.TestBuildVersion = &testBuildVersion
	return func() { clusterupgrade.TestBuildVersion = &buildVersion }
}

func TestSupportsSkipUpgradeTo(t *testing.T) {
	mvt := newTest()
	prev := clusterupgrade.MustParseVersion("24.2.1")

	expect := func(verStr string, expected bool) {
		t.Helper()
		v := clusterupgrade.MustParseVersion(verStr)
		require.Equal(t, expected, mvt.supportsSkipUpgradeTo(prev, v))
	}
	for _, v := range []string{"v24.3.0", "v24.3.0-beta.1", "v25.2.1", "v25.2.0-rc.1"} {
		expect(v, true)
	}

	for _, v := range []string{"v25.1.0", "v25.1.0-beta.1", "v25.3.1", "v25.3.0-rc.1"} {
		expect(v, false)
	}
}

// Regression test for #157852.
// This tests that our skip upgrade logic does not break when we are in the middle
// of the version bump process. We bump the current version and the minSupportedVersion
// separately, so we want to make sure that:
//  1. supportsSkipUpgradeTo does not let us perform a skip upgrade from a non supportedVersion.
//     This edge case happens if we bump the minSupportedVersion first and there is only 1 supported
//     previous release in the current series despite being a .2 or .4 release.
//  2. supportsSkipUpgradeTo does not let us perform a skip upgrade over a .2 or .4 release.
//     This edge case happens if we bump the current version first and there could be 3 supported
//     previous releases in the current series.
func TestSupportsSkipCurrentVersion(t *testing.T) {
	mvt := newTest()

	// Case 1: If the minimumSupportedVersion on the current release says there is only 1 supported
	// previous release, then it doesn't matter if the previous release is an innovation or not,
	// we can't skip over it.
	numSupportedVersions := len(clusterversion.SupportedPreviousReleases())

	// Case 2: If the current release is an innovation release, it means the previous release
	// is _not_ an innovation, i.e. we can't skip over it.
	currentRelease := clusterversion.Latest.ReleaseSeries()
	isInnovationRelease := currentRelease.Minor == 1 || currentRelease.Minor == 3

	// We can only perform a skip upgrade to the current version if it's not an innovation and
	// there are multiple supported previous versions.
	expected := !isInnovationRelease && numSupportedVersions > 1

	// N.B. The predecessor is only used to determine if we should skip over the
	// mixedversion test's minimum supported version, and not relevant for this test.
	pred := clusterupgrade.MustParseVersion("24.2.1")
	actual := mvt.supportsSkipUpgradeTo(pred, &clusterupgrade.Version{Version: version.MustParse(build.BinaryVersion())})
	require.Equal(t, expected, actual)
}
