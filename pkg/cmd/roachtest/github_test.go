// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

var (
	teamsYaml = `cockroachdb/unowned:
  aliases:
    cockroachdb/rfc-prs: other
  triage_column_id: 0
cockroachdb/test-eng:
  label: T-testeng
  triage_column_id: 14041337
cockroachdb/dev-inf:
  triage_column_id: 10210759`

	validTeamsFn   = func() (team.Map, error) { return loadYamlTeams(teamsYaml) }
	invalidTeamsFn = func() (team.Map, error) { return loadYamlTeams("invalid yaml") }
)

func loadYamlTeams(yaml string) (team.Map, error) {
	return team.LoadTeams(strings.NewReader(yaml))
}

func prefixAll(params map[string]string) map[string]string {
	updated := make(map[string]string)

	for k, v := range params {
		updated[roachtestPrefix(k)] = v
	}

	return updated
}

func TestShouldPost(t *testing.T) {
	testCases := []struct {
		disableIssues     bool
		nodeCount         int
		envGithubAPIToken string
		envTcBuildBranch  string
		expectedPost      bool
		expectedReason    string
	}{
		/* Cases 1 - 4 verify that issues are not posted if any of the relevant criteria checks fail */
		// disable
		{true, 1, "token", "master", false, "issue posting was disabled via command line flag"},
		// nodeCount
		{false, 0, "token", "master", false, "Cluster.NodeCount is zero"},
		// apiToken
		{false, 1, "", "master", false, "GitHub API token not set"},
		// branch
		{false, 1, "token", "", false, `not a release branch: "branch-not-found-in-env"`},
		{false, 1, "token", "master", true, ""},
	}

	reg := makeTestRegistry()
	for _, c := range testCases {
		t.Setenv("GITHUB_API_TOKEN", c.envGithubAPIToken)
		t.Setenv("TC_BUILD_BRANCH", c.envTcBuildBranch)
		defaultOpts = issues.DefaultOptionsFromEnv() // recompute options from env

		clusterSpec := reg.MakeClusterSpec(c.nodeCount)
		testSpec := &registry.TestSpec{
			Name:    "githubPost",
			Owner:   OwnerUnitTest,
			Cluster: clusterSpec,
			// `shouldPost` explicitly checks to ensure that the run function is defined
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {},
		}

		ti := &testImpl{spec: testSpec}
		github := &githubIssues{disable: c.disableIssues}

		doPost, skipReason := github.shouldPost(ti)
		require.Equal(t, c.expectedPost, doPost)
		require.Equal(t, c.expectedReason, skipReason)
	}
}

func TestGenerateHelpCommand(t *testing.T) {
	start := time.Date(2023, time.July, 21, 16, 34, 3, 817, time.UTC)
	end := time.Date(2023, time.July, 21, 16, 42, 13, 137, time.UTC)

	r := &issues.Renderer{}
	generateHelpCommand("acceptance/gossip/locality-address", "foo-cluster", spec.GCE, start, end)(r)

	echotest.Require(t, r.String(), filepath.Join("testdata", "help_command.txt"))

	r = &issues.Renderer{}
	generateHelpCommand("acceptance/gossip/locality-address", "foo-cluster", spec.AWS, start, end)(r)

	echotest.Require(t, r.String(), filepath.Join("testdata", "help_command_non_gce.txt"))
}

func TestCreatePostRequest(t *testing.T) {
	createFailure := func(ref error) failure {
		return failure{squashedErr: ref}
	}

	const testName = "github_test"

	// TODO(radu): these tests should be converted to datadriven tests which
	// output the full rendering of the github issue message along with the
	// metadata.
	testCases := []struct {
		nonReleaseBlocker       bool
		clusterCreationFailed   bool
		loadTeamsFailed         bool
		localSSD                bool
		metamorphicBuild        bool
		coverageBuild           bool
		extraLabels             []string
		arch                    vm.CPUArch
		failures                []failure
		expectedPost            bool
		expectedLabels          []string
		expectedTeam            string
		expectedName            string
		expectedMessagePrefix   string
		expectedReleaseBlocker  bool
		expectedSkipTestFailure bool
		expectedParams          map[string]string
		message                 string
	}{
		// 1.
		{
			nonReleaseBlocker: true,
			failures:          []failure{createFailure(errors.New("other"))},
			expectedPost:      true,
			expectedLabels:    []string{"C-test-failure"},
			expectedTeam:      "@cockroachdb/unowned",
			expectedName:      testName,
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"encrypted":        "false",
				"fs":               "ext4",
				"ssd":              "0",
				"cpu":              "4",
				"arch":             "amd64",
				"localSSD":         "false",
				"metamorphicBuild": "false",
				"coverageBuild":    "false",
			}),
		},
		// 2.
		{
			localSSD:         true,
			metamorphicBuild: true,
			arch:             vm.ArchARM64,
			failures: []failure{
				createFailure(errClusterProvisioningFailed(errors.New("gcloud error"))),
			},
			expectedPost:          true,
			expectedLabels:        []string{"T-testeng", "X-infra-flake"},
			expectedTeam:          "@cockroachdb/test-eng",
			expectedName:          "cluster_creation",
			expectedMessagePrefix: testName + " failed",
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"encrypted":        "false",
				"fs":               "ext4",
				"ssd":              "0",
				"cpu":              "4",
				"arch":             "arm64",
				"localSSD":         "true",
				"metamorphicBuild": "true",
				"coverageBuild":    "false",
			}),
		},
		// 3. Assert that release-blocker label doesn't exist when
		// !nonReleaseBlocker and issue is an SSH flake. Also ensure that
		// in the event of a failed cluster creation, nil `vmOptions` and
		// `clusterImpl` are not dereferenced
		{
			clusterCreationFailed: true,
			failures:              []failure{createFailure(rperrors.ErrSSH255)},
			expectedPost:          true,
			expectedLabels:        []string{"T-testeng", "X-infra-flake"},
			expectedTeam:          "@cockroachdb/test-eng",
			expectedName:          "ssh_problem",
			expectedMessagePrefix: testName + " failed",
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"ssd":              "0",
				"cpu":              "4",
				"metamorphicBuild": "false",
				"coverageBuild":    "false",
			}),
		},
		// 4. Simulate failure loading TEAMS.yaml
		{
			nonReleaseBlocker: true,
			loadTeamsFailed:   true,
			failures:          []failure{createFailure(errors.New("other"))},
			expectedLabels:    []string{"C-test-failure"},
		},
		// 5. Error during dns operation.
		{
			nonReleaseBlocker:     true,
			failures:              []failure{createFailure(gce.ErrDNSOperation)},
			expectedPost:          true,
			expectedLabels:        []string{"T-testeng", "X-infra-flake"},
			expectedTeam:          "@cockroachdb/test-eng",
			expectedName:          "dns_problem",
			expectedMessagePrefix: testName + " failed",
		},
		// 6. Assert that extra labels in the test spec are added to the issue.
		{
			extraLabels:    []string{"foo-label"},
			failures:       []failure{createFailure(errors.New("other"))},
			expectedPost:   true,
			expectedLabels: []string{"C-test-failure", "release-blocker", "foo-label"},
			expectedTeam:   "@cockroachdb/unowned",
			expectedName:   testName,
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"encrypted":        "false",
				"fs":               "ext4",
				"ssd":              "0",
				"cpu":              "4",
				"arch":             "amd64",
				"localSSD":         "false",
				"metamorphicBuild": "false",
				"coverageBuild":    "false",
			}),
		},
		// 7. Verify that release-blocker label is not applied on metamorphic builds
		// (for now).
		{
			metamorphicBuild: true,
			failures:         []failure{createFailure(errors.New("other"))},
			expectedPost:     true,
			expectedLabels:   []string{"C-test-failure", "B-metamorphic-enabled"},
			expectedTeam:     "@cockroachdb/unowned",
			expectedName:     testName,
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"encrypted":        "false",
				"fs":               "ext4",
				"ssd":              "0",
				"cpu":              "4",
				"arch":             "amd64",
				"localSSD":         "false",
				"metamorphicBuild": "true",
				"coverageBuild":    "false",
			}),
		},
		// 8. Verify that release-blocker label is not applied on coverage builds (for
		// now).
		{
			extraLabels:    []string{"foo-label"},
			coverageBuild:  true,
			failures:       []failure{createFailure(errors.New("other"))},
			expectedPost:   true,
			expectedTeam:   "@cockroachdb/unowned",
			expectedName:   testName,
			expectedLabels: []string{"C-test-failure", "B-coverage-enabled", "foo-label"},
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"encrypted":        "false",
				"fs":               "ext4",
				"ssd":              "0",
				"cpu":              "4",
				"arch":             "amd64",
				"localSSD":         "false",
				"metamorphicBuild": "false",
				"coverageBuild":    "true",
			}),
		},
		// 9. Verify preemption failure are routed to test-eng and marked as infra-flake, when the
		// first failure is a non-handled error.
		{
			nonReleaseBlocker:     true,
			failures:              []failure{createFailure(errors.New("random")), createFailure(vmPreemptionError("my_VM"))},
			expectedPost:          true,
			expectedTeam:          "@cockroachdb/test-eng",
			expectedName:          "vm_preemption",
			expectedMessagePrefix: testName + " failed",
			expectedLabels:        []string{"T-testeng", "X-infra-flake"},
		},
		// 10. Verify preemption failure are routed to test-eng and marked as infra-flake, when the only error is
		// preemption failure
		{
			nonReleaseBlocker: true,
			failures: []failure{
				{errors: []error{vmPreemptionError("my_VM")}},
			},
			expectedPost:          true,
			expectedTeam:          "@cockroachdb/test-eng",
			expectedName:          "vm_preemption",
			expectedMessagePrefix: testName + " failed",
			expectedLabels:        []string{"T-testeng", "X-infra-flake"},
		},
		// 11. Errors with ownership that happen as a result of roachprod
		// errors are ignored -- roachprod errors are routed directly to
		// test-eng.
		{
			nonReleaseBlocker: true,
			failures: []failure{
				createFailure(gce.ErrDNSOperation),
				createFailure(registry.ErrorWithOwner(registry.OwnerSQLFoundations, errors.New("oops"))),
			},
			expectedPost:          true,
			expectedTeam:          "@cockroachdb/test-eng",
			expectedName:          "dns_problem",
			expectedMessagePrefix: testName + " failed",
			expectedLabels:        []string{"T-testeng", "X-infra-flake"},
		},
	}

	reg := makeTestRegistry()
	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("%d", idx+1), func(t *testing.T) {
			clusterSpec := reg.MakeClusterSpec(1, spec.Arch(testCase.arch))

			testSpec := &registry.TestSpec{
				Name:              testName,
				Owner:             OwnerUnitTest,
				Cluster:           clusterSpec,
				NonReleaseBlocker: testCase.nonReleaseBlocker,
				ExtraLabels:       testCase.extraLabels,
			}

			ti := &testImpl{
				spec:  testSpec,
				l:     nilLogger(),
				start: time.Date(2023, time.July, 21, 16, 34, 3, 817, time.UTC),
				end:   time.Date(2023, time.July, 21, 16, 42, 13, 137, time.UTC),
			}

			testClusterImpl := &clusterImpl{spec: clusterSpec, arch: vm.ArchAMD64, name: "foo"}
			vo := vm.DefaultCreateOpts()
			vmOpts := &vo

			if testCase.clusterCreationFailed {
				testClusterImpl = nil
				vmOpts = nil
			} else if !testCase.localSSD {
				// The default is true set in `vm.DefaultCreateOpts`
				vmOpts.SSDOpts.UseLocalSSD = false
			}

			teamLoadFn := validTeamsFn

			if testCase.loadTeamsFailed {
				teamLoadFn = invalidTeamsFn
			}

			github := &githubIssues{
				vmCreateOpts: vmOpts,
				cluster:      testClusterImpl,
				teamLoader:   teamLoadFn,
			}

			req, err := github.createPostRequest(
				testName, ti.start, ti.end, testSpec, testCase.failures,
				testCase.message, testCase.metamorphicBuild, testCase.coverageBuild,
			)
			if testCase.loadTeamsFailed {
				// Assert that if TEAMS.yaml cannot be loaded then function errors.
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			r := &issues.Renderer{}
			req.HelpCommand(r)
			file := fmt.Sprintf("help_command_createpost_%d.txt", idx+1)
			echotest.Require(t, r.String(), filepath.Join("testdata", file))

			if testCase.expectedParams != nil {
				require.Equal(t, testCase.expectedParams, req.ExtraParams)
			}

			expLabels := append([]string{"O-roachtest"}, testCase.expectedLabels...)
			sort.Strings(expLabels)
			sort.Strings(req.Labels)
			require.Equal(t, expLabels, req.Labels)

			require.Contains(t, req.MentionOnCreate, testCase.expectedTeam)
			require.Equal(t, testCase.expectedName, req.TestName)
			require.Contains(t, req.Message, testCase.expectedMessagePrefix)
		})
	}
}
