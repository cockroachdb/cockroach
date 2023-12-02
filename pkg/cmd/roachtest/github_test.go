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
	"github.com/stretchr/testify/assert"
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

	testCases := []struct {
		nonReleaseBlocker       bool
		clusterCreationFailed   bool
		loadTeamsFailed         bool
		localSSD                bool
		metamorphicBuild        bool
		extraLabels             []string
		arch                    vm.CPUArch
		failure                 failure
		expectedPost            bool
		expectedLabels          []string
		expectedReleaseBlocker  bool
		expectedSkipTestFailure bool
		expectedParams          map[string]string
	}{
		{
			nonReleaseBlocker: true,
			failure:           createFailure(errors.New("other")),
			expectedPost:      true,
			expectedLabels:    []string{"C-test-failure"},
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"encrypted":        "false",
				"fs":               "ext4",
				"ssd":              "0",
				"cpu":              "4",
				"arch":             "amd64",
				"localSSD":         "false",
				"metamorphicBuild": "false",
			}),
		},
		{
			localSSD:         true,
			metamorphicBuild: true,
			arch:             vm.ArchARM64,
			failure:          createFailure(errClusterProvisioningFailed),
			expectedPost:     true,
			expectedLabels:   []string{"T-testeng", "X-infra-flake"},
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"encrypted":        "false",
				"fs":               "ext4",
				"ssd":              "0",
				"cpu":              "4",
				"arch":             "arm64",
				"localSSD":         "true",
				"metamorphicBuild": "true",
			}),
		},
		// Assert that release-blocker label doesn't exist when
		// !nonReleaseBlocker and issue is an SSH flake. Also ensure that
		// in the event of a failed cluster creation, nil `vmOptions` and
		// `clusterImpl` are not dereferenced
		{
			clusterCreationFailed: true,
			failure:               createFailure(rperrors.ErrSSH255),
			expectedPost:          true,
			expectedLabels:        []string{"T-testeng", "X-infra-flake"},
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"ssd":              "0",
				"cpu":              "4",
				"metamorphicBuild": "false",
			}),
		},
		// Simulate failure loading TEAMS.yaml
		{
			nonReleaseBlocker: true,
			loadTeamsFailed:   true,
			failure:           createFailure(errors.New("other")),
			expectedLabels:    []string{"C-test-failure"},
		},
		// Error during post test assertions
		{
			nonReleaseBlocker: true,
			failure:           createFailure(errDuringPostAssertions),
			expectedLabels:    []string{"C-test-failure"},
		},
		// Error during dns operation.
		{
			nonReleaseBlocker: true,
			failure:           createFailure(gce.ErrDNSOperation),
			expectedPost:      true,
			expectedLabels:    []string{"T-testeng", "X-infra-flake"},
		},
		// Assert that extra labels in the test spec are added to the issue.
		{
			extraLabels:    []string{"foo-label"},
			failure:        createFailure(errors.New("other")),
			expectedPost:   true,
			expectedLabels: []string{"C-test-failure", "release-blocker", "foo-label"},
			expectedParams: prefixAll(map[string]string{
				"cloud":            "gce",
				"encrypted":        "false",
				"fs":               "ext4",
				"ssd":              "0",
				"cpu":              "4",
				"arch":             "amd64",
				"localSSD":         "false",
				"metamorphicBuild": "false",
			}),
		},
	}

	reg := makeTestRegistry()
	for idx, c := range testCases {
		t.Run("", func(t *testing.T) {
			clusterSpec := reg.MakeClusterSpec(1, spec.Arch(c.arch))

			testSpec := &registry.TestSpec{
				Name:              "github_test",
				Owner:             OwnerUnitTest,
				Cluster:           clusterSpec,
				NonReleaseBlocker: c.nonReleaseBlocker,
				ExtraLabels:       c.extraLabels,
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

			if c.clusterCreationFailed {
				testClusterImpl = nil
				vmOpts = nil
			} else if !c.localSSD {
				// The default is true set in `vm.DefaultCreateOpts`
				vmOpts.SSDOpts.UseLocalSSD = false
			}

			teamLoadFn := validTeamsFn

			if c.loadTeamsFailed {
				teamLoadFn = invalidTeamsFn
			}

			github := &githubIssues{
				vmCreateOpts: vmOpts,
				cluster:      testClusterImpl,
				teamLoader:   teamLoadFn,
			}

			if c.loadTeamsFailed {
				// Assert that if TEAMS.yaml cannot be loaded then function errors.
				_, err := github.createPostRequest("github_test", ti.start, ti.end, testSpec, c.failure, "message", c.metamorphicBuild)
				assert.Error(t, err, "Expected an error in createPostRequest when loading teams fails, but got nil")
			} else {
				req, err := github.createPostRequest("github_test", ti.start, ti.end, testSpec, c.failure, "message", c.metamorphicBuild)
				assert.NoError(t, err, "Expected no error in createPostRequest")

				r := &issues.Renderer{}
				req.HelpCommand(r)
				file := fmt.Sprintf("help_command_createpost_%d.txt", idx+1)
				echotest.Require(t, r.String(), filepath.Join("testdata", file))

				if c.expectedParams != nil {
					require.Equal(t, c.expectedParams, req.ExtraParams)
				}

				expLabels := append([]string{"O-roachtest"}, c.expectedLabels...)
				sort.Strings(expLabels)
				labels := append([]string{}, req.Labels...)
				sort.Strings(expLabels)
				sort.Strings(labels)
				require.Equal(t, expLabels, labels)

				expectedTeam := "@cockroachdb/unowned"
				expectedName := "github_test"
				expectedMessagePrefix := ""

				if errors.Is(c.failure.squashedErr, gce.ErrDNSOperation) {
					expectedTeam = "@cockroachdb/test-eng"
					expectedName = "dns_problem"
					expectedMessagePrefix = "test github_test failed due to "
				} else if errors.Is(c.failure.squashedErr, errClusterProvisioningFailed) {
					expectedTeam = "@cockroachdb/test-eng"
					expectedName = "cluster_creation"
					expectedMessagePrefix = "test github_test was skipped due to "
				} else if errors.Is(c.failure.squashedErr, rperrors.ErrSSH255) {
					expectedTeam = "@cockroachdb/test-eng"
					expectedName = "ssh_problem"
					expectedMessagePrefix = "test github_test failed due to "
				} else if errors.Is(c.failure.squashedErr, errDuringPostAssertions) {
					expectedMessagePrefix = "test github_test failed during post test assertions (see test-post-assertions.log) due to "
				}

				require.Contains(t, req.MentionOnCreate, expectedTeam)
				require.Equal(t, expectedName, req.TestName)
				require.True(t, strings.HasPrefix(req.Message, expectedMessagePrefix), req.Message)
			}
		})
	}
}
