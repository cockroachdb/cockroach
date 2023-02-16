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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
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

	reg := makeTestRegistry(spec.GCE, "", "", false)
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

func TestCreatePostRequest(t *testing.T) {
	testCases := []struct {
		nonReleaseBlocker     bool
		clusterCreationFailed bool
		loadTeamsFailed       bool
		localSSD              bool
		category              issueCategory
		expectedPost          bool
		expectedParams        map[string]string
	}{
		{true, false, false, false, otherErr, true,
			prefixAll(map[string]string{
				"cloud":     "gce",
				"encrypted": "false",
				"fs":        "ext4",
				"ssd":       "0",
				"cpu":       "4",
				"localSSD":  "false",
			}),
		},
		{true, false, false, true, clusterCreationErr, true,
			prefixAll(map[string]string{
				"cloud":     "gce",
				"encrypted": "false",
				"fs":        "ext4",
				"ssd":       "0",
				"cpu":       "4",
				"localSSD":  "true",
			}),
		},
		// Assert that release-blocker label exists when !nonReleaseBlocker
		// Also ensure that in the event of a failed cluster creation,
		// nil `vmOptions` and `clusterImpl` are not dereferenced
		{false, true, false, false, sshErr, true,
			prefixAll(map[string]string{
				"cloud": "gce",
				"ssd":   "0",
				"cpu":   "4",
			}),
		},
		//Simulate failure loading TEAMS.yaml
		{true, false, true, false, otherErr, false, nil},
	}

	reg := makeTestRegistry(spec.GCE, "", "", false)
	for _, c := range testCases {
		clusterSpec := reg.MakeClusterSpec(1)

		testSpec := &registry.TestSpec{
			Name:              "github_test",
			Owner:             OwnerUnitTest,
			Cluster:           clusterSpec,
			NonReleaseBlocker: c.nonReleaseBlocker,
		}

		ti := &testImpl{
			spec: testSpec,
			l:    nilLogger(),
		}

		testClusterImpl := &clusterImpl{spec: clusterSpec}
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
			// Assert that if TEAMS.yaml cannot be loaded then function panics.
			assert.Panics(t, func() { github.createPostRequest(ti, c.category, "message") })
		} else {
			req := github.createPostRequest(ti, c.category, "message")

			if c.expectedParams != nil {
				require.Equal(t, c.expectedParams, req.ExtraParams)
			}

			require.True(t, contains(req.ExtraLabels, nil, "O-roachtest"))

			if !c.nonReleaseBlocker {
				require.True(t, contains(req.ExtraLabels, nil, "release-blocker"))
			}

			expectedTeam := "@cockroachdb/unowned"
			expectedName := "github_test"
			expectedLabel := ""
			expectedMessagePrefix := ""

			if c.category == clusterCreationErr {
				expectedTeam = "@cockroachdb/dev-inf"
				expectedName = "cluster_creation"
				expectedMessagePrefix = "test github_test was skipped due to "
			} else if c.category == sshErr {
				expectedTeam = "@cockroachdb/test-eng"
				expectedLabel = "T-testeng"
				expectedName = "ssh_problem"
				expectedMessagePrefix = "test github_test failed due to "
			}

			require.Contains(t, req.MentionOnCreate, expectedTeam)
			require.Equal(t, expectedName, req.TestName)
			require.True(t, strings.HasPrefix(req.Message, expectedMessagePrefix), req.Message)
			if expectedLabel != "" {
				require.Contains(t, req.ExtraLabels, expectedLabel)
			}
		}
	}
}
