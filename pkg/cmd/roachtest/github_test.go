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
  triage_column_id: 0`

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
		expected          bool
	}{
		/* 1 - 4 verify that issues are not posted if any of on the relevant criteria checks fail */
		// disable
		{true, 1, "token", "master", false},
		// nodeCount
		{false, 0, "token", "master", false},
		// apiToken
		{false, 1, "", "master", false},
		// branch
		{false, 1, "token", "", false},
		{false, 1, "token", "master", true},
	}

	reg, _ := makeTestRegistry(spec.GCE, "", "", false)
	for _, c := range testCases {
		t.Setenv("GITHUB_API_TOKEN", c.envGithubAPIToken)
		t.Setenv("TC_BUILD_BRANCH", c.envTcBuildBranch)

		clusterSpec := reg.MakeClusterSpec(c.nodeCount)
		testSpec := &registry.TestSpec{
			Name:    "githubPost",
			Owner:   OwnerUnitTest,
			Cluster: clusterSpec,
			Run:     func(ctx context.Context, t test.Test, c cluster.Cluster) {},
		}

		ti := &testImpl{
			spec: testSpec,
			l:    nilLogger(),
		}

		github := &githubIssues{
			disable: c.disableIssues,
		}

		require.Equal(t, c.expected, github.shouldPost(ti))
	}
}

func TestCreatePostRequest(t *testing.T) {
	testCases := []struct {
		nonReleaseBlocker     bool
		clusterCreationFailed bool
		loadTeamsFailed       bool
		expectedPost          bool
		expectedParams        map[string]string
	}{
		{true, false, false, true,
			prefixAll(map[string]string{
				"cloud":     "gce",
				"encrypted": "false",
				"fs":        "ext4",
				"ssd":       "0",
				"cpu":       "4",
			}),
		},
		//assert release-blocker label
		// and failed cluster creation - nil vmOptions and clusterImpl
		{false, true, false, true,
			prefixAll(map[string]string{
				"cloud": "gce",
				"ssd":   "0",
				"cpu":   "4",
			}),
		},
		//loading TEAMS.yaml failed
		{true, false, true, false, nil},
	}

	reg, _ := makeTestRegistry(spec.GCE, "", "", false)

	for _, c := range testCases {
		clusterSpec := reg.MakeClusterSpec(1)
		testSpec := &registry.TestSpec{
			Name:              "githubPost",
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
		}

		teamLoadFn := validTeamsFn

		if c.loadTeamsFailed {
			teamLoadFn = invalidTeamsFn
		}

		github := &githubIssues{
			vmCreateOpts: vmOpts,
			cluster:      testClusterImpl,
			l:            nilLogger(),
			teamLoader:   teamLoadFn,
		}

		if c.loadTeamsFailed {
			assert.Panics(t, func() { github.createPostRequest(ti, "message") })
		} else {
			req := github.createPostRequest(ti, "message")

			if c.expectedParams != nil {
				require.Equal(t, c.expectedParams, req.ExtraParams)

				for k, v := range c.expectedParams {
					if req.ExtraParams[k] != v {
						require.Equal(t, v, req.ExtraParams[k])
					}
				}
			}

			lastLabel := req.ExtraLabels[len(req.ExtraLabels)-1]
			if !c.nonReleaseBlocker {
				require.Equal(t, "release-blocker", lastLabel)
			} else {
				require.Equal(t, "O-roachtest", lastLabel)
			}
		}
	}
}
