// Copyright 2018 The Cockroach Authors.
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
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

//teams by default expects to find TEAMS.yaml in the tree of the execution root
//unittests are executed from elsewhere, hence we create one where it can be found
func writeTeams(t *testing.T) {
	var teams = `cockroachdb/unowned:
  aliases:
    cockroachdb/rfc-prs: other
  triage_column_id: 0`

	data := []byte(teams)
	err := os.WriteFile("TEAMS.yaml", data, 0644)

	if err != nil {
		t.Fatal(err)
	}
}

func prefixAll(params map[string]string) map[string]string {
	updated := make(map[string]string)

	for k, v := range params {
		updated[roachtestPrefix(k)] = v
	}

	return updated
}

func TestMaybePost(t *testing.T) {
	testCases := []struct {
		disableIssues         bool
		nodeCount             int
		envGithubAPIToken     string
		envTcBuildBranch      string
		nonReleaseBlocker     bool
		clusterCreationFailed bool
		expectedPost          bool
		expectedParams        map[string]string
	}{
		/* 1 - 4 verify that issues are not posted if any of on the relevant criteria checks fail */
		// disable
		{true, 1, "token", "master", false, false, false, nil},
		// nodeCount
		{false, 0, "token", "master", false, false, false, nil},
		// apiToken
		{false, 1, "", "master", false, false, false, nil},
		// branch
		{false, 1, "token", "", false, false, false, nil},
		{false, 1, "token", "master", false, false, true,
			prefixAll(map[string]string{
				"cloud":     "gce",
				"encrypted": "false",
				"fs":        "ext4",
				"ssd":       "0",
				"cpu":       "4",
			}),
		},
		//assert release-blocker label
		{false, 1, "token", "master", true, false, true, nil},
		// failed cluster creation - nil vmOptions and clusterImpl
		{false, 1, "token", "master", true, true, true,
			prefixAll(map[string]string{
				"cloud": "gce",
				"ssd":   "0",
				"cpu":   "4",
			}),
		},
	}

	writeTeams(t)
	reg, _ := makeTestRegistry(spec.GCE, "", "", false)

	for _, c := range testCases {
		t.Setenv("GITHUB_API_TOKEN", c.envGithubAPIToken)
		t.Setenv("TC_BUILD_BRANCH", c.envTcBuildBranch)

		clusterSpec := reg.MakeClusterSpec(c.nodeCount)
		testSpec := &registry.TestSpec{
			Name:    "githubPost",
			Owner:   "unowned",
			Run:     func(ctx context.Context, t test.Test, c cluster.Cluster) {},
			Cluster: clusterSpec,
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

		github := &githubIssuesImpl{
			disable:      c.disableIssues,
			vmCreateOpts: vmOpts,
			cluster:      testClusterImpl,
			l:            nilLogger(),
			issuePoster: func(ctx context.Context, formatter issues.IssueFormatter, req issues.PostRequest) error {
				if !c.expectedPost {
					t.Logf("issue should not have been posted")
					t.FailNow()
				}

				if c.expectedParams != nil {
					if len(c.expectedParams) != len(req.ExtraParams) {
						t.Logf("expected %v, actual %v", c.expectedParams, req.ExtraParams)
						t.FailNow()
					}

					for k, v := range c.expectedParams {
						if req.ExtraParams[k] != v {
							t.Logf("expected %v, actual %v", c.expectedParams, req.ExtraParams)
							t.FailNow()
						}
					}
				}

				if c.nonReleaseBlocker && req.ExtraLabels[len(req.ExtraLabels)-1] != "release-blocker" {
					t.Logf("Expected label release-blocker")
					t.FailNow()
				}
				return nil
			},
		}

		github.maybePost(context.Background(), ti, nil, "body")
	}
}
