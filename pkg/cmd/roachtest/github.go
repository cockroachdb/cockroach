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
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

type githubIssues struct {
	disable      bool
	cluster      *clusterImpl
	vmCreateOpts *vm.CreateOpts
	issuePoster  func(context.Context, *logger.Logger, issues.IssueFormatter, issues.PostRequest) error
	teamLoader   func() (team.Map, error)
}

type issueCategory int

const (
	otherErr issueCategory = iota
	clusterCreationErr
	sshErr
)

func newGithubIssues(disable bool, c *clusterImpl, vmCreateOpts *vm.CreateOpts) *githubIssues {
	return &githubIssues{
		disable:      disable,
		vmCreateOpts: vmCreateOpts,
		cluster:      c,
		issuePoster:  issues.Post,
		teamLoader:   team.DefaultLoadTeams,
	}
}

func roachtestPrefix(p string) string {
	return "ROACHTEST_" + p
}

// postIssueCondition encapsulates a condition that causes issue
// posting to be skipped. The `reason` field contains a textual
// description as to why issue posting was skipped.
type postIssueCondition struct {
	cond   func(g *githubIssues, t test.Test) bool
	reason string
}

var defaultOpts = issues.DefaultOptionsFromEnv()

var skipConditions = []postIssueCondition{
	{
		cond:   func(g *githubIssues, _ test.Test) bool { return g.disable },
		reason: "issue posting was disabled via command line flag",
	},
	{
		cond:   func(g *githubIssues, _ test.Test) bool { return !defaultOpts.CanPost() },
		reason: "GitHub API token not set",
	},
	{
		cond:   func(g *githubIssues, _ test.Test) bool { return !defaultOpts.IsReleaseBranch() },
		reason: fmt.Sprintf("not a release branch: %q", defaultOpts.Branch),
	},
	{
		cond:   func(_ *githubIssues, t test.Test) bool { return t.Spec().(*registry.TestSpec).Run == nil },
		reason: "TestSpec.Run is nil",
	},
	{
		cond:   func(_ *githubIssues, t test.Test) bool { return t.Spec().(*registry.TestSpec).Cluster.NodeCount == 0 },
		reason: "Cluster.NodeCount is zero",
	},
}

// shouldPost two values: whether GitHub posting should happen, and a
// reason for skipping (non-empty only when posting should *not*
// happen).
func (g *githubIssues) shouldPost(t test.Test) (bool, string) {
	post := true
	var reason string

	for _, sc := range skipConditions {
		if sc.cond(g, t) {
			post = false
			reason = sc.reason
			break
		}
	}

	return post, reason
}

func (g *githubIssues) createPostRequest(
	t test.Test, cat issueCategory, message string,
) issues.PostRequest {
	var mention []string
	var projColID int

	issueOwner := t.Spec().(*registry.TestSpec).Owner
	issueName := t.Name()

	messagePrefix := ""
	// Overrides to shield eng teams from potential flakes
	if cat == clusterCreationErr {
		issueOwner = registry.OwnerDevInf
		issueName = "cluster_creation"
		messagePrefix = fmt.Sprintf("test %s was skipped due to ", t.Name())
	} else if cat == sshErr {
		issueOwner = registry.OwnerTestEng
		issueName = "ssh_problem"
		messagePrefix = fmt.Sprintf("test %s failed due to ", t.Name())
	}

	// Issues posted from roachtest are identifiable as such and
	// they are also release blockers (this label may be removed
	// by a human upon closer investigation).
	spec := t.Spec().(*registry.TestSpec)
	labels := []string{"O-roachtest"}
	if !spec.NonReleaseBlocker {
		labels = append(labels, "release-blocker")
	}

	teams, err := g.teamLoader()
	if err != nil {
		t.Fatalf("could not load teams: %v", err)
	}

	if sl, ok := teams.GetAliasesForPurpose(ownerToAlias(issueOwner), team.PurposeRoachtest); ok {
		for _, alias := range sl {
			mention = append(mention, "@"+string(alias))
			if label := teams[alias].Label; label != "" {
				labels = append(labels, label)
			}
		}
		projColID = teams[sl[0]].TriageColumnID
	}

	branch := os.Getenv("TC_BUILD_BRANCH")
	if branch == "" {
		branch = "<unknown branch>"
	}

	artifacts := fmt.Sprintf("/%s", t.Name())

	clusterParams := map[string]string{
		roachtestPrefix("cloud"): spec.Cluster.Cloud,
		roachtestPrefix("cpu"):   fmt.Sprintf("%d", spec.Cluster.CPUs),
		roachtestPrefix("ssd"):   fmt.Sprintf("%d", spec.Cluster.SSDs),
	}

	// These params can be probabilistically set, so we pass them here to
	// show what their actual values are in the posted issue.
	if g.vmCreateOpts != nil {
		clusterParams[roachtestPrefix("fs")] = g.vmCreateOpts.SSDOpts.FileSystem
		clusterParams[roachtestPrefix("localSSD")] = fmt.Sprintf("%v", g.vmCreateOpts.SSDOpts.UseLocalSSD)
	}

	if g.cluster != nil {
		clusterParams[roachtestPrefix("encrypted")] = fmt.Sprintf("%v", g.cluster.encAtRest)
	}

	return issues.PostRequest{
		MentionOnCreate: mention,
		ProjectColumnID: projColID,
		PackageName:     "roachtest",
		TestName:        issueName,
		Message:         messagePrefix + message,
		Artifacts:       artifacts,
		ExtraLabels:     labels,
		ExtraParams:     clusterParams,
		HelpCommand: func(renderer *issues.Renderer) {
			issues.HelpCommandAsLink(
				"roachtest README",
				"https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/README.md",
			)(renderer)
			issues.HelpCommandAsLink(
				"How To Investigate (internal)",
				"https://cockroachlabs.atlassian.net/l/c/SSSBr8c7",
			)(renderer)
		},
	}
}

func (g *githubIssues) MaybePost(t *testImpl, l *logger.Logger, message string) error {
	doPost, skipReason := g.shouldPost(t)
	if !doPost {
		l.Printf("skipping GitHub issue posting (%s)", skipReason)
		return nil
	}

	cat := otherErr

	// Overrides to shield eng teams from potential flakes
	firstFailure := t.firstFailure()
	if failureContainsError(firstFailure, errClusterProvisioningFailed) {
		cat = clusterCreationErr
	} else if failureContainsError(firstFailure, rperrors.ErrSSH255) {
		cat = sshErr
	}

	return g.issuePoster(
		context.Background(),
		l,
		issues.UnitTestFormatter,
		g.createPostRequest(t, cat, message),
	)
}
