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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

type githubIssues struct {
	disable      bool
	l            *logger.Logger
	cluster      *clusterImpl
	vmCreateOpts *vm.CreateOpts
	issuePoster  func(ctx context.Context, formatter issues.IssueFormatter, req issues.PostRequest) error
	teamLoader   func() (team.Map, error)
}

func newGithubIssues(
	disable bool, c *clusterImpl, vmCreateOpts *vm.CreateOpts, l *logger.Logger,
) *githubIssues {

	return &githubIssues{
		disable:      disable,
		vmCreateOpts: vmCreateOpts,
		cluster:      c,
		l:            l,
		issuePoster:  issues.Post,
		teamLoader:   team.DefaultLoadTeams,
	}
}

func roachtestPrefix(p string) string {
	return "ROACHTEST_" + p
}

func (g *githubIssues) shouldPost(t test.Test) bool {
	opts := issues.DefaultOptionsFromEnv()
	return !g.disable && opts.CanPost() &&
		opts.IsReleaseBranch() &&
		t.Spec().(*registry.TestSpec).Run != nil &&
		// NB: check NodeCount > 0 to avoid posting issues from this pkg's unit tests.
		t.Spec().(*registry.TestSpec).Cluster.NodeCount > 0
}

type issueOverride struct {
	owner     registry.Owner
	testName  string
	msgPrefix string
}

func (g *githubIssues) createPostRequest(
	t test.Test, override issueOverride, message string,
) issues.PostRequest {
	var mention []string
	var projColID int

	issueOwner := t.Spec().(*registry.TestSpec).Owner
	if o := override.owner; o != "" {
		issueOwner = o
	}
	issueName := t.Name()
	if n := override.testName; n != "" {
		issueName = n
	}
	messagePrefix := override.msgPrefix

	teams, err := g.teamLoader()
	if err != nil {
		t.Fatalf("could not load teams: %v", err)
	}

	if sl, ok := teams.GetAliasesForPurpose(ownerToAlias(issueOwner), team.PurposeRoachtest); ok {
		for _, alias := range sl {
			mention = append(mention, "@"+string(alias))
		}
		projColID = teams[sl[0]].TriageColumnID
	}

	branch := os.Getenv("TC_BUILD_BRANCH")
	if branch == "" {
		branch = "<unknown branch>"
	}

	artifacts := fmt.Sprintf("/%s", t.Name())

	// Issues posted from roachtest are identifiable as such and
	// they are also release blockers (this label may be removed
	// by a human upon closer investigation).
	spec := t.Spec().(*registry.TestSpec)
	labels := []string{"O-roachtest"}
	if !spec.NonReleaseBlocker {
		labels = append(labels, "release-blocker")
	}

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

func (g *githubIssues) MaybePost(t *testImpl, message string) error {
	if !g.shouldPost(t) {
		return nil
	}

	var o issueOverride
	// Overrides to shield eng teams from potential flakes
	firstFailure := t.firstFailure()
	if failureContainsError(firstFailure, errClusterProvisioningFailed) {
		o.owner = registry.OwnerDevInf
		o.testName = "cluster_creation"
		o.msgPrefix = fmt.Sprintf("test %s was skipped due to ", t.Name())
	} else if failureContainsError(firstFailure, rperrors.ErrSSH255) {
		o.owner = registry.OwnerTestEng
		o.testName = "ssh_problem"
		o.msgPrefix = fmt.Sprintf("test %s failed due to ", t.Name())
	} else if failureContainsError(firstFailure, roachtestutil.ErrMarkConsistencyCheckFailed) {
		o.owner = registry.OwnerKV
		o.testName = "consistency_check"
		o.msgPrefix = fmt.Sprintf("consistency check failed after running %s", t.Name())
	} else {
		for _, err := range firstFailure.errors {
			owner, ok := registry.OwnerFromErr(err)
			if !ok {
				continue
			}
			o.owner = owner
			o.msgPrefix = "owner overridden by error"
			break
		}
	}

	return g.issuePoster(
		context.Background(),
		issues.UnitTestFormatter,
		g.createPostRequest(t, o, message),
	)
}
