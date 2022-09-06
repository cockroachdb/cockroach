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
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

type githubIssues interface {
	maybePost(
		ctx context.Context,
		t test.Test,
		stdout io.Writer,
		output string,
	)
}

type githubIssuesImpl struct {
	disable      bool
	l            *logger.Logger
	cluster      *clusterImpl
	vmCreateOpts *vm.CreateOpts
	issuePoster  func(ctx context.Context, formatter issues.IssueFormatter, req issues.PostRequest) error
}

func (g *githubIssuesImpl) shouldPost(t test.Test) bool {
	opts := issues.DefaultOptionsFromEnv()
	return !g.disable && opts.CanPost() &&
		opts.IsReleaseBranch() &&
		t.Spec().(*registry.TestSpec).Run != nil &&
		// NB: check NodeCount > 0 to avoid posting issues from this pkg's unit tests.
		t.Spec().(*registry.TestSpec).Cluster.NodeCount > 0
}
func roachtestPrefix(p string) string {
	return "ROACHTEST_" + p
}

func (g *githubIssuesImpl) maybePost(
	ctx context.Context, t test.Test, stdout io.Writer, output string,
) {
	if !g.shouldPost(t) {
		return
	}

	teams, err := team.DefaultLoadTeams()
	if err != nil {
		t.Fatalf("could not load teams: %v", err)
	}

	var mention []string
	var projColID int
	if sl, ok := teams.GetAliasesForPurpose(ownerToAlias(t.Spec().(*registry.TestSpec).Owner), team.PurposeRoachtest); ok {
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

	// these params can be probabilistically set if requested
	if g.vmCreateOpts != nil {
		clusterParams[roachtestPrefix("fs")] = g.vmCreateOpts.SSDOpts.FileSystem
	}

	if g.cluster != nil {
		clusterParams[roachtestPrefix("encrypted")] = fmt.Sprintf("%v", g.cluster.encAtRest)
	}

	req := issues.PostRequest{
		MentionOnCreate: mention,
		ProjectColumnID: projColID,
		PackageName:     "roachtest",
		TestName:        t.Name(),
		Message:         output,
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
	if err := g.issuePoster(
		context.Background(),
		issues.UnitTestFormatter,
		req,
	); err != nil {
		shout(ctx, g.l, stdout, "failed to post issue: %s", err)
	}
}
