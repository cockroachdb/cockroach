// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

type githubIssues struct {
	disable      bool
	cluster      *clusterImpl
	vmCreateOpts *vm.CreateOpts
	issuePoster  func(context.Context, issues.Logger, issues.IssueFormatter, issues.PostRequest, *issues.Options) (*issues.TestFailureIssue, error)
	teamLoader   func() (team.Map, error)
}

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

// generateHelpCommand creates a HelpCommand for createPostRequest
func generateHelpCommand(
	testName string, clusterName string, cloud string, start time.Time, end time.Time,
) func(renderer *issues.Renderer) {
	return func(renderer *issues.Renderer) {
		issues.HelpCommandAsLink(
			"roachtest README",
			"https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/README.md",
		)(renderer)
		issues.HelpCommandAsLink(
			"How To Investigate (internal)",
			"https://cockroachlabs.atlassian.net/l/c/SSSBr8c7",
		)(renderer)
		// An empty clusterName corresponds to a cluster creation failure.
		// We only scrape metrics from GCE clusters for now.
		if clusterName != "" {
			if spec.GCE == cloud {
				// N.B. This assumes we are posting from a source that does not run a test more than once.
				// Otherwise, we'd need to use `testRunId`, which encodes the run number and allows us
				// to distinguish between multiple runs of the same test, instead of `testName`.
				issues.HelpCommandAsLink(
					"Grafana",
					fmt.Sprintf("https://go.crdb.dev/roachtest-grafana/%s/%s/%d/%d", vm.SanitizeLabel(runID),
						vm.SanitizeLabel(testName), start.UnixMilli(), end.Add(2*time.Minute).UnixMilli()),
				)(renderer)
			} else {
				renderer.Escaped(fmt.Sprintf("_Grafana is not yet available for %s clusters_", cloud))
			}
		}
	}
}

func failuresAsErrorWithOwnership(failures []failure) *registry.ErrorWithOwnership {
	var transientError rperrors.TransientError
	var err registry.ErrorWithOwnership
	if failuresMatchingError(failures, &transientError) {
		err = registry.ErrorWithOwner(
			registry.OwnerTestEng, transientError,
			registry.WithTitleOverride(transientError.Cause),
			registry.InfraFlake,
		)

		return &err
	}

	if errWithOwner := failuresSpecifyOwner(failures); errWithOwner != nil {
		return errWithOwner
	}

	return nil
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
	testName string,
	start time.Time,
	end time.Time,
	spec *registry.TestSpec,
	failures []failure,
	message string,
	metamorphicBuild bool,
	coverageBuild bool,
) (issues.PostRequest, error) {
	var mention []string
	var projColID int

	var (
		issueOwner    = spec.Owner
		issueName     = testName
		messagePrefix string
		infraFlake    bool
	)

	// handleErrorWithOwnership updates the local variables in this
	// function that contain the name of the issue being created,
	// message prefix, and team that will own it.
	handleErrorWithOwnership := func(err registry.ErrorWithOwnership) {
		issueOwner = err.Owner
		infraFlake = err.InfraFlake

		if err.TitleOverride != "" {
			issueName = err.TitleOverride
			messagePrefix = fmt.Sprintf("test %s failed: ", testName)
		}
	}

	issueClusterName := ""
	// If we find a failure that was labeled as a roachprod transient
	// error, redirect that to Test Eng with the corresponding label as
	// title override.
	errWithOwner := failuresAsErrorWithOwnership(failures)
	if errWithOwner != nil {
		handleErrorWithOwnership(*errWithOwner)
	}

	// Issues posted from roachtest are identifiable as such, and they are also release blockers
	// (this label may be removed by a human upon closer investigation).
	const infraFlakeLabel = "X-infra-flake"
	const metamorphicLabel = "B-metamorphic-enabled"
	const coverageLabel = "B-coverage-enabled"
	labels := []string{"O-roachtest"}
	if infraFlake {
		labels = append(labels, infraFlakeLabel)
	} else {
		labels = append(labels, issues.TestFailureLabel)
		if !spec.NonReleaseBlocker {
			// TODO(radu): remove this check once these build types are stabilized.
			if !metamorphicBuild && !coverageBuild {
				labels = append(labels, issues.ReleaseBlockerLabel)
			}
		}
		if metamorphicBuild {
			labels = append(labels, metamorphicLabel)
		}
		if coverageBuild {
			labels = append(labels, coverageLabel)
		}
	}
	labels = append(labels, spec.ExtraLabels...)

	teams, err := g.teamLoader()
	if err != nil {
		return issues.PostRequest{}, err
	}

	if sl, ok := teams.GetAliasesForPurpose(issueOwner.ToTeamAlias(), team.PurposeRoachtest); ok {
		mentionTeam := !teams[sl[0]].SilenceMentions
		for _, alias := range sl {
			if mentionTeam {
				mention = append(mention, "@"+string(alias))
			}
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

	artifacts := fmt.Sprintf("/%s", testName)

	clusterParams := map[string]string{
		roachtestPrefix("cloud"):            roachtestflags.Cloud,
		roachtestPrefix("cpu"):              fmt.Sprintf("%d", spec.Cluster.CPUs),
		roachtestPrefix("ssd"):              fmt.Sprintf("%d", spec.Cluster.SSDs),
		roachtestPrefix("metamorphicBuild"): fmt.Sprintf("%t", metamorphicBuild),
		roachtestPrefix("coverageBuild"):    fmt.Sprintf("%t", coverageBuild),
	}
	// Emit CPU architecture only if it was specified; otherwise, it's captured below, assuming cluster was created.
	if spec.Cluster.Arch != "" {
		clusterParams[roachtestPrefix("arch")] = string(spec.Cluster.Arch)
	}
	// These params can be probabilistically set, so we pass them here to
	// show what their actual values are in the posted issue.
	if g.vmCreateOpts != nil {
		clusterParams[roachtestPrefix("fs")] = g.vmCreateOpts.SSDOpts.FileSystem
		clusterParams[roachtestPrefix("localSSD")] = fmt.Sprintf("%v", g.vmCreateOpts.SSDOpts.UseLocalSSD)
	}

	if g.cluster != nil {
		clusterParams[roachtestPrefix("encrypted")] = fmt.Sprintf("%v", g.cluster.encAtRest)
		if spec.Cluster.Arch == "" {
			// N.B. when Arch is specified, it cannot differ from cluster's arch.
			// Hence, we only emit when arch was unspecified.
			clusterParams[roachtestPrefix("arch")] = string(g.cluster.arch)
		}
		issueClusterName = g.cluster.name
	}

	issueMessage := messagePrefix + message
	if spec.RedactResults {
		issueMessage = "The details about this test failure may contain sensitive information; " +
			"consult the logs for details. WARNING: DO NOT COPY UNREDACTED ARTIFACTS TO THIS ISSUE."
	}
	var topLevelNotes []string
	if coverageBuild {
		topLevelNotes = append(topLevelNotes,
			"This is a special code-coverage build. If the same failure was hit in a non-coverage run, "+
				"there should be a similar issue without the "+coverageLabel+" label. If there isn't one, it is "+
				"possible that this failure is related to the code coverage infrastructure or overhead.")
	}
	if metamorphicBuild {
		topLevelNotes = append(topLevelNotes,
			"This build has metamorphic test constants enabled. If the same failure was hit in a "+
				"non-metamorphic run, there should be a similar issue without the "+metamorphicLabel+" label. If there "+
				"isn't one, it is possible that this failure is caused by a metamorphic constant.")
	}

	return issues.PostRequest{
		MentionOnCreate: mention,
		ProjectColumnID: projColID,
		PackageName:     "roachtest",
		TestName:        issueName,
		Labels:          labels,
		// Keep issues separate unless the if these labels don't match.
		AdoptIssueLabelMatchSet: []string{infraFlakeLabel, coverageLabel, metamorphicLabel},
		TopLevelNotes:           topLevelNotes,
		Message:                 issueMessage,
		Artifacts:               artifacts,
		ExtraParams:             clusterParams,
		HelpCommand:             generateHelpCommand(testName, issueClusterName, roachtestflags.Cloud, start, end),
	}, nil
}

func (g *githubIssues) MaybePost(
	t *testImpl, l *logger.Logger, message string,
) (*issues.TestFailureIssue, error) {
	doPost, skipReason := g.shouldPost(t)
	if !doPost {
		l.Printf("skipping GitHub issue posting (%s)", skipReason)
		return nil, nil
	}

	var metamorphicBuild bool
	switch t.spec.CockroachBinary {
	case registry.StandardCockroach:
		metamorphicBuild = false
	case registry.RuntimeAssertionsCockroach:
		metamorphicBuild = true
	default:
		metamorphicBuild = tests.UsingRuntimeAssertions(t)
	}
	postRequest, err := g.createPostRequest(t.Name(), t.start, t.end, t.spec, t.failures(), message, metamorphicBuild, t.goCoverEnabled)
	if err != nil {
		return nil, err
	}
	opts := issues.DefaultOptionsFromEnv()

	return g.issuePoster(
		context.Background(),
		l,
		issues.UnitTestFormatter,
		postRequest,
		opts,
	)
}
