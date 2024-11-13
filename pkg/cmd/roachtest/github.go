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

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
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

// generateHelpCommand creates a HelpCommand for createPostRequest
func generateHelpCommand(
	testName string, clusterName string, cloud spec.Cloud, start time.Time, end time.Time,
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
	var errWithOwner registry.ErrorWithOwnership
	if failuresMatchingError(failures, &transientError) {
		errWithOwner = registry.ErrorWithOwner(
			registry.OwnerTestEng, transientError,
			registry.WithTitleOverride(transientError.Cause),
			registry.InfraFlake,
		)

		return &errWithOwner
	}

	if failuresMatchingError(failures, &errWithOwner) {
		return &errWithOwner
	}

	return nil
}

func failuresAsNonReportableError(failures []failure) *registry.NonReportableError {
	var nonReportable registry.NonReportableError
	if failuresMatchingError(failures, &nonReportable) {
		return &nonReportable
	}

	return nil
}

// postIssueCondition is a condition that causes issue posting to be
// skipped. If it returns a non-empty string, posting is skipped for
// the returned reason.
type postIssueCondition func(g *githubIssues, t test.Test) string

var defaultOpts = issues.DefaultOptionsFromEnv()

var skipConditions = []postIssueCondition{
	func(g *githubIssues, _ test.Test) string {
		if g.disable {
			return "issue posting was disabled via command line flag"
		}

		return ""
	},
	func(g *githubIssues, _ test.Test) string {
		if defaultOpts.CanPost() {
			return ""
		}

		return "GitHub API token not set"
	},
	func(g *githubIssues, _ test.Test) string {
		if defaultOpts.IsReleaseBranch() {
			return ""
		}

		return fmt.Sprintf("not a release branch: %q", defaultOpts.Branch)
	},
	func(_ *githubIssues, t test.Test) string {
		if nonReportable := failuresAsNonReportableError(t.(*testImpl).failures()); nonReportable != nil {
			return nonReportable.Error()
		}

		return ""
	},
	func(_ *githubIssues, t test.Test) string {
		if t.Spec().(*registry.TestSpec).Run == nil {
			return "TestSpec.Run is nil"
		}

		return ""
	},
	func(_ *githubIssues, t test.Test) string {
		if t.Spec().(*registry.TestSpec).Cluster.NodeCount == 0 {
			return "Cluster.NodeCount is zero"
		}

		return ""
	},
}

// shouldPost checks whether we should post a GitHub issue: if we do,
// the return value will be the empty string. Otherwise, this function
// returns the reason for not posting.
func (g *githubIssues) shouldPost(t test.Test) string {
	for _, sc := range skipConditions {
		if skipReason := sc(g, t); skipReason != "" {
			return skipReason
		}
	}

	return ""
}

func (g *githubIssues) createPostRequest(
	testName string,
	start time.Time,
	end time.Time,
	spec *registry.TestSpec,
	failures []failure,
	message string,
	sideEyeTimeoutSnapshotURL string,
	runtimeAssertionsBuild bool,
	coverageBuild bool,
	params map[string]string,
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
	if errWithOwner == nil {
		errWithOwner = transientErrorOwnershipFallback(failures)
	}
	if errWithOwner != nil {
		handleErrorWithOwnership(*errWithOwner)
	}

	// Issues posted from roachtest are identifiable as such, and they are also release blockers
	// (this label may be removed by a human upon closer investigation).
	const infraFlakeLabel = "X-infra-flake"
	const runtimeAssertionsLabel = "B-runtime-assertions-enabled"
	const coverageLabel = "B-coverage-enabled"
	labels := []string{"O-roachtest"}
	if infraFlake {
		labels = append(labels, infraFlakeLabel)
	} else {
		labels = append(labels, issues.TestFailureLabel)
		if !spec.NonReleaseBlocker {
			// TODO(radu): remove this check once these build types are stabilized.
			if !coverageBuild {
				labels = append(labels, issues.ReleaseBlockerLabel)
			}
		}
		if runtimeAssertionsBuild {
			labels = append(labels, runtimeAssertionsLabel)
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

	if g.cluster != nil {
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
	if runtimeAssertionsBuild {
		topLevelNotes = append(topLevelNotes,
			"This build has runtime assertions enabled. If the same failure was hit in a run without assertions "+
				"enabled, there should be a similar failure without this message. If there isn't one, "+
				"then this failure is likely due to an assertion violation or (assertion) timeout.")
	}

	sideEyeMsg := ""
	if sideEyeTimeoutSnapshotURL != "" {
		sideEyeMsg = "A Side-Eye cluster snapshot was captured on timeout: "
	}

	return issues.PostRequest{
		MentionOnCreate: mention,
		ProjectColumnID: projColID,
		PackageName:     "roachtest",
		TestName:        issueName,
		Labels:          labels,
		// Keep issues separate unless the if these labels don't match.
		AdoptIssueLabelMatchSet: []string{infraFlakeLabel, coverageLabel},
		TopLevelNotes:           topLevelNotes,
		Message:                 issueMessage,
		Artifacts:               artifacts,
		SideEyeSnapshotMsg:      sideEyeMsg,
		SideEyeSnapshotURL:      sideEyeTimeoutSnapshotURL,
		ExtraParams:             params,
		HelpCommand:             generateHelpCommand(testName, issueClusterName, roachtestflags.Cloud, start, end),
	}, nil
}

func (g *githubIssues) MaybePost(
	t *testImpl,
	l *logger.Logger,
	message string,
	sideEyeTimeoutSnapshotURL string,
	params map[string]string,
) (*issues.TestFailureIssue, error) {
	skipReason := g.shouldPost(t)
	if skipReason != "" {
		l.Printf("skipping GitHub issue posting (%s)", skipReason)
		return nil, nil
	}

	postRequest, err := g.createPostRequest(
		t.Name(), t.start, t.end, t.spec, t.failures(),
		message, sideEyeTimeoutSnapshotURL,
		roachtestutil.UsingRuntimeAssertions(t), t.goCoverEnabled, params,
	)

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
