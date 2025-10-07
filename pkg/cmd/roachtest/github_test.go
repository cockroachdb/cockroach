// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

var (
	teamsYaml = `cockroachdb/unowned:
 aliases:
   cockroachdb/rfc-prs: other
cockroachdb/test-eng:
 label: T-testeng
cockroachdb/dev-inf:
 label: T-dev-inf`

	validTeamsFn   = func() (team.Map, error) { return loadYamlTeams(teamsYaml) }
	invalidTeamsFn = func() (team.Map, error) { return loadYamlTeams("invalid yaml") }
)

func loadYamlTeams(yaml string) (team.Map, error) {
	return team.LoadTeams(strings.NewReader(yaml))
}

func TestShouldPost(t *testing.T) {
	preemptionFailure := []failure{
		{errors: []error{vmPreemptionError("vm1")}},
	}
	testCases := []struct {
		disableIssues     bool
		nodeCount         int
		envGithubAPIToken string
		envTcBuildBranch  string
		failures          []failure
		expectedReason    string
	}{
		/* Cases 1 - 4 verify that issues are not posted if any of the relevant criteria checks fail */
		// disable
		{true, 1, "token", "master", nil, "issue posting was disabled via command line flag"},
		// nodeCount
		{false, 0, "token", "master", nil, "Cluster.NodeCount is zero"},
		// apiToken
		{false, 1, "", "master", nil, "GitHub API token not set"},
		// branch
		{false, 1, "token", "", nil, `not a release branch: "branch-not-found-in-env"`},
		// VM preemtion while test ran
		{false, 1, "token", "master", preemptionFailure, "non-reportable: preempted VMs: vm1 [owner=test-eng]"},
		{false, 1, "token", "master", nil, ""},
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
		ti.mu.failures = c.failures
		github := &githubIssues{disable: c.disableIssues, dryRun: false}

		skipReason := github.shouldPost(ti)
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
	reg := makeTestRegistry()
	const testName = "github_test"

	type githubIssueOpts struct {
		failures        []failure
		loadTeamsFailed bool
	}

	datadriven.Walk(t, datapathutils.TestDataPath(t, "github"), func(t *testing.T, path string) {
		clusterSpec := reg.MakeClusterSpec(1)

		testSpec := &registry.TestSpec{
			Name:            testName,
			Owner:           OwnerUnitTest,
			Cluster:         clusterSpec,
			CockroachBinary: registry.StandardCockroach,
		}

		ti := &testImpl{
			spec:        testSpec,
			start:       time.Date(2023, time.July, 21, 16, 34, 3, 817, time.UTC),
			end:         time.Date(2023, time.July, 21, 16, 42, 13, 137, time.UTC),
			cockroach:   "cockroach",
			cockroachEA: "cockroach-ea",
		}
		ti.ReplaceL(nilLogger())

		testClusterImpl := &clusterImpl{spec: clusterSpec, arch: vm.ArchAMD64, name: "foo"}
		vo := vm.DefaultCreateOpts()
		vmOpts := &vo
		teamLoadFn := validTeamsFn

		testCase := githubIssueOpts{}

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			if d.Cmd == "post" {
				github := &githubIssues{
					teamLoader: teamLoadFn,
				}
				issueInfo := newGithubIssueInfo(testClusterImpl, vmOpts)

				// See: `formatFailure` which formats failures for roachtests. Try to
				// follow it here.
				var b strings.Builder
				for i, f := range testCase.failures {
					if i > 0 {
						fmt.Fprintln(&b)
					}
					// N.B. Don't use %+v here even though roachtest does. We don't
					// want the stack trace to be outputted which will differ based
					// on where this test is run and prone to flaking.
					fmt.Fprintf(&b, "%v", f.squashedErr)
				}
				message := b.String()

				params := getTestParameters(ti, issueInfo.cluster, issueInfo.vmCreateOpts)
				req, err := github.createPostRequest(
					testName, ti.start, ti.end, testSpec, testCase.failures,
					message, roachtestutil.UsingRuntimeAssertions(ti), ti.goCoverEnabled, params,
					issueInfo,
				)
				if testCase.loadTeamsFailed {
					// Assert that if TEAMS.yaml cannot be loaded then function errors.
					require.Error(t, err)
					return ""
				}
				require.NoError(t, err)

				post, _, err := formatPostRequest(req)
				require.NoError(t, err)

				return post
			}

			switch d.Cmd {
			case "add-failure":
				refError := errors.Newf("%s", d.CmdArgs[0].Vals[0])

				// The type(s) of error, listed from innermost to outermost.
				if len(d.CmdArgs) == 2 {
					errorTypes := d.CmdArgs[1].Vals
					for _, e := range errorTypes {
						switch e {
						case "cluster-provision":
							refError = errClusterProvisioningFailed(refError)
						case "transient-error":
							refError = rperrors.TransientFailure(refError, "some_problem")
						case "ssh-flake":
							refError = rperrors.NewSSHError(refError)
						case "dns-flake":
							refError = rperrors.TransientFailure(refError, "dns_problem")
						case "vm-preemption":
							refError = vmPreemptionError("my_VM")
						case "vm-host-error":
							refError = vmHostError("my_VM")
						case "live-migration-error":
							refError = liveMigrationError("my_VM")
						case "error-with-owner-sql-foundations":
							refError = registry.ErrorWithOwner(registry.OwnerSQLFoundations, refError)
						case "error-with-owner-test-eng":
							refError = registry.ErrorWithOwner(registry.OwnerTestEng, refError)
						case "require-no-error-failed":
							// Attempts to mimic how the require package creates failures by losing
							// the error object and prepending a message. Similar to above we don't use
							// %+v to avoid stack traces.
							refError = errors.Newf("Received unexpected error:\n%s", redact.SafeString(refError.Error()))
						case "lose-error-object":
							// Lose the error object which should make our flake detection fail.
							refError = errors.Newf("%s", redact.SafeString(refError.Error()))
						case "node-fatal":
							refError = errors.Newf(`(monitor.go:267).Wait: monitor failure: dial tcp 127.0.0.1:29000: connect: connection refused
test artifacts and logs in: artifacts/roachtest/manual/monitor/test-failure/node-fatal-explicit-monitor/cpu_arch=arm64/run_1
F250826 19:49:07.194443 3106 sql/sem/builtins/builtins.go:6063 ⋮ [T1,Vsystem,n1,client=127.0.0.1:54552,hostssl,user=‹roachprod›] 250  force_log_fatal(): ‹oops›
`)
						case "error-with-ip-node-info":
							refError = errors.Newf(`(roachtest.go:93).func6: manual failure
test artifacts and logs in: artifacts/roachtest/manual/fail/run_1
| Node | Private IP | Public IP | Machine Type |
| --- | --- | --- | --- |
| teamcity-1758834520-01-n1cpu4-0001 | 10.142.0.2 | 34.139.44.53 | n2-standard-4 |
`)
						}
					}
				}

				testCase.failures = append(testCase.failures, createFailure(refError))
			case "add-label":
				ti.spec.ExtraLabels = append(ti.spec.ExtraLabels, d.CmdArgs[0].Vals...)
			case "add-param":
				ti.AddParam(d.CmdArgs[0].Vals[0], d.CmdArgs[1].Vals[0])
			case "set-cluster-create-failed":
				// We won't have either if cluster create fails.
				vmOpts = nil
				testClusterImpl = nil
			case "set-non-release-blocker":
				ti.spec.NonReleaseBlocker = true
			case "set-load-teams-failed":
				teamLoadFn = invalidTeamsFn
				testCase.loadTeamsFailed = true
			case "set-runtime-assertions-build":
				ti.spec.CockroachBinary = registry.RuntimeAssertionsCockroach
			case "set-coverage-enabled-build":
				ti.goCoverEnabled = true
			}

			return "ok"
		})
	})
}
