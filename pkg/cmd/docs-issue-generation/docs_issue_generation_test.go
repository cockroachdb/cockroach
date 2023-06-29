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
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestParseDocsIssueBody(t *testing.T) {
	testCases := []struct {
		testName string
		body     string
		result   struct {
			prNumber  int
			commitSha string
			err       error
		}
	}{
		{
			testName: "no PR or commit",
			body:     "Test test 12345",
			result: struct {
				prNumber  int
				commitSha string
				err       error
			}{
				prNumber:  0,
				commitSha: "",
				err:       fmt.Errorf("error: No PR number found in issue body"),
			},
		},
		{
			testName: "Result with markdown URLs",
			body: `Exalate commented:

Related PR: [https://github.com/cockroachdb/cockroach/pull/80670](https://github.com/cockroachdb/cockroach/pull/80670) 

Commit: [https://github.com/cockroachdb/cockroach/commit/84a3833ee30eed278de0571c8c7d9f2f5e3b8b5d](https://github.com/cockroachdb/cockroach/commit/84a3833ee30eed278de0571c8c7d9f2f5e3b8b5d)

— Release note (enterprise change): Backups run by secondary tenants now write protected timestamp records to protect their target schema objects from garbage collection during backup execution.

Jira Issue: DOC-3619`,
			result: struct {
				prNumber  int
				commitSha string
				err       error
			}{
				prNumber:  80670,
				commitSha: "84a3833ee30eed278de0571c8c7d9f2f5e3b8b5d",
				err:       nil,
			},
		},
		{
			testName: "Result with regular URLs",
			body: `Related PR: https://github.com/cockroachdb/cockroach/pull/90789
Commit: https://github.com/cockroachdb/cockroach/commit/c7ce65697535daacf2a75df245c3bae1179faeda

---

Release note (ops change): The cluster setting
` + "server.web_session.auto_logout.timeout" + ` has been removed. It had
never been effective.`,
			result: struct {
				prNumber  int
				commitSha string
				err       error
			}{
				prNumber:  90789,
				commitSha: "c7ce65697535daacf2a75df245c3bae1179faeda",
				err:       nil,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			prNumber, commitSha, err := parseDocsIssueBody(tc.body)
			result := struct {
				prNumber  int
				commitSha string
				err       error
			}{prNumber: prNumber, commitSha: commitSha, err: err}
			assert.Equal(t, tc.result, result)
		})
	}
}

func TestConstructDocsIssues(t *testing.T) {
	testCases := []struct {
		testName     string
		cockroachPRs []cockroachPR
		docsIssues   []docsIssue
	}{
		{
			testName: "Single PR - 91345 - Epic: none",
			cockroachPRs: []cockroachPR{{
				Title:       "release-22.2: clusterversion: allow forcing release binary to dev version",
				Number:      91345,
				Body:        "Backport 1/1 commits from #90002, using the simplifications from #91344\r\n\r\n/cc @cockroachdb/release\r\n\r\n---\r\n\r\nPreviously it was impossible to start a release binary that supported up to say, 23.1, in a cluster where the cluster version was in the 'development' range (+1m). While this was somewhat intentional -- to mark a dev cluster as dev forever -- we still want the option to try to run release binaries in that cluster.\r\n\r\nThe new environment variable COCKROACH_FORCE_DEV_VERSION will cause a binary to identify itself as development and offset its version even if it is a release binary.\r\n\r\nEpic: none.\r\n\r\nRelease note (ops change): Release version binaries can now be instructed via the enviroment variable COCKROACH_FORCE_DEV_VERSION to override their cluster version support to match that of develeopment builds, which can allow a release binary to be started in a cluster that is run or has previously run a development build.\r\n\r\nRelease justification: bug fix in new functionality.",
				BaseRefName: "release-22.2",
				Commits: []cockroachCommit{{
					Sha:             "8dc44d23bb7e0688cd435b6f7908fab615f1aa39",
					MessageHeadline: "clusterversion: allow forcing release binary to dev version",
					MessageBody:     "Previously it was impossible to start a release binary that supported up\nto say, 23.1, in a cluster where the cluster version was in the 'development'\nrange (+1m). While this was somewhat intentional -- to mark a dev cluster as dev\nforever -- we still want the option to try to run release binaries in that cluster.\n\nThe new environment variable COCKROACH_FORCE_DEV_VERSION will cause a binary to\nidentify itself as development and offset its version even if it is a release binary.\n\nEpic: none.\n\nRelease note (ops change): Release version binaries can now be instructed via the enviroment\nvariable COCKROACH_FORCE_DEV_VERSION to override their cluster version support to match that\nof develeopment builds, which can allow a release binary to be started in a cluster that is\nrun or has previously run a development build.",
				}},
			}},
			docsIssues: []docsIssue{{
				sourceCommitSha: "8dc44d23bb7e0688cd435b6f7908fab615f1aa39",
				title:           "PR #91345 - clusterversion: allow forcing release binary to dev version",
				body:            "Related PR: https://github.com/cockroachdb/cockroach/pull/91345\nCommit: https://github.com/cockroachdb/cockroach/commit/8dc44d23bb7e0688cd435b6f7908fab615f1aa39\nEpic: none\n\n---\n\nRelease note (ops change): Release version binaries can now be instructed via the enviroment\nvariable COCKROACH_FORCE_DEV_VERSION to override their cluster version support to match that\nof develeopment builds, which can allow a release binary to be started in a cluster that is\nrun or has previously run a development build.",
				labels:          []string{"C-product-change", "release-22.2"},
			}},
		},
		{
			testName: "Multiple PRs",
			cockroachPRs: []cockroachPR{
				{
					Title:       "release-22.1: ui: update filter labels",
					Number:      91294,
					Body:        "Backport 1/1 commits from #88078.\r\n\r\n/cc @cockroachdb/release\r\n\r\n---\r\n\r\nUpdate filter label from \"App\" to \"Application Name\" on SQL Activity.\r\n\r\nFixes #87960\r\n\r\n<img width=\"467\" alt=\"Screen Shot 2022-09-16 at 6 40 51 PM\" src=\"https://user-images.githubusercontent.com/1017486/190827281-36a9c6df-3e16-4689-bcae-6649b1c2ff86.png\">\r\n\r\n\r\nRelease note (ui change): Update filter labels from \"App\" to \"Application Name\" and from \"Username\" to \"User Name\" on SQL Activity and Insights pages.\r\n\r\n---\r\n\r\nRelease justification: small change\r\n",
					BaseRefName: "release-22.1",
					Commits: []cockroachCommit{{
						Sha:             "8d15073f329cf8d72e09977b34a3b339d1436000",
						MessageHeadline: "ui: update filter labels",
						MessageBody:     "Update filter label from \"App\" to \"Application Name\"\nand \"Username\" to \"User Name\" on SQL Activity pages.\n\nFixes #87960\n\nRelease note (ui change): Update filter labels from\n\"App\" to \"Application Name\" and from \"Username\" to\n\"User Name\" on SQL Activity pages.",
					}},
				},
				{
					Title:       "release-22.2.0: sql/ttl: rename num_active_ranges metrics",
					Number:      90381,
					Body:        "Backport 1/1 commits from #90175.\r\n\r\n/cc @cockroachdb/release\r\n\r\nRelease justification: metrics rename that should be done in a major release\r\n\r\n---\r\n\r\nfixes https://github.com/cockroachdb/cockroach/issues/90094\r\n\r\nRelease note (ops change): These TTL metrics have been renamed: \r\njobs.row_level_ttl.range_total_duration -> jobs.row_level_ttl.span_total_duration\r\njobs.row_level_ttl.num_active_ranges -> jobs.row_level_ttl.num_active_spans\r\n",
					BaseRefName: "release-22.2.0",
					Commits: []cockroachCommit{{
						Sha:             "1829a72664f28ddfa50324c9ff5352380029560b",
						MessageHeadline: "sql/ttl: rename num_active_ranges metrics",
						MessageBody:     "fixes https://github.com/cockroachdb/cockroach/issues/90094\n\nRelease note (ops change): These TTL metrics have been renamed:\njobs.row_level_ttl.range_total_duration -> jobs.row_level_ttl.span_total_duration\njobs.row_level_ttl.num_active_ranges -> jobs.row_level_ttl.num_active_spans",
					}},
				},
				{
					Title:       "release-22.2: opt/props: shallow-copy props.Histogram when applying selectivity",
					Number:      89957,
					Body:        "Backport 2/2 commits from #88526 on behalf of @michae2.\r\n\r\n/cc @cockroachdb/release\r\n\r\n----\r\n\r\n**opt/props: add benchmark for props.Histogram**\r\n\r\nAdd a benchmark that measures various common props.Histogram operations.\r\n\r\nRelease note: None\r\n\r\n**opt/props: shallow-copy props.Histogram when applying selectivity**\r\n\r\n`pkg/sql/opt/props.(*Histogram).copy` showed up as a heavy allocator in\r\na recent customer OOM. The only caller of this function is\r\n`Histogram.ApplySelectivity` which deep-copies the histogram before\r\nadjusting each bucket's `NumEq`, `NumRange`, and `DistinctRange` by the\r\ngiven selectivity.\r\n\r\nInstead of deep-copying the histogram, we can change `ApplySelectivity`\r\nto shallow-copy the histogram and remember the current selectivity. We\r\ncan then wait to adjust counts in each bucket by the selectivity until\r\nsomeone actually calls `ValuesCount` or `DistinctValuesCount`.\r\n\r\nThis doesn't eliminate all copying of histograms. We're still doing some\r\ncopying in `Filter` when applying a constraint to the histogram.\r\n\r\nFixes: #89941\r\n\r\nRelease note (performance improvement): The optimizer now does less\r\ncopying of histograms while planning queries, which will reduce memory\r\npressure a little.\r\n\r\n----\r\n\r\nRelease justification: Low risk, high reward change to existing functionality.",
					BaseRefName: "release-22.2",
					Commits: []cockroachCommit{{
						Sha:             "43de8ff30e3e6e1d9b2272ed4f62c543dc0a037c",
						MessageHeadline: "opt/props: shallow-copy props.Histogram when applying selectivity",
						MessageBody:     "`pkg/sql/opt/props.(*Histogram).copy` showed up as a heavy allocator in\na recent customer OOM. The only caller of this function is\n`Histogram.ApplySelectivity` which deep-copies the histogram before\nadjusting each bucket's `NumEq`, `NumRange`, and `DistinctRange` by the\ngiven selectivity.\n\nInstead of deep-copying the histogram, we can change `ApplySelectivity`\nto shallow-copy the histogram and remember the current selectivity. We\ncan then wait to adjust counts in each bucket by the selectivity until\nsomeone actually calls `ValuesCount` or `DistinctValuesCount`.\n\nThis doesn't eliminate all copying of histograms. We're still doing some\ncopying in `Filter` when applying a constraint to the histogram.\n\nFixes: #89941\n\nRelease note (performance improvement): The optimizer now does less\ncopying of histograms while planning queries, which will reduce memory\npressure a little.",
					}},
				},
			},
			docsIssues: []docsIssue{
				{
					sourceCommitSha: "8d15073f329cf8d72e09977b34a3b339d1436000",
					title:           "PR #91294 - ui: update filter labels",
					body:            "Related PR: https://github.com/cockroachdb/cockroach/pull/91294\nCommit: https://github.com/cockroachdb/cockroach/commit/8d15073f329cf8d72e09977b34a3b339d1436000\nFixes: CRDB-19614\n\n---\n\nRelease note (ui change): Update filter labels from\n\"App\" to \"Application Name\" and from \"Username\" to\n\"User Name\" on SQL Activity pages.",
					labels:          []string{"C-product-change", "release-22.1"},
				},
				{
					sourceCommitSha: "1829a72664f28ddfa50324c9ff5352380029560b",
					title:           "PR #90381 - sql/ttl: rename num_active_ranges metrics",
					body:            "Related PR: https://github.com/cockroachdb/cockroach/pull/90381\nCommit: https://github.com/cockroachdb/cockroach/commit/1829a72664f28ddfa50324c9ff5352380029560b\nFixes: CRDB-20636\n\n---\n\nRelease note (ops change): These TTL metrics have been renamed:\njobs.row_level_ttl.range_total_duration -> jobs.row_level_ttl.span_total_duration\njobs.row_level_ttl.num_active_ranges -> jobs.row_level_ttl.num_active_spans",
					labels:          []string{"C-product-change", "release-22.2.0"},
				},
				{
					sourceCommitSha: "43de8ff30e3e6e1d9b2272ed4f62c543dc0a037c",
					title:           "PR #89957 - opt/props: shallow-copy props.Histogram when applying selectivity",
					body:            "Related PR: https://github.com/cockroachdb/cockroach/pull/89957\nCommit: https://github.com/cockroachdb/cockroach/commit/43de8ff30e3e6e1d9b2272ed4f62c543dc0a037c\nFixes: CRDB-20505\n\n---\n\nRelease note (performance improvement): The optimizer now does less\ncopying of histograms while planning queries, which will reduce memory\npressure a little.",
					labels:          []string{"C-product-change", "release-22.2"},
				},
			},
		},
		{
			testName: "Epic none",
			cockroachPRs: []cockroachPR{{
				Title:       "Epic none PR",
				Number:      123456,
				Body:        "This fixes a thing.\n\nEpic: none\nRelease note (enterprise change): enterprise changes",
				BaseRefName: "master",
				Commits: []cockroachCommit{{
					Sha:             "690da3e2ac3b1b7268accfb1dcbe5464e948e9d1",
					MessageHeadline: "Epic none PR",
					MessageBody:     "Epic: none\nRelease note (enterprise change): enterprise changes",
				}},
			}},
			docsIssues: []docsIssue{{
				sourceCommitSha: "690da3e2ac3b1b7268accfb1dcbe5464e948e9d1",
				title:           "PR #123456 - Epic none PR",
				body:            "Related PR: https://github.com/cockroachdb/cockroach/pull/123456\nCommit: https://github.com/cockroachdb/cockroach/commit/690da3e2ac3b1b7268accfb1dcbe5464e948e9d1\nEpic: none\n\n---\n\nRelease note (enterprise change): enterprise changes",
				labels:          []string{"C-product-change", "master"},
			}},
		},
		{
			testName: "Epic extraction",
			cockroachPRs: []cockroachPR{{
				Title:       "Epic extraction PR",
				Number:      123456,
				Body:        "This fixes another thing.\n\nEpic: CRDB-24680\nRelease note (cli change): cli changes",
				BaseRefName: "master",
				Commits: []cockroachCommit{{
					Sha:             "aaada3e2ac3b1b7268accfb1dcbe5464e948e9d1",
					MessageHeadline: "Epic extraction PR",
					MessageBody:     "Epic: CRDB-24680\nRelease note (cli change): cli changes",
				}},
			}},
			docsIssues: []docsIssue{{
				sourceCommitSha: "aaada3e2ac3b1b7268accfb1dcbe5464e948e9d1",
				title:           "PR #123456 - Epic extraction PR",
				body:            "Related PR: https://github.com/cockroachdb/cockroach/pull/123456\nCommit: https://github.com/cockroachdb/cockroach/commit/aaada3e2ac3b1b7268accfb1dcbe5464e948e9d1\nEpic: CRDB-24680\n\n---\n\nRelease note (cli change): cli changes",
				labels:          []string{"C-product-change", "master"},
			}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			defer testutils.TestingHook(&getJiraIssueFromGitHubIssue, func(org, repo string, issue int, token string) (string, error) {
				// getJiraIssueFromGitHubIssue requires a network call to the GitHub GraphQL API to calculate the Jira issue given
				// a GitHub org/repo/issue. To help eliminate the need of a network call and minimize the chances of this test
				// flaking, we define a pre-built map that is used to mock the network call and allow the tests to run as expected.
				var ghJiraIssueMap = make(map[string]map[string]map[int]string)
				ghJiraIssueMap["cockroachdb"] = make(map[string]map[int]string)
				ghJiraIssueMap["cockroachdb"]["cockroach"] = make(map[int]string)
				ghJiraIssueMap["cockroachdb"]["cockroach"][87960] = "CRDB-19614"
				ghJiraIssueMap["cockroachdb"]["cockroach"][90094] = "CRDB-20636"
				ghJiraIssueMap["cockroachdb"]["cockroach"][89941] = "CRDB-20505"
				return ghJiraIssueMap[org][repo][issue], nil
			})()
			result := constructDocsIssues(tc.cockroachPRs, "")
			assert.Equal(t, tc.docsIssues, result)
		})
	}
}

func TestFormatReleaseNotes(t *testing.T) {

	testCases := []struct {
		prNum         string
		prBody        string
		sha           string
		commitMessage string
		rns           []string
	}{
		{
			prNum: "79069",
			sha:   "5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
			commitMessage: `sql: ignore non-existent columns when injecting stats

Previously, an ` + "`ALTER TABLE ... INJECT STATS`" + ` command would return an
error if the given stats JSON included any columns that were not present
in the table descriptor. Statistics in statement bundles often include
dropped columns, so reproducing docsIssues with a bundle required tediously
removing stats for these columns. This commit changes the stats
injection behavior so that a notice is issued for stats with
non-existent columns rather than an error. Any stats for existing
columns will be injected successfully.

Informs #68184

Release note (sql change): ` + "`ALTER TABLE ... INJECT STATISTICS ...`" + ` will
now docsIssue notices when the given statistics JSON includes non-existent
columns, rather than resulting in an error. Any statistics in the JSON
for existing columns will be injected successfully.`,
			rns: []string{`Related PR: https://github.com/cockroachdb/cockroach/pull/79069
Commit: https://github.com/cockroachdb/cockroach/commit/5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9
Informs: CRDB-8919

---

Release note (sql change): ` + "`ALTER TABLE ... INJECT STATISTICS ...`" + ` will
now docsIssue notices when the given statistics JSON includes non-existent
columns, rather than resulting in an error. Any statistics in the JSON
for existing columns will be injected successfully.`},
		},
		{
			prNum: "79361",
			sha:   "88be04bd64283b1d77000a3f88588e603465e81b",
			commitMessage: `changefeedccl: remove the default values from SHOW
CHANGEFEED JOB output

Currently, when a user alters a changefeed, we
include the default options in the SHOW CHANGEFEED
JOB output. In this PR we prevent the default values
from being displayed.

Release note (enterprise change): Remove the default
values from the SHOW CHANGEFEED JOB output`,
			rns: []string{`Related PR: https://github.com/cockroachdb/cockroach/pull/79361
Commit: https://github.com/cockroachdb/cockroach/commit/88be04bd64283b1d77000a3f88588e603465e81b

---

Release note (enterprise change): Remove the default
values from the SHOW CHANGEFEED JOB output`},
		},
		{
			prNum: "78685",
			sha:   "1d7811d5d14f9c7e106c3ec92de9c66192f19604",
			commitMessage: `opt: do not cross-join input of semi-join

This commit fixes a logical correctness bug caused when
` + "`GenerateLookupJoins`" + ` cross-joins the input of a semi-join with a set of
constant values to constrain the prefix columns of the lookup index. The
cross-join is an invalid transformation because it increases the size of
the join's input and can increase the size of the join's output.

We already avoid these cross-joins for left and anti-joins (see #59646).
When addressing those cases, the semi-join case was incorrectly assumed
to be safe.

Fixes #78681

Release note (bug fix): A bug has been fixed which caused the optimizer
to generate invalid query plans which could result in incorrect query
results. The bug, which has been present since version 21.1.0, can
appear if all of the following conditions are true: 1) the query
contains a semi-join, such as queries in the form:
` + "`SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.a = t2.a);`" + `,
2) the inner table has an index containing the equality column, like
` + "`t2.a`" + ` in the example query, 3) the index contains one or more
columns that prefix the equality column, and 4) the prefix columns are
` + "`NOT NULL`" + " and are constrained to a set of constant values via a " + "`CHECK`" + `
constraint or an ` + "`IN`" + " condition in the filter.",
			rns: []string{},
		},
		{
			prNum: "66328",
			sha:   "0f329965acccb3e771ec1657c7def9e881dc78bb",
			commitMessage: `util/log: report the logging format at the start of new files

Release note (cli change): When log entries are written to disk,
the first few header lines written at the start of every new file
now report the configured logging format.`,
			rns: []string{`Related PR: https://github.com/cockroachdb/cockroach/pull/66328
Commit: https://github.com/cockroachdb/cockroach/commit/0f329965acccb3e771ec1657c7def9e881dc78bb

---

Release note (cli change): When log entries are written to disk,
the first few header lines written at the start of every new file
now report the configured logging format.`},
		},
		{
			prNum: "66328",
			sha:   "fb249c7140b634a53dca2967c946bc78ba927e1a",
			commitMessage: `cli: report explicit log config in logs

This increases troubleshootability.

Release note: None`,
			rns: []string{},
		},
		{
			prNum: "104265",
			sha:   "d756dec1b9d7245305ab706e68e2ec3de0e61ffc",
			commitMessage: `Release note (cli change): The log output formats ` + "`crdb-v1`" + ` and
` + "`crdb-v2`" + ` now support the format option ` + "`timezone`" + `. When specified,
the corresponding time zone is used to produce the timestamp column.

For example:
` + "```" + `yaml
file-defaults:
	format: crdb-v2
	format-options: {timezone: america/new_york}
` + "```" + `

Example logging output:
` + "```" + `
I230606 12:43:01.553407-040000 1 1@cli/start.go:575 ⋮ [n?] 4  soft memory limit of Go runtime is set to 35 GiB
^^^^^^^ indicates GMT-4 was used.
` + "```" + `

The timezone offset is also always included in the format if it is not
zero (e.g. for non-UTC time zones). This is necessary to ensure that
the times can be read back precisely.

Release note (cli change): The command ` + "`cockroach debug merge-log`" + ` was
adapted to understand time zones in input files read with format
` + "`crdb-v1`" + ` or ` + "`crdb-v2`" + `.

Release note (backward-incompatible change): When a deployment is
configured to use a time zone (new feature) for log file output using
formats ` + "`crdb-v1`" + ` or ` + "`crdb-v2`" + `, it becomes impossible to process the
new output log files using the ` + "`cockroach debug merge-log`" + ` command
from a previous version. The newest ` + "`cockroach debug merge-log`" + ` code
must be used instead.`,
			rns: []string{`Related PR: https://github.com/cockroachdb/cockroach/pull/104265
Commit: https://github.com/cockroachdb/cockroach/commit/d756dec1b9d7245305ab706e68e2ec3de0e61ffc
Related product changes: https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2Fd756dec1b9d7245305ab706e68e2ec3de0e61ffc%22%20ORDER%20BY%20created%20DESC

---

Release note (cli change): The log output formats ` + "`crdb-v1`" + ` and
` + "`crdb-v2`" + ` now support the format option ` + "`timezone`" + `. When specified,
the corresponding time zone is used to produce the timestamp column.

For example:
` + "```" + `yaml
file-defaults:
	format: crdb-v2
	format-options: {timezone: america/new_york}
` + "```" + `

Example logging output:
` + "```" + `
I230606 12:43:01.553407-040000 1 1@cli/start.go:575 ⋮ [n?] 4  soft memory limit of Go runtime is set to 35 GiB
^^^^^^^ indicates GMT-4 was used.
` + "```" + `

The timezone offset is also always included in the format if it is not
zero (e.g. for non-UTC time zones). This is necessary to ensure that
the times can be read back precisely.`,
				`Related PR: https://github.com/cockroachdb/cockroach/pull/104265
Commit: https://github.com/cockroachdb/cockroach/commit/d756dec1b9d7245305ab706e68e2ec3de0e61ffc
Related product changes: https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2Fd756dec1b9d7245305ab706e68e2ec3de0e61ffc%22%20ORDER%20BY%20created%20DESC

---

Release note (cli change): The command ` + "`cockroach debug merge-log`" + ` was
adapted to understand time zones in input files read with format
` + "`crdb-v1`" + ` or ` + "`crdb-v2`" + `.`,
				`Related PR: https://github.com/cockroachdb/cockroach/pull/104265
Commit: https://github.com/cockroachdb/cockroach/commit/d756dec1b9d7245305ab706e68e2ec3de0e61ffc
Related product changes: https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2Fd756dec1b9d7245305ab706e68e2ec3de0e61ffc%22%20ORDER%20BY%20created%20DESC

---

Release note (backward-incompatible change): When a deployment is
configured to use a time zone (new feature) for log file output using
formats ` + "`crdb-v1`" + ` or ` + "`crdb-v2`" + `, it becomes impossible to process the
new output log files using the ` + "`cockroach debug merge-log`" + ` command
from a previous version. The newest ` + "`cockroach debug merge-log`" + ` code
must be used instead.`},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.prNum, func(t *testing.T) {
			defer testutils.TestingHook(&getJiraIssueFromGitHubIssue, func(org, repo string, issue int, token string) (string, error) {
				var ghJiraIssueMap = make(map[string]map[string]map[int]string)
				ghJiraIssueMap["cockroachdb"] = make(map[string]map[int]string)
				ghJiraIssueMap["cockroachdb"]["cockroach"] = make(map[int]string)
				ghJiraIssueMap["cockroachdb"]["cockroach"][68184] = "CRDB-8919"
				return ghJiraIssueMap[org][repo][issue], nil
			})()
			prNumInt, _ := strconv.Atoi(tc.prNum)
			result := formatReleaseNotes(tc.commitMessage, prNumInt, tc.prBody, tc.sha, "")
			assert.Equal(t, tc.rns, result)
		})
	}
}

func TestFormatTitle(t *testing.T) {
	testCases := []struct {
		testName    string
		message     string
		prNumber    int
		index       int
		totalLength int
		title       string
	}{
		{
			testName:    "Format Title 1",
			message:     `sql: ignore non-existent columns when injecting stats`,
			prNumber:    12345,
			index:       1,
			totalLength: 1,
			title:       "PR #12345 - sql: ignore non-existent columns when injecting stats",
		},
		{
			testName:    "Format Title 2",
			message:     `changefeedccl: remove the default values from SHOW CHANGEFEED JOB output`,
			prNumber:    54321,
			index:       1,
			totalLength: 1,
			title:       "PR #54321 - changefeedccl: remove the default values from SHOW CHANGEFEED JOB output",
		},
		{
			testName:    "Format Title 3",
			message:     `opt: do not cross-join input of semi-join`,
			prNumber:    65432,
			index:       1,
			totalLength: 1,
			title:       "PR #65432 - opt: do not cross-join input of semi-join",
		},
		{
			testName:    "Format Title 4",
			message:     `util/log: report the logging format at the start of new files`,
			prNumber:    23456,
			index:       1,
			totalLength: 1,
			title:       "PR #23456 - util/log: report the logging format at the start of new files",
		},
		{
			testName:    "Format Title 5",
			message:     `sql: ignore non-existent columns when injecting stats`,
			prNumber:    34567,
			index:       2,
			totalLength: 3,
			title:       "PR #34567 - sql: ignore non-existent columns when injecting stats (2 of 3)",
		},
		{
			testName:    "Format Title 6",
			message:     `changefeedccl: remove the default values from SHOW CHANGEFEED JOB output`,
			prNumber:    76543,
			index:       1,
			totalLength: 6,
			title:       "PR #76543 - changefeedccl: remove the default values from SHOW CHANGEFEED JOB output (1 of 6)",
		},
		{
			testName:    "Format Title 7",
			message:     `opt: do not cross-join input of semi-join`,
			prNumber:    45678,
			index:       5,
			totalLength: 7,
			title:       "PR #45678 - opt: do not cross-join input of semi-join (5 of 7)",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			result := formatTitle(tc.message, tc.prNumber, tc.index, tc.totalLength)
			assert.Equal(t, tc.title, result)
		})
	}
}

func TestExtractFixIssuesIDs(t *testing.T) {
	testCases := []struct {
		message  string
		expected map[string]int
	}{
		{
			message: `workload: Fix folder name generation.
Fixes #75200 #98922
close #75201
closed #592
RESOLVE #5555
Release Notes: None`,
			expected: map[string]int{"#75200": 1, "#98922": 1, "#75201": 1, "#592": 1, "#5555": 1},
		},
		{
			message: `logictestccl: skip flaky TestCCLLogic/fakedist-metadata/partitioning_enum
Informs #75227
Epic CRDB-491
Release note (bug fix): Fixin the bug`,
			expected: map[string]int{},
		},
		{
			message: `lots of variations
fixes #74932; we were reading from the replicas map without...
Closes #74889.
Resolves #74482, #74784  #65117   #79299.
Fix:  #73834
epic: CRDB-9234.
epic CRDB-235, CRDB-40192;   DEVINF-392
Fixed:  #29833 example/repo#941
see also:  #9243
informs: #912,   #4729   #2911  cockroachdb/cockroach#2934
Release note (sql change): Import now checks readability...`,
			expected: map[string]int{"#74932": 1, "#74889": 1, "#74482": 1, "#74784": 1, "#65117": 1, "#79299": 1, "#73834": 1, "#29833": 1, "example/repo#941": 1},
		},
		{
			message: `lots of variations 2
Resolved: #4921
This comes w/ support for Applie Silicon Macs. Closes #72829.
This doesn't completely fix #71901 in that some...
      Fixes #491
Resolves #71614, Resolves #71607
Thereby, fixes #69765
Informs #69765 (point 4).
Fixes #65200. The last remaining 21.1 version (V21_1) can be removed as
Release note (build change): Upgrade to Go 1.17.6
Release note (ops change): Added a metric
Release notes (enterprise change): Client certificates may...`,
			expected: map[string]int{"#4921": 1, "#72829": 1, "#71901": 1, "#491": 1, "#71614": 1, "#71607": 1, "#69765": 1, "#65200": 1},
		},
		{
			message: `testing JIRA tickets
Resolved: doc-4321
This fixes everything. Closes CC-1234.
      Fixes CRDB-12345
Resolves crdb-23456, Resolves DEVINFHD-12345
Fixes #12345
Release notes (sql change): Excellent sql change...`,
			expected: map[string]int{"doc-4321": 1, "CC-1234": 1, "CRDB-12345": 1, "crdb-23456": 1, "DEVINFHD-12345": 1, "#12345": 1},
		},
		{
			message: `testing URL refs
Resolves: https://github.com/cockroachlabs/support/issues/1833
This fixes everything. Closes https://github.com/cockroachdb/cockroach/issues/83912.
Fix: https://cockroachlabs.atlassian.net/browse/DOC-9492
Release notes (sql change): Excellent sql change...`,
			expected: map[string]int{
				"https://github.com/cockroachlabs/support/issues/1833":  1,
				"https://github.com/cockroachdb/cockroach/issues/83912": 1,
				"https://cockroachlabs.atlassian.net/browse/DOC-9492":   1,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := extractFixIssueIDs(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractInformIssueIDs(t *testing.T) {
	testCases := []struct {
		message  string
		expected map[string]int
	}{
		{
			message: `logictestccl: skip flaky TestCCLLogic/fakedist-metadata/partitioning_enum
Informs #75227
Part of #45791
Epic CRDB-491
Fix:  #73834
Release note (bug fix): Fixin the bug`,
			expected: map[string]int{"#75227": 1, "#45791": 1},
		},
		{
			message: `lots of variations
Fixed:  #29833 example/repo#941
see also:  #9243
informs: #912,   #4729   #2911  cockroachdb/cockroach#2934
Informs #69765 (point 4).
This informs #59293 with these additions:
Release note (sql change): Import now checks readability...`,
			expected: map[string]int{"#9243": 1, "#912": 1, "#4729": 1, "#2911": 1, "cockroachdb/cockroach#2934": 1, "#69765": 1, "#59293": 1},
		},
		{
			message: `testing JIRA keys with varying cases
Fixed:  CRDB-12345 example/repo#941
informs: doc-1234, crdb-24680
Informs DEVINF-123, #69765 and part of DEVINF-3891
Release note (sql change): Something something something...`,
			expected: map[string]int{"doc-1234": 1, "DEVINF-123": 1, "#69765": 1, "crdb-24680": 1, "DEVINF-3891": 1},
		},
		{
			message: `testing URL refs
Fixed:  CRDB-12345 example/repo#941
informs: https://github.com/cockroachdb/cockroach/issues/8892, https://github.com/cockroachdb/pebble/issues/309
PART OF https://cockroachlabs.atlassian.net/browse/DOC-3891
Release note (sql change): Something something something...`,
			expected: map[string]int{
				"https://github.com/cockroachdb/cockroach/issues/8892": 1,
				"https://github.com/cockroachdb/pebble/issues/309":     1,
				"https://cockroachlabs.atlassian.net/browse/DOC-3891":  1,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := extractInformIssueIDs(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractEpicIDs(t *testing.T) {
	testCases := []struct {
		message  string
		expected map[string]int
	}{
		{
			message: `logictestccl: skip flaky TestCCLLogic/fakedist-metadata/partitioning_enum
Informs #75227
Epic CRDB-491
Fix:  #73834
Release note (bug fix): Fixin the bug`,
			expected: map[string]int{"CRDB-491": 1},
		},
		{
			message: `lots of variations
epic: CRDB-9234.
epic CRDB-235, CRDB-40192;   DEVINF-392	https://cockroachlabs.atlassian.net/browse/CRDB-97531
Epic doc-9238
Release note (sql change): Import now checks readability...`,
			expected: map[string]int{
				"CRDB-9234":  1,
				"CRDB-235":   1,
				"CRDB-40192": 1,
				"DEVINF-392": 1,
				"doc-9238":   1,
				"https://cockroachlabs.atlassian.net/browse/CRDB-97531": 1,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := extractEpicIDs(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractEpicNone(t *testing.T) {
	testCases := []struct {
		message  string
		expected bool
	}{
		{
			message: `logictestccl: skip flaky TestCCLLogic/fakedist-metadata/partitioning_enum
Epic CRDB-491
Fix:  #73834
Release note (bug fix): Fixin the bug`,
			expected: false,
		},
		{
			message: `lots of variations
epic: None
Release note (sql change): Import now checks readability...`,
			expected: true,
		},
		{
			message: `another test
Epic nONE
Release note (sql change): Import now checks readability...`,
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := containsEpicNone(tc.message)
			assert.Equal(t, tc.expected, result)
		})
	}
}
