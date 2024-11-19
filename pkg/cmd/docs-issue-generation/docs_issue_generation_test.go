// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestExtractPRNumberCommitFromDocsIssueBody(t *testing.T) {
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
			testName: "Result with regular URLs",
			body: `<p>Related PR: <a href="https://github.com/cockroachdb/cockroach/pull/108627" class="external-link" rel="nofollow noreferrer">https://github.com/cockroachdb/cockroach/pull/108627</a><br/>
Commit: <a href="https://github.com/cockroachdb/cockroach/commit/c5219e38b61d070bceb260d2c1b6d1f4c6ff5426" class="external-link" rel="nofollow noreferrer">https://github.com/cockroachdb/cockroach/commit/c5219e38b61d070bceb260d2c1b6d1f4c6ff5426</a><br/>
Epic: none</p>

<p>&#8212;</p>

<p>Release note (ops change): BACKUP now skips contacting the ranges for tables on which exclude_data_from_backup is set, and can thus succeed even if an excluded table is unavailable.<br/>
Epic: none.</p>

<p>Jira Issue: 
    <span class="jira-issue-macro" data-jira-key="DOC-8571" >
                <a href="https://cockroachlabs.atlassian.net/browse/DOC-8571" class="jira-issue-macro-key issue-link"  title="PR #108627 - backupccl: skip backing up excluded spans" >
            <img class="icon" src="https://cockroachlabs.atlassian.net/rest/api/2/universal_avatar/view/type/issuetype/avatar/10568?size=medium" />
            DOC-8571
        </a>
                                                    <span class="aui-lozenge aui-lozenge-subtle aui-lozenge-current jira-macro-single-issue-export-pdf">In Review/Testing</span>
            </span>
</p>`,
			result: struct {
				prNumber  int
				commitSha string
				err       error
			}{
				prNumber:  108627,
				commitSha: "c5219e38b61d070bceb260d2c1b6d1f4c6ff5426",
				err:       nil,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			prNumber, commitSha, err := extractPRNumberCommitFromDocsIssueBody(tc.body)
			result := struct {
				prNumber  int
				commitSha string
				err       error
			}{
				prNumber:  prNumber,
				commitSha: commitSha,
				err:       err,
			}
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
			cockroachPRs: []cockroachPR{
				{
					Title:       "release-22.2: clusterversion: allow forcing release binary to dev version",
					Number:      91345,
					Body:        "Backport 1/1 commits from #90002, using the simplifications from #91344\r\n\r\n/cc @cockroachdb/release\r\n\r\n---\r\n\r\nPreviously it was impossible to start a release binary that supported up to say, 23.1, in a cluster where the cluster version was in the 'development' range (+1m). While this was somewhat intentional -- to mark a dev cluster as dev forever -- we still want the option to try to run release binaries in that cluster.\r\n\r\nThe new environment variable COCKROACH_FORCE_DEV_VERSION will cause a binary to identify itself as development and offset its version even if it is a release binary.\r\n\r\nEpic: none.\r\n\r\nRelease note (ops change): Release version binaries can now be instructed via the enviroment variable COCKROACH_FORCE_DEV_VERSION to override their cluster version support to match that of develeopment builds, which can allow a release binary to be started in a cluster that is run or has previously run a development build.\r\n\r\nRelease justification: bug fix in new functionality.",
					BaseRefName: "release-22.2",
					Commits: []cockroachCommit{
						{
							Sha:             "8dc44d23bb7e0688cd435b6f7908fab615f1aa39",
							MessageHeadline: "clusterversion: allow forcing release binary to dev version",
							MessageBody:     "Previously it was impossible to start a release binary that supported up\nto say, 23.1, in a cluster where the cluster version was in the 'development'\nrange (+1m). While this was somewhat intentional -- to mark a dev cluster as dev\nforever -- we still want the option to try to run release binaries in that cluster.\n\nThe new environment variable COCKROACH_FORCE_DEV_VERSION will cause a binary to\nidentify itself as development and offset its version even if it is a release binary.\n\nEpic: none.\n\nRelease note (ops change): Release version binaries can now be instructed via the enviroment\nvariable COCKROACH_FORCE_DEV_VERSION to override their cluster version support to match that\nof develeopment builds, which can allow a release binary to be started in a cluster that is\nrun or has previously run a development build.",
						},
					},
				},
			},
			docsIssues: []docsIssue{
				{
					Fields: docsIssueFields{
						IssueType: jiraFieldId{Id: "10084"},
						Project:   jiraFieldId{Id: "10047"},
						Summary:   "PR #91345 - clusterversion: allow forcing release binary to dev version",
						Reporter:  jiraFieldId{Id: "712020:f8672db2-443f-4232-b01a-f97746f89805"},
						Description: adfRoot{
							Version: 1,
							Type:    "doc",
							Content: []adfNode{
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Related PR: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/pull/91345",
											Marks: []adfMark{
												{
													Type: "link",
													Attrs: map[string]string{
														"href": "https://github.com/cockroachdb/cockroach/pull/91345",
													},
												},
											},
										},
										{
											Type: "hardBreak",
											Text: "",
										},
										{
											Type: "text",
											Text: "Commit: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/commit/8dc44d23bb7e0688cd435b6f7908fab615f1aa39",
											Marks: []adfMark{
												{
													Type: "link",
													Attrs: map[string]string{
														"href": "https://github.com/cockroachdb/cockroach/commit/8dc44d23bb7e0688cd435b6f7908fab615f1aa39",
													},
												},
											},
										},
										{
											Type: "hardBreak",
										},
										{
											Type: "text",
											Text: "Epic: none",
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "---",
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Release note (ops change): Release version binaries can now be instructed via the enviroment\nvariable COCKROACH_FORCE_DEV_VERSION to override their cluster version support to match that\nof develeopment builds, which can allow a release binary to be started in a cluster that is\nrun or has previously run a development build.",
										},
									},
								},
							},
						},
						DocType: jiraFieldId{Id: "10779"},
						FixVersions: []jiraFieldId{
							{
								Id: "10186",
							},
						},
						EpicLink:               "",
						ProductChangePrNumber:  "91345",
						ProductChangeCommitSHA: "8dc44d23bb7e0688cd435b6f7908fab615f1aa39",
					},
				},
			},
		},
		{
			testName: "Multiple PRs",
			cockroachPRs: []cockroachPR{
				{
					Title:       "release-22.1: ui: update filter labels",
					Number:      91294,
					Body:        "Backport 1/1 commits from #88078.\r\n\r\n/cc @cockroachdb/release\r\n\r\n---\r\n\r\nUpdate filter label from \"App\" to \"Application Name\" on SQL Activity.\r\n\r\nFixes #87960\r\n\r\n<img width=\"467\" alt=\"Screen Shot 2022-09-16 at 6 40 51 PM\" src=\"https://user-images.githubusercontent.com/1017486/190827281-36a9c6df-3e16-4689-bcae-6649b1c2ff86.png\">\r\n\r\n\r\nRelease note (ui change): Update filter labels from \"App\" to \"Application Name\" and from \"Username\" to \"User Name\" on SQL Activity and Insights pages.\r\n\r\n---\r\n\r\nRelease justification: small change\r\n",
					BaseRefName: "release-22.1",
					Commits: []cockroachCommit{
						{
							Sha:             "8d15073f329cf8d72e09977b34a3b339d1436000",
							MessageHeadline: "ui: update filter labels",
							MessageBody:     "Update filter label from \"App\" to \"Application Name\"\nand \"Username\" to \"User Name\" on SQL Activity pages.\n\nFixes #87960\n\nRelease note (ui change): Update filter labels from\n\"App\" to \"Application Name\" and from \"Username\" to\n\"User Name\" on SQL Activity pages.",
						},
					},
				},
				{
					Title:       "release-22.2.0: sql/ttl: rename num_active_ranges metrics",
					Number:      90381,
					Body:        "Backport 1/1 commits from #90175.\r\n\r\n/cc @cockroachdb/release\r\n\r\nRelease justification: metrics rename that should be done in a major release\r\n\r\n---\r\n\r\nfixes https://github.com/cockroachdb/cockroach/issues/90094\r\n\r\nRelease note (ops change): These TTL metrics have been renamed: \r\njobs.row_level_ttl.range_total_duration -> jobs.row_level_ttl.span_total_duration\r\njobs.row_level_ttl.num_active_ranges -> jobs.row_level_ttl.num_active_spans\r\n",
					BaseRefName: "release-22.2.0",
					Commits: []cockroachCommit{
						{
							Sha:             "1829a72664f28ddfa50324c9ff5352380029560b",
							MessageHeadline: "sql/ttl: rename num_active_ranges metrics",
							MessageBody:     "fixes https://github.com/cockroachdb/cockroach/issues/90094\n\nRelease note (ops change): These TTL metrics have been renamed:\njobs.row_level_ttl.range_total_duration -> jobs.row_level_ttl.span_total_duration\njobs.row_level_ttl.num_active_ranges -> jobs.row_level_ttl.num_active_spans",
						},
					},
				},
				{
					Title:       "release-22.2: opt/props: shallow-copy props.Histogram when applying selectivity",
					Number:      89957,
					Body:        "Backport 2/2 commits from #88526 on behalf of @michae2.\r\n\r\n/cc @cockroachdb/release\r\n\r\n----\r\n\r\n**opt/props: add benchmark for props.Histogram**\r\n\r\nAdd a benchmark that measures various common props.Histogram operations.\r\n\r\nRelease note: None\r\n\r\n**opt/props: shallow-copy props.Histogram when applying selectivity**\r\n\r\n`pkg/sql/opt/props.(*Histogram).copy` showed up as a heavy allocator in\r\na recent customer OOM. The only caller of this function is\r\n`Histogram.ApplySelectivity` which deep-copies the histogram before\r\nadjusting each bucket's `NumEq`, `NumRange`, and `DistinctRange` by the\r\ngiven selectivity.\r\n\r\nInstead of deep-copying the histogram, we can change `ApplySelectivity`\r\nto shallow-copy the histogram and remember the current selectivity. We\r\ncan then wait to adjust counts in each bucket by the selectivity until\r\nsomeone actually calls `ValuesCount` or `DistinctValuesCount`.\r\n\r\nThis doesn't eliminate all copying of histograms. We're still doing some\r\ncopying in `Filter` when applying a constraint to the histogram.\r\n\r\nFixes: #89941\r\n\r\nRelease note (performance improvement): The optimizer now does less\r\ncopying of histograms while planning queries, which will reduce memory\r\npressure a little.\r\n\r\n----\r\n\r\nRelease justification: Low risk, high reward change to existing functionality.",
					BaseRefName: "release-22.2",
					Commits: []cockroachCommit{
						{
							Sha:             "43de8ff30e3e6e1d9b2272ed4f62c543dc0a037c",
							MessageHeadline: "opt/props: shallow-copy props.Histogram when applying selectivity",
							MessageBody:     "`pkg/sql/opt/props.(*Histogram).copy` showed up as a heavy allocator in\na recent customer OOM. The only caller of this function is\n`Histogram.ApplySelectivity` which deep-copies the histogram before\nadjusting each bucket's `NumEq`, `NumRange`, and `DistinctRange` by the\ngiven selectivity.\n\nInstead of deep-copying the histogram, we can change `ApplySelectivity`\nto shallow-copy the histogram and remember the current selectivity. We\ncan then wait to adjust counts in each bucket by the selectivity until\nsomeone actually calls `ValuesCount` or `DistinctValuesCount`.\n\nThis doesn't eliminate all copying of histograms. We're still doing some\ncopying in `Filter` when applying a constraint to the histogram.\n\nFixes: #89941\n\nRelease note (performance improvement): The optimizer now does less\ncopying of histograms while planning queries, which will reduce memory\npressure a little.",
						},
					},
				},
			},
			docsIssues: []docsIssue{
				{
					Fields: docsIssueFields{
						IssueType: jiraFieldId{Id: "10084"},
						Project:   jiraFieldId{Id: "10047"},
						Summary:   "PR #91294 - ui: update filter labels",
						Reporter:  jiraFieldId{Id: "712020:f8672db2-443f-4232-b01a-f97746f89805"},
						Description: adfRoot{
							Version: 1,
							Type:    "doc",
							Content: []adfNode{
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Related PR: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/pull/91294",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/91294"},
												},
											},
										},
										{Type: "hardBreak"},
										{
											Type: "text",
											Text: "Commit: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/commit/8d15073f329cf8d72e09977b34a3b339d1436000",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/8d15073f329cf8d72e09977b34a3b339d1436000"},
												},
											},
										},
										{Type: "hardBreak"},
										{
											Type: "text",
											Text: "Fixes:",
										},
										{
											Type: "text",
											Text: " ",
										},
										{
											Type: "text",
											Text: "CRDB-19614",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://cockroachlabs.atlassian.net/browse/CRDB-19614"},
												},
											},
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "---",
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Release note (ui change): Update filter labels from\n\"App\" to \"Application Name\" and from \"Username\" to\n\"User Name\" on SQL Activity pages.",
										},
									},
								},
							},
						},
						DocType: jiraFieldId{Id: "10779"},
						FixVersions: []jiraFieldId{
							{
								Id: "10185",
							},
						},
						EpicLink:               "",
						ProductChangePrNumber:  "91294",
						ProductChangeCommitSHA: "8d15073f329cf8d72e09977b34a3b339d1436000",
					},
				},
				{
					Fields: docsIssueFields{
						IssueType: jiraFieldId{Id: "10084"},
						Project:   jiraFieldId{Id: "10047"},
						Summary:   "PR #90381 - sql/ttl: rename num_active_ranges metrics",
						Reporter:  jiraFieldId{Id: "712020:f8672db2-443f-4232-b01a-f97746f89805"},
						Description: adfRoot{
							Version: 1,
							Type:    "doc",
							Content: []adfNode{
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Related PR: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/pull/90381",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/90381"},
												},
											},
										},
										{Type: "hardBreak"},
										{
											Type: "text",
											Text: "Commit: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/commit/1829a72664f28ddfa50324c9ff5352380029560b",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/1829a72664f28ddfa50324c9ff5352380029560b"},
												},
											},
										},
										{Type: "hardBreak"},
										{
											Type: "text",
											Text: "Fixes:",
										},
										{
											Type: "text",
											Text: " ",
										},
										{
											Type: "text",
											Text: "CRDB-20636",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://cockroachlabs.atlassian.net/browse/CRDB-20636"},
												},
											},
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "---",
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Release note (ops change): These TTL metrics have been renamed:\njobs.row_level_ttl.range_total_duration -> jobs.row_level_ttl.span_total_duration\njobs.row_level_ttl.num_active_ranges -> jobs.row_level_ttl.num_active_spans",
										},
									},
								},
							},
						},

						DocType: jiraFieldId{Id: "10779"},
						FixVersions: []jiraFieldId{
							{
								Id: "10186",
							},
						},
						EpicLink:               "",
						ProductChangePrNumber:  "90381",
						ProductChangeCommitSHA: "1829a72664f28ddfa50324c9ff5352380029560b",
					},
				},
				{
					Fields: docsIssueFields{
						IssueType: jiraFieldId{Id: "10084"},
						Project:   jiraFieldId{Id: "10047"},
						Summary:   "PR #89957 - opt/props: shallow-copy props.Histogram when applying selectivity",
						Reporter:  jiraFieldId{Id: "712020:f8672db2-443f-4232-b01a-f97746f89805"},
						Description: adfRoot{
							Version: 1,
							Type:    "doc",
							Content: []adfNode{
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Related PR: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/pull/89957",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/89957"},
												},
											},
										},
										{Type: "hardBreak"},
										{
											Type: "text",
											Text: "Commit: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/commit/43de8ff30e3e6e1d9b2272ed4f62c543dc0a037c",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/43de8ff30e3e6e1d9b2272ed4f62c543dc0a037c"},
												},
											},
										},
										{Type: "hardBreak"},
										{
											Type: "text",
											Text: "Fixes:",
										},
										{
											Type: "text",
											Text: " ",
										},
										{
											Type: "text",
											Text: "CRDB-20505",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://cockroachlabs.atlassian.net/browse/CRDB-20505"},
												},
											},
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "---",
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Release note (performance improvement): The optimizer now does less\ncopying of histograms while planning queries, which will reduce memory\npressure a little.",
										},
									},
								},
							},
						},
						DocType: jiraFieldId{Id: "10779"},
						FixVersions: []jiraFieldId{
							{
								Id: "10186",
							},
						},
						EpicLink:               "",
						ProductChangePrNumber:  "89957",
						ProductChangeCommitSHA: "43de8ff30e3e6e1d9b2272ed4f62c543dc0a037c",
					},
				},
			},
		},
		{
			testName: "Epic none",
			cockroachPRs: []cockroachPR{
				{
					Title:       "Epic none PR",
					Number:      123456,
					Body:        "This fixes a thing.\n\nEpic: none\nRelease note (enterprise change): enterprise changes",
					BaseRefName: "master",
					Commits: []cockroachCommit{
						{
							Sha:             "690da3e2ac3b1b7268accfb1dcbe5464e948e9d1",
							MessageHeadline: "Epic none PR",
							MessageBody:     "Epic: none\nRelease note (enterprise change): enterprise changes",
						},
					},
				},
			},
			docsIssues: []docsIssue{
				{
					Fields: docsIssueFields{
						IssueType: jiraFieldId{Id: "10084"},
						Project:   jiraFieldId{Id: "10047"},
						Summary:   "PR #123456 - Epic none PR",
						Reporter:  jiraFieldId{Id: "712020:f8672db2-443f-4232-b01a-f97746f89805"},
						Description: adfRoot{
							Version: 1,
							Type:    "doc",
							Content: []adfNode{
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Related PR: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/pull/123456",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/123456"},
												},
											},
										},
										{
											Type: "hardBreak",
										},
										{
											Type: "text",
											Text: "Commit: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/commit/690da3e2ac3b1b7268accfb1dcbe5464e948e9d1",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/690da3e2ac3b1b7268accfb1dcbe5464e948e9d1"},
												},
											},
										},
										{
											Type: "hardBreak",
										},
										{
											Type: "text",
											Text: "Epic: none",
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "---",
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Release note (enterprise change): enterprise changes",
										},
									},
								},
							},
						},
						DocType: jiraFieldId{Id: "10779"},
						FixVersions: []jiraFieldId{
							{
								Id: "10365",
							},
						},
						EpicLink:               "",
						ProductChangePrNumber:  "123456",
						ProductChangeCommitSHA: "690da3e2ac3b1b7268accfb1dcbe5464e948e9d1",
					},
				},
			},
		},
		{
			testName: "Epic extraction",
			cockroachPRs: []cockroachPR{
				{
					Title:       "Epic extraction PR",
					Number:      123456,
					Body:        "This fixes another thing.\n\nEpic: CRDB-31495\nRelease note (cli change): cli changes",
					BaseRefName: "master",
					Commits: []cockroachCommit{
						{
							Sha:             "aaada3e2ac3b1b7268accfb1dcbe5464e948e9d1",
							MessageHeadline: "Epic extraction PR",
							MessageBody:     "Epic: CRDB-31495\nRelease note (cli change): cli changes",
						},
					},
				},
			},
			docsIssues: []docsIssue{
				{
					Fields: docsIssueFields{
						IssueType: jiraFieldId{Id: "10084"},
						Project:   jiraFieldId{Id: "10047"},
						Summary:   "PR #123456 - Epic extraction PR",
						Reporter:  jiraFieldId{Id: "712020:f8672db2-443f-4232-b01a-f97746f89805"},
						Description: adfRoot{
							Version: 1,
							Type:    "doc",
							Content: []adfNode{
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Related PR: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/pull/123456",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/123456"},
												},
											},
										},
										{Type: "hardBreak"},
										{
											Type: "text",
											Text: "Commit: ",
										},
										{
											Type: "text",
											Text: "https://github.com/cockroachdb/cockroach/commit/aaada3e2ac3b1b7268accfb1dcbe5464e948e9d1",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/aaada3e2ac3b1b7268accfb1dcbe5464e948e9d1"},
												},
											},
										},
										{Type: "hardBreak"},
										{
											Type: "text",
											Text: "Epic:",
										},
										{
											Type: "text",
											Text: " ",
										},
										{
											Type: "text",
											Text: "CRDB-31495",
											Marks: []adfMark{
												{
													Type:  "link",
													Attrs: map[string]string{"href": "https://cockroachlabs.atlassian.net/browse/CRDB-31495"},
												},
											},
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "---",
										},
									},
								},
								{
									Type: "paragraph",
									Content: []adfNode{
										{
											Type: "text",
											Text: "Release note (cli change): cli changes",
										},
									},
								},
							},
						},
						DocType: jiraFieldId{Id: "10779"},
						FixVersions: []jiraFieldId{
							{
								Id: "10365",
							},
						},
						EpicLink:               "CRDB-31495",
						ProductChangePrNumber:  "123456",
						ProductChangeCommitSHA: "aaada3e2ac3b1b7268accfb1dcbe5464e948e9d1",
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			defer testutils.TestingHook(&getJiraIssueFromGitHubIssue, func(org, repo string, issue int) (string, error) {
				// getJiraIssueFromGitHubIssue requires a network call to the GitHub GraphQL API to calculate the Jira issue given
				// a GitHub org/repo/issue. To help eliminate the need of a network call and minimize the chances of this test
				// flaking, we define a pre-built map that is used to mock the network call and allow the tests to run as expected.
				var ghJiraIssueMap = make(map[string]map[string]map[int]string)
				ghJiraIssueMap["cockroachdb"] = make(map[string]map[int]string)
				ghJiraIssueMap["cockroachdb"]["cockroach"] = make(map[int]string)
				ghJiraIssueMap["cockroachdb"]["cockroach"][87960] = "CRDB-19614"
				ghJiraIssueMap["cockroachdb"]["cockroach"][90094] = "CRDB-20636"
				ghJiraIssueMap["cockroachdb"]["cockroach"][89941] = "CRDB-20505"
				ghJiraIssueMap["cockroachdb"]["cockroach"][123456] = "CRDB-31495"
				return ghJiraIssueMap[org][repo][issue], nil
			})()
			defer testutils.TestingHook(&getJiraIssueCreateMeta, func() (jiraIssueCreateMeta, error) {
				result := jiraIssueCreateMeta{
					Projects: []struct {
						Issuetypes []struct {
							Fields struct {
								Issuetype struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type   string "json:\"type\""
										System string "json:\"system\""
									} "json:\"schema\""
									Name            string        "json:\"name\""
									Key             string        "json:\"key\""
									HasDefaultValue bool          "json:\"hasDefaultValue\""
									Operations      []interface{} "json:\"operations\""
									AllowedValues   []struct {
										Self           string "json:\"self\""
										Id             string "json:\"id\""
										Description    string "json:\"description\""
										IconUrl        string "json:\"iconUrl\""
										Name           string "json:\"name\""
										Subtask        bool   "json:\"subtask\""
										AvatarId       int    "json:\"avatarId\""
										HierarchyLevel int    "json:\"hierarchyLevel\""
									} "json:\"allowedValues\""
								} "json:\"issuetype\""
								Description struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type   string "json:\"type\""
										System string "json:\"system\""
									} "json:\"schema\""
									Name            string   "json:\"name\""
									Key             string   "json:\"key\""
									HasDefaultValue bool     "json:\"hasDefaultValue\""
									Operations      []string "json:\"operations\""
								} "json:\"description\""
								Project struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type   string "json:\"type\""
										System string "json:\"system\""
									} "json:\"schema\""
									Name            string   "json:\"name\""
									Key             string   "json:\"key\""
									HasDefaultValue bool     "json:\"hasDefaultValue\""
									Operations      []string "json:\"operations\""
									AllowedValues   []struct {
										Self           string "json:\"self\""
										Id             string "json:\"id\""
										Key            string "json:\"key\""
										Name           string "json:\"name\""
										ProjectTypeKey string "json:\"projectTypeKey\""
										Simplified     bool   "json:\"simplified\""
										AvatarUrls     struct {
											X48 string "json:\"48x48\""
											X24 string "json:\"24x24\""
											X16 string "json:\"16x16\""
											X32 string "json:\"32x32\""
										} "json:\"avatarUrls\""
										ProjectCategory struct {
											Self        string "json:\"self\""
											Id          string "json:\"id\""
											Description string "json:\"description\""
											Name        string "json:\"name\""
										} "json:\"projectCategory\""
									} "json:\"allowedValues\""
								} "json:\"project\""
								DocType struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type     string "json:\"type\""
										Custom   string "json:\"custom\""
										CustomId int    "json:\"customId\""
									} "json:\"schema\""
									Name            string                                   "json:\"name\""
									Key             string                                   "json:\"key\""
									HasDefaultValue bool                                     "json:\"hasDefaultValue\""
									Operations      []string                                 "json:\"operations\""
									AllowedValues   []jiraCreateIssueMetaDocTypeAllowedValue "json:\"allowedValues\""
								} "json:\"customfield_10175\""
								FixVersions struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type   string "json:\"type\""
										Items  string "json:\"items\""
										System string "json:\"system\""
									} "json:\"schema\""
									Name            string                                       "json:\"name\""
									Key             string                                       "json:\"key\""
									HasDefaultValue bool                                         "json:\"hasDefaultValue\""
									Operations      []string                                     "json:\"operations\""
									AllowedValues   []jiraCreateIssueMetaFixVersionsAllowedValue "json:\"allowedValues\""
								} "json:\"fixVersions\""
								EpicLink struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type     string "json:\"type\""
										Custom   string "json:\"custom\""
										CustomId int    "json:\"customId\""
									} "json:\"schema\""
									Name            string   "json:\"name\""
									Key             string   "json:\"key\""
									HasDefaultValue bool     "json:\"hasDefaultValue\""
									Operations      []string "json:\"operations\""
								} "json:\"customfield_10014\""
								Summary struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type   string "json:\"type\""
										System string "json:\"system\""
									} "json:\"schema\""
									Name            string   "json:\"name\""
									Key             string   "json:\"key\""
									HasDefaultValue bool     "json:\"hasDefaultValue\""
									Operations      []string "json:\"operations\""
								} "json:\"summary\""
								Reporter struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type   string "json:\"type\""
										System string "json:\"system\""
									} "json:\"schema\""
									Name            string   "json:\"name\""
									Key             string   "json:\"key\""
									AutoCompleteUrl string   "json:\"autoCompleteUrl\""
									HasDefaultValue bool     "json:\"hasDefaultValue\""
									Operations      []string "json:\"operations\""
								} "json:\"reporter\""
								ProductChangePRNumber struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type     string "json:\"type\""
										Custom   string "json:\"custom\""
										CustomId int    "json:\"customId\""
									} "json:\"schema\""
									Name            string   "json:\"name\""
									Key             string   "json:\"key\""
									HasDefaultValue bool     "json:\"hasDefaultValue\""
									Operations      []string "json:\"operations\""
								} "json:\"customfield_10435\""
								ProductChangeCommitSHA struct {
									Required bool "json:\"required\""
									Schema   struct {
										Type     string "json:\"type\""
										Custom   string "json:\"custom\""
										CustomId int    "json:\"customId\""
									} "json:\"schema\""
									Name            string   "json:\"name\""
									Key             string   "json:\"key\""
									HasDefaultValue bool     "json:\"hasDefaultValue\""
									Operations      []string "json:\"operations\""
								} "json:\"customfield_10436\""
							} "json:\"fields\""
						} "json:\"issuetypes\""
					}{
						{
							Issuetypes: []struct {
								Fields struct {
									Issuetype struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type   string "json:\"type\""
											System string "json:\"system\""
										} "json:\"schema\""
										Name            string        "json:\"name\""
										Key             string        "json:\"key\""
										HasDefaultValue bool          "json:\"hasDefaultValue\""
										Operations      []interface{} "json:\"operations\""
										AllowedValues   []struct {
											Self           string "json:\"self\""
											Id             string "json:\"id\""
											Description    string "json:\"description\""
											IconUrl        string "json:\"iconUrl\""
											Name           string "json:\"name\""
											Subtask        bool   "json:\"subtask\""
											AvatarId       int    "json:\"avatarId\""
											HierarchyLevel int    "json:\"hierarchyLevel\""
										} "json:\"allowedValues\""
									} "json:\"issuetype\""
									Description struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type   string "json:\"type\""
											System string "json:\"system\""
										} "json:\"schema\""
										Name            string   "json:\"name\""
										Key             string   "json:\"key\""
										HasDefaultValue bool     "json:\"hasDefaultValue\""
										Operations      []string "json:\"operations\""
									} "json:\"description\""
									Project struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type   string "json:\"type\""
											System string "json:\"system\""
										} "json:\"schema\""
										Name            string   "json:\"name\""
										Key             string   "json:\"key\""
										HasDefaultValue bool     "json:\"hasDefaultValue\""
										Operations      []string "json:\"operations\""
										AllowedValues   []struct {
											Self           string "json:\"self\""
											Id             string "json:\"id\""
											Key            string "json:\"key\""
											Name           string "json:\"name\""
											ProjectTypeKey string "json:\"projectTypeKey\""
											Simplified     bool   "json:\"simplified\""
											AvatarUrls     struct {
												X48 string "json:\"48x48\""
												X24 string "json:\"24x24\""
												X16 string "json:\"16x16\""
												X32 string "json:\"32x32\""
											} "json:\"avatarUrls\""
											ProjectCategory struct {
												Self        string "json:\"self\""
												Id          string "json:\"id\""
												Description string "json:\"description\""
												Name        string "json:\"name\""
											} "json:\"projectCategory\""
										} "json:\"allowedValues\""
									} "json:\"project\""
									DocType struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type     string "json:\"type\""
											Custom   string "json:\"custom\""
											CustomId int    "json:\"customId\""
										} "json:\"schema\""
										Name            string                                   "json:\"name\""
										Key             string                                   "json:\"key\""
										HasDefaultValue bool                                     "json:\"hasDefaultValue\""
										Operations      []string                                 "json:\"operations\""
										AllowedValues   []jiraCreateIssueMetaDocTypeAllowedValue "json:\"allowedValues\""
									} "json:\"customfield_10175\""
									FixVersions struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type   string "json:\"type\""
											Items  string "json:\"items\""
											System string "json:\"system\""
										} "json:\"schema\""
										Name            string                                       "json:\"name\""
										Key             string                                       "json:\"key\""
										HasDefaultValue bool                                         "json:\"hasDefaultValue\""
										Operations      []string                                     "json:\"operations\""
										AllowedValues   []jiraCreateIssueMetaFixVersionsAllowedValue "json:\"allowedValues\""
									} "json:\"fixVersions\""
									EpicLink struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type     string "json:\"type\""
											Custom   string "json:\"custom\""
											CustomId int    "json:\"customId\""
										} "json:\"schema\""
										Name            string   "json:\"name\""
										Key             string   "json:\"key\""
										HasDefaultValue bool     "json:\"hasDefaultValue\""
										Operations      []string "json:\"operations\""
									} "json:\"customfield_10014\""
									Summary struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type   string "json:\"type\""
											System string "json:\"system\""
										} "json:\"schema\""
										Name            string   "json:\"name\""
										Key             string   "json:\"key\""
										HasDefaultValue bool     "json:\"hasDefaultValue\""
										Operations      []string "json:\"operations\""
									} "json:\"summary\""
									Reporter struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type   string "json:\"type\""
											System string "json:\"system\""
										} "json:\"schema\""
										Name            string   "json:\"name\""
										Key             string   "json:\"key\""
										AutoCompleteUrl string   "json:\"autoCompleteUrl\""
										HasDefaultValue bool     "json:\"hasDefaultValue\""
										Operations      []string "json:\"operations\""
									} "json:\"reporter\""
									ProductChangePRNumber struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type     string "json:\"type\""
											Custom   string "json:\"custom\""
											CustomId int    "json:\"customId\""
										} "json:\"schema\""
										Name            string   "json:\"name\""
										Key             string   "json:\"key\""
										HasDefaultValue bool     "json:\"hasDefaultValue\""
										Operations      []string "json:\"operations\""
									} "json:\"customfield_10435\""
									ProductChangeCommitSHA struct {
										Required bool "json:\"required\""
										Schema   struct {
											Type     string "json:\"type\""
											Custom   string "json:\"custom\""
											CustomId int    "json:\"customId\""
										} "json:\"schema\""
										Name            string   "json:\"name\""
										Key             string   "json:\"key\""
										HasDefaultValue bool     "json:\"hasDefaultValue\""
										Operations      []string "json:\"operations\""
									} "json:\"customfield_10436\""
								} "json:\"fields\""
							}{
								{
									Fields: struct {
										Issuetype struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string        "json:\"name\""
											Key             string        "json:\"key\""
											HasDefaultValue bool          "json:\"hasDefaultValue\""
											Operations      []interface{} "json:\"operations\""
											AllowedValues   []struct {
												Self           string "json:\"self\""
												Id             string "json:\"id\""
												Description    string "json:\"description\""
												IconUrl        string "json:\"iconUrl\""
												Name           string "json:\"name\""
												Subtask        bool   "json:\"subtask\""
												AvatarId       int    "json:\"avatarId\""
												HierarchyLevel int    "json:\"hierarchyLevel\""
											} "json:\"allowedValues\""
										} "json:\"issuetype\""
										Description struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										} "json:\"description\""
										Project struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
											AllowedValues   []struct {
												Self           string "json:\"self\""
												Id             string "json:\"id\""
												Key            string "json:\"key\""
												Name           string "json:\"name\""
												ProjectTypeKey string "json:\"projectTypeKey\""
												Simplified     bool   "json:\"simplified\""
												AvatarUrls     struct {
													X48 string "json:\"48x48\""
													X24 string "json:\"24x24\""
													X16 string "json:\"16x16\""
													X32 string "json:\"32x32\""
												} "json:\"avatarUrls\""
												ProjectCategory struct {
													Self        string "json:\"self\""
													Id          string "json:\"id\""
													Description string "json:\"description\""
													Name        string "json:\"name\""
												} "json:\"projectCategory\""
											} "json:\"allowedValues\""
										} "json:\"project\""
										DocType struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											} "json:\"schema\""
											Name            string                                   "json:\"name\""
											Key             string                                   "json:\"key\""
											HasDefaultValue bool                                     "json:\"hasDefaultValue\""
											Operations      []string                                 "json:\"operations\""
											AllowedValues   []jiraCreateIssueMetaDocTypeAllowedValue "json:\"allowedValues\""
										} "json:\"customfield_10175\""
										FixVersions struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												Items  string "json:\"items\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string                                       "json:\"name\""
											Key             string                                       "json:\"key\""
											HasDefaultValue bool                                         "json:\"hasDefaultValue\""
											Operations      []string                                     "json:\"operations\""
											AllowedValues   []jiraCreateIssueMetaFixVersionsAllowedValue "json:\"allowedValues\""
										} "json:\"fixVersions\""
										EpicLink struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										} "json:\"customfield_10014\""
										Summary struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										} "json:\"summary\""
										Reporter struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											AutoCompleteUrl string   "json:\"autoCompleteUrl\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										} "json:\"reporter\""
										ProductChangePRNumber struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										} "json:\"customfield_10435\""
										ProductChangeCommitSHA struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										} "json:\"customfield_10436\""
									}{
										Issuetype: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string        "json:\"name\""
											Key             string        "json:\"key\""
											HasDefaultValue bool          "json:\"hasDefaultValue\""
											Operations      []interface{} "json:\"operations\""
											AllowedValues   []struct {
												Self           string "json:\"self\""
												Id             string "json:\"id\""
												Description    string "json:\"description\""
												IconUrl        string "json:\"iconUrl\""
												Name           string "json:\"name\""
												Subtask        bool   "json:\"subtask\""
												AvatarId       int    "json:\"avatarId\""
												HierarchyLevel int    "json:\"hierarchyLevel\""
											} "json:\"allowedValues\""
										}{
											Required: true,
											Schema: struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											}{
												Type:   "issuetype",
												System: "issuetype",
											},
											Name:            "Issue Type",
											Key:             "issuetype",
											HasDefaultValue: false,
											Operations:      []interface{}{},
											AllowedValues: []struct {
												Self           string "json:\"self\""
												Id             string "json:\"id\""
												Description    string "json:\"description\""
												IconUrl        string "json:\"iconUrl\""
												Name           string "json:\"name\""
												Subtask        bool   "json:\"subtask\""
												AvatarId       int    "json:\"avatarId\""
												HierarchyLevel int    "json:\"hierarchyLevel\""
											}{
												{
													Self:           "https://cockroachlabs.atlassian.net/rest/api/3/issuetype/10084",
													Id:             "10084",
													Description:    "",
													IconUrl:        "https://cockroachlabs.atlassian.net/rest/api/2/universal_avatar/view/type/issuetype/avatar/10568?size=medium",
													Name:           "Docs",
													Subtask:        false,
													AvatarId:       10568,
													HierarchyLevel: 0,
												},
											},
										},
										Description: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										}{
											Required: false,
											Schema: struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											}{
												Type:   "string",
												System: "description",
											},
											Name:            "Description",
											Key:             "description",
											HasDefaultValue: false,
											Operations:      []string{"set"},
										},
										Project: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
											AllowedValues   []struct {
												Self           string "json:\"self\""
												Id             string "json:\"id\""
												Key            string "json:\"key\""
												Name           string "json:\"name\""
												ProjectTypeKey string "json:\"projectTypeKey\""
												Simplified     bool   "json:\"simplified\""
												AvatarUrls     struct {
													X48 string "json:\"48x48\""
													X24 string "json:\"24x24\""
													X16 string "json:\"16x16\""
													X32 string "json:\"32x32\""
												} "json:\"avatarUrls\""
												ProjectCategory struct {
													Self        string "json:\"self\""
													Id          string "json:\"id\""
													Description string "json:\"description\""
													Name        string "json:\"name\""
												} "json:\"projectCategory\""
											} "json:\"allowedValues\""
										}{
											Required: true,
											Schema: struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											}{
												Type:   "project",
												System: "project",
											},
											Name:            "Project",
											Key:             "project",
											HasDefaultValue: false,
											Operations:      []string{"set"},
											AllowedValues: []struct {
												Self           string "json:\"self\""
												Id             string "json:\"id\""
												Key            string "json:\"key\""
												Name           string "json:\"name\""
												ProjectTypeKey string "json:\"projectTypeKey\""
												Simplified     bool   "json:\"simplified\""
												AvatarUrls     struct {
													X48 string "json:\"48x48\""
													X24 string "json:\"24x24\""
													X16 string "json:\"16x16\""
													X32 string "json:\"32x32\""
												} "json:\"avatarUrls\""
												ProjectCategory struct {
													Self        string "json:\"self\""
													Id          string "json:\"id\""
													Description string "json:\"description\""
													Name        string "json:\"name\""
												} "json:\"projectCategory\""
											}{
												{
													Self:           "https://cockroachlabs.atlassian.net/rest/api/3/project/10047",
													Id:             "10047",
													Key:            "DOC",
													Name:           "Documentation",
													ProjectTypeKey: "software",
													Simplified:     false,
													AvatarUrls: struct {
														X48 string "json:\"48x48\""
														X24 string "json:\"24x24\""
														X16 string "json:\"16x16\""
														X32 string "json:\"32x32\""
													}{
														X48: "https://cockroachlabs.atlassian.net/rest/api/3/universal_avatar/view/type/project/avatar/10412",
														X24: "https://cockroachlabs.atlassian.net/rest/api/3/universal_avatar/view/type/project/avatar/10412?size=small",
														X16: "https://cockroachlabs.atlassian.net/rest/api/3/universal_avatar/view/type/project/avatar/10412?size=xsmall",
														X32: "https://cockroachlabs.atlassian.net/rest/api/3/universal_avatar/view/type/project/avatar/10412?size=medium",
													},
													ProjectCategory: struct {
														Self        string "json:\"self\""
														Id          string "json:\"id\""
														Description string "json:\"description\""
														Name        string "json:\"name\""
													}{
														Self:        "https://cockroachlabs.atlassian.net/rest/api/3/projectCategory/10001",
														Id:          "10001",
														Description: "",
														Name:        "Agile Delivery",
													},
												},
											},
										},
										DocType: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											} "json:\"schema\""
											Name            string                                   "json:\"name\""
											Key             string                                   "json:\"key\""
											HasDefaultValue bool                                     "json:\"hasDefaultValue\""
											Operations      []string                                 "json:\"operations\""
											AllowedValues   []jiraCreateIssueMetaDocTypeAllowedValue "json:\"allowedValues\""
										}{
											Required: false,
											Schema: struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											}{
												Type:     "option",
												Custom:   "com.atlassian.jira.plugin.system.customfieldtypes:select",
												CustomId: 10175,
											},
											Name:            "Doc Type",
											Key:             "customfield_10175",
											HasDefaultValue: false,
											Operations:      []string{"set"},
											AllowedValues: []jiraCreateIssueMetaDocTypeAllowedValue{
												{
													Self:  "https://cockroachlabs.atlassian.net/rest/api/3/customFieldOption/10779",
													Value: "Doc Bug",
													Id:    "10779",
												},
												{
													Self:  "https://cockroachlabs.atlassian.net/rest/api/3/customFieldOption/10780",
													Value: "Doc Improvement",
													Id:    "10780",
												},
												{
													Self:  "https://cockroachlabs.atlassian.net/rest/api/3/customFieldOption/11164",
													Value: "General admin",
													Id:    "11164",
												},
												{
													Self:  "https://cockroachlabs.atlassian.net/rest/api/3/customFieldOption/10782",
													Value: "Microcopy",
													Id:    "10782",
												},
												{
													Self:  "https://cockroachlabs.atlassian.net/rest/api/3/customFieldOption/10779",
													Value: "Product Change",
													Id:    "10779",
												},
												{
													Self:  "https://cockroachlabs.atlassian.net/rest/api/3/customFieldOption/11432",
													Value: "Release Notes",
													Id:    "11432",
												},
												{
													Self:  "https://cockroachlabs.atlassian.net/rest/api/3/customFieldOption/11493",
													Value: "Roadmap Feature",
													Id:    "11493",
												},
												{
													Self:  "https://cockroachlabs.atlassian.net/rest/api/3/customFieldOption/11569",
													Value: "Style",
													Id:    "11569",
												},
											},
										},
										FixVersions: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												Items  string "json:\"items\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string                                       "json:\"name\""
											Key             string                                       "json:\"key\""
											HasDefaultValue bool                                         "json:\"hasDefaultValue\""
											Operations      []string                                     "json:\"operations\""
											AllowedValues   []jiraCreateIssueMetaFixVersionsAllowedValue "json:\"allowedValues\""
										}{
											Required: false,
											Schema: struct {
												Type   string "json:\"type\""
												Items  string "json:\"items\""
												System string "json:\"system\""
											}{
												Type:   "array",
												Items:  "version",
												System: "fixVersions",
											},
											Name:            "Fix versions",
											Key:             "fixVersions",
											HasDefaultValue: false,
											Operations: []string{
												"set",
												"add",
												"remove",
											},
											AllowedValues: []jiraCreateIssueMetaFixVersionsAllowedValue{
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10181",
													Id:              "10181",
													Description:     "20.1 (Spring '20)",
													Name:            "20.1 (Spring '20)",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10182",
													Id:              "10182",
													Description:     "20.2 (Fall '20)",
													Name:            "20.2 (Fall '20)",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10183",
													Id:              "10183",
													Description:     "21.1 (Spring '21)",
													Name:            "21.1 (Spring '21)",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10184",
													Id:              "10184",
													Description:     "21.2 (Fall '21)",
													Name:            "21.2 (Fall '21)",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10185",
													Id:              "10185",
													Description:     "22.1 (Spring '22)",
													Name:            "22.1 (Spring '22)",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10186",
													Id:              "10186",
													Description:     "22.2 (Fall '22)",
													Name:            "22.2 (Fall '22)",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10187",
													Id:              "10187",
													Description:     "Later",
													Name:            "Later",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10213",
													Id:              "10213",
													Description:     "22.FEB",
													Name:            "22.FEB",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-02-01",
													ReleaseDate:     "2022-02-28",
													Overdue:         true,
													UserStartDate:   "31/Jan/22",
													UserReleaseDate: "27/Feb/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10214",
													Id:              "10214",
													Description:     "22.MAR",
													Name:            "22.MAR",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-03-01",
													ReleaseDate:     "2022-03-31",
													Overdue:         true,
													UserStartDate:   "28/Feb/22",
													UserReleaseDate: "30/Mar/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10215",
													Id:              "10215",
													Description:     "22.APR",
													Name:            "22.APR",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-04-01",
													ReleaseDate:     "2022-04-30",
													Overdue:         true,
													UserStartDate:   "31/Mar/22",
													UserReleaseDate: "29/Apr/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10216",
													Id:              "10216",
													Description:     "22.MAY",
													Name:            "22.MAY",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-05-01",
													ReleaseDate:     "2022-05-31",
													Overdue:         true,
													UserStartDate:   "30/Apr/22",
													UserReleaseDate: "30/May/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10217",
													Id:              "10217",
													Description:     "22.JUN",
													Name:            "22.JUN",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-06-01",
													ReleaseDate:     "2022-06-30",
													Overdue:         true,
													UserStartDate:   "31/May/22",
													UserReleaseDate: "29/Jun/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10218",
													Id:              "10218",
													Description:     "22.JUL",
													Name:            "22.JUL",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-07-01",
													ReleaseDate:     "2022-07-31",
													Overdue:         true,
													UserStartDate:   "30/Jun/22",
													UserReleaseDate: "30/Jul/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10219",
													Id:              "10219",
													Description:     "22.AUG",
													Name:            "22.AUG",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-08-01",
													ReleaseDate:     "2022-08-31",
													Overdue:         true,
													UserStartDate:   "31/Jul/22",
													UserReleaseDate: "30/Aug/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10220",
													Id:              "10220",
													Description:     "22.SEP",
													Name:            "22.SEP",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-09-01",
													ReleaseDate:     "2022-09-30",
													Overdue:         true,
													UserStartDate:   "31/Aug/22",
													UserReleaseDate: "29/Sep/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10221",
													Id:              "10221",
													Description:     "22.OCT",
													Name:            "22.OCT",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-10-01",
													ReleaseDate:     "2022-10-31",
													Overdue:         true,
													UserStartDate:   "30/Sep/22",
													UserReleaseDate: "30/Oct/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10222",
													Id:              "10222",
													Description:     "22.NOV",
													Name:            "22.NOV",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-11-01",
													ReleaseDate:     "2022-11-30",
													Overdue:         true,
													UserStartDate:   "31/Oct/22",
													UserReleaseDate: "29/Nov/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10223",
													Id:              "10223",
													Description:     "22.DEC",
													Name:            "22.DEC",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "2022-12-01",
													ReleaseDate:     "2022-12-31",
													Overdue:         true,
													UserStartDate:   "30/Nov/22",
													UserReleaseDate: "30/Dec/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10237",
													Id:              "10237",
													Description:     "23.1 (Spring 23)",
													Name:            "23.1 (Spring 23)",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10310",
													Id:              "10310",
													Description:     "22.DEC PCI SAQ-D",
													Name:            "22.DEC PCI SAQ-D",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "2022-12-30",
													Overdue:         true,
													UserStartDate:   "",
													UserReleaseDate: "29/Dec/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10311",
													Id:              "10311",
													Description:     "22.SEP PCI Customer Enablement",
													Name:            "22.SEP PCI Customer Enablement",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "2022-09-30",
													Overdue:         true,
													UserStartDate:   "",
													UserReleaseDate: "29/Sep/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10312",
													Id:              "10312",
													Description:     "22.JUL PCI SAQ-A",
													Name:            "22.JUL PCI SAQ-A",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "2022-07-29",
													Overdue:         true,
													UserStartDate:   "",
													UserReleaseDate: "28/Jul/22",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10328",
													Id:              "10328",
													Description:     "",
													Name:            "23.JAN",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10329",
													Id:              "10329",
													Description:     "",
													Name:            "23.FEB",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10330",
													Id:              "10330",
													Description:     "",
													Name:            "23.MAR",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10331",
													Id:              "10331",
													Description:     "",
													Name:            "23.APR",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10332",
													Id:              "10332",
													Description:     "",
													Name:            "23.MAY",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10333",
													Id:              "10333",
													Description:     "",
													Name:            "23.JUN",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
												{
													Self:            "https://cockroachlabs.atlassian.net/rest/api/3/version/10365",
													Id:              "10365",
													Description:     "",
													Name:            "23.2 (Fall 23)",
													Archived:        false,
													Released:        false,
													ProjectId:       10047,
													StartDate:       "",
													ReleaseDate:     "",
													Overdue:         false,
													UserStartDate:   "",
													UserReleaseDate: "",
												},
											},
										},
										EpicLink: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										}{
											Required: false,
											Schema: struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											}{
												Type:     "any",
												Custom:   "com.pyxis.greenhopper.jira:gh-epic-link",
												CustomId: 10014,
											},
											Name:            "Epic Link",
											Key:             "customfield_10014",
											HasDefaultValue: false,
											Operations:      []string{"set"},
										},
										Summary: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										}{
											Required: true,
											Schema: struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											}{
												Type:   "string",
												System: "summary",
											},
											Name:            "Summary",
											Key:             "summary",
											HasDefaultValue: false,
											Operations:      []string{"set"},
										},
										Reporter: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											AutoCompleteUrl string   "json:\"autoCompleteUrl\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										}{
											Required: true,
											Schema: struct {
												Type   string "json:\"type\""
												System string "json:\"system\""
											}{
												Type:   "user",
												System: "reporter",
											},
											Name:            "Reporter",
											Key:             "reporter",
											AutoCompleteUrl: "https://cockroachlabs.atlassian.net/rest/api/3/user/search?query=",
											HasDefaultValue: true,
											Operations:      []string{"set"},
										},
										ProductChangePRNumber: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										}{
											Required: false,
											Schema: struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											}{
												Type:     "string",
												Custom:   "com.atlassian.jira.plugin.system.customfieldtypes:textfield",
												CustomId: 10435,
											},
											Name:            "Product Change PR Number",
											Key:             "customfield_10435",
											HasDefaultValue: false,
											Operations:      []string{"set"},
										},
										ProductChangeCommitSHA: struct {
											Required bool "json:\"required\""
											Schema   struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											} "json:\"schema\""
											Name            string   "json:\"name\""
											Key             string   "json:\"key\""
											HasDefaultValue bool     "json:\"hasDefaultValue\""
											Operations      []string "json:\"operations\""
										}{
											Required: false,
											Schema: struct {
												Type     string "json:\"type\""
												Custom   string "json:\"custom\""
												CustomId int    "json:\"customId\""
											}{
												Type:     "string",
												Custom:   "com.atlassian.jira.plugin.system.customfieldtypes:textfield",
												CustomId: 10436,
											},
											Name:            "Product Change Commit SHA",
											Key:             "customfield_10436",
											HasDefaultValue: false,
											Operations:      []string{"set"},
										},
									},
								},
							},
						},
					},
				}
				return result, nil
			})()
			defer testutils.TestingHook(&searchCockroachReleaseBranches, func() ([]string, error) {
				result := []string{
					"release-23.1.10-rc",
					"release-23.1.9-rc.FROZEN",
					"release-23.1",
					"release-22.2",
					"release-22.2.0",
					"release-22.1",
					"release-21.2",
					"release-21.1",
					"release-20.2",
					"release-20.1",
					"release-19.2",
					"release-19.1",
					"release-2.1",
					"release-2.0",
					"release-1.1",
					"release-1.0",
				}
				return result, nil
			})()
			defer testutils.TestingHook(&getValidEpicRef, func(issueKey string) (bool, string, error) {
				var epicMap = make(map[string]struct {
					IsEpic  bool
					EpicKey string
				})
				epicMap["CRDB-31495"] = struct {
					IsEpic  bool
					EpicKey string
				}{IsEpic: true, EpicKey: "CRDB-31495"}
				return epicMap[issueKey].IsEpic, epicMap[issueKey].EpicKey, nil
			})()
			result, _ := constructDocsIssues(tc.cockroachPRs)
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
		rns           []adfRoot
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
			rns: []adfRoot{
				{
					Version: 1,
					Type:    "doc",
					Content: []adfNode{
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type: "text",
									Text: "Related PR: ",
								},
								{
									Type: "text",
									Text: "https://github.com/cockroachdb/cockroach/pull/79069",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/79069"},
										},
									},
								},
								{Type: "hardBreak"},
								{
									Type: "text",
									Text: "Commit: ",
								},
								{
									Type: "text",
									Text: "https://github.com/cockroachdb/cockroach/commit/5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9"},
										},
									},
								},
								{Type: "hardBreak"},
								{
									Type: "text",
									Text: "Informs:",
								},
								{
									Type: "text",
									Text: " ",
								},
								{
									Type: "text",
									Text: "CRDB-8919",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://cockroachlabs.atlassian.net/browse/CRDB-8919"},
										},
									},
								},
							},
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type: "text",
									Text: "---",
								},
							},
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type: "text",
									Text: "Release note (sql change): `ALTER TABLE ... INJECT STATISTICS ...` will\nnow docsIssue notices when the given statistics JSON includes non-existent\ncolumns, rather than resulting in an error. Any statistics in the JSON\nfor existing columns will be injected successfully.",
								},
							},
						},
					},
				},
			},
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
			rns: []adfRoot{
				{
					Version: 1,
					Type:    "doc",
					Content: []adfNode{
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Related PR: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/pull/79361",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/79361"},
										},
									},
								},
								{
									Type:    "hardBreak",
									Content: []adfNode(nil),
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Commit: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/commit/88be04bd64283b1d77000a3f88588e603465e81b",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/88be04bd64283b1d77000a3f88588e603465e81b"},
										},
									},
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "---",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Release note (enterprise change): Remove the default\nvalues from the SHOW CHANGEFEED JOB output",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
					},
				},
			},
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
			rns: []adfRoot{},
		},
		{
			prNum: "66328",
			sha:   "0f329965acccb3e771ec1657c7def9e881dc78bb",
			commitMessage: `util/log: report the logging format at the start of new files

Release note (cli change): When log entries are written to disk,
the first few header lines written at the start of every new file
now report the configured logging format.`,
			rns: []adfRoot{
				{
					Version: 1,
					Type:    "doc",
					Content: []adfNode{
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Related PR: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/pull/66328",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/66328"},
										},
									},
								},
								{
									Type:    "hardBreak",
									Content: []adfNode(nil),
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Commit: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/commit/0f329965acccb3e771ec1657c7def9e881dc78bb",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/0f329965acccb3e771ec1657c7def9e881dc78bb"},
										},
									},
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "---",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Release note (cli change): When log entries are written to disk,\nthe first few header lines written at the start of every new file\nnow report the configured logging format.",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
					},
				},
			},
		},
		{
			prNum: "66328",
			sha:   "fb249c7140b634a53dca2967c946bc78ba927e1a",
			commitMessage: `cli: report explicit log config in logs

This increases troubleshootability.

Release note: None`,
			rns: []adfRoot{},
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
			rns: []adfRoot{
				{
					Version: 1,
					Type:    "doc",
					Content: []adfNode{
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Related PR: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/pull/104265",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/104265"},
										},
									},
								},
								{
									Type:    "hardBreak",
									Content: []adfNode(nil),
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Commit: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/commit/d756dec1b9d7245305ab706e68e2ec3de0e61ffc",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/d756dec1b9d7245305ab706e68e2ec3de0e61ffc"},
										},
									},
								},
								{
									Type:    "hardBreak",
									Content: []adfNode(nil),
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Related product changes: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2Fd756dec1b9d7245305ab706e68e2ec3de0e61ffc%22%20ORDER%20BY%20created%20DESC\n\n---",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2Fd756dec1b9d7245305ab706e68e2ec3de0e61ffc%22%20ORDER%20BY%20created%20DESC\n\n---"},
										},
									},
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "---",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Release note (cli change): The log output formats `crdb-v1` and\n`crdb-v2` now support the format option `timezone`. When specified,\nthe corresponding time zone is used to produce the timestamp column.\n\nFor example:\n```yaml\nfile-defaults:\n\tformat: crdb-v2\n\tformat-options: {timezone: america/new_york}\n```\n\nExample logging output:\n```\nI230606 12:43:01.553407-040000 1 1@cli/start.go:575 ⋮ [n?] 4  soft memory limit of Go runtime is set to 35 GiB\n^^^^^^^ indicates GMT-4 was used.\n```\n\nThe timezone offset is also always included in the format if it is not\nzero (e.g. for non-UTC time zones). This is necessary to ensure that\nthe times can be read back precisely.",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
					},
				},
				{
					Version: 1,
					Type:    "doc",
					Content: []adfNode{
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Related PR: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/pull/104265",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/104265"},
										},
									},
								},
								{
									Type:    "hardBreak",
									Content: []adfNode(nil),
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Commit: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/commit/d756dec1b9d7245305ab706e68e2ec3de0e61ffc",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/d756dec1b9d7245305ab706e68e2ec3de0e61ffc"},
										},
									},
								},
								{
									Type:    "hardBreak",
									Content: []adfNode(nil),
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Related product changes: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2Fd756dec1b9d7245305ab706e68e2ec3de0e61ffc%22%20ORDER%20BY%20created%20DESC\n\n---",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2Fd756dec1b9d7245305ab706e68e2ec3de0e61ffc%22%20ORDER%20BY%20created%20DESC\n\n---"},
										},
									},
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "---",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Release note (cli change): The command `cockroach debug merge-log` was\nadapted to understand time zones in input files read with format\n`crdb-v1` or `crdb-v2`.",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
					},
				},
				{
					Version: 1,
					Type:    "doc",
					Content: []adfNode{
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Related PR: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/pull/104265",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/pull/104265"},
										},
									},
								},
								{
									Type:    "hardBreak",
									Content: []adfNode(nil),
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Commit: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://github.com/cockroachdb/cockroach/commit/d756dec1b9d7245305ab706e68e2ec3de0e61ffc",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://github.com/cockroachdb/cockroach/commit/d756dec1b9d7245305ab706e68e2ec3de0e61ffc"},
										},
									},
								},
								{
									Type:    "hardBreak",
									Content: []adfNode(nil),
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Related product changes: ",
									Marks:   []adfMark(nil),
								},
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2Fd756dec1b9d7245305ab706e68e2ec3de0e61ffc%22%20ORDER%20BY%20created%20DESC\n\n---",
									Marks: []adfMark{
										{
											Type:  "link",
											Attrs: map[string]string{"href": "https://cockroachlabs.atlassian.net/issues/?jql=project%20%3D%20%22DOC%22%20and%20%22Doc%20Type%5BDropdown%5D%22%20%3D%20%22Product%20Change%22%20AND%20description%20~%20%22commit%2Fd756dec1b9d7245305ab706e68e2ec3de0e61ffc%22%20ORDER%20BY%20created%20DESC\n\n---"},
										},
									},
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "---",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
						{
							Type: "paragraph",
							Content: []adfNode{
								{
									Type:    "text",
									Content: []adfNode(nil),
									Text:    "Release note (backward-incompatible change): When a deployment is\nconfigured to use a time zone (new feature) for log file output using\nformats `crdb-v1` or `crdb-v2`, it becomes impossible to process the\nnew output log files using the `cockroach debug merge-log` command\nfrom a previous version. The newest `cockroach debug merge-log` code\nmust be used instead.",
									Marks:   []adfMark(nil),
								},
							},
							Marks: []adfMark(nil),
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.prNum, func(t *testing.T) {
			defer testutils.TestingHook(&getJiraIssueFromGitHubIssue, func(org, repo string, issue int) (string, error) {
				var ghJiraIssueMap = make(map[string]map[string]map[int]string)
				ghJiraIssueMap["cockroachdb"] = make(map[string]map[int]string)
				ghJiraIssueMap["cockroachdb"]["cockroach"] = make(map[int]string)
				ghJiraIssueMap["cockroachdb"]["cockroach"][68184] = "CRDB-8919"
				return ghJiraIssueMap[org][repo][issue], nil
			})()
			prNumInt, _ := strconv.Atoi(tc.prNum)
			result, _ := formatReleaseNotes(tc.commitMessage, prNumInt, tc.prBody, tc.sha)
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
			expected: map[string]int{
				"#75200": 1,
				"#98922": 1,
				"#75201": 1,
				"#592":   1,
				"#5555":  1,
			},
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
			expected: map[string]int{
				"#74932":           1,
				"#74889":           1,
				"#74482":           1,
				"#74784":           1,
				"#65117":           1,
				"#79299":           1,
				"#73834":           1,
				"#29833":           1,
				"example/repo#941": 1,
			},
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
			expected: map[string]int{
				"#4921":  1,
				"#72829": 1,
				"#71901": 1,
				"#491":   1,
				"#71614": 1,
				"#71607": 1,
				"#69765": 1,
				"#65200": 1,
			},
		},
		{
			message: `testing JIRA tickets
Resolved: doc-4321
This fixes everything. Closes CC-1234.
      Fixes CRDB-12345
Resolves crdb-23456, Resolves DEVINFHD-12345
Fixes #12345
Release notes (sql change): Excellent sql change...`,
			expected: map[string]int{
				"doc-4321":       1,
				"CC-1234":        1,
				"CRDB-12345":     1,
				"crdb-23456":     1,
				"DEVINFHD-12345": 1,
				"#12345":         1,
			},
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
			expected: map[string]int{
				"#75227": 1,
				"#45791": 1,
			},
		},
		{
			message: `lots of variations
Fixed:  #29833 example/repo#941
see also:  #9243
informs: #912,   #4729   #2911  cockroachdb/cockroach#2934
Informs #69765 (point 4).
This informs #59293 with these additions:
Release note (sql change): Import now checks readability...`,
			expected: map[string]int{
				"#9243":                      1,
				"#912":                       1,
				"#4729":                      1,
				"#2911":                      1,
				"cockroachdb/cockroach#2934": 1,
				"#69765":                     1,
				"#59293":                     1,
			},
		},
		{
			message: `testing JIRA keys with varying cases
Fixed:  CRDB-12345 example/repo#941
informs: doc-1234, crdb-24680
Informs DEVINF-123, #69765 and part of DEVINF-3891
Release note (sql change): Something something something...`,
			expected: map[string]int{
				"doc-1234":    1,
				"DEVINF-123":  1,
				"#69765":      1,
				"crdb-24680":  1,
				"DEVINF-3891": 1,
			},
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
epic CRDB-235, DOC-6883;   DEVINF-392	https://cockroachlabs.atlassian.net/browse/CRDB-28708
Release note (sql change): Import now checks readability...`,
			expected: map[string]int{"CRDB-18955": 1, "CRDB-235": 1, "DOC-6883": 1},
		},
	}

	for _, tc := range testCases {
		defer testutils.TestingHook(&getValidEpicRef, func(issueKey string) (bool, string, error) {
			var epicMap = map[string]struct {
				IsEpic  bool
				EpicKey string
			}{
				"CRDB-491": {
					IsEpic:  true,
					EpicKey: "CRDB-491",
				},
				"CRDB-9234": {
					IsEpic:  false,
					EpicKey: "",
				},
				"CRDB-235": {
					IsEpic:  true,
					EpicKey: "CRDB-235",
				},
				"DOC-6883": {
					IsEpic:  true,
					EpicKey: "DOC-6883",
				},
				"DEVINF-392": {
					IsEpic:  false,
					EpicKey: "",
				},
				"https://cockroachlabs.atlassian.net/browse/CRDB-28708": {
					IsEpic:  false,
					EpicKey: "CRDB-18955",
				},
				"CRDB-18955": {
					IsEpic:  true,
					EpicKey: "CRDB-18955",
				},
			}
			return epicMap[issueKey].IsEpic, epicMap[issueKey].EpicKey, nil
		})()
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
