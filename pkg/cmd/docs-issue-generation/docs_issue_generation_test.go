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
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func timeFromGhTime(t string) *time.Time {
	x, err := time.Parse(time.RFC3339, t)
	if err != nil {
		log.Fatal(err)
	}
	return &x
}

func TestPrNums(t *testing.T) {
	testCases := []struct {
		testName string
		search   ghSearch
		prs      []pr
	}{
		{
			testName: "Single merged commit",
			search: ghSearch{
				Items: []ghSearchItem{
					{
						Number: 80158,
						PullRequest: ghSearchItemPr{
							MergedAt: timeFromGhTime("2022-04-19T17:48:55Z"),
						},
					},
				},
			},
			prs: []pr{
				{
					number: 80158,
				},
			},
		},
		{
			testName: "Single unmerged commit",
			search: ghSearch{
				Items: []ghSearchItem{
					{
						Number: 80157,
						PullRequest: ghSearchItemPr{
							MergedAt: nil,
						},
					},
				},
			},
			prs: nil,
		},
		{
			testName: "Multiple merged and unmerged commits",
			search: ghSearch{
				Items: []ghSearchItem{
					{
						Number: 66328,
						PullRequest: ghSearchItemPr{
							MergedAt: timeFromGhTime("2021-12-01T06:00:00Z"),
						},
					},
					{
						Number: 74525,
						PullRequest: ghSearchItemPr{
							MergedAt: nil,
						},
					},
					{
						Number: 75077,
						PullRequest: ghSearchItemPr{
							MergedAt: nil,
						},
					},
				},
			},
			prs: []pr{
				{
					number: 66328,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			result := prNums(tc.search)
			assert.Equal(t, tc.prs, result)
		})
	}
}

func TestGetCommits(t *testing.T) {
	testCases := []struct {
		testName   string
		pullCommit []ghPullCommit
		prNumber   int
		commits    []commit
	}{
		{
			testName: "66328",
			pullCommit: []ghPullCommit{
				{
					Sha: "4dd8da9609adb3acce6795cea93b67ccacfc0270",
					Commit: ghPullCommitMsg{
						Message: "util/log: move the crdb-v1 test code to a separate file\n\nRelease note: None",
					},
				},
				{
					Sha: "cae4e525511a7d87f3f5ce74f02a6cf6edab4f3d",
					Commit: ghPullCommitMsg{
						Message: "util/log: use a datadriven test for the crdb-v1 format/parse test\n\nThis simplifies the test and makes it more extensible.\n\nRelease note: None",
					},
				},
				{
					Sha: "3be8458eda90aa922fa2a22ad7d6531c57aa12e1",
					Commit: ghPullCommitMsg{
						Message: "util/log: make `logpb.Entry` slightly more able to represent crdb-v2 entries\n\nPrior to this change, we did not have a public data structure able to\nrepresent the various fields available in a `crdb-v2` log entry: the\nexisting `logpb.Entry` was not able to distinguish structured and\nnon-structured entries, and did not have the ability to delimit the\nmessage and the stack trace.\n\nThis patch extends `logpb.Entry` to make it more able to represent\n`crdb-v2` entries, at least for the purpose of extending `debug\nmerge-log` towards conversion between logging formats.\n\nAdditionally, the patch adds a *best effort* attempt at populating\nthe new fields in the `crdb-v1` entry parser. This is best effort\nbecause the crdb-v1 parser is lossy so we cannot faithfully\nparse its entries reliably.\n\nRelease note: None",
					},
				},
				{
					Sha: "0f329965acccb3e771ec1657c7def9e881dc78bb",
					Commit: ghPullCommitMsg{
						Message: "util/log: report the logging format at the start of new files\n\nRelease note (cli change): When log entries are written to disk,\nthe first few header lines written at the start of every new file\nnow report the configured logging format.",
					},
				},
				{
					Sha: "934c7da83035fb78108daa23fa9cb8925d7b6d10",
					Commit: ghPullCommitMsg{
						Message: "logconfig: fix the handling of crdb-v1 explicit config\n\nRelease note (bug fix): Previously, `--log='file-defaults: {format:\ncrdb-v1}'` was not handled properly. This has been fixed. This bug\nexisted since v21.1.0.",
					},
				},
				{
					Sha: "fb249c7140b634a53dca2967c946bc78ba927e1a",
					Commit: ghPullCommitMsg{
						Message: "cli: report explicit log config in logs\n\nThis increases troubleshootability.\n\nRelease note: None",
					},
				},
				{
					Sha: "e22a0ebb46806a0054115edbeef0d6205203eef5",
					Commit: ghPullCommitMsg{
						Message: "util/log,logspy: make the logspy mechanism better\n\nPrior to this patch, the logspy mechanism was utterly broken: any time\nit was running, it would cut off any and all log entries from going to\nfiles, stderr, network sinks etc. This was a gross violation\nof the guarantees we have constructed around structured logging,\nas well as a security vulnerability (see release note below).\n\nAdditionally, it was impossible to launch multiple logspy sessions\nsimultaneously on the same node, for example using different grep\nfilters.\n\nThis commit rectifies the situation.\n\nAt a technical level, we have two changes: one in the logging Go API\nand one in the `/debug/logspy` HTTP API.\n\n**For the logging changes**, this patch replaces the \"interceptor\" singleton\ncallback function that takes over `(*loggerT).outputLogEntry()`, by a\nnew *interceptor sink* that lives alongside the other sinks on every\nchannel.\n\nBecause the interceptor logic is now a regular sink, log entries\ncontinue to be delivered to other sinks while an interceptor is\nactive.\n\nReusing the sink abstraction, with its own sink configuration with no\nseverity filter, clarifies that the interceptor accepts all the log\nentries regardless of which filters are configured on other sinks.\n\nAdditionally, the interceptor sink now supports multiple concurrent\ninterception sessions. Each log entry is delivered to all current\ninterceptors. The `logspy` logic is adapted to use this facility,\nso that multiple `logspy` requests can run side-by-side.\n\n**For the HTTP API change**, we are changing the `/debug/logspy`\nsemantics. This is explained in the release note below.\n\nRelease note (security update): All the logging output to files\nor network sinks was previously disabled temporarily while an operator\nwas using the `/debug/logspy` HTTP API, resulting in lost entries\nand a breach of auditability guarantees. This behavior has been corrected.\n\nRelease note (bug fix): Log entries are not lost any more while the\n`/debug/logspy` HTTP API is being used. This bug had existed since\nCockroachDB v1.1.\n\nRelease note (api change): The `/debug/logspy` HTTP API has changed.\nThe endpoint now returns JSON data by default.\nThis change is motivated as follows:\n\n- the previous format, `crdb-v1`, cannot be parsed reliably.\n- using JSON entries guarantees that the text of each entry\n  all fits on a single line of output (newline characters\n  inside the messages are escaped). This makes filtering\n  easier and more reliable.\n- using JSON enables the user to apply `jq` on the output, for\n  example via `curl -s .../debug/logspy | jq ...`\n\nIf the previous format is desired, the user can pass the query\nargument `&flatten=1` to the `logspy` URL to obtain the previous flat\ntext format (`crdb-v1`) instead.\n\nCo-authored-by: Yevgeniy Miretskiy <yevgeniy@cockroachlabs.com>",
					},
				},
				{
					Sha: "44836265f924a14f8c996a714d954e0e7e35dff7",
					Commit: ghPullCommitMsg{
						Message: "util/log,server/debug: new API `/debug/vmodule`, change `logspy`\n\nPrior to this patch, any ongoing `/debug/logspy` query would\ntrigger maximum verbosity in the logging package - i.e.\ncause all logging API calls under `log.V` to be activated.\n\nThis was problematic, as it would cause a large amount\nof logging traffic to be pumped through the interceptor logic,\nincreasing the chance for entries to be dropped (especially\nwhen `logspy?grep=...` is not used).\n\nAdditionally, since the previous change to make the interceptor logic\na regular sink, all the entries captured by the interceptors are now\nalso duplicated to the other sinks (this is a feature / bug fix,\nas explained in the previous commit message).\n\nHowever, this change also meant that any ongoing `logspy` request\nwould cause maximum verbosity to all the sinks with threshold\nINFO (most logging calls under `log.V` have severity INFO). For\nexample, the DEV channel accepts all entries at level INFO or higher\nin the default config. This in turn could incur unacceptable disk\nspace or IOPS consumption in certain deployments.\n\nIn orde to mitigate this new problem, this patch removes the special\nconditional from the logging package. From this point forward,\nthe verbosity of the entries delivered via `/debug/logspy` are those\nconfigured via `vmodule` -- no more and no less.\n\nTo make this configurable for a running server, including one where\nthe SQL endpoint may not be available yet, this patch also introduces\na new `/debug/vmodule` HTTP API and extends `/debug/logspy` with\nthe ability to temporarily change `vmodule` *upon explicit request*.\n\nRelease note (api change): The `/debug/logspy` API does not any more\nenable maximum logging verbosity automatically. To change the\nverbosity, use the new `/debug/vmodule` endpoint or pass the\n`&vmodule=` query parameter to the `/debug/logspy` endpoint.\n\nFor example, suppose you wish to run a 20s logspy session:\n\n- Before:\n\n  ```\n  curl 'https://.../debug/logspy?duration=20s&...'\n  ```\n\n- Now:\n\n  ```\n  curl 'https://.../debug/logspy?duration=20s&vmodule=...'\n  ```\n\n  OR\n\n  ```\n  curl 'https://.../debug/vmodule?duration=22s&vmodule=...'\n  curl 'https://.../debug/logspy?duration=20s'\n  ```\n\nAs for the regular `vmodule` command-line flag, the maximum verbosity\nacross all the source code can be selected with the pattern `*=4`.\n\nNote: at most one in-flight HTTP API request is allowed to modify the\n`vmodule` parameter. This maintain the invariant that the\nconfiguration restored at the end of each request is the same as when\nthe request started.\n\nRelease note (api change): The new `/debug/vmodule` API makes it\npossible for an operator to configure the logging verbosity in a\nsimilar way as the SQL built-in function\n`crdb_internal.set_vmodule()`, or to query the current configuration\nas in `crdb_internal.get_vmodule()`. Additionally, any configuration\nchange performed via this API can be automatically reverted after a\nconfigurable delay. The API forms are:\n\n- `/debug/vmodule` - retrieve the current configuration.\n- `/debug/vmodule?set=[vmodule config]&duration=[duration]` - change\n  the configuration to `[vmodule config]` . The previous configuration\n  at the time the `/debug/vmodule` request started is restored after\n  `[duration]`. This duration, if not specified, defaults to twice the\n  default duration of a `logspy` request (currently, the `logspy`\n  default duration is 5s, so the `vmodule` default duration is 10s).\n  If the duration is zero or negative, the previous configuration\n  is never restored.",
					},
				},
			},
			prNumber: 66328,
			commits: []commit{
				{
					sha:         "0f329965acccb3e771ec1657c7def9e881dc78bb",
					title:       "util/log: report the logging format at the start of new files",
					releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/66328\nCommit: https://github.com/cockroachdb/cockroach/commit/0f329965acccb3e771ec1657c7def9e881dc78bb\n\n---\n\nRelease note (cli change): When log entries are written to disk,\nthe first few header lines written at the start of every new file\nnow report the configured logging format.",
				},
				{
					sha:         "44836265f924a14f8c996a714d954e0e7e35dff7",
					title:       "util/log,server/debug: new API `/debug/vmodule`, change `logspy`",
					releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/66328\nCommit: https://github.com/cockroachdb/cockroach/commit/44836265f924a14f8c996a714d954e0e7e35dff7\n\n---\n\nRelease note (api change): The `/debug/logspy` API does not any more\nenable maximum logging verbosity automatically. To change the\nverbosity, use the new `/debug/vmodule` endpoint or pass the\n`&vmodule=` query parameter to the `/debug/logspy` endpoint.\n\nFor example, suppose you wish to run a 20s logspy session:\n\n- Before:\n\n  ```\n  curl 'https://.../debug/logspy?duration=20s&...'\n  ```\n\n- Now:\n\n  ```\n  curl 'https://.../debug/logspy?duration=20s&vmodule=...'\n  ```\n\n  OR\n\n  ```\n  curl 'https://.../debug/vmodule?duration=22s&vmodule=...'\n  curl 'https://.../debug/logspy?duration=20s'\n  ```\n\nAs for the regular `vmodule` command-line flag, the maximum verbosity\nacross all the source code can be selected with the pattern `*=4`.\n\nNote: at most one in-flight HTTP API request is allowed to modify the\n`vmodule` parameter. This maintain the invariant that the\nconfiguration restored at the end of each request is the same as when\nthe request started.\n\nRelease note (api change): The new `/debug/vmodule` API makes it\npossible for an operator to configure the logging verbosity in a\nsimilar way as the SQL built-in function\n`crdb_internal.set_vmodule()`, or to query the current configuration\nas in `crdb_internal.get_vmodule()`. Additionally, any configuration\nchange performed via this API can be automatically reverted after a\nconfigurable delay. The API forms are:\n\n- `/debug/vmodule` - retrieve the current configuration.\n- `/debug/vmodule?set=[vmodule config]&duration=[duration]` - change\n  the configuration to `[vmodule config]` . The previous configuration\n  at the time the `/debug/vmodule` request started is restored after\n  `[duration]`. This duration, if not specified, defaults to twice the\n  default duration of a `logspy` request (currently, the `logspy`\n  default duration is 5s, so the `vmodule` default duration is 10s).\n  If the duration is zero or negative, the previous configuration\n  is never restored.",
				},
			},
		},
		{
			testName: "78685",
			pullCommit: []ghPullCommit{
				{
					Sha: "1d7811d5d14f9c7e106c3ec92de9c66192f19604",
					Commit: ghPullCommitMsg{
						Message: "opt: do not cross-join input of semi-join\n\nThis commit fixes a logical correctness bug caused when\n`GenerateLookupJoins` cross-joins the input of a semi-join with a set of\nconstant values to constrain the prefix columns of the lookup index. The\ncross-join is an invalid transformation because it increases the size of\nthe join's input and can increase the size of the join's output.\n\nWe already avoid these cross-joins for left and anti-joins (see #59646).\nWhen addressing those cases, the semi-join case was incorrectly assumed\nto be safe.\n\nFixes #78681\n\nRelease note (bug fix): A bug has been fixed which caused the optimizer\nto generate invalid query plans which could result in incorrect query\nresults. The bug, which has been present since version 21.1.0, can\nappear if all of the following conditions are true: 1) the query\ncontains a semi-join, such as queries in the form:\n`SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.a = t2.a);`,\n2) the inner table has an index containing the equality column, like\n`t2.a` in the example query, 3) the index contains one or more\ncolumns that prefix the equality column, and 4) the prefix columns are\n`NOT NULL` and are constrained to a set of constant values via a `CHECK`\nconstraint or an `IN` condition in the filter.",
					},
				},
			},
			prNumber: 78685,
			commits:  nil,
		},
		{
			testName: "79069",
			pullCommit: []ghPullCommit{
				{
					Sha: "5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
					Commit: ghPullCommitMsg{
						Message: "sql: ignore non-existent columns when injecting stats\n\nPreviously, an `ALTER TABLE ... INJECT STATS` command would return an\nerror if the given stats JSON included any columns that were not present\nin the table descriptor. Statistics in statement bundles often include\ndropped columns, so reproducing issues with a bundle required tediously\nremoving stats for these columns. This commit changes the stats\ninjection behavior so that a notice is issued for stats with\nnon-existent columns rather than an error. Any stats for existing\ncolumns will be injected successfully.\n\nInforms #68184\n\nRelease note (sql change): `ALTER TABLE ... INJECT STATISTICS ...` will\nnow issue notices when the given statistics JSON includes non-existent\ncolumns, rather than resulting in an error. Any statistics in the JSON\nfor existing columns will be injected successfully.",
					},
				},
			},
			prNumber: 79069,
			commits: []commit{
				{
					sha:         "5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
					title:       "sql: ignore non-existent columns when injecting stats",
					releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/79069\nCommit: https://github.com/cockroachdb/cockroach/commit/5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9\n\n---\n\nRelease note (sql change): `ALTER TABLE ... INJECT STATISTICS ...` will\nnow issue notices when the given statistics JSON includes non-existent\ncolumns, rather than resulting in an error. Any statistics in the JSON\nfor existing columns will be injected successfully.",
				},
			},
		},
		{
			testName: "79361",
			pullCommit: []ghPullCommit{
				{
					Sha: "88be04bd64283b1d77000a3f88588e603465e81b",
					Commit: ghPullCommitMsg{
						Message: "changefeedccl: remove the default values from SHOW\nCHANGEFEED JOB output\n\nCurrently, when a user alters a changefeed, we\ninclude the default options in the SHOW CHANGEFEED\nJOB output. In this PR we prevent the default values\nfrom being displayed.\n\nRelease note (enterprise change): Remove the default\nvalues from the SHOW CHANGEFEED JOB output",
					},
				},
			},
			prNumber: 79361,
			commits: []commit{
				{
					sha:         "88be04bd64283b1d77000a3f88588e603465e81b",
					title:       "changefeedccl: remove the default values from SHOW",
					releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/79361\nCommit: https://github.com/cockroachdb/cockroach/commit/88be04bd64283b1d77000a3f88588e603465e81b\n\n---\n\nRelease note (enterprise change): Remove the default\nvalues from the SHOW CHANGEFEED JOB output",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			result := getCommits(tc.pullCommit, tc.prNumber)
			assert.Equal(t, tc.commits, result)
		})
	}
}

func TestFormatReleaseNote(t *testing.T) {
	testCases := []struct {
		prNum   string
		sha     string
		message string
		rn      string
	}{
		{
			prNum:   "79069",
			sha:     "5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
			message: "sql: ignore non-existent columns when injecting stats\n\nPreviously, an `ALTER TABLE ... INJECT STATS` command would return an\nerror if the given stats JSON included any columns that were not present\nin the table descriptor. Statistics in statement bundles often include\ndropped columns, so reproducing issues with a bundle required tediously\nremoving stats for these columns. This commit changes the stats\ninjection behavior so that a notice is issued for stats with\nnon-existent columns rather than an error. Any stats for existing\ncolumns will be injected successfully.\n\nInforms #68184\n\nRelease note (sql change): `ALTER TABLE ... INJECT STATISTICS ...` will\nnow issue notices when the given statistics JSON includes non-existent\ncolumns, rather than resulting in an error. Any statistics in the JSON\nfor existing columns will be injected successfully.",
			rn:      "Related PR: https://github.com/cockroachdb/cockroach/pull/79069\nCommit: https://github.com/cockroachdb/cockroach/commit/5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9\n\n---\n\nRelease note (sql change): `ALTER TABLE ... INJECT STATISTICS ...` will\nnow issue notices when the given statistics JSON includes non-existent\ncolumns, rather than resulting in an error. Any statistics in the JSON\nfor existing columns will be injected successfully.",
		},
		{
			prNum:   "79361",
			sha:     "88be04bd64283b1d77000a3f88588e603465e81b",
			message: "changefeedccl: remove the default values from SHOW\nCHANGEFEED JOB output\n\nCurrently, when a user alters a changefeed, we\ninclude the default options in the SHOW CHANGEFEED\nJOB output. In this PR we prevent the default values\nfrom being displayed.\n\nRelease note (enterprise change): Remove the default\nvalues from the SHOW CHANGEFEED JOB output",
			rn:      "Related PR: https://github.com/cockroachdb/cockroach/pull/79361\nCommit: https://github.com/cockroachdb/cockroach/commit/88be04bd64283b1d77000a3f88588e603465e81b\n\n---\n\nRelease note (enterprise change): Remove the default\nvalues from the SHOW CHANGEFEED JOB output",
		},
		{
			prNum:   "78685",
			sha:     "1d7811d5d14f9c7e106c3ec92de9c66192f19604",
			message: "opt: do not cross-join input of semi-join\n\nThis commit fixes a logical correctness bug caused when\n`GenerateLookupJoins` cross-joins the input of a semi-join with a set of\nconstant values to constrain the prefix columns of the lookup index. The\ncross-join is an invalid transformation because it increases the size of\nthe join's input and can increase the size of the join's output.\n\nWe already avoid these cross-joins for left and anti-joins (see #59646).\nWhen addressing those cases, the semi-join case was incorrectly assumed\nto be safe.\n\nFixes #78681\n\nRelease note (bug fix): A bug has been fixed which caused the optimizer\nto generate invalid query plans which could result in incorrect query\nresults. The bug, which has been present since version 21.1.0, can\nappear if all of the following conditions are true: 1) the query\ncontains a semi-join, such as queries in the form:\n`SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.a = t2.a);`,\n2) the inner table has an index containing the equality column, like\n`t2.a` in the example query, 3) the index contains one or more\ncolumns that prefix the equality column, and 4) the prefix columns are\n`NOT NULL` and are constrained to a set of constant values via a `CHECK`\nconstraint or an `IN` condition in the filter.",
			rn:      "",
		},
		{
			prNum:   "66328",
			sha:     "0f329965acccb3e771ec1657c7def9e881dc78bb",
			message: "util/log: report the logging format at the start of new files\n\nRelease note (cli change): When log entries are written to disk,\nthe first few header lines written at the start of every new file\nnow report the configured logging format.",
			rn:      "Related PR: https://github.com/cockroachdb/cockroach/pull/66328\nCommit: https://github.com/cockroachdb/cockroach/commit/0f329965acccb3e771ec1657c7def9e881dc78bb\n\n---\n\nRelease note (cli change): When log entries are written to disk,\nthe first few header lines written at the start of every new file\nnow report the configured logging format.",
		},
		{
			prNum:   "66328",
			sha:     "fb249c7140b634a53dca2967c946bc78ba927e1a",
			message: "cli: report explicit log config in logs\n\nThis increases troubleshootability.\n\nRelease note: None",
			rn:      "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.prNum, func(t *testing.T) {
			prNumInt, _ := strconv.Atoi(tc.prNum)
			result := formatReleaseNote(tc.message, prNumInt, tc.sha)
			assert.Equal(t, tc.rn, result)
		})
	}
}

func TestFormatTitle(t *testing.T) {
	testCases := []struct {
		testName string
		message  string
		title    string
	}{

		{
			testName: "Format Title 1",
			message:  "sql: ignore non-existent columns when injecting stats\n\nPreviously, an `ALTER TABLE ... INJECT STATS` command would return an\nerror if the given stats JSON included any columns that were not present\nin the table descriptor. Statistics in statement bundles often include\ndropped columns, so reproducing issues with a bundle required tediously\nremoving stats for these columns. This commit changes the stats\ninjection behavior so that a notice is issued for stats with\nnon-existent columns rather than an error. Any stats for existing\ncolumns will be injected successfully.\n\nInforms #68184\n\nRelease note (sql change): `ALTER TABLE ... INJECT STATISTICS ...` will\nnow issue notices when the given statistics JSON includes non-existent\ncolumns, rather than resulting in an error. Any statistics in the JSON\nfor existing columns will be injected successfully.",
			title:    "sql: ignore non-existent columns when injecting stats",
		},
		{
			testName: "Format Title 2",
			message:  "changefeedccl: remove the default values from SHOW\nCHANGEFEED JOB output\n\nCurrently, when a user alters a changefeed, we\ninclude the default options in the SHOW CHANGEFEED\nJOB output. In this PR we prevent the default values\nfrom being displayed.\n\nRelease note (enterprise change): Remove the default\nvalues from the SHOW CHANGEFEED JOB output",
			title:    "changefeedccl: remove the default values from SHOW",
		},
		{
			testName: "Format Title 3",
			message:  "opt: do not cross-join input of semi-join\n\nThis commit fixes a logical correctness bug caused when\n`GenerateLookupJoins` cross-joins the input of a semi-join with a set of\nconstant values to constrain the prefix columns of the lookup index. The\ncross-join is an invalid transformation because it increases the size of\nthe join's input and can increase the size of the join's output.\n\nWe already avoid these cross-joins for left and anti-joins (see #59646).\nWhen addressing those cases, the semi-join case was incorrectly assumed\nto be safe.\n\nFixes #78681\n\nRelease note (bug fix): A bug has been fixed which caused the optimizer\nto generate invalid query plans which could result in incorrect query\nresults. The bug, which has been present since version 21.1.0, can\nappear if all of the following conditions are true: 1) the query\ncontains a semi-join, such as queries in the form:\n`SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.a = t2.a);`,\n2) the inner table has an index containing the equality column, like\n`t2.a` in the example query, 3) the index contains one or more\ncolumns that prefix the equality column, and 4) the prefix columns are\n`NOT NULL` and are constrained to a set of constant values via a `CHECK`\nconstraint or an `IN` condition in the filter.",
			title:    "opt: do not cross-join input of semi-join",
		},
		{
			testName: "Format Title 4",
			message:  "util/log: report the logging format at the start of new files\n\nRelease note (cli change): When log entries are written to disk,\nthe first few header lines written at the start of every new file\nnow report the configured logging format.",
			title:    "util/log: report the logging format at the start of new files",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			result := formatTitle(tc.message)
			assert.Equal(t, tc.title, result)
		})
	}
}
