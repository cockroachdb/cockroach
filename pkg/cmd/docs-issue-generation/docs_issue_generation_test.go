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
						PRNumber: 80158,
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
						PRNumber: 80157,
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
						PRNumber: 66328,
						PullRequest: ghSearchItemPr{
							MergedAt: timeFromGhTime("2021-12-01T06:00:00Z"),
						},
					},
					{
						PRNumber: 74525,
						PullRequest: ghSearchItemPr{
							MergedAt: nil,
						},
					},
					{
						PRNumber: 75077,
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

func TestGetIssues(t *testing.T) {
	testCases := []struct {
		testName   string
		pullCommit []ghPullCommit
		prNumber   int
		issues     []docsIssue
	}{
		{
			testName: "66328",
			pullCommit: []ghPullCommit{
				{
					Sha: "4dd8da9609adb3acce6795cea93b67ccacfc0270",
					Commit: ghPullCommitMsg{
						Message: `util/log: move the crdb-v1 test code to a separate file

Release note: None`,
					},
				},
				{
					Sha: "cae4e525511a7d87f3f5ce74f02a6cf6edab4f3d",
					Commit: ghPullCommitMsg{
						Message: `util/log: use a datadriven test for the crdb-v1 format/parse test

This simplifies the test and makes it more extensible.

Release note: None`,
					},
				},
				{
					Sha: "3be8458eda90aa922fa2a22ad7d6531c57aa12e1",
					Commit: ghPullCommitMsg{
						Message: "util/log: make " + "`logpb.Entry`" + ` slightly more able to represent crdb-v2 entries

Prior to this change, we did not have a public data structure able to
represent the various fields available in a ` + "`crdb-v2`" + ` log entry: the
existing ` + "`logpb.Entry`" + ` was not able to distinguish structured and
non-structured entries, and did not have the ability to delimit the
message and the stack trace.

This patch extends ` + "`logpb.Entry`" + ` to make it more able to represent
` + "`crdb-v2`" + " entries, at least for the purpose of extending " + "`debug\nmerge-log`" +
							` towards conversion between logging formats.

Additionally, the patch adds a *best effort* attempt at populating
the new fields in the ` + "`crdb-v1`" + ` entry parser. This is best effort
because the crdb-v1 parser is lossy so we cannot faithfully
parse its entries reliably.

Release note: None`,
					},
				},
				{
					Sha: "0f329965acccb3e771ec1657c7def9e881dc78bb",
					Commit: ghPullCommitMsg{
						Message: `util/log: report the logging format at the start of new files

Release note (cli change): When log entries are written to disk,
the first few header lines written at the start of every new file
now report the configured logging format.`,
					},
				},
				{
					Sha: "934c7da83035fb78108daa23fa9cb8925d7b6d10",
					Commit: ghPullCommitMsg{
						Message: `logconfig: fix the handling of crdb-v1 explicit config

Release note (bug fix): Previously, ` + "`--log='file-defaults: {format:\ncrdb-v1}'`" +
							` was not handled properly. This has been fixed. This bug
existed since v21.1.0.`,
					},
				},
				{
					Sha: "fb249c7140b634a53dca2967c946bc78ba927e1a",
					Commit: ghPullCommitMsg{
						Message: `cli: report explicit log config in logs

This increases troubleshootability.

Release note: None`,
					},
				},
				{
					Sha: "e22a0ebb46806a0054115edbeef0d6205203eef5",
					Commit: ghPullCommitMsg{
						Message: `util/log,logspy: make the logspy mechanism better

Prior to this patch, the logspy mechanism was utterly broken: any time
it was running, it would cut off any and all log entries from going to
files, stderr, network sinks etc. This was a gross violation
of the guarantees we have constructed around structured logging,
as well as a security vulnerability (see release note below).

Additionally, it was impossible to launch multiple logspy sessions
simultaneously on the same node, for example using different grep
filters.

This commit rectifies the situation.

At a technical level, we have two changes: one in the logging Go API
and one in the ` + "`/debug/logspy`" + ` HTTP API.

**For the logging changes**, this patch replaces the "interceptor" singleton
callback function that takes over ` + "`(*loggerT).outputLogEntry()`" + `, by a
new *interceptor sink* that lives alongside the other sinks on every
channel.

Because the interceptor logic is now a regular sink, log entries
continue to be delivered to other sinks while an interceptor is
active.

Reusing the sink abstraction, with its own sink configuration with no
severity filter, clarifies that the interceptor accepts all the log
entries regardless of which filters are configured on other sinks.

Additionally, the interceptor sink now supports multiple concurrent
interception sessions. Each log entry is delivered to all current
interceptors. The ` + "`logspy`" + ` logic is adapted to use this facility,
so that multiple ` + "`logspy`" + ` requests can run side-by-side.

**For the HTTP API change**, we are changing the ` + "`/debug/logspy`" + `
semantics. This is explained in the release note below.

Release note (security update): All the logging output to files
or network sinks was previously disabled temporarily while an operator
was using the ` + "`/debug/logspy`" + ` HTTP API, resulting in lost entries
and a breach of auditability guarantees. This behavior has been corrected.

Release note (bug fix): Log entries are not lost any more while the
` + "`/debug/logspy`" + ` HTTP API is being used. This bug had existed since
CockroachDB v1.1.

Release note (api change): The ` + "`/debug/logspy`" + ` HTTP API has changed.
The endpoint now returns JSON data by default.
This change is motivated as follows:

- the previous format, ` + "`crdb-v1`" + `, cannot be parsed reliably.
- using JSON entries guarantees that the text of each entry
  all fits on a single line of output (newline characters
  inside the messages are escaped). This makes filtering
  easier and more reliable.
- using JSON enables the user to apply ` + "`jq`" + ` on the output, for
  example via ` + "`curl -s .../debug/logspy | jq ...`" + `

If the previous format is desired, the user can pass the query
argument ` + "`&flatten=1`" + " to the " + "`logspy`" + ` URL to obtain the previous flat
text format (` + "`crdb-v1`" + `) instead.

Co-authored-by: Yevgeniy Miretskiy <yevgeniy@cockroachlabs.com>`,
					},
				},
				{
					Sha: "44836265f924a14f8c996a714d954e0e7e35dff7",
					Commit: ghPullCommitMsg{
						Message: "util/log,server/debug: new API " + "`/debug/vmodule`" + ", change " + "`logspy`" + `

Prior to this patch, any ongoing ` + "`/debug/logspy`" + ` query would
trigger maximum verbosity in the logging package - i.e.
cause all logging API calls under ` + "`log.V`" + ` to be activated.

This was problematic, as it would cause a large amount
of logging traffic to be pumped through the interceptor logic,
increasing the chance for entries to be dropped (especially
when ` + "`logspy?grep=...`" + ` is not used).

Additionally, since the previous change to make the interceptor logic
a regular sink, all the entries captured by the interceptors are now
also duplicated to the other sinks (this is a feature / bug fix,
as explained in the previous commit message).

However, this change also meant that any ongoing ` + "`logspy`" + ` request
would cause maximum verbosity to all the sinks with threshold
INFO (most logging calls under ` + "`log.V`" + ` have severity INFO). For
example, the DEV channel accepts all entries at level INFO or higher
in the default config. This in turn could incur unacceptable disk
space or IOPS consumption in certain deployments.

In orde to mitigate this new problem, this patch removes the special
conditional from the logging package. From this point forward,
the verbosity of the entries delivered via ` + "`/debug/logspy`" + ` are those
configured via ` + "`vmodule`" + ` -- no more and no less.

To make this configurable for a running server, including one where
the SQL endpoint may not be available yet, this patch also introduces
a new ` + "`/debug/vmodule`" + " HTTP API and extends " + "`/debug/logspy`" + ` with
the ability to temporarily change ` + "`vmodule`" + ` *upon explicit request*.

Release note (api change): The ` + "`/debug/logspy`" + ` API does not any more
enable maximum logging verbosity automatically. To change the
verbosity, use the new ` + "`/debug/vmodule`" + ` endpoint or pass the
` + "`&vmodule=`" + " query parameter to the " + "`/debug/logspy`" + ` endpoint.

For example, suppose you wish to run a 20s logspy session:

- Before:

  ` + "```\n  curl 'https://.../debug/logspy?duration=20s&...'\n  ```" + `

- Now:

  ` + "```\n  curl 'https://.../debug/logspy?duration=20s&vmodule=...'\n  ```" + `

  OR

  ` +
							"```\n  curl 'https://.../debug/vmodule?duration=22s&vmodule=...'\n  curl 'https://.../debug/logspy?duration=20s'\n  ```" + `

As for the regular ` + "`vmodule`" + ` command-line flag, the maximum verbosity
across all the source code can be selected with the pattern ` + "`*=4`" + `.

Note: at most one in-flight HTTP API request is allowed to modify the
` + "`vmodule`" + ` parameter. This maintain the invariant that the
configuration restored at the end of each request is the same as when
the request started.

Release note (api change): The new ` + "`/debug/vmodule`" + ` API makes it
possible for an operator to configure the logging verbosity in a
similar way as the SQL built-in function
` + "`crdb_internal.set_vmodule()`" + `, or to query the current configuration
as in ` + "`crdb_internal.get_vmodule()`" + `. Additionally, any configuration
change performed via this API can be automatically reverted after a
configurable delay. The API forms are:

- ` + "`/debug/vmodule`" + ` - retrieve the current configuration.
- ` + "`/debug/vmodule?set=[vmodule config]&duration=[duration]`" + ` - change
  the configuration to ` + "`[vmodule config]`" + ` . The previous configuration
  at the time the ` + "`/debug/vmodule`" + ` request started is restored after
  ` + "`[duration]`" + `. This duration, if not specified, defaults to twice the
  default duration of a ` + "`logspy`" + " request (currently, the " + "`logspy`" + `
  default duration is 5s, so the ` + "`vmodule`" + ` default duration is 10s).
  If the duration is zero or negative, the previous configuration
  is never restored.`,
					},
				},
			},
			prNumber: 66328,
			issues: []docsIssue{
				{
					sourceCommitSha: "0f329965acccb3e771ec1657c7def9e881dc78bb",
					title:           "PR #66328 - util/log: report the logging format at the start of new files",
					body: `Related PR: https://github.com/cockroachdb/cockroach/pull/66328
Commit: https://github.com/cockroachdb/cockroach/commit/0f329965acccb3e771ec1657c7def9e881dc78bb

---

Release note (cli change): When log entries are written to disk,
the first few header lines written at the start of every new file
now report the configured logging format.`,
				},
				{
					sourceCommitSha: "e22a0ebb46806a0054115edbeef0d6205203eef5",
					title:           "PR #66328 - util/log,logspy: make the logspy mechanism better (1 of 2)",
					body: `Related PR: https://github.com/cockroachdb/cockroach/pull/66328
Commit: https://github.com/cockroachdb/cockroach/commit/e22a0ebb46806a0054115edbeef0d6205203eef5

---

Release note (security update): All the logging output to files
or network sinks was previously disabled temporarily while an operator
was using the ` + "`/debug/logspy`" + ` HTTP API, resulting in lost entries
and a breach of auditability guarantees. This behavior has been corrected.`,
				},
				{
					sourceCommitSha: "e22a0ebb46806a0054115edbeef0d6205203eef5",
					title:           "PR #66328 - util/log,logspy: make the logspy mechanism better (2 of 2)",
					body: `Related PR: https://github.com/cockroachdb/cockroach/pull/66328
Commit: https://github.com/cockroachdb/cockroach/commit/e22a0ebb46806a0054115edbeef0d6205203eef5

---

Release note (api change): The ` + "`/debug/logspy`" + ` HTTP API has changed.
The endpoint now returns JSON data by default.
This change is motivated as follows:

- the previous format, ` + "`crdb-v1`" + `, cannot be parsed reliably.
- using JSON entries guarantees that the text of each entry
  all fits on a single line of output (newline characters
  inside the messages are escaped). This makes filtering
  easier and more reliable.
- using JSON enables the user to apply ` + "`jq`" + ` on the output, for
  example via ` + "`curl -s .../debug/logspy | jq ...`" + `

If the previous format is desired, the user can pass the query
argument ` + "`&flatten=1`" + " to the " + "`logspy`" + ` URL to obtain the previous flat
text format (` + "`crdb-v1`" + `) instead.

Co-authored-by: Yevgeniy Miretskiy <yevgeniy@cockroachlabs.com>`,
				},
				{
					sourceCommitSha: "44836265f924a14f8c996a714d954e0e7e35dff7",
					title:           "PR #66328 - util/log,server/debug: new API `/debug/vmodule`, change `logspy` (1 of 2)",
					body: `Related PR: https://github.com/cockroachdb/cockroach/pull/66328
Commit: https://github.com/cockroachdb/cockroach/commit/44836265f924a14f8c996a714d954e0e7e35dff7

---

Release note (api change): The ` + "`/debug/logspy`" + ` API does not any more
enable maximum logging verbosity automatically. To change the
verbosity, use the new ` + "`/debug/vmodule`" + ` endpoint or pass the
` + "`&vmodule=`" + " query parameter to the " + "`/debug/logspy`" + ` endpoint.

For example, suppose you wish to run a 20s logspy session:

- Before:

  ` + "```\n  curl 'https://.../debug/logspy?duration=20s&...'\n  ```" + `

- Now:

  ` + "```\n  curl 'https://.../debug/logspy?duration=20s&vmodule=...'\n  ```" + `

  OR

  ` + "```\n  curl 'https://.../debug/vmodule?duration=22s&vmodule=...'\n  curl 'https://.../debug/logspy?duration=20s'\n  ```" + `

As for the regular ` + "`vmodule`" + ` command-line flag, the maximum verbosity
across all the source code can be selected with the pattern ` + "`*=4`" + `.

Note: at most one in-flight HTTP API request is allowed to modify the
` + "`vmodule`" + ` parameter. This maintain the invariant that the
configuration restored at the end of each request is the same as when
the request started.`,
				},
				{
					sourceCommitSha: "44836265f924a14f8c996a714d954e0e7e35dff7",
					title:           "PR #66328 - util/log,server/debug: new API `/debug/vmodule`, change `logspy` (2 of 2)",
					body: `Related PR: https://github.com/cockroachdb/cockroach/pull/66328
Commit: https://github.com/cockroachdb/cockroach/commit/44836265f924a14f8c996a714d954e0e7e35dff7

---

Release note (api change): The new ` + "`/debug/vmodule`" + ` API makes it
possible for an operator to configure the logging verbosity in a
similar way as the SQL built-in function
` + "`crdb_internal.set_vmodule()`" + `, or to query the current configuration
as in ` + "`crdb_internal.get_vmodule()`" + `. Additionally, any configuration
change performed via this API can be automatically reverted after a
configurable delay. The API forms are:

- ` + "`/debug/vmodule`" + ` - retrieve the current configuration.
- ` + "`/debug/vmodule?set=[vmodule config]&duration=[duration]`" + ` - change
  the configuration to ` + "`[vmodule config]`" + ` . The previous configuration
  at the time the ` + "`/debug/vmodule`" + ` request started is restored after
  ` + "`[duration]`" + `. This duration, if not specified, defaults to twice the
  default duration of a ` + "`logspy`" + " request (currently, the " + "`logspy`" + `
  default duration is 5s, so the ` + "`vmodule`" + ` default duration is 10s).
  If the duration is zero or negative, the previous configuration
  is never restored.`,
				},
			},
		},
		{
			testName: "78685",
			pullCommit: []ghPullCommit{
				{
					Sha: "1d7811d5d14f9c7e106c3ec92de9c66192f19604",
					Commit: ghPullCommitMsg{
						Message: `opt: do not cross-join input of semi-join

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
					},
				},
			},
			prNumber: 78685,
			issues:   nil,
		},
		{
			testName: "79069",
			pullCommit: []ghPullCommit{
				{
					Sha: "5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
					Commit: ghPullCommitMsg{
						Message: `sql: ignore non-existent columns when injecting stats

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
					},
				},
			},
			prNumber: 79069,
			issues: []docsIssue{
				{
					sourceCommitSha: "5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
					title:           "PR #79069 - sql: ignore non-existent columns when injecting stats",
					body: `Related PR: https://github.com/cockroachdb/cockroach/pull/79069
Commit: https://github.com/cockroachdb/cockroach/commit/5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9

---

Release note (sql change): ` + "`ALTER TABLE ... INJECT STATISTICS ...`" + ` will
now docsIssue notices when the given statistics JSON includes non-existent
columns, rather than resulting in an error. Any statistics in the JSON
for existing columns will be injected successfully.`,
				},
			},
		},
		{
			testName: "79361",
			pullCommit: []ghPullCommit{
				{
					Sha: "88be04bd64283b1d77000a3f88588e603465e81b",
					Commit: ghPullCommitMsg{
						Message: `changefeedccl: remove the default values from SHOW
CHANGEFEED JOB output

Currently, when a user alters a changefeed, we
include the default options in the SHOW CHANGEFEED
JOB output. In this PR we prevent the default values
from being displayed.

Release note (enterprise change): Remove the default
values from the SHOW CHANGEFEED JOB output`,
					},
				},
			},
			prNumber: 79361,
			issues: []docsIssue{
				{
					sourceCommitSha: "88be04bd64283b1d77000a3f88588e603465e81b",
					title:           "PR #79361 - changefeedccl: remove the default values from SHOW",
					body: `Related PR: https://github.com/cockroachdb/cockroach/pull/79361
Commit: https://github.com/cockroachdb/cockroach/commit/88be04bd64283b1d77000a3f88588e603465e81b

---

Release note (enterprise change): Remove the default
values from the SHOW CHANGEFEED JOB output`,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			result := getIssues(tc.pullCommit, tc.prNumber)
			assert.Equal(t, tc.issues, result)
		})
	}
}

func TestFormatReleaseNotes(t *testing.T) {
	testCases := []struct {
		prNum   string
		sha     string
		message string
		rns     []string
	}{
		{
			prNum: "79069",
			sha:   "5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
			message: `sql: ignore non-existent columns when injecting stats

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

---

Release note (sql change): ` + "`ALTER TABLE ... INJECT STATISTICS ...`" + ` will
now docsIssue notices when the given statistics JSON includes non-existent
columns, rather than resulting in an error. Any statistics in the JSON
for existing columns will be injected successfully.`},
		},
		{
			prNum: "79361",
			sha:   "88be04bd64283b1d77000a3f88588e603465e81b",
			message: `changefeedccl: remove the default values from SHOW
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
			message: `opt: do not cross-join input of semi-join

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
			message: `util/log: report the logging format at the start of new files

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
			message: `cli: report explicit log config in logs

This increases troubleshootability.

Release note: None`,
			rns: []string{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.prNum, func(t *testing.T) {
			prNumInt, _ := strconv.Atoi(tc.prNum)
			result := formatReleaseNotes(tc.message, prNumInt, tc.sha)
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
			testName: "Format Title 1",
			message: `sql: ignore non-existent columns when injecting stats

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
			prNumber:    12345,
			index:       1,
			totalLength: 1,
			title:       "PR #12345 - sql: ignore non-existent columns when injecting stats",
		},
		{
			testName: "Format Title 2",
			message: `changefeedccl: remove the default values from SHOW
CHANGEFEED JOB output

Currently, when a user alters a changefeed, we
include the default options in the SHOW CHANGEFEED
JOB output. In this PR we prevent the default values
from being displayed.

Release note (enterprise change): Remove the default
values from the SHOW CHANGEFEED JOB output`,
			prNumber:    54321,
			index:       1,
			totalLength: 1,
			title:       "PR #54321 - changefeedccl: remove the default values from SHOW",
		},
		{
			testName: "Format Title 3",
			message: `opt: do not cross-join input of semi-join

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
			prNumber:    65432,
			index:       1,
			totalLength: 1,
			title:       "PR #65432 - opt: do not cross-join input of semi-join",
		},
		{
			testName: "Format Title 4",
			message: `util/log: report the logging format at the start of new files

Release note (cli change): When log entries are written to disk,
the first few header lines written at the start of every new file
now report the configured logging format.`,
			prNumber:    23456,
			index:       1,
			totalLength: 1,
			title:       "PR #23456 - util/log: report the logging format at the start of new files",
		},
		{
			testName: "Format Title 5",
			message: `sql: ignore non-existent columns when injecting stats

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
			prNumber:    34567,
			index:       2,
			totalLength: 3,
			title:       "PR #34567 - sql: ignore non-existent columns when injecting stats (2 of 3)",
		},
		{
			testName: "Format Title 6",
			message: `changefeedccl: remove the default values from SHOW
CHANGEFEED JOB output

Currently, when a user alters a changefeed, we
include the default options in the SHOW CHANGEFEED
JOB output. In this PR we prevent the default values
from being displayed.

Release note (enterprise change): Remove the default
values from the SHOW CHANGEFEED JOB output`,
			prNumber:    76543,
			index:       1,
			totalLength: 6,
			title:       "PR #76543 - changefeedccl: remove the default values from SHOW (1 of 6)",
		},
		{
			testName: "Format Title 7",
			message: `opt: do not cross-join input of semi-join

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
` +
				"`SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.a = t2.a);`" + `,
2) the inner table has an index containing the equality column, like
` + "`t2.a`" + ` in the example query, 3) the index contains one or more
columns that prefix the equality column, and 4) the prefix columns are
` + "`NOT NULL`" + " and are constrained to a set of constant values via a " + "`CHECK`" + `
constraint or an ` + "`IN`" + " condition in the filter.",
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
