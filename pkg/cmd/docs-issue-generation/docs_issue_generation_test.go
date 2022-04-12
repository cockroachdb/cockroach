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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var token string = defaultEnvParameters().Token

func TestPrNums(t *testing.T) {
	testCases := []struct {
		sha string
		prs []pr
	}{
		{
			sha: "1d7811d5d14f9c7e106c3ec92de9c66192f19604",
			prs: []pr{{number: 78685}},
		},
		{
			sha: "5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
			prs: []pr{{number: 79069}},
		},
		{
			sha: "88be04bd64283b1d77000a3f88588e603465e81b",
			prs: []pr{{number: 79361}},
		},
		{
			sha: "4dd8da9609adb3acce6795cea93b67ccacfc0270",
			prs: []pr{{number: 66328}},
		},
		{
			sha: `964b5b4d70f058a612b7f1ea25e292740b199139`,
			prs: []pr{{number: 64255}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.sha, func(t *testing.T) {
			params := parameters{
				Sha:   tc.sha,
				Token: token,
			}
			result := prNums(params)
			assert.Equal(t, tc.prs, result)
		})
	}
}

func TestSetMergeBranch(t *testing.T) {
	testCases := []struct {
		prNum       string
		mergeBranch string
	}{
		{
			prNum:       "78685",
			mergeBranch: "master",
		},
		{
			prNum:       "79069",
			mergeBranch: "release-21.1",
		},
		{
			prNum:       "79361",
			mergeBranch: "release-22.1",
		},
		{
			prNum:       "66328",
			mergeBranch: "release-21.1",
		},
		{
			prNum:       "64255",
			mergeBranch: "release-21.1",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.prNum, func(t *testing.T) {
			prNumInt, _ := strconv.Atoi(tc.prNum)
			pr := pr{number: prNumInt}
			result := setMergeBranch(pr, token)
			assert.Equal(t, pr, result)
		})
	}
}

func TestSetCommits(t *testing.T) {
	testCases := []struct {
		prNum   string
		commits []commit
	}{
		{
			prNum:   "78685",
			commits: []commit{},
		},
		{
			prNum: "79069",
			commits: []commit{{
				sha:         "5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9",
				title:       "sql: ignore non-existent columns when injecting stats",
				releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/79069\nCommit: https://github.com/cockroachdb/cockroach/commit/5ec9343b0e0a00bfd4603e55ca6533e2b77db2f9\n\n---\n\nRelease note (sql change): `ALTER TABLE ... INJECT STATISTICS ...` will\nnow issue notices when the given statistics JSON includes non-existent\ncolumns, rather than resulting in an error. Any statistics in the JSON\nfor existing columns will be injected successfully.",
			}},
		},
		{
			prNum: "79361",
			commits: []commit{{
				sha:         "88be04bd64283b1d77000a3f88588e603465e81b",
				title:       "changefeedccl: remove the default values from SHOW",
				releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/79361\nCommit: https://github.com/cockroachdb/cockroach/commit/88be04bd64283b1d77000a3f88588e603465e81b\n\n---\n\nRelease note (enterprise change): Remove the default\nvalues from the SHOW CHANGEFEED JOB output",
			}},
		},
		{
			prNum: "66328",
			commits: []commit{{
				sha:         "0f329965acccb3e771ec1657c7def9e881dc78bb",
				title:       "util/log: report the logging format at the start of new files",
				releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/66328\nCommit: https://github.com/cockroachdb/cockroach/commit/0f329965acccb3e771ec1657c7def9e881dc78bb\n\n---\n\nRelease note (cli change): When log entries are written to disk,\nthe first few header lines written at the start of every new file\nnow report the configured logging format.",
			},
				{
					sha:         "44836265f924a14f8c996a714d954e0e7e35dff7",
					title:       "util/log,server/debug: new API `/debug/vmodule`, change `logspy`",
					releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/66328\nCommit: https://github.com/cockroachdb/cockroach/commit/44836265f924a14f8c996a714d954e0e7e35dff7\n\n---\n\nRelease note (api change): The `/debug/logspy` API does not any more\nenable maximum logging verbosity automatically. To change the\nverbosity, use the new `/debug/vmodule` endpoint or pass the\n`&vmodule=` query parameter to the `/debug/logspy` endpoint.\n\nFor example, suppose you wish to run a 20s logspy session:\n\n- Before:\n\n  ```\n  curl 'https://.../debug/logspy?duration=20s&...'\n  ```\n\n- Now:\n\n  ```\n  curl 'https://.../debug/logspy?duration=20s&vmodule=...'\n  ```\n\n  OR\n\n  ```\n  curl 'https://.../debug/vmodule?duration=22s&vmodule=...'\n  curl 'https://.../debug/logspy?duration=20s'\n  ```\n\nAs for the regular `vmodule` command-line flag, the maximum verbosity\nacross all the source code can be selected with the pattern `*=4`.\n\nNote: at most one in-flight HTTP API request is allowed to modify the\n`vmodule` parameter. This maintain the invariant that the\nconfiguration restored at the end of each request is the same as when\nthe request started.\n\nRelease note (api change): The new `/debug/vmodule` API makes it\npossible for an operator to configure the logging verbosity in a\nsimilar way as the SQL built-in function\n`crdb_internal.set_vmodule()`, or to query the current configuration\nas in `crdb_internal.get_vmodule()`. Additionally, any configuration\nchange performed via this API can be automatically reverted after a\nconfigurable delay. The API forms are:\n\n- `/debug/vmodule` - retrieve the current configuration.\n- `/debug/vmodule?set=[vmodule config]&duration=[duration]` - change\n  the configuration to `[vmodule config]` . The previous configuration\n  at the time the `/debug/vmodule` request started is restored after\n  `[duration]`. This duration, if not specified, defaults to twice the\n  default duration of a `logspy` request (currently, the `logspy`\n  default duration is 5s, so the `vmodule` default duration is 10s).\n  If the duration is zero or negative, the previous configuration\n  is never restored.",
				}},
		},
		{
			prNum: "64255",
			commits: []commit{{
				sha:         "a560c9c753457511a1f491cef52d697282191a6c",
				title:       "sql: disallow ADD/DROP REGION during certain REGIONAL BY ROW ops",
				releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/64255\nCommit: https://github.com/cockroachdb/cockroach/commit/a560c9c753457511a1f491cef52d697282191a6c\n\n---\n\nRelease note (sql change): Disallow ADD/DROP REGION whilst a\nREGIONAL BY ROW table has index changes underway or a table is\ntransitioning to or from REGIONAL BY ROW.",
			},
				{
					sha:         "964b5b4d70f058a612b7f1ea25e292740b199139",
					title:       "sql: prevent certain operations if regions are being added or dropped",
					releaseNote: "Related PR: https://github.com/cockroachdb/cockroach/pull/64255\nCommit: https://github.com/cockroachdb/cockroach/commit/964b5b4d70f058a612b7f1ea25e292740b199139\n\n---\n\nRelease note (sql change): Prevent index modification on REGIONAL BY ROW\ntables and locality to/from REGIONAL BY ROW changes during a ADD/DROP\nREGION.",
				}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.prNum, func(t *testing.T) {
			prNumInt, _ := strconv.Atoi(tc.prNum)
			pr := pr{number: prNumInt}
			result := setCommits(pr, token)
			assert.Equal(t, pr, result)
		})
	}
}
