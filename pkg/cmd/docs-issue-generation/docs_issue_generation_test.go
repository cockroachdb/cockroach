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

func TestFormatReleaseNote(t *testing.T) {
	testCases := []struct {
		prNum   string
		sha     string
		message string
		rn      string
	}{
		{
			prNum:   "78685",
			sha:     "1d7811d5d14f9c7e106c3ec92de9c66192f19604",
			message: "opt: do not cross-join input of semi-join\n\nThis commit fixes a logical correctness bug caused when\n`GenerateLookupJoins` cross-joins the input of a semi-join with a set of\nconstant values to constrain the prefix columns of the lookup index. The\ncross-join is an invalid transformation because it increases the size of\nthe join's input and can increase the size of the join's output.\n\nWe already avoid these cross-joins for left and anti-joins (see #59646).\nWhen addressing those cases, the semi-join case was incorrectly assumed\nto be safe.\n\nFixes #78681\n\nRelease note (bug fix): A bug has been fixed which caused the optimizer\nto generate invalid query plans which could result in incorrect query\nresults. The bug, which has been present since version 21.1.0, can\nappear if all of the following conditions are true: 1) the query\ncontains a semi-join, such as queries in the form:\n`SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.a = t2.a);`,\n2) the inner table has an index containing the equality column, like\n`t2.a` in the example query, 3) the index contains one or more\ncolumns that prefix the equality column, and 4) the prefix columns are\n`NOT NULL` and are constrained to a set of constant values via a `CHECK`\nconstraint or an `IN` condition in the filter.",
			rn:      "",
		},
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
		t.Run(tc.message, func(t *testing.T) {
			prNumInt, _ := strconv.Atoi(tc.prNum)
			result := formatReleaseNote(tc.message, prNumInt, tc.sha)
			assert.Equal(t, tc.rn, result)
		})
	}
}

func TestFormatTitle(t *testing.T) {
	testCases := []struct {
		message string
		title   string
	}{

		{
			message: "sql: ignore non-existent columns when injecting stats\n\nPreviously, an `ALTER TABLE ... INJECT STATS` command would return an\nerror if the given stats JSON included any columns that were not present\nin the table descriptor. Statistics in statement bundles often include\ndropped columns, so reproducing issues with a bundle required tediously\nremoving stats for these columns. This commit changes the stats\ninjection behavior so that a notice is issued for stats with\nnon-existent columns rather than an error. Any stats for existing\ncolumns will be injected successfully.\n\nInforms #68184\n\nRelease note (sql change): `ALTER TABLE ... INJECT STATISTICS ...` will\nnow issue notices when the given statistics JSON includes non-existent\ncolumns, rather than resulting in an error. Any statistics in the JSON\nfor existing columns will be injected successfully.",
			title:   "sql: ignore non-existent columns when injecting stats",
		},
		{
			message: "changefeedccl: remove the default values from SHOW\nCHANGEFEED JOB output\n\nCurrently, when a user alters a changefeed, we\ninclude the default options in the SHOW CHANGEFEED\nJOB output. In this PR we prevent the default values\nfrom being displayed.\n\nRelease note (enterprise change): Remove the default\nvalues from the SHOW CHANGEFEED JOB output",
			title:   "changefeedccl: remove the default values from SHOW",
		},
		{
			message: "opt: do not cross-join input of semi-join\n\nThis commit fixes a logical correctness bug caused when\n`GenerateLookupJoins` cross-joins the input of a semi-join with a set of\nconstant values to constrain the prefix columns of the lookup index. The\ncross-join is an invalid transformation because it increases the size of\nthe join's input and can increase the size of the join's output.\n\nWe already avoid these cross-joins for left and anti-joins (see #59646).\nWhen addressing those cases, the semi-join case was incorrectly assumed\nto be safe.\n\nFixes #78681\n\nRelease note (bug fix): A bug has been fixed which caused the optimizer\nto generate invalid query plans which could result in incorrect query\nresults. The bug, which has been present since version 21.1.0, can\nappear if all of the following conditions are true: 1) the query\ncontains a semi-join, such as queries in the form:\n`SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.a = t2.a);`,\n2) the inner table has an index containing the equality column, like\n`t2.a` in the example query, 3) the index contains one or more\ncolumns that prefix the equality column, and 4) the prefix columns are\n`NOT NULL` and are constrained to a set of constant values via a `CHECK`\nconstraint or an `IN` condition in the filter.",
			title:   "opt: do not cross-join input of semi-join",
		},
		{
			message: "util/log: report the logging format at the start of new files\n\nRelease note (cli change): When log entries are written to disk,\nthe first few header lines written at the start of every new file\nnow report the configured logging format.",
			title:   "util/log: report the logging format at the start of new files",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.message, func(t *testing.T) {
			result := formatTitle(tc.message)
			assert.Equal(t, tc.title, result)
		})
	}
}
