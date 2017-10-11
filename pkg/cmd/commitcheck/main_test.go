// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func TestCommitMessages(t *testing.T) {
	testCases := []struct{ message, err string }{
		{"server,base,storage: frobulate", ""},
		{"github-pull-request-make: paginate results\n\nfoo", ""},
		{"distsqlrun: properly account for top-k sorting memory", ""},
		{"sql/distsqlrun: do not call Finish on a nil span", ""},
		{"base,engine: don't accept StoreSpec.SizeInBytes for temp engines", ""},
		{"cache: cacheStore.del(key interface{}) -> cacheStore.del(e *Entry)", ""},
		{"storage: bring comments on TestLeaseExtensionNotBlockedByRead up to date", ""},
		{".gitattributes: mark help_messages.go to avoid diffs", ""},
		{"RFCS: fix decomissioning RFC's markdown rendering", ""},
		{"CODEOWNERS: avoid @cockroachdb/sql ownership", ""},
		{"azworker: persistent SSH sessions", ""},
		{".gitignore: remove stale entries", ""},
		{"*: force zone.o to link on macOS", ""},
		{"vendor: bump etcd/raft", ""},
		{`Revert "scripts: point pprof at binary"`, ""},

		{
			"",
			"missing summary",
		},
		{
			"\n\n",
			"missing summary",
		},
		{
			"do some stuff",
			"summary does not match format `scope\\[,scope\\]: message`",
		},
		{
			"storage, server: don't reuse leases obtained before a restart",
			`scope "server" has extraneous whitespace`,
		},
		{
			"server:",
			"missing text after colon",
		},
		{
			"storage:    ",
			`missing text after colon`,
		},
		{
			"build: Update instructions for building a deployable docker image",
			"text after colon in summary should not begin with capital letter",
		},
		{
			"distsql: increase intermediate precision of decimal calculations in square difference calculations",
			"line 1 exceeds maximum line length of 72 characters",
		},
		{
			"distsql: summary is fine\nbut blank line is missing",
			"missing blank line after summary",
		},
		{
			"distsql: summary is fine\n\nbut the extended description is really much too long whoever wrote this commit message needs to add a line break",
			"line 3 exceeds maximum line length of 72 characters",
		},
		{
			": foo",
			`unknown scope ""`,
		},
		{
			"server,: foo",
			`unknown scope ""`,
		},
		{
			"lint: forbid golang.org/x/sync/singleflight import",
			`unknown scope "lint"`,
		},
		{
			"sql+cli: fix UUID column dumping",
			`unknown scope "sql\+cli"`,
		},
		{
			"all: require go 1.9",
			`unknown scope "all"`,
		},
		{
			"sql/parser: produce context-sensitive help during error recovery.",
			"summary should not end in period",
		},
	}

	for _, testCase := range testCases {
		err := checkCommitMessage(testCase.message)
		if !testutils.IsError(err, testCase.err) {
			errDisplay := "<nil>"
			if err != nil {
				errDisplay = fmt.Sprintf("%q", err)
			}
			t.Errorf(
				"%q:\nexpected error matching %q, but got %s",
				testCase.message, testCase.err, errDisplay,
			)
		}
	}
}
