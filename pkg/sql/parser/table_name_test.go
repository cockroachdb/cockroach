// Copyright 2015 The Cockroach Authors.
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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func (tn *TableName) resetRepr() {
	tn.DBNameOriginallyOmitted = false
}

func TestNormalizeTableName(t *testing.T) {
	testCases := []struct {
		in, out string
		db      string
		err     string
	}{
		{`foo`, `test.foo`, `test`, ``},
		{`test.foo`, `test.foo`, ``, ``},
		{`bar.foo`, `bar.foo`, `test`, ``},

		{`""`, ``, ``, `empty table name`},
		{`foo`, ``, ``, `no database specified`},
		{`foo@bar`, ``, ``, `syntax error`},
		{`test.foo.bar`, ``, ``, `invalid table name: "test.foo.bar"`},
		{`test.*`, ``, ``, `invalid table name: "test.*"`},
	}

	for _, tc := range testCases {
		tn, err := ParseTableName(tc.in)
		if err == nil {
			err = tn.QualifyWithDatabase(tc.db)
		}
		if tc.err != "" {
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: expected success, but found %v", tc.in, err)
		}
		tn.resetRepr()
		if out := tn.String(); tc.out != out {
			t.Errorf("%s: expected %s, but found %s", tc.in, tc.out, out)
		}
	}
}
