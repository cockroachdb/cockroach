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

package tree_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func resetRepr(tn *tree.TableName) {
	tn.OmitDBNameDuringFormatting = false
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
		{`p.foo.bar`, `p.foo.bar`, ``, ``},

		{`""`, ``, ``, `empty table name`},
		{`foo`, ``, ``, `no database specified`},
		{`foo@bar`, ``, ``, `syntax error`},
		{`test.*`, ``, ``, `invalid table name: "test\.\*"`},
		{`p."".bar`, ``, ``, `empty database name: "p\.\.bar"`},
	}

	for _, tc := range testCases {
		tn, err := func() (*tree.TableName, error) {
			stmt, err := parser.ParseOne(fmt.Sprintf("ALTER TABLE %s RENAME TO x", tc.in))
			if err != nil {
				return nil, err
			}
			tn, err := stmt.(*tree.RenameTable).Name.Normalize()
			if err != nil {
				return nil, err
			}
			err = tn.QualifyWithDatabase(tc.db)
			return tn, err
		}()
		if !testutils.IsError(err, tc.err) {
			t.Errorf("%s: expected %s, but found %s", tc.in, tc.err, err.Error())
		}
		if tc.err != "" {
			continue
		}
		resetRepr(tn)
		if out := tn.String(); tc.out != out {
			t.Errorf("%s: expected %s, but found %s", tc.in, tc.out, out)
		}
	}
}
