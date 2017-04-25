// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDescriptorsMatchingTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()

	descriptors := []sqlbase.Descriptor{
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{ID: 0, Name: "system"}),
		*sqlbase.WrapDescriptor(&sqlbase.TableDescriptor{ID: 1, Name: "foo", ParentID: 0}),
		*sqlbase.WrapDescriptor(&sqlbase.TableDescriptor{ID: 2, Name: "bar", ParentID: 0}),
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{ID: 3, Name: "data"}),
		*sqlbase.WrapDescriptor(&sqlbase.TableDescriptor{ID: 4, Name: "baz", ParentID: 3}),
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{ID: 5, Name: "empty"}),
	}

	tests := []struct {
		sessionDatabase string
		pattern         string
		expected        []string
		err             string
	}{
		{"", "DATABASE system", []string{"system", "foo", "bar"}, ``},
		{"", "DATABASE system, noexist", nil, `database "noexist" does not exist`},
		{"", "DATABASE data", []string{"data", "baz"}, ``},
		{"", "DATABASE system, data", []string{"system", "foo", "bar", "data", "baz"}, ``},
		{"", "DATABASE system, data, noexist", nil, `database "noexist" does not exist`},
		{"system", "DATABASE system", []string{"system", "foo", "bar"}, ``},
		{"system", "DATABASE system, noexist", nil, `database "noexist" does not exist`},
		{"system", "DATABASE data", []string{"data", "baz"}, ``},
		{"system", "DATABASE system, data", []string{"system", "foo", "bar", "data", "baz"}, ``},
		{"system", "DATABASE system, data, noexist", nil, `database "noexist" does not exist`},

		{"", "TABLE foo", nil, `table "foo" does not exist`},
		{"system", "TABLE foo", []string{"system", "foo"}, ``},
		{"data", "TABLE foo", nil, `table "foo" does not exist`},

		{"", "TABLE *", nil, `no database specified for wildcard`},
		{"", "TABLE *, system.foo", nil, `no database specified for wildcard`},
		{"noexist", "TABLE *", nil, `database "noexist" does not exist`},
		{"system", "TABLE *", []string{"system", "foo", "bar"}, ``},
		{"data", "TABLE *", []string{"data", "baz"}, ``},
		{"empty", "TABLE *", []string{"empty"}, ``},

		{"", "TABLE foo, baz", nil, `table "(foo|baz)" does not exist`},
		{"system", "TABLE foo, baz", nil, `table "baz" does not exist`},
		{"data", "TABLE foo, baz", nil, `table "foo" does not exist`},

		{"", "TABLE system.foo", []string{"system", "foo"}, ``},
		{"", "TABLE system.foo, foo", []string{"system", "foo"}, `table "foo" does not exist`},

		{"", "TABLE system.foo, bar", []string{"system", "foo"}, `table "bar" does not exist`},
		{"system", "TABLE system.foo, bar", []string{"system", "foo", "bar"}, ``},

		{"", "TABLE noexist.*", nil, `database "noexist" does not exist`},
		{"", "TABLE empty.*", []string{"empty"}, ``},
		{"", "TABLE system.*", []string{"system", "foo", "bar"}, ``},
		{"", "TABLE system.*, foo, baz", nil, `table "(foo|baz)" does not exist`},
		{"system", "TABLE system.*, foo, baz", nil, `table "baz" does not exist`},
		{"data", "TABLE system.*, baz", []string{"system", "foo", "bar", "data", "baz"}, ``},
		{"data", "TABLE system.*, foo, baz", nil, `table "(foo|baz)" does not exist`},

		{"", "TABLE SyStEm.FoO", []string{"system", "foo"}, ``},

		{"", `TABLE system."foo"`, []string{"system", "foo"}, ``},
		{"system", `TABLE "foo"`, []string{"system", "foo"}, ``},
		// TODO(dan): Enable these tests once #8862 is fixed.
		// {"", `TABLE system."FOO"`, []string{"system"}},
		// {"system", `TABLE "FOO"`, []string{"system"}},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			sql := fmt.Sprintf(`GRANT ALL ON %s TO ignored`, test.pattern)
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			targets := stmt.(*parser.Grant).Targets

			matched, err := descriptorsMatchingTargets(test.sessionDatabase, descriptors, targets)
			if test.err != "" {
				if !testutils.IsError(err, test.err) {
					t.Fatalf("expected error matching '%v', but got '%v'", test.err, err)
				}
			} else if err != nil {
				t.Fatal(err)
			} else {
				var matchedNames []string
				for _, m := range matched {
					matchedNames = append(matchedNames, m.GetName())
				}
				sort.Strings(test.expected)
				sort.Strings(matchedNames)
				if !reflect.DeepEqual(test.expected, matchedNames) {
					t.Fatalf("expected %q got %q", test.expected, matchedNames)
				}
			}
		})
	}
}
