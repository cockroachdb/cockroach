// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cat_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestExpandDataSourceGlob(t *testing.T) {
	testcat := testcat.New()
	ctx := context.Background()

	exec := func(sql string) {
		if _, err := testcat.ExecuteDDL(sql); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE a (x INT)")
	exec("CREATE TABLE b (x INT)")
	exec("CREATE TABLE c (x INT)")

	testCases := []struct {
		pattern  tree.TablePattern
		expected string
	}{
		{
			pattern:  tree.NewTableName("t", "a"),
			expected: `[t.public.a]`,
		},
		{
			pattern:  tree.NewTableName("t", "z"),
			expected: `error: no data source matches prefix: "t.public.z"`,
		},
		{
			pattern:  &tree.AllTablesSelector{ObjectNamePrefix: tree.ObjectNamePrefix{}},
			expected: `[t.public.a t.public.b t.public.c]`,
		},
		{
			pattern: &tree.AllTablesSelector{ObjectNamePrefix: tree.ObjectNamePrefix{
				SchemaName: "t", ExplicitSchema: true,
			}},
			expected: `[t.public.a t.public.b t.public.c]`,
		},
		{
			pattern: &tree.AllTablesSelector{ObjectNamePrefix: tree.ObjectNamePrefix{
				SchemaName: "z", ExplicitSchema: true,
			}},
			expected: `error: target database or schema does not exist`,
		},
	}

	for _, tc := range testCases {
		var res string
		names, err := cat.ExpandDataSourceGlob(ctx, testcat, cat.Flags{}, tc.pattern)
		if err != nil {
			res = fmt.Sprintf("error: %v", err)
		} else {
			var r []string
			for _, n := range names {
				r = append(r, n.FQString())
			}
			res = fmt.Sprintf("%v", r)
		}
		if res != tc.expected {
			t.Errorf("pattern: %v  expected: %s  got: %s", tc.pattern, tc.expected, res)
		}
	}
}

func TestResolveTableIndex(t *testing.T) {
	testcat := testcat.New()
	ctx := context.Background()

	exec := func(sql string) {
		if _, err := testcat.ExecuteDDL(sql); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE a (x INT, INDEX idx1(x))")
	exec("CREATE TABLE b (x INT, INDEX idx2(x))")
	exec("CREATE TABLE c (x INT, INDEX idx2(x))")

	testCases := []struct {
		name     tree.TableIndexName
		expected string
	}{
		// Both table name and index are set.
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", "a"),
				Index: "idx1",
			},
			expected: `t.public.a@idx1`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", "a"),
				Index: "idx2",
			},
			expected: `error: index "idx2" does not exist`,
		},

		// Only table name is set.
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", "a"),
			},
			expected: `t.public.a@primary`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("z", "a"),
			},
			expected: `error: no data source matches prefix: "z.public.a"`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", "z"),
			},
			expected: `error: no data source matches prefix: "t.public.z"`,
		},

		// Only index name is set.
		{
			name: tree.TableIndexName{
				Index: "idx1",
			},
			expected: `t.public.a@idx1`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("t", ""),
				Index: "idx1",
			},
			expected: `t.public.a@idx1`,
		},
		{
			name: tree.TableIndexName{
				Table: func() tree.TableName {
					var t tree.TableName
					t.SchemaName = "public"
					t.ExplicitSchema = true
					return t
				}(),
				Index: "idx1",
			},
			expected: `t.public.a@idx1`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableName("z", ""),
				Index: "idx1",
			},
			expected: `error: target database or schema does not exist`,
		},
		{
			name: tree.TableIndexName{
				Index: "idx2",
			},
			expected: `error: index name "idx2" is ambiguous (found in t.public.c and t.public.b)`,
		},
	}

	for _, tc := range testCases {
		var res string
		idx, tn, err := cat.ResolveTableIndex(ctx, testcat, cat.Flags{}, &tc.name)
		if err != nil {
			res = fmt.Sprintf("error: %v", err)
		} else {
			res = fmt.Sprintf("%s@%s", tn.FQString(), idx.Name())
		}
		if res != tc.expected {
			t.Errorf("pattern: %v  expected: %s  got: %s", tc.name.String(), tc.expected, res)
		}
	}
}
