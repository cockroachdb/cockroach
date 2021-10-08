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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
		pattern       tree.TablePattern
		expectedNames string
		expectedIDs   string
		expectedError string
	}{
		{
			pattern:       tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "a"),
			expectedNames: `[t.public.a]`,
			expectedIDs:   `[53]`,
		},
		{
			pattern:       tree.NewTableNameWithSchema("t", tree.PublicSchemaName, "z"),
			expectedError: `error: no data source matches prefix: "t.public.z"`,
		},
		{
			pattern:       &tree.AllTablesSelector{ObjectNamePrefix: tree.ObjectNamePrefix{}},
			expectedNames: `[t.public.a t.public.b t.public.c]`,
			expectedIDs:   `[53 54 55]`,
		},
		{
			pattern: &tree.AllTablesSelector{ObjectNamePrefix: tree.ObjectNamePrefix{
				SchemaName: "t", ExplicitSchema: true,
			}},
			expectedNames: `[t.public.a t.public.b t.public.c]`,
			expectedIDs:   `[53 54 55]`,
		},
		{
			pattern: &tree.AllTablesSelector{ObjectNamePrefix: tree.ObjectNamePrefix{
				SchemaName: "z", ExplicitSchema: true,
			}},
			expectedError: `error: target database or schema does not exist`,
		},
	}

	for _, tc := range testCases {
		var namesRes string
		var errRes string
		var IDsRes string
		names, IDs, err := cat.ExpandDataSourceGlob(ctx, testcat, cat.Flags{}, tc.pattern)
		if err != nil {
			errRes = fmt.Sprintf("error: %v", err)
		} else {
			var namesArr []string
			var IDsArr []descpb.ID
			for i := range names {
				namesArr = append(namesArr, names[i].FQString())
				IDsArr = append(IDsArr, IDs[i])
			}
			namesRes = fmt.Sprintf("%v", namesArr)
			IDsRes = fmt.Sprintf("%v", IDsArr)
		}
		if len(tc.expectedError) > 0 && errRes != tc.expectedError {
			t.Errorf("pattern: %v  expectedError: %s  got: %s", tc.pattern, tc.expectedError, errRes)
		}
		if len(tc.expectedNames) > 0 && namesRes != tc.expectedNames {
			t.Errorf("pattern: %v  expectedNames: %s  got: %s", tc.pattern, tc.expectedNames, namesRes)
		}
		if len(tc.expectedIDs) > 0 && IDsRes != tc.expectedIDs {
			t.Errorf("pattern: %v  expectedIDs: %s  got: %s", tc.pattern, tc.expectedIDs, IDsRes)
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
				Table: tree.MakeTableNameWithSchema("t", tree.PublicSchemaName, "a"),
				Index: "idx1",
			},
			expected: `t.public.a@idx1`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableNameWithSchema("t", tree.PublicSchemaName, "a"),
				Index: "idx2",
			},
			expected: `error: index "idx2" does not exist`,
		},

		// Only table name is set.
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableNameWithSchema("t", tree.PublicSchemaName, "a"),
			},
			expected: `t.public.a@primary`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableNameWithSchema("z", tree.PublicSchemaName, "a"),
			},
			expected: `error: no data source matches prefix: "z.public.a"`,
		},
		{
			name: tree.TableIndexName{
				Table: tree.MakeTableNameWithSchema("t", tree.PublicSchemaName, "z"),
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
				Table: tree.MakeTableNameWithSchema("t", tree.PublicSchemaName, ""),
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
				Table: tree.MakeTableNameWithSchema("z", tree.PublicSchemaName, ""),
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
