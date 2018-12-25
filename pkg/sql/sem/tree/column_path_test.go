// Copyright 2018 The Cockroach Authors.
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

package tree

import "testing"

func TestMakeColumnPath(t *testing.T) {
	catalog := "foo"
	schema := "bar"
	table := "baz"
	column := "qux"

	testCases := []struct {
		NumParts int
		Parts    NameParts
	}{
		{
			NumParts: 1,
			Parts:    NameParts{column},
		},
		{
			NumParts: 2,
			Parts:    NameParts{column, table},
		},
		{
			NumParts: 3,
			Parts:    NameParts{column, table, schema},
		},
		{
			NumParts: 4,
			Parts:    NameParts{column, table, schema, catalog},
		},
	}

	for _, tc := range testCases {
		un := &UnresolvedName{NumParts: tc.NumParts, Parts: tc.Parts}
		cp := makeColumnPathFromUnresolvedName(un)

		if cp.Column.String() != column {
			t.Errorf("expected cp.ColumnPath == %s, got %s", column, cp.Column)
		}

		if 2 <= tc.NumParts {
			if cp.TableName.String() != table {
				t.Errorf("expected cp.TableName == %s, got %s", table, cp.TableName)
			}
		}

		if 3 <= tc.NumParts {
			if cp.SchemaName.String() != schema {
				t.Errorf("expected cp.SchemaName == %s, got %s", schema, cp.SchemaName)
			}
		}

		if 4 <= tc.NumParts {
			if cp.CatalogName.String() != catalog {
				t.Errorf("expected cp.CatalogName == %s, got %s", catalog, cp.CatalogName)
			}
		}
	}
}

func TestMakeTableName(t *testing.T) {
	catalog := "foo"
	schema := "bar"
	table := "baz"
	column := "qux"

	un := &UnresolvedName{NumParts: 4, Parts: NameParts{column, table, schema, catalog}}
	cp := makeColumnPathFromUnresolvedName(un)
	tn := cp.MakeTableName()
	if cp.TableName != tn.TableName ||
		cp.CatalogName != tn.CatalogName ||
		cp.ExplicitCatalog != tn.ExplicitCatalog ||
		cp.SchemaName != tn.SchemaName ||
		cp.ExplicitSchema != tn.ExplicitSchema {
		t.Errorf("TableName is different ColumnPath %v, TableName %v", cp, tn)
	}
}
