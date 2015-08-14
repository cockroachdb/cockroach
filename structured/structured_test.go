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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package structured

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestAllocateIDs(t *testing.T) {
	defer leaktest.AfterTest(t)

	desc := TableDescriptor{
		ID:   1,
		Name: "foo",
		Columns: []ColumnDescriptor{
			{Name: "a"},
			{Name: "b"},
		},
		PrimaryIndex: IndexDescriptor{Name: "c", ColumnNames: []string{"a"}},
		Indexes: []IndexDescriptor{
			{Name: "d", ColumnNames: []string{"b", "a"}},
		},
		Privileges: NewDefaultDatabasePrivilegeDescriptor(),
	}
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}

	expected := TableDescriptor{
		ID:   1,
		Name: "foo",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "a"},
			{ID: 2, Name: "b"},
		},
		PrimaryIndex: IndexDescriptor{ID: 1, Name: "c", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"a"}},
		Indexes: []IndexDescriptor{
			{ID: 2, Name: "d", ColumnIDs: []ColumnID{2, 1}, ColumnNames: []string{"b", "a"}},
		},
		Privileges:   NewDefaultDatabasePrivilegeDescriptor(),
		NextColumnID: 3,
		NextIndexID:  3,
	}
	if !reflect.DeepEqual(expected, desc) {
		t.Fatalf("expected %+v, but found %+v", expected, desc)
	}
}

func TestValidateTableDesc(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		err  string
		desc TableDescriptor
	}{
		{`empty table name`,
			TableDescriptor{}},
		{`invalid table ID 0`,
			TableDescriptor{ID: 0, Name: "foo"}},
		{`table must contain at least 1 column`,
			TableDescriptor{ID: 1, Name: "foo"}},
		{`empty column name`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 0},
				},
				NextColumnID: 2,
			}},
		{`invalid column ID 0`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 0, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`table must contain a primary key`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`duplicate column name: "bar"`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`column "blah" duplicate ID of column "bar": 1`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "blah"},
				},
				NextColumnID: 2,
			}},
		{`table must contain a primary key`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				PrimaryIndex: IndexDescriptor{ID: 0},
				NextColumnID: 2,
			}},
		{`invalid index ID 0`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				PrimaryIndex: IndexDescriptor{ID: 0, Name: "bar", ColumnIDs: []ColumnID{0}},
				NextColumnID: 2,
			}},
		{`index "bar" must contain at least 1 column`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				Indexes: []IndexDescriptor{
					{ID: 2, Name: "bar"},
				},

				NextColumnID: 2,
				NextIndexID:  3,
			}},
		{`mismatched column IDs (1) and names (0)`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1}},
				NextColumnID: 2,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and names (2)`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "blah"},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar", "blah"}},
				NextColumnID: 3,
				NextIndexID:  2,
			}},
		{`duplicate index name: "bar"`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				Indexes: []IndexDescriptor{
					{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextIndexID:  2,
			}},
		{`index "blah" duplicate ID of index "bar": 1`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				Indexes: []IndexDescriptor{
					{ID: 1, Name: "blah", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextIndexID:  2,
			}},
		{`index "bar" column "bar" should have ID 1, but found ID 2`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{2}, ColumnNames: []string{"bar"}},
				NextColumnID: 2,
				NextIndexID:  2,
			}},
		{`index "bar" contains unknown column "blah"`,
			TableDescriptor{
				ID:   1,
				Name: "foo",
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"blah"}},
				NextColumnID: 2,
				NextIndexID:  2,
			}},
	}
	for i, d := range testData {
		if err := d.desc.Validate(); err == nil {
			t.Errorf("%d: expected \"%s\", but found success: %+v", i, d.err, d.desc)
		} else if d.err != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, d.err, err.Error())
		}
	}
}

func TestColumnTypeSQLString(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		colType     ColumnType
		expectedSQL string
	}{
		{ColumnType{Kind: ColumnType_BIT}, "BIT"},
		{ColumnType{Kind: ColumnType_BIT, Width: 1}, "BIT(1)"},
		{ColumnType{Kind: ColumnType_INT}, "INT"},
		{ColumnType{Kind: ColumnType_INT, Width: 2}, "INT(2)"},
		{ColumnType{Kind: ColumnType_FLOAT}, "FLOAT"},
		{ColumnType{Kind: ColumnType_FLOAT, Precision: 3}, "FLOAT(3)"},
		{ColumnType{Kind: ColumnType_DECIMAL}, "DECIMAL"},
		{ColumnType{Kind: ColumnType_DECIMAL, Precision: 6}, "DECIMAL(6)"},
		{ColumnType{Kind: ColumnType_DECIMAL, Precision: 7, Width: 8}, "DECIMAL(7,8)"},
		{ColumnType{Kind: ColumnType_DATE}, "DATE"},
		{ColumnType{Kind: ColumnType_TIME}, "TIME"},
		{ColumnType{Kind: ColumnType_TIMESTAMP}, "TIMESTAMP"},
		{ColumnType{Kind: ColumnType_CHAR}, "CHAR"},
		{ColumnType{Kind: ColumnType_CHAR, Width: 10}, "CHAR(10)"},
		{ColumnType{Kind: ColumnType_TEXT}, "TEXT"},
		{ColumnType{Kind: ColumnType_BLOB}, "BLOB"},
	}
	for i, d := range testData {
		sql := d.colType.SQLString()
		if d.expectedSQL != sql {
			t.Errorf("%d: expected %s, but got %s", i, d.expectedSQL, sql)
		}
	}
}
