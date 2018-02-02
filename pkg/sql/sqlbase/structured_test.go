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

package sqlbase

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Makes an IndexDescriptor with all columns being ascending.
func makeIndexDescriptor(name string, columnNames []string) IndexDescriptor {
	dirs := make([]IndexDescriptor_Direction, 0, len(columnNames))
	for range columnNames {
		dirs = append(dirs, IndexDescriptor_ASC)
	}
	idx := IndexDescriptor{
		ID:               IndexID(0),
		Name:             name,
		ColumnNames:      columnNames,
		ColumnDirections: dirs,
	}
	return idx
}

func TestAllocateIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := TableDescriptor{
		ID:       keys.MaxReservedDescID + 2,
		ParentID: keys.MaxReservedDescID + 1,
		Name:     "foo",
		Columns: []ColumnDescriptor{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		},
		PrimaryIndex: makeIndexDescriptor("c", []string{"a", "b"}),
		Indexes: []IndexDescriptor{
			makeIndexDescriptor("d", []string{"b", "a"}),
			makeIndexDescriptor("e", []string{"b"}),
		},
		Privileges:    NewDefaultPrivilegeDescriptor(),
		FormatVersion: FamilyFormatVersion,
	}
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}

	expected := TableDescriptor{
		ID:       keys.MaxReservedDescID + 2,
		ParentID: keys.MaxReservedDescID + 1,
		Version:  1,
		Name:     "foo",
		Columns: []ColumnDescriptor{
			{ID: 1, Name: "a"},
			{ID: 2, Name: "b"},
			{ID: 3, Name: "c"},
		},
		Families: []ColumnFamilyDescriptor{
			{
				ID: 0, Name: "primary",
				ColumnNames:     []string{"a", "b", "c"},
				ColumnIDs:       []ColumnID{1, 2, 3},
				DefaultColumnID: 3,
			},
		},
		PrimaryIndex: IndexDescriptor{
			ID: 1, Name: "c", ColumnIDs: []ColumnID{1, 2},
			ColumnNames: []string{"a", "b"},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC,
				IndexDescriptor_ASC}},
		Indexes: []IndexDescriptor{
			{ID: 2, Name: "d", ColumnIDs: []ColumnID{2, 1}, ColumnNames: []string{"b", "a"},
				ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC,
					IndexDescriptor_ASC}},
			{ID: 3, Name: "e", ColumnIDs: []ColumnID{2}, ColumnNames: []string{"b"},
				ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				ExtraColumnIDs:   []ColumnID{1}},
		},
		Privileges:     NewDefaultPrivilegeDescriptor(),
		NextColumnID:   4,
		NextFamilyID:   1,
		NextIndexID:    4,
		NextMutationID: 1,
		FormatVersion:  FamilyFormatVersion,
	}
	if !reflect.DeepEqual(expected, desc) {
		a, _ := json.MarshalIndent(expected, "", "  ")
		b, _ := json.MarshalIndent(desc, "", "  ")
		t.Fatalf("expected %s, but found %s", a, b)
	}

	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, desc) {
		a, _ := json.MarshalIndent(expected, "", "  ")
		b, _ := json.MarshalIndent(desc, "", "  ")
		t.Fatalf("expected %s, but found %s", a, b)
	}
}

func TestValidateTableDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		err  string
		desc TableDescriptor
	}{
		{`empty table name`,
			TableDescriptor{}},
		{`invalid table ID 0`,
			TableDescriptor{ID: 0, Name: "foo"}},
		{`invalid parent ID 0`,
			TableDescriptor{ID: 2, Name: "foo"}},
		{`table "foo" is encoded using using version 0, but this client only supports version 2 and 3`,
			TableDescriptor{ID: 2, ParentID: 1, Name: "foo"}},
		{`table must contain at least 1 column`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
			}},
		{`empty column name`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 0},
				},
				NextColumnID: 2,
			}},
		{`invalid column ID 0`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 0, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`table must contain a primary key`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`duplicate column name: "bar"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`column "blah" duplicate ID of column "bar": 1`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "blah"},
				},
				NextColumnID: 2,
			}},
		{`at least 1 column family must be specified`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`the 0th family must have ID 0`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 1},
				},
				NextColumnID: 2,
			}},
		{`duplicate family name: "baz"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 1, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`family "qux" duplicate ID of family "baz": 0`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 0, Name: "qux"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`duplicate family name: "baz"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 3, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`mismatched column ID size (1) and name size (0)`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []ColumnID{1}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" contains unknown column "2"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []ColumnID{2}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" column 1 should have name "bar", but found name "qux"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"qux"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column 1 is not in any column family`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column 1 is in both family 0 and 1`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
					{ID: 1, Name: "qux", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`primary key column 1 is not in column family 0`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 1, Name: "qux", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "quux",
					ColumnIDs:        []ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
				NextIndexID:  2,
			}},
		{`table must contain a primary key`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{
					ID:               0,
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`invalid index ID 0`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 0, Name: "bar",
					ColumnIDs:        []ColumnID{0},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`index "bar" must contain at least 1 column`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				Indexes: []IndexDescriptor{
					{ID: 2, Name: "bar"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`mismatched column IDs (1) and names (0)`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1}},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and names (2)`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "blah"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1, 2}, ColumnNames: []string{"bar", "blah"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar", "blah"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`duplicate index name: "bar"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				Indexes: []IndexDescriptor{
					{ID: 2, Name: "bar", ColumnIDs: []ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "blah" duplicate ID of index "bar": 1`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				Indexes: []IndexDescriptor{
					{ID: 1, Name: "blah", ColumnIDs: []ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" column "bar" should have ID 1, but found ID 2`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{2},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" contains unknown column "blah"`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1},
					ColumnNames:      []string{"blah"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and directions (0)`,
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []ColumnID{1},
					ColumnNames: []string{"blah"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`at least one of LIST or RANGE partitioning must be used`,
			// Verify that validatePartitioning is hooked up. The rest of these
			// tests are in TestValidatePartitionion.
			TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: FamilyFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
	}
	for i, d := range testData {
		if err := d.desc.ValidateTable(); err == nil {
			t.Errorf("%d: expected \"%s\", but found success: %+v", i, d.err, d.desc)
		} else if d.err != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%+v\"", i, d.err, err)
		}
	}
}

func TestValidateCrossTableReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	tests := []struct {
		err        string
		desc       TableDescriptor
		referenced []TableDescriptor
	}{
		// Foreign keys
		{
			err: `invalid foreign key: missing table=52 index=2: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:         1,
					ForeignKey: ForeignKeyReference{Table: 52, Index: 2},
				},
			},
			referenced: nil,
		},
		{
			err: `invalid foreign key: missing table=baz index=2: index-id "2" does not exist`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:         1,
					ForeignKey: ForeignKeyReference{Table: 52, Index: 2},
				},
			},
			referenced: []TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `missing fk back reference to "foo"@"bar" from "baz"@"qux"`,
			desc: TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: IndexDescriptor{
					ID:         1,
					Name:       "bar",
					ForeignKey: ForeignKeyReference{Table: 52, Index: 2},
				},
			},
			referenced: []TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{
			err: `invalid fk backreference table=52 index=2: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:           1,
					ReferencedBy: []ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
		},
		{
			err: `invalid fk backreference table=baz index=2: index-id "2" does not exist`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:           1,
					ReferencedBy: []ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			referenced: []TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `broken fk backward reference from "foo"@"bar" to "baz"@"qux"`,
			desc: TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: IndexDescriptor{
					ID:           1,
					Name:         "bar",
					ReferencedBy: []ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			referenced: []TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},

		// Interleaves
		{
			err: `invalid interleave: missing table=52 index=2: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID: 1,
					Interleave: InterleaveDescriptor{Ancestors: []InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			referenced: nil,
		},
		{
			err: `invalid interleave: missing table=baz index=2: index-id "2" does not exist`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID: 1,
					Interleave: InterleaveDescriptor{Ancestors: []InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			referenced: []TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `missing interleave back reference to "foo"@"bar" from "baz"@"qux"`,
			desc: TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: IndexDescriptor{
					ID:   1,
					Name: "bar",
					Interleave: InterleaveDescriptor{Ancestors: []InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			referenced: []TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{
			err: `invalid interleave backreference table=52 index=2: descriptor not found`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:            1,
					InterleavedBy: []ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
		},
		{
			err: `invalid interleave backreference table=baz index=2: index-id "2" does not exist`,
			desc: TableDescriptor{
				ID: 51,
				PrimaryIndex: IndexDescriptor{
					ID:            1,
					InterleavedBy: []ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			referenced: []TableDescriptor{{
				ID:   52,
				Name: "baz",
			}},
		},
		{
			err: `broken interleave backward reference from "foo"@"bar" to "baz"@"qux"`,
			desc: TableDescriptor{
				ID:   51,
				Name: "foo",
				PrimaryIndex: IndexDescriptor{
					ID:            1,
					Name:          "bar",
					InterleavedBy: []ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			referenced: []TableDescriptor{{
				ID:   52,
				Name: "baz",
				PrimaryIndex: IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
	}

	{
		var v roachpb.Value
		desc := &Descriptor{Union: &Descriptor_Database{}}
		if err := v.SetProto(desc); err != nil {
			t.Fatal(err)
		}
		if err := kvDB.Put(context.TODO(), MakeDescMetadataKey(0), &v); err != nil {
			t.Fatal(err)
		}
	}

	for i, test := range tests {
		for _, referencedDesc := range test.referenced {
			var v roachpb.Value
			desc := &Descriptor{Union: &Descriptor_Table{Table: &referencedDesc}}
			if err := v.SetProto(desc); err != nil {
				t.Fatal(err)
			}
			if err := kvDB.Put(context.TODO(), MakeDescMetadataKey(referencedDesc.ID), &v); err != nil {
				t.Fatal(err)
			}
		}
		txn := client.NewTxn(kvDB, s.NodeID(), client.RootTxn)
		if err := test.desc.validateCrossReferences(context.TODO(), txn); err == nil {
			t.Errorf("%d: expected \"%s\", but found success: %+v", i, test.err, test.desc)
		} else if test.err != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, test.err, err.Error())
		}
		for _, referencedDesc := range test.referenced {
			if err := kvDB.Del(context.TODO(), MakeDescMetadataKey(referencedDesc.ID)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestValidatePartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		err  string
		desc TableDescriptor
	}{
		{"at least one of LIST or RANGE partitioning must be used",
			TableDescriptor{
				PrimaryIndex: IndexDescriptor{
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			TableDescriptor{
				PrimaryIndex: IndexDescriptor{
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			TableDescriptor{
				PrimaryIndex: IndexDescriptor{
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{Name: "p1", Values: [][]byte{{}}}},
					},
				},
			},
		},
		{"only one LIST or RANGE partitioning may used",
			TableDescriptor{
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{}},
						Range:      []PartitioningDescriptor_Range{{}},
					},
				},
			},
		},
		{"PARTITION name must be non-empty",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: ColumnType{SemanticType: ColumnType_INT}}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{}},
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: ColumnType{SemanticType: ColumnType_INT}}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List:       []PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: empty array",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: ColumnType{SemanticType: ColumnType_INT}}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{{
							Name: "p1", Values: [][]byte{{}},
						}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: int64 varint decoding failed: 0",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: ColumnType{SemanticType: ColumnType_INT}}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03}}},
						},
					},
				},
			},
		},
		{"PARTITION p1: superfluous data in encoded value",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: ColumnType{SemanticType: ColumnType_INT}}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02, 0x00}}},
						},
					},
				},
			},
		},
		{"partitions p1 and p2 overlap",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: ColumnType{SemanticType: ColumnType_INT}}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1, 1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						Range: []PartitioningDescriptor_Range{
							{Name: "p1", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
							{Name: "p2", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
						},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: ColumnType{SemanticType: ColumnType_INT}}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{Name: "p1", Values: [][]byte{{0x03, 0x04}}},
						},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: ColumnType{SemanticType: ColumnType_INT}}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{{
							Name:   "p1",
							Values: [][]byte{{0x03, 0x02}},
							Subpartitioning: PartitioningDescriptor{
								NumColumns: 1,
								List:       []PartitioningDescriptor_List{{Name: "p1_1", Values: [][]byte{{}}}},
							},
						}},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			TableDescriptor{
				Columns: []ColumnDescriptor{{ID: 1, Type: ColumnType{SemanticType: ColumnType_INT}}},
				PrimaryIndex: IndexDescriptor{
					ColumnIDs:        []ColumnID{1, 1},
					ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_ASC},
					Partitioning: PartitioningDescriptor{
						NumColumns: 1,
						List: []PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{
								Name:   "p2",
								Values: [][]byte{{0x03, 0x04}},
								Subpartitioning: PartitioningDescriptor{
									NumColumns: 1,
									List: []PartitioningDescriptor_List{
										{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for i, test := range tests {
		err := test.desc.validatePartitioning()
		if !testutils.IsError(err, test.err) {
			t.Errorf(`%d: got "%v" expected "%v"`, i, err, test.err)
		}
	}
}

func TestColumnTypeSQLString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		colType     ColumnType
		expectedSQL string
	}{
		{ColumnType{SemanticType: ColumnType_INT}, "INT"},
		{ColumnType{SemanticType: ColumnType_INT, VisibleType: ColumnType_BIT, Width: 2}, "BIT(2)"},
		{ColumnType{SemanticType: ColumnType_FLOAT}, "FLOAT"},
		{ColumnType{SemanticType: ColumnType_FLOAT, Precision: 3}, "FLOAT(3)"},
		{ColumnType{SemanticType: ColumnType_DECIMAL}, "DECIMAL"},
		{ColumnType{SemanticType: ColumnType_DECIMAL, Precision: 6}, "DECIMAL(6)"},
		{ColumnType{SemanticType: ColumnType_DECIMAL, Precision: 7, Width: 8}, "DECIMAL(7,8)"},
		{ColumnType{SemanticType: ColumnType_DATE}, "DATE"},
		{ColumnType{SemanticType: ColumnType_TIMESTAMP}, "TIMESTAMP"},
		{ColumnType{SemanticType: ColumnType_INTERVAL}, "INTERVAL"},
		{ColumnType{SemanticType: ColumnType_STRING}, "STRING"},
		{ColumnType{SemanticType: ColumnType_STRING, Width: 10}, "STRING(10)"},
		{ColumnType{SemanticType: ColumnType_BYTES}, "BYTES"},
	}
	for i, d := range testData {
		sql := d.colType.SQLString()
		if d.expectedSQL != sql {
			t.Errorf("%d: expected %s, but got %s", i, d.expectedSQL, sql)
		}
	}
}

func TestColumnValueEncodedSize(t *testing.T) {
	tests := []struct {
		colType ColumnType
		size    int // -1 means unbounded
	}{
		{ColumnType{SemanticType: ColumnType_BOOL}, 1},
		{ColumnType{SemanticType: ColumnType_INT}, 10},
		{ColumnType{SemanticType: ColumnType_INT, Width: 2}, 10},
		{ColumnType{SemanticType: ColumnType_FLOAT}, 9},
		{ColumnType{SemanticType: ColumnType_FLOAT, Precision: 100}, 9},
		{ColumnType{SemanticType: ColumnType_DECIMAL}, -1},
		{ColumnType{SemanticType: ColumnType_DECIMAL, Precision: 100}, 69},
		{ColumnType{SemanticType: ColumnType_DECIMAL, Precision: 100, Width: 100}, 69},
		{ColumnType{SemanticType: ColumnType_DATE}, 10},
		{ColumnType{SemanticType: ColumnType_TIMESTAMP}, 10},
		{ColumnType{SemanticType: ColumnType_INTERVAL}, 28},
		{ColumnType{SemanticType: ColumnType_STRING}, -1},
		{ColumnType{SemanticType: ColumnType_STRING, Width: 100}, 110},
		{ColumnType{SemanticType: ColumnType_BYTES}, -1},
	}
	for i, test := range tests {
		testIsBounded := test.size != -1
		size, isBounded := upperBoundColumnValueEncodedSize(ColumnDescriptor{
			Type: test.colType,
		})
		if isBounded != testIsBounded {
			if isBounded {
				t.Errorf("%d: expected unbounded but got bounded", i)
			} else {
				t.Errorf("%d: expected bounded but got unbounded", i)
			}
			continue
		}
		if isBounded && size != test.size {
			t.Errorf("%d: got size %d but expected %d", i, size, test.size)
		}
	}
}

func TestFitColumnToFamily(t *testing.T) {
	intEncodedSize, _ := upperBoundColumnValueEncodedSize(ColumnDescriptor{
		ID:   8,
		Type: ColumnType{SemanticType: ColumnType_INT},
	})

	makeTestTableDescriptor := func(familyTypes [][]ColumnType) TableDescriptor {
		nextColumnID := ColumnID(8)
		var desc TableDescriptor
		for _, fTypes := range familyTypes {
			var family ColumnFamilyDescriptor
			for _, t := range fTypes {
				desc.Columns = append(desc.Columns, ColumnDescriptor{
					ID:   nextColumnID,
					Type: t,
				})
				family.ColumnIDs = append(family.ColumnIDs, nextColumnID)
				nextColumnID++
			}
			desc.Families = append(desc.Families, family)
		}
		return desc
	}

	emptyFamily := []ColumnType{}
	partiallyFullFamily := []ColumnType{
		{SemanticType: ColumnType_INT},
		{SemanticType: ColumnType_BYTES, Width: 10},
	}
	fullFamily := []ColumnType{
		{SemanticType: ColumnType_BYTES, Width: FamilyHeuristicTargetBytes + 1},
	}
	maxIntsInOneFamily := make([]ColumnType, FamilyHeuristicTargetBytes/intEncodedSize)
	for i := range maxIntsInOneFamily {
		maxIntsInOneFamily[i] = ColumnType{SemanticType: ColumnType_INT}
	}

	tests := []struct {
		newCol           ColumnType
		existingFamilies [][]ColumnType
		colFits          bool
		idx              int // not applicable if colFits is false
	}{
		// Bounded size column.
		{colFits: true, idx: 0, newCol: ColumnType{SemanticType: ColumnType_BOOL},
			existingFamilies: nil,
		},
		{colFits: true, idx: 0, newCol: ColumnType{SemanticType: ColumnType_BOOL},
			existingFamilies: [][]ColumnType{emptyFamily},
		},
		{colFits: true, idx: 0, newCol: ColumnType{SemanticType: ColumnType_BOOL},
			existingFamilies: [][]ColumnType{partiallyFullFamily},
		},
		{colFits: true, idx: 0, newCol: ColumnType{SemanticType: ColumnType_BOOL},
			existingFamilies: [][]ColumnType{fullFamily},
		},
		{colFits: true, idx: 0, newCol: ColumnType{SemanticType: ColumnType_BOOL},
			existingFamilies: [][]ColumnType{fullFamily, emptyFamily},
		},

		// Unbounded size column.
		{colFits: true, idx: 0, newCol: ColumnType{SemanticType: ColumnType_DECIMAL},
			existingFamilies: [][]ColumnType{emptyFamily},
		},
		{colFits: true, idx: 0, newCol: ColumnType{SemanticType: ColumnType_DECIMAL},
			existingFamilies: [][]ColumnType{partiallyFullFamily},
		},
	}
	for i, test := range tests {
		desc := makeTestTableDescriptor(test.existingFamilies)
		idx, colFits := fitColumnToFamily(desc, ColumnDescriptor{Type: test.newCol})
		if colFits != test.colFits {
			if colFits {
				t.Errorf("%d: expected no fit for the column but got one", i)
			} else {
				t.Errorf("%d: expected fit for the column but didn't get one", i)
			}
			continue
		}
		if colFits && idx != test.idx {
			t.Errorf("%d: got a fit in family offset %d but expected offset %d", i, idx, test.idx)
		}
	}
}

func TestMaybeUpgradeFormatVersion(t *testing.T) {
	tests := []struct {
		desc       TableDescriptor
		expUpgrade bool
		verify     func(int, TableDescriptor) // nil means no extra verification.
	}{
		{
			desc: TableDescriptor{
				FormatVersion: BaseFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "foo"},
				},
			},
			expUpgrade: true,
			verify: func(i int, desc TableDescriptor) {
				if len(desc.Families) == 0 {
					t.Errorf("%d: expected families to be set, but it was empty", i)
				}
			},
		},
		// Test that a version from the future is left alone.
		{
			desc: TableDescriptor{
				FormatVersion: InterleavedFormatVersion,
				Columns: []ColumnDescriptor{
					{ID: 1, Name: "foo"},
				},
			},
			expUpgrade: false,
			verify:     nil,
		},
	}
	for i, test := range tests {
		desc := test.desc
		upgraded := desc.MaybeUpgradeFormatVersion()
		if upgraded != test.expUpgrade {
			t.Fatalf("%d: expected upgraded=%t, but got upgraded=%t", i, test.expUpgrade, upgraded)
		}
		if test.verify != nil {
			test.verify(i, desc)
		}
	}
}

func TestUnvalidateConstraints(t *testing.T) {
	desc := TableDescriptor{
		Name:          "test",
		ParentID:      ID(1),
		Columns:       []ColumnDescriptor{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		FormatVersion: FamilyFormatVersion,
		Indexes:       []IndexDescriptor{makeIndexDescriptor("d", []string{"b", "a"})},
		Privileges:    NewDefaultPrivilegeDescriptor(),
	}
	desc.Indexes[0].ForeignKey = ForeignKeyReference{
		Name:     "fk",
		Table:    ID(1),
		Index:    IndexID(1),
		Validity: ConstraintValidity_Validated,
	}
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	lookup := func(_ ID) (*TableDescriptor, error) {
		return &desc, nil
	}

	before, err := desc.GetConstraintInfoWithLookup(lookup)
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := before["fk"]; !ok || c.Unvalidated {
		t.Fatalf("expected to find a validated constraint fk before, found %v", c)
	}
	desc.InvalidateFKConstraints()

	after, err := desc.GetConstraintInfoWithLookup(lookup)
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := after["fk"]; !ok || !c.Unvalidated {
		t.Fatalf("expected to find a unvalididated constraint fk before, found %v", c)
	}
}

func TestKeysPerRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(dan): This server is only used to turn a CREATE TABLE statement into
	// a TableDescriptor. It should be possible to move MakeTableDesc into
	// sqlbase. If/when that happens, use it here instead of this server.
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	if _, err := conn.Exec(`CREATE DATABASE d`); err != nil {
		t.Fatalf("%+v", err)
	}

	tests := []struct {
		createTable string
		indexID     IndexID
		expected    int
	}{
		{"(a SERIAL PRIMARY KEY, b INT, INDEX (b))", 1, 1},                         // Primary index
		{"(a SERIAL PRIMARY KEY, b INT, INDEX (b))", 2, 1},                         // 'b' index
		{"(a SERIAL PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))", 1, 2}, // Primary index
		{"(a SERIAL PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))", 2, 1}, // 'b' index
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%s - %d", test.createTable, test.indexID), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(conn)
			tableName := fmt.Sprintf("t%d", i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE d.public.%s %s`, tableName, test.createTable))

			var descBytes []byte
			// Grab the most recently created descriptor.
			row := sqlDB.QueryRow(t,
				`SELECT descriptor FROM system.public.descriptor ORDER BY id DESC LIMIT 1`)
			row.Scan(&descBytes)
			var desc Descriptor
			if err := protoutil.Unmarshal(descBytes, &desc); err != nil {
				t.Fatalf("%+v", err)
			}

			keys := desc.GetTable().KeysPerRow(test.indexID)
			if test.expected != keys {
				t.Errorf("expected %d keys got %d", test.expected, keys)
			}
		})
	}
}
