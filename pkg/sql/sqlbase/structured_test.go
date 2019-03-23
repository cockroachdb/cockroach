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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/descid"
	"github.com/cockroachdb/cockroach/pkg/sql/privilegepb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Makes an IndexDescriptor with all columns being ascending.
func makeIndexDescriptor(name string, columnNames []string) catpb.IndexDescriptor {
	dirs := make([]catpb.IndexDescriptor_Direction, 0, len(columnNames))
	for range columnNames {
		dirs = append(dirs, catpb.IndexDescriptor_ASC)
	}
	idx := catpb.IndexDescriptor{
		ID:               catpb.IndexID(0),
		Name:             name,
		ColumnNames:      columnNames,
		ColumnDirections: dirs,
	}
	return idx
}

func TestAllocateIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := NewMutableCreatedTableDescriptor(catpb.TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Name:     "foo",
		Columns: []catpb.ColumnDescriptor{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		},
		PrimaryIndex: makeIndexDescriptor("c", []string{"a", "b"}),
		Indexes: []catpb.IndexDescriptor{
			makeIndexDescriptor("d", []string{"b", "a"}),
			makeIndexDescriptor("e", []string{"b"}),
		},
		Privileges:    privilegepb.NewDefaultPrivilegeDescriptor(),
		FormatVersion: catpb.FamilyFormatVersion,
	})
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}

	expected := NewMutableCreatedTableDescriptor(catpb.TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Version:  1,
		Name:     "foo",
		Columns: []catpb.ColumnDescriptor{
			{ID: 1, Name: "a"},
			{ID: 2, Name: "b"},
			{ID: 3, Name: "c"},
		},
		Families: []catpb.ColumnFamilyDescriptor{
			{
				ID: 0, Name: "primary",
				ColumnNames:     []string{"a", "b", "c"},
				ColumnIDs:       []catpb.ColumnID{1, 2, 3},
				DefaultColumnID: 3,
			},
		},
		PrimaryIndex: catpb.IndexDescriptor{
			ID: 1, Name: "c", ColumnIDs: []catpb.ColumnID{1, 2},
			ColumnNames: []string{"a", "b"},
			ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC,
				catpb.IndexDescriptor_ASC}},
		Indexes: []catpb.IndexDescriptor{
			{ID: 2, Name: "d", ColumnIDs: []catpb.ColumnID{2, 1}, ColumnNames: []string{"b", "a"},
				ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC,
					catpb.IndexDescriptor_ASC}},
			{ID: 3, Name: "e", ColumnIDs: []catpb.ColumnID{2}, ColumnNames: []string{"b"},
				ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
				ExtraColumnIDs:   []catpb.ColumnID{1}},
		},
		Privileges:     privilegepb.NewDefaultPrivilegeDescriptor(),
		NextColumnID:   4,
		NextFamilyID:   1,
		NextIndexID:    4,
		NextMutationID: 1,
		FormatVersion:  catpb.FamilyFormatVersion,
	})
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
		desc catpb.TableDescriptor
	}{
		{`empty table name`,
			catpb.TableDescriptor{}},
		{`invalid table ID 0`,
			catpb.TableDescriptor{ID: 0, Name: "foo"}},
		{`invalid parent ID 0`,
			catpb.TableDescriptor{ID: 2, Name: "foo"}},
		{`table "foo" is encoded using using version 0, but this client only supports version 2 and 3`,
			catpb.TableDescriptor{ID: 2, ParentID: 1, Name: "foo"}},
		{`table must contain at least 1 column`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
			}},
		{`empty column name`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 0},
				},
				NextColumnID: 2,
			}},
		{`invalid column ID 0`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 0, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`table must contain a primary key`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`duplicate column name: "bar"`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`column "blah" duplicate ID of column "bar": 1`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "blah"},
				},
				NextColumnID: 2,
			}},
		{`at least 1 column family must be specified`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`the 0th family must have ID 0`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 1},
				},
				NextColumnID: 2,
			}},
		{`duplicate family name: "baz"`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 1, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`family "qux" duplicate ID of family "baz": 0`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 0, Name: "qux"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`duplicate family name: "baz"`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 3, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`mismatched column ID size (1) and name size (0)`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []catpb.ColumnID{1}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" contains unknown column "2"`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []catpb.ColumnID{2}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" column 1 should have name "bar", but found name "qux"`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"qux"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column 1 is not in any column family`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column 1 is in both family 0 and 1`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
					{ID: 1, Name: "qux", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`primary key column 1 is not in column family 0`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 1, Name: "qux", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{ID: 1, Name: "quux",
					ColumnIDs:        []catpb.ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
				NextIndexID:  2,
			}},
		{`table must contain a primary key`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{
					ID:               0,
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`invalid index ID 0`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{ID: 0, Name: "bar",
					ColumnIDs:        []catpb.ColumnID{0},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`index "bar" must contain at least 1 column`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
				},
				Indexes: []catpb.IndexDescriptor{
					{ID: 2, Name: "bar"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`mismatched column IDs (1) and names (0)`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []catpb.ColumnID{1}},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and names (2)`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "blah"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1, 2}, ColumnNames: []string{"bar", "blah"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar", "blah"},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`duplicate index name: "bar"`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
				},
				Indexes: []catpb.IndexDescriptor{
					{ID: 2, Name: "bar", ColumnIDs: []catpb.ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "blah" duplicate ID of index "bar": 1`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []catpb.ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
				},
				Indexes: []catpb.IndexDescriptor{
					{ID: 1, Name: "blah", ColumnIDs: []catpb.ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" column "bar" should have ID 1, but found ID 2`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []catpb.ColumnID{2},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" contains unknown column "blah"`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []catpb.ColumnID{1},
					ColumnNames:      []string{"blah"},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and directions (0)`,
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []catpb.ColumnID{1},
					ColumnNames: []string{"blah"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`at least one of LIST or RANGE partitioning must be used`,
			// Verify that validatePartitioning is hooked up. The rest of these
			// tests are in TestValidatePartitionion.
			catpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: catpb.FamilyFormatVersion,
				Columns: []catpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []catpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: catpb.IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []catpb.ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
	}
	for i, d := range testData {
		if err := ValidateSingleTable(&d.desc, cluster.MakeTestingClusterSettings()); err == nil {
			t.Errorf("%d: expected \"%s\", but found success: %+v", i, d.err, d.desc)
		} else if d.err != err.Error() && "internal error: "+d.err != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%+v\"", i, d.err, err)
		}
	}
}

func TestColumnTypeSQLString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		colType     catpb.ColumnType
		expectedSQL string
	}{
		{catpb.ColumnType{SemanticType: catpb.ColumnType_BIT, Width: 2}, "BIT(2)"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_BIT, VisibleType: catpb.ColumnType_VARBIT, Width: 2}, "VARBIT(2)"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_INT}, "INT"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_FLOAT}, "FLOAT8"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_FLOAT, VisibleType: catpb.ColumnType_REAL}, "FLOAT4"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_FLOAT, VisibleType: catpb.ColumnType_DOUBLE_PRECISION}, "FLOAT8"}, // Pre-2.1.
		{catpb.ColumnType{SemanticType: catpb.ColumnType_FLOAT, Precision: -1}, "FLOAT8"},                                  // Pre-2.1.
		{catpb.ColumnType{SemanticType: catpb.ColumnType_FLOAT, Precision: 20}, "FLOAT4"},                                  // Pre-2.1.
		{catpb.ColumnType{SemanticType: catpb.ColumnType_FLOAT, Precision: 40}, "FLOAT8"},                                  // Pre-2.1.
		{catpb.ColumnType{SemanticType: catpb.ColumnType_FLOAT, Precision: 120}, "FLOAT8"},                                 // Pre-2.1.
		{catpb.ColumnType{SemanticType: catpb.ColumnType_DECIMAL}, "DECIMAL"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_DECIMAL, Precision: 6}, "DECIMAL(6)"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_DECIMAL, Precision: 7, Width: 8}, "DECIMAL(7,8)"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_DATE}, "DATE"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_TIMESTAMP}, "TIMESTAMP"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_INTERVAL}, "INTERVAL"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_STRING}, "STRING"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_STRING, Width: 10}, "STRING(10)"},
		{catpb.ColumnType{SemanticType: catpb.ColumnType_BYTES}, "BYTES"},
	}
	for i, d := range testData {
		t.Run(d.colType.String(), func(t *testing.T) {
			sql := d.colType.SQLString()
			if d.expectedSQL != sql {
				t.Errorf("%d: expected %s, but got %s", i, d.expectedSQL, sql)
			}
		})
	}
}

func TestFitColumnToFamily(t *testing.T) {
	intEncodedSize := 10 // 1 byte tag + 9 bytes max varint encoded size

	makeTestTableDescriptor := func(familyTypes [][]catpb.ColumnType) *MutableTableDescriptor {
		nextColumnID := catpb.ColumnID(8)
		var desc catpb.TableDescriptor
		for _, fTypes := range familyTypes {
			var family catpb.ColumnFamilyDescriptor
			for _, t := range fTypes {
				desc.Columns = append(desc.Columns, catpb.ColumnDescriptor{
					ID:   nextColumnID,
					Type: t,
				})
				family.ColumnIDs = append(family.ColumnIDs, nextColumnID)
				nextColumnID++
			}
			desc.Families = append(desc.Families, family)
		}
		return NewMutableCreatedTableDescriptor(desc)
	}

	emptyFamily := []catpb.ColumnType{}
	partiallyFullFamily := []catpb.ColumnType{
		{SemanticType: catpb.ColumnType_INT},
		{SemanticType: catpb.ColumnType_BYTES, Width: 10},
	}
	fullFamily := []catpb.ColumnType{
		{SemanticType: catpb.ColumnType_BYTES, Width: FamilyHeuristicTargetBytes + 1},
	}
	maxIntsInOneFamily := make([]catpb.ColumnType, FamilyHeuristicTargetBytes/intEncodedSize)
	for i := range maxIntsInOneFamily {
		maxIntsInOneFamily[i] = catpb.ColumnType{SemanticType: catpb.ColumnType_INT}
	}

	tests := []struct {
		newCol           catpb.ColumnType
		existingFamilies [][]catpb.ColumnType
		colFits          bool
		idx              int // not applicable if colFits is false
	}{
		// Bounded size column.
		{colFits: true, idx: 0, newCol: catpb.ColumnType{SemanticType: catpb.ColumnType_BOOL},
			existingFamilies: nil,
		},
		{colFits: true, idx: 0, newCol: catpb.ColumnType{SemanticType: catpb.ColumnType_BOOL},
			existingFamilies: [][]catpb.ColumnType{emptyFamily},
		},
		{colFits: true, idx: 0, newCol: catpb.ColumnType{SemanticType: catpb.ColumnType_BOOL},
			existingFamilies: [][]catpb.ColumnType{partiallyFullFamily},
		},
		{colFits: true, idx: 0, newCol: catpb.ColumnType{SemanticType: catpb.ColumnType_BOOL},
			existingFamilies: [][]catpb.ColumnType{fullFamily},
		},
		{colFits: true, idx: 0, newCol: catpb.ColumnType{SemanticType: catpb.ColumnType_BOOL},
			existingFamilies: [][]catpb.ColumnType{fullFamily, emptyFamily},
		},

		// Unbounded size column.
		{colFits: true, idx: 0, newCol: catpb.ColumnType{SemanticType: catpb.ColumnType_DECIMAL},
			existingFamilies: [][]catpb.ColumnType{emptyFamily},
		},
		{colFits: true, idx: 0, newCol: catpb.ColumnType{SemanticType: catpb.ColumnType_DECIMAL},
			existingFamilies: [][]catpb.ColumnType{partiallyFullFamily},
		},
	}
	for i, test := range tests {
		desc := makeTestTableDescriptor(test.existingFamilies)
		idx, colFits := fitColumnToFamily(desc, catpb.ColumnDescriptor{Type: test.newCol})
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

func TestUnvalidateConstraints(t *testing.T) {
	desc := NewMutableCreatedTableDescriptor(catpb.TableDescriptor{
		Name:          "test",
		ParentID:      descid.T(1),
		Columns:       []catpb.ColumnDescriptor{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		FormatVersion: catpb.FamilyFormatVersion,
		Indexes:       []catpb.IndexDescriptor{makeIndexDescriptor("d", []string{"b", "a"})},
		Privileges:    privilegepb.NewDefaultPrivilegeDescriptor(),
	})
	desc.Indexes[0].ForeignKey = catpb.ForeignKeyReference{
		Name:     "fk",
		Table:    descid.T(1),
		Index:    catpb.IndexID(1),
		Validity: catpb.ConstraintValidity_Validated,
	}
	if err := desc.AllocateIDs(); err != nil {
		t.Fatal(err)
	}
	lookup := func(_ descid.T) (*catpb.TableDescriptor, error) {
		return desc.TableDesc(), nil
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
		indexID     catpb.IndexID
		expected    int
	}{
		{"(a INT PRIMARY KEY, b INT, INDEX (b))", 1, 1},                         // Primary index
		{"(a INT PRIMARY KEY, b INT, INDEX (b))", 2, 1},                         // 'b' index
		{"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))", 1, 2}, // Primary index
		{"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))", 2, 1}, // 'b' index
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%s - %d", test.createTable, test.indexID), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(conn)
			tableName := fmt.Sprintf("t%d", i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE d.%s %s`, tableName, test.createTable))

			var descBytes []byte
			// Grab the most recently created descriptor.
			row := sqlDB.QueryRow(t,
				`SELECT descriptor FROM system.descriptor ORDER BY id DESC LIMIT 1`)
			row.Scan(&descBytes)
			var desc catpb.Descriptor
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

func TestDatumTypeToColumnSemanticType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, typ := range types.AnyNonArray {
		_, err := catpb.DatumTypeToColumnSemanticType(typ)
		if err != nil {
			t.Errorf("couldn't get semantic type: %s", err)
		}
	}
}
