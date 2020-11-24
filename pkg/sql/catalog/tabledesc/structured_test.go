// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// Makes an descpb.IndexDescriptor with all columns being ascending.
func makeIndexDescriptor(name string, columnNames []string) descpb.IndexDescriptor {
	dirs := make([]descpb.IndexDescriptor_Direction, 0, len(columnNames))
	for range columnNames {
		dirs = append(dirs, descpb.IndexDescriptor_ASC)
	}
	idx := descpb.IndexDescriptor{
		ID:               descpb.IndexID(0),
		Name:             name,
		ColumnNames:      columnNames,
		ColumnDirections: dirs,
	}
	return idx
}

func TestAllocateIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	desc := NewCreatedMutable(descpb.TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Name:     "foo",
		Columns: []descpb.ColumnDescriptor{
			{Name: "a", Type: types.Int},
			{Name: "b", Type: types.Int},
			{Name: "c", Type: types.Int},
		},
		PrimaryIndex: makeIndexDescriptor("c", []string{"a", "b"}),
		Indexes: []descpb.IndexDescriptor{
			makeIndexDescriptor("d", []string{"b", "a"}),
			makeIndexDescriptor("e", []string{"b"}),
			func() descpb.IndexDescriptor {
				idx := makeIndexDescriptor("f", []string{"c"})
				idx.EncodingType = descpb.PrimaryIndexEncoding
				return idx
			}(),
		},
		Privileges:    descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName()),
		FormatVersion: descpb.FamilyFormatVersion,
	})
	if err := desc.AllocateIDs(ctx); err != nil {
		t.Fatal(err)
	}

	expected := NewCreatedMutable(descpb.TableDescriptor{
		ParentID: keys.MinUserDescID,
		ID:       keys.MinUserDescID + 1,
		Version:  1,
		Name:     "foo",
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "a", Type: types.Int},
			{ID: 2, Name: "b", Type: types.Int},
			{ID: 3, Name: "c", Type: types.Int},
		},
		Families: []descpb.ColumnFamilyDescriptor{
			{
				ID: 0, Name: "primary",
				ColumnNames:     []string{"a", "b", "c"},
				ColumnIDs:       []descpb.ColumnID{1, 2, 3},
				DefaultColumnID: 3,
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			ID: 1, Name: "c", ColumnIDs: []descpb.ColumnID{1, 2},
			ColumnNames: []string{"a", "b"},
			ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC,
				descpb.IndexDescriptor_ASC}},
		Indexes: []descpb.IndexDescriptor{
			{ID: 2, Name: "d", ColumnIDs: []descpb.ColumnID{2, 1}, ColumnNames: []string{"b", "a"},
				ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC,
					descpb.IndexDescriptor_ASC}},
			{ID: 3, Name: "e", ColumnIDs: []descpb.ColumnID{2}, ColumnNames: []string{"b"},
				ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				ExtraColumnIDs:   []descpb.ColumnID{1}},
			{ID: 4, Name: "f", ColumnIDs: []descpb.ColumnID{3}, ColumnNames: []string{"c"},
				ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				EncodingType:     descpb.PrimaryIndexEncoding},
		},
		Privileges:     descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName()),
		NextColumnID:   4,
		NextFamilyID:   1,
		NextIndexID:    5,
		NextMutationID: 1,
		FormatVersion:  descpb.FamilyFormatVersion,
	})
	if !reflect.DeepEqual(expected, desc) {
		a, _ := json.MarshalIndent(expected, "", "  ")
		b, _ := json.MarshalIndent(desc, "", "  ")
		t.Fatalf("expected %s, but found %s", a, b)
	}

	if err := desc.AllocateIDs(ctx); err != nil {
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

	ctx := context.Background()

	testData := []struct {
		err  string
		desc descpb.TableDescriptor
	}{
		{`empty table name`,
			descpb.TableDescriptor{}},
		{`invalid table ID 0`,
			descpb.TableDescriptor{ID: 0, Name: "foo"}},
		{`invalid parent ID 0`,
			descpb.TableDescriptor{ID: 2, Name: "foo"}},
		{`table "foo" is encoded using using version 0, but this client only supports version 2 and 3`,
			descpb.TableDescriptor{ID: 2, ParentID: 1, Name: "foo"}},
		{`table must contain at least 1 column`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
			}},
		{`empty column name`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 0},
				},
				NextColumnID: 2,
			}},
		{`invalid column ID 0`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 0, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`table must contain a primary key`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`duplicate column name: "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`column "blah" duplicate ID of column "bar": 1`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "blah"},
				},
				NextColumnID: 2,
			}},
		{`at least 1 column family must be specified`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`the 0th family must have ID 0`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 1},
				},
				NextColumnID: 2,
			}},
		{`duplicate family name: "baz"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 1, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`family "qux" duplicate ID of family "baz": 0`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 0, Name: "qux"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`duplicate family name: "baz"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 3, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`mismatched column ID size (1) and name size (0)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{1}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" contains unknown column "2"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{2}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" column 1 should have name "bar", but found name "qux"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"qux"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column 1 is not in any column family`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column 1 is in both family 0 and 1`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
					{ID: 1, Name: "qux", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`table must contain a primary key`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:               0,
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`invalid index ID 0`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 0, Name: "bar",
					ColumnIDs:        []descpb.ColumnID{0},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`index "bar" must contain at least 1 column`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "bar"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`mismatched column IDs (1) and names (0)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{1}},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and names (2)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "blah"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"bar", "blah"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar", "blah"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`duplicate index name: "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "bar", ColumnIDs: []descpb.ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "blah" duplicate ID of index "bar": 1`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 1, Name: "blah", ColumnIDs: []descpb.ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" column "bar" should have ID 1, but found ID 2`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{2},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" contains unknown column "blah"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{1},
					ColumnNames:      []string{"blah"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and directions (0)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{1},
					ColumnNames: []string{"blah"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`at least one of LIST or RANGE partitioning must be used`,
			// Verify that validatePartitioning is hooked up. The rest of these
			// tests are in TestValidatePartitionion.
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "foo_crdb_internal_bar_shard_5_bar_idx" refers to non-existent shard column "does not exist"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "crdb_internal_bar_shard_5"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"bar", "crdb_internal_bar_shard_5"},
					},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "primary",
					Unique:           true,
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					StoreColumnNames: []string{"crdb_internal_bar_shard_5"},
					StoreColumnIDs:   []descpb.ColumnID{2},
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "foo_crdb_internal_bar_shard_5_bar_idx",
						ColumnIDs:        []descpb.ColumnID{2, 1},
						ColumnNames:      []string{"crdb_internal_bar_shard_5", "bar"},
						ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
						Sharded: descpb.ShardedDescriptor{
							IsSharded:    true,
							Name:         "does not exist",
							ShardBuckets: 5,
						},
					},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
	}
	for i, d := range testData {
		t.Run(d.err, func(t *testing.T) {
			desc := NewImmutable(d.desc)
			if err := desc.ValidateTable(ctx); err == nil {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, d.err, d.desc)
			} else if d.err != err.Error() && "internal error: "+d.err != err.Error() {
				t.Errorf("%d: expected \"%s\", but found \"%+v\"", i, d.err, err)
			}
		})
	}
}

func TestValidateCrossTableReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	pointer := func(s string) *string {
		return &s
	}

	tests := []struct {
		err        string
		desc       descpb.TableDescriptor
		otherDescs []descpb.TableDescriptor
	}{
		// Foreign keys
		{
			err: `invalid foreign key: missing table=52: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				OutboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   52,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       51,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
			otherDescs: nil,
		},
		{
			err: `missing fk back reference "fk" to "foo" from "baz"`,
			desc: descpb.TableDescriptor{
				ID:                      51,
				Name:                    "foo",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				OutboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   52,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       51,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
			}},
		},
		{
			err: `invalid foreign key backreference: missing table=52: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				InboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   51,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       52,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
		},
		{
			err: `missing fk forward reference "fk" to "foo" from "baz"`,
			desc: descpb.TableDescriptor{
				ID:                      51,
				Name:                    "foo",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:   1,
					Name: "bar",
				},
				InboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   51,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       52,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
			}},
		},

		// Interleaves
		{
			err: `invalid interleave: missing table=52 index=2: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1,
					Interleave: descpb.InterleaveDescriptor{Ancestors: []descpb.InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			otherDescs: nil,
		},
		{
			err: `invalid interleave: missing table=baz index=2: index-id "2" does not exist`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1,
					Interleave: descpb.InterleaveDescriptor{Ancestors: []descpb.InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
			}},
		},
		{
			err: `missing interleave back reference to "foo"@"bar" from "baz"@"qux"`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:   1,
					Name: "bar",
					Interleave: descpb.InterleaveDescriptor{Ancestors: []descpb.InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{
			err: `invalid interleave backreference table=52 index=2: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:            1,
					InterleavedBy: []descpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
		},
		{
			err: `invalid interleave backreference table=baz index=2: index-id "2" does not exist`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:            1,
					InterleavedBy: []descpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
			}},
		},
		{
			err: `broken interleave backward reference from "foo"@"bar" to "baz"@"qux"`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:            1,
					Name:          "bar",
					InterleavedBy: []descpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				Name:                    "baz",
				ID:                      52,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{
			err: `type ID 500 in descriptor not found: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:          1,
					Name:        "bar",
					ColumnIDs:   []descpb.ColumnID{1},
					ColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{
						Name: "a",
						ID:   1,
						Type: types.MakeEnum(typedesc.TypeIDToOID(500), typedesc.TypeIDToOID(100500)),
					},
				},
			},
		},
		// Add some expressions with invalid type references.
		{
			err: `type ID 500 in descriptor not found: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:          1,
					Name:        "bar",
					ColumnIDs:   []descpb.ColumnID{1},
					ColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{
						Name:        "a",
						ID:          1,
						Type:        types.Int,
						DefaultExpr: pointer("a::@100500"),
					},
				},
			},
		},
		{
			err: `type ID 500 in descriptor not found: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:          1,
					Name:        "bar",
					ColumnIDs:   []descpb.ColumnID{1},
					ColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{
						Name:        "a",
						ID:          1,
						Type:        types.Int,
						ComputeExpr: pointer("a:::@100500"),
					},
				},
			},
		},
		{
			err: `type ID 500 in descriptor not found: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				Checks: []*descpb.TableDescriptor_CheckConstraint{
					{
						Expr: "a::@100500",
					},
				},
			},
		},
	}

	for i, test := range tests {
		descs := catalog.MapDescGetter{}
		descs[1] = dbdesc.NewImmutable(descpb.DatabaseDescriptor{ID: 1})
		for _, otherDesc := range test.otherDescs {
			otherDesc.Privileges = descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName())
			descs[otherDesc.ID] = NewImmutable(otherDesc)
		}
		desc := NewImmutable(test.desc)
		if err := desc.ValidateCrossReferences(ctx, descs); err == nil {
			t.Errorf("%d: expected \"%s\", but found success: %+v", i, test.err, test.desc)
		} else if test.err != err.Error() && "internal error: "+test.err != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, test.err, err.Error())
		}
	}
}

func TestValidatePartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		err  string
		desc descpb.TableDescriptor
	}{
		{"at least one of LIST or RANGE partitioning must be used",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{Name: "p1", Values: [][]byte{{}}}},
					},
				},
			},
		},
		{"only one LIST or RANGE partitioning may used",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{}},
						Range:      []descpb.PartitioningDescriptor_Range{{}},
					},
				},
			},
		},
		{"PARTITION name must be non-empty",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{}},
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: empty array",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{{
							Name: "p1", Values: [][]byte{{}},
						}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: int64 varint decoding failed: 0",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03}}},
						},
					},
				},
			},
		},
		{"PARTITION p1: superfluous data in encoded value",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02, 0x00}}},
						},
					},
				},
			},
		},
		{"partitions p1 and p2 overlap",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1, 1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						Range: []descpb.PartitioningDescriptor_Range{
							{Name: "p1", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
							{Name: "p2", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
						},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{Name: "p1", Values: [][]byte{{0x03, 0x04}}},
						},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{{
							Name:   "p1",
							Values: [][]byte{{0x03, 0x02}},
							Subpartitioning: descpb.PartitioningDescriptor{
								NumColumns: 1,
								List:       []descpb.PartitioningDescriptor_List{{Name: "p1_1", Values: [][]byte{{}}}},
							},
						}},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					ColumnIDs:        []descpb.ColumnID{1, 1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{
								Name:   "p2",
								Values: [][]byte{{0x03, 0x04}},
								Subpartitioning: descpb.PartitioningDescriptor{
									NumColumns: 1,
									List: []descpb.PartitioningDescriptor_List{
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
		t.Run(test.err, func(t *testing.T) {
			desc := NewImmutable(test.desc)
			err := desc.ValidatePartitioning()
			if !testutils.IsError(err, test.err) {
				t.Errorf(`%d: got "%v" expected "%v"`, i, err, test.err)
			}
		})
	}
}

func TestColumnTypeSQLString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		colType     *types.T
		expectedSQL string
	}{
		{types.MakeBit(2), "BIT(2)"},
		{types.MakeVarBit(2), "VARBIT(2)"},
		{types.Int, "INT8"},
		{types.Float, "FLOAT8"},
		{types.Float4, "FLOAT4"},
		{types.Decimal, "DECIMAL"},
		{types.MakeDecimal(6, 0), "DECIMAL(6)"},
		{types.MakeDecimal(8, 7), "DECIMAL(8,7)"},
		{types.Date, "DATE"},
		{types.Timestamp, "TIMESTAMP"},
		{types.Interval, "INTERVAL"},
		{types.String, "STRING"},
		{types.MakeString(10), "STRING(10)"},
		{types.Bytes, "BYTES"},
	}
	for i, d := range testData {
		t.Run(d.colType.DebugString(), func(t *testing.T) {
			sql := d.colType.SQLString()
			if d.expectedSQL != sql {
				t.Errorf("%d: expected %s, but got %s", i, d.expectedSQL, sql)
			}
		})
	}
}

func TestFitColumnToFamily(t *testing.T) {
	intEncodedSize := 10 // 1 byte tag + 9 bytes max varint encoded size

	makeTestTableDescriptor := func(familyTypes [][]*types.T) *Mutable {
		nextColumnID := descpb.ColumnID(8)
		var desc descpb.TableDescriptor
		for _, fTypes := range familyTypes {
			var family descpb.ColumnFamilyDescriptor
			for _, t := range fTypes {
				desc.Columns = append(desc.Columns, descpb.ColumnDescriptor{
					ID:   nextColumnID,
					Type: t,
				})
				family.ColumnIDs = append(family.ColumnIDs, nextColumnID)
				nextColumnID++
			}
			desc.Families = append(desc.Families, family)
		}
		return NewCreatedMutable(desc)
	}

	emptyFamily := []*types.T{}
	partiallyFullFamily := []*types.T{
		types.Int,
		types.Bytes,
	}
	fullFamily := []*types.T{
		types.Bytes,
	}
	maxIntsInOneFamily := make([]*types.T, FamilyHeuristicTargetBytes/intEncodedSize)
	for i := range maxIntsInOneFamily {
		maxIntsInOneFamily[i] = types.Int
	}

	tests := []struct {
		newCol           *types.T
		existingFamilies [][]*types.T
		colFits          bool
		idx              int // not applicable if colFits is false
	}{
		// Bounded size column.
		{colFits: true, idx: 0, newCol: types.Bool,
			existingFamilies: nil,
		},
		{colFits: true, idx: 0, newCol: types.Bool,
			existingFamilies: [][]*types.T{emptyFamily},
		},
		{colFits: true, idx: 0, newCol: types.Bool,
			existingFamilies: [][]*types.T{partiallyFullFamily},
		},
		{colFits: true, idx: 0, newCol: types.Bool,
			existingFamilies: [][]*types.T{fullFamily},
		},
		{colFits: true, idx: 0, newCol: types.Bool,
			existingFamilies: [][]*types.T{fullFamily, emptyFamily},
		},

		// Unbounded size column.
		{colFits: true, idx: 0, newCol: types.Decimal,
			existingFamilies: [][]*types.T{emptyFamily},
		},
		{colFits: true, idx: 0, newCol: types.Decimal,
			existingFamilies: [][]*types.T{partiallyFullFamily},
		},
	}
	for i, test := range tests {
		desc := makeTestTableDescriptor(test.existingFamilies)
		idx, colFits := FitColumnToFamily(desc, descpb.ColumnDescriptor{Type: test.newCol})
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
		desc       descpb.TableDescriptor
		expUpgrade bool
		verify     func(int, *Immutable) // nil means no extra verification.
	}{
		{
			desc: descpb.TableDescriptor{
				FormatVersion: descpb.BaseFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo"},
				},
				Privileges: descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
			},
			expUpgrade: true,
			verify: func(i int, desc *Immutable) {
				if len(desc.Families) == 0 {
					t.Errorf("%d: expected families to be set, but it was empty", i)
				}
			},
		},
		// Test that a version from the future is left alone.
		{
			desc: descpb.TableDescriptor{
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo"},
				},
				Privileges: descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
			},
			expUpgrade: false,
			verify:     nil,
		},
	}
	for i, test := range tests {
		desc, err := NewFilledInImmutable(context.Background(), nil, &test.desc)
		require.NoError(t, err)
		upgraded := desc.GetPostDeserializationChanges().UpgradedFormatVersion
		if upgraded != test.expUpgrade {
			t.Fatalf("%d: expected upgraded=%t, but got upgraded=%t", i, test.expUpgrade, upgraded)
		}
		if test.verify != nil {
			test.verify(i, desc)
		}
	}
}

func TestUnvalidateConstraints(t *testing.T) {
	ctx := context.Background()

	desc := NewCreatedMutable(descpb.TableDescriptor{
		Name:     "test",
		ParentID: descpb.ID(1),
		Columns: []descpb.ColumnDescriptor{
			{Name: "a", Type: types.Int},
			{Name: "b", Type: types.Int},
			{Name: "c", Type: types.Int}},
		FormatVersion: descpb.FamilyFormatVersion,
		Indexes:       []descpb.IndexDescriptor{makeIndexDescriptor("d", []string{"b", "a"})},
		Privileges:    descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName()),
		OutboundFKs: []descpb.ForeignKeyConstraint{
			{
				Name:              "fk",
				ReferencedTableID: descpb.ID(1),
				Validity:          descpb.ConstraintValidity_Validated,
			},
		},
	})
	if err := desc.AllocateIDs(ctx); err != nil {
		t.Fatal(err)
	}
	lookup := func(_ descpb.ID) (catalog.TableDescriptor, error) {
		return desc.ImmutableCopy().(catalog.TableDescriptor), nil
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
		t.Fatalf("expected to find an unvalidated constraint fk before, found %v", c)
	}
}

func TestKeysPerRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(dan): This server is only used to turn a CREATE TABLE statement into
	// a descpb.TableDescriptor. It should be possible to move MakeTableDesc into
	// sqlbase. If/when that happens, use it here instead of this server.
	s, conn, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	if _, err := conn.Exec(`CREATE DATABASE d`); err != nil {
		t.Fatalf("%+v", err)
	}

	tests := []struct {
		createTable string
		indexID     descpb.IndexID
		expected    int
	}{
		{"(a INT PRIMARY KEY, b INT, INDEX (b))", 1, 1},                                     // Primary index
		{"(a INT PRIMARY KEY, b INT, INDEX (b))", 2, 1},                                     // 'b' index
		{"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))", 1, 2},             // Primary index
		{"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))", 2, 1},             // 'b' index
		{"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (a) STORING (b))", 2, 2}, // 'a' index
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%s - %d", test.createTable, test.indexID), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(conn)
			tableName := fmt.Sprintf("t%d", i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE d.%s %s`, tableName, test.createTable))

			desc := catalogkv.TestingGetImmutableTableDescriptor(db, keys.SystemSQLCodec, "d", tableName)
			require.NotNil(t, desc)
			keys, err := desc.KeysPerRow(test.indexID)
			if err != nil {
				t.Fatal(err)
			}
			if test.expected != keys {
				t.Errorf("expected %d keys got %d", test.expected, keys)
			}
		})
	}
}

func TestColumnNeedsBackfill(t *testing.T) {
	// Define variable strings here such that we can pass their address below.
	null := "NULL"
	four := "4:::INT8"
	// Create Column Descriptors that reflect the definition of a column with a
	// default value of NULL that was set implicitly, one that was set explicitly,
	// and one that has an INT default value, respectively.
	implicitNull := &descpb.ColumnDescriptor{
		Name: "im", ID: 2, Type: types.Int, DefaultExpr: nil, Nullable: true, ComputeExpr: nil,
	}
	explicitNull := &descpb.ColumnDescriptor{
		Name: "ex", ID: 3, Type: types.Int, DefaultExpr: &null, Nullable: true, ComputeExpr: nil,
	}
	defaultNotNull := &descpb.ColumnDescriptor{
		Name: "four", ID: 4, Type: types.Int, DefaultExpr: &four, Nullable: true, ComputeExpr: nil,
	}
	// Verify that a backfill doesn't occur according to the ColumnNeedsBackfill
	// function for the default NULL values, and that it does occur for an INT
	// default value.
	if ColumnNeedsBackfill(implicitNull) != false {
		t.Fatal("Expected implicit SET DEFAULT NULL to not require a backfill," +
			" ColumnNeedsBackfill states that it does.")
	}
	if ColumnNeedsBackfill(explicitNull) != false {
		t.Fatal("Expected explicit SET DEFAULT NULL to not require a backfill," +
			" ColumnNeedsBackfill states that it does.")
	}
	if ColumnNeedsBackfill(defaultNotNull) != true {
		t.Fatal("Expected explicit SET DEFAULT NULL to require a backfill," +
			" ColumnNeedsBackfill states that it does not.")
	}
}

func TestDefaultExprNil(t *testing.T) {
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	if _, err := conn.Exec(`CREATE DATABASE t`); err != nil {
		t.Fatalf("%+v", err)
	}
	t.Run(fmt.Sprintf("%s - %d", "(a INT PRIMARY KEY)", 1), func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(conn)
		// Execute SQL commands with both implicit and explicit setting of the
		// default expression.
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO t (a) VALUES (1), (2)`)
		sqlDB.Exec(t, `ALTER TABLE t ADD COLUMN b INT NULL`)
		sqlDB.Exec(t, `INSERT INTO t (a) VALUES (3)`)
		sqlDB.Exec(t, `ALTER TABLE t ADD COLUMN c INT DEFAULT NULL`)

		var descBytes []byte
		// Grab the most recently created descriptor.
		row := sqlDB.QueryRow(t,
			`SELECT descriptor FROM system.descriptor ORDER BY id DESC LIMIT 1`)
		row.Scan(&descBytes)
		var desc descpb.Descriptor
		if err := protoutil.Unmarshal(descBytes, &desc); err != nil {
			t.Fatalf("%+v", err)
		}
		// Test and verify that the default expressions of the column descriptors
		// are all nil.
		// nolint:descriptormarshal
		for _, col := range desc.GetTable().Columns {
			if col.DefaultExpr != nil {
				t.Errorf("expected Column Default Expression to be 'nil', got %s instead.", *col.DefaultExpr)
			}
		}
	})
}

func TestLogicalColumnID(t *testing.T) {
	tests := []struct {
		desc     descpb.TableDescriptor
		expected uint32
	}{
		{descpb.TableDescriptor{Columns: []descpb.ColumnDescriptor{{ID: 1, PGAttributeNum: 1}}}, 1},
		// If LogicalColumnID is not explicitly set, it should be lazy loaded as ID.
		{descpb.TableDescriptor{Columns: []descpb.ColumnDescriptor{{ID: 2}}}, 2},
	}
	for i := range tests {
		actual := tests[i].desc.Columns[0].GetPGAttributeNum()
		expected := tests[i].expected

		if expected != actual {
			t.Fatalf("Expected PGAttributeNum to be %d, got %d.", expected, actual)
		}
	}

}
