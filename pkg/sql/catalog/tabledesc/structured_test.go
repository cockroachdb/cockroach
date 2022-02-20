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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	. "github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// Makes an descpb.IndexDescriptor with all columns being ascending.
func makeIndexDescriptor(name string, columnNames []string) descpb.IndexDescriptor {
	dirs := make([]descpb.IndexDescriptor_Direction, 0, len(columnNames))
	for range columnNames {
		dirs = append(dirs, descpb.IndexDescriptor_ASC)
	}
	idx := descpb.IndexDescriptor{
		ID:                  descpb.IndexID(0),
		Name:                name,
		KeyColumnNames:      columnNames,
		KeyColumnDirections: dirs,
		Version:             descpb.EmptyArraysInInvertedIndexesVersion,
	}
	return idx
}

func TestAllocateIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	desc := NewBuilder(&descpb.TableDescriptor{
		ParentID: descpb.ID(bootstrap.TestingUserDescID(0)),
		ID:       descpb.ID(bootstrap.TestingUserDescID(1)),
		Name:     "foo",
		Columns: []descpb.ColumnDescriptor{
			{Name: "a", Type: types.Int},
			{Name: "b", Type: types.Int},
			{Name: "c", Type: types.Int},
		},
		PrimaryIndex: func() descpb.IndexDescriptor {
			idx := makeIndexDescriptor("c", []string{"a", "b"})
			idx.StoreColumnNames = []string{"c"}
			idx.EncodingType = descpb.PrimaryIndexEncoding
			idx.Version = descpb.PrimaryIndexWithStoredColumnsVersion
			return idx
		}(),
		Indexes: []descpb.IndexDescriptor{
			makeIndexDescriptor("d", []string{"b", "a"}),
			makeIndexDescriptor("e", []string{"b"}),
			func() descpb.IndexDescriptor {
				idx := makeIndexDescriptor("f", []string{"c"})
				idx.EncodingType = descpb.PrimaryIndexEncoding
				return idx
			}(),
		},
		Privileges:    catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()),
		FormatVersion: descpb.InterleavedFormatVersion,
	}).BuildCreatedMutableTable()
	if err := desc.AllocateIDs(ctx, clusterversion.TestingClusterVersion); err != nil {
		t.Fatal(err)
	}

	expected := NewBuilder(&descpb.TableDescriptor{
		ParentID: descpb.ID(bootstrap.TestingUserDescID(0)),
		ID:       descpb.ID(bootstrap.TestingUserDescID(1)),
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
			ID: 1, Name: "c", KeyColumnIDs: []descpb.ColumnID{1, 2},
			KeyColumnNames: []string{"a", "b"},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC,
				descpb.IndexDescriptor_ASC},
			StoreColumnIDs:   descpb.ColumnIDs{3},
			StoreColumnNames: []string{"c"},
			EncodingType:     descpb.PrimaryIndexEncoding,
			Version:          descpb.PrimaryIndexWithStoredColumnsVersion,
			ConstraintID:     1,
		},
		Indexes: []descpb.IndexDescriptor{
			{ID: 2, Name: "d", KeyColumnIDs: []descpb.ColumnID{2, 1}, KeyColumnNames: []string{"b", "a"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC,
					descpb.IndexDescriptor_ASC},
				Version: descpb.PrimaryIndexWithStoredColumnsVersion},
			{ID: 3, Name: "e", KeyColumnIDs: []descpb.ColumnID{2}, KeyColumnNames: []string{"b"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion},
			{ID: 4, Name: "f", KeyColumnIDs: []descpb.ColumnID{3}, KeyColumnNames: []string{"c"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				EncodingType:        descpb.PrimaryIndexEncoding,
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion},
		},
		Privileges:       catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()),
		NextColumnID:     4,
		NextFamilyID:     1,
		NextIndexID:      5,
		NextMutationID:   1,
		NextConstraintID: 2,
		FormatVersion:    descpb.InterleavedFormatVersion,
	}).BuildCreatedMutableTable()
	if !reflect.DeepEqual(expected, desc) {
		a, _ := json.MarshalIndent(expected, "", "  ")
		b, _ := json.MarshalIndent(desc, "", "  ")
		t.Fatalf("expected %s, but found %s", a, b)
	}

	if err := desc.AllocateIDs(ctx, clusterversion.TestingClusterVersion); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expected, desc) {
		a, _ := json.MarshalIndent(expected, "", "  ")
		b, _ := json.MarshalIndent(desc, "", "  ")
		t.Fatalf("expected %s, but found %s", a, b)
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
		return NewBuilder(&desc).BuildCreatedMutableTable()
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
		verify     func(int, catalog.TableDescriptor) // nil means no extra verification.
	}{
		{
			desc: descpb.TableDescriptor{
				FormatVersion: descpb.BaseFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo"},
				},
				Privileges: catpb.NewBasePrivilegeDescriptor(security.RootUserName()),
			},
			expUpgrade: true,
			verify: func(i int, desc catalog.TableDescriptor) {
				if len(desc.GetFamilies()) == 0 {
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
				Privileges: catpb.NewBasePrivilegeDescriptor(security.RootUserName()),
			},
			expUpgrade: false,
			verify:     nil,
		},
	}
	for i, test := range tests {
		b := NewBuilder(&test.desc)
		b.RunPostDeserializationChanges()
		desc := b.BuildImmutableTable()
		changes, err := GetPostDeserializationChanges(desc)
		require.NoError(t, err)
		upgraded := changes.Contains(catalog.UpgradedFormatVersion)
		if upgraded != test.expUpgrade {
			t.Fatalf("%d: expected upgraded=%t, but got upgraded=%t", i, test.expUpgrade, upgraded)
		}
		if test.verify != nil {
			test.verify(i, desc)
		}
	}
}

func TestMaybeUpgradeIndexFormatVersion(t *testing.T) {
	tests := []struct {
		desc        descpb.TableDescriptor
		upgraded    *descpb.TableDescriptor
		expValidErr string
	}{
		{ // 1
			// In this simple case, we exercise most of the post-deserialization
			// upgrades, in particular the primary index will have its format version
			// properly set.
			desc: descpb.TableDescriptor{
				FormatVersion: descpb.BaseFormatVersion,
				ID:            51,
				Name:          "tbl",
				ParentID:      52,
				NextColumnID:  3,
				NextIndexID:   2,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo"},
					{ID: 2, Name: "bar"},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  descpb.IndexID(1),
					Name:                "primary",
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"foo", "bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				},
			},
			upgraded: &descpb.TableDescriptor{
				FormatVersion:    descpb.InterleavedFormatVersion,
				ID:               51,
				Name:             "tbl",
				ParentID:         52,
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo"},
					{ID: 2, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{
						ID:          0,
						Name:        "primary",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"foo", "bar"},
					},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  descpb.IndexID(1),
					Name:                "primary",
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"foo", "bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					ConstraintID:        1,
				},
				Privileges: catpb.NewBasePrivilegeDescriptor(security.RootUserName()),
			},
		},
		{ // 2
			// This test case is defined to be a no-op.
			desc: descpb.TableDescriptor{
				FormatVersion:    descpb.InterleavedFormatVersion,
				ID:               51,
				Name:             "tbl",
				ParentID:         52,
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo"},
					{ID: 2, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{
						ID:          0,
						Name:        "primary",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"foo", "bar"},
					},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  descpb.IndexID(1),
					Name:                "primary",
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"foo", "bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					ConstraintID:        1,
				},
				Indexes: []descpb.IndexDescriptor{
					{
						ID:                  descpb.IndexID(2),
						Name:                "other",
						KeyColumnIDs:        []descpb.ColumnID{1},
						KeyColumnNames:      []string{"foo"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						KeySuffixColumnIDs:  []descpb.ColumnID{1}, // This corruption is benign but prevents bumping Version.
						Version:             descpb.SecondaryIndexFamilyFormatVersion,
					},
				},
				Privileges: catpb.NewBasePrivilegeDescriptor(security.RootUserName()),
			},
			upgraded: nil,
		},
		{ // 3
			// In this case we expect validation to fail owing to a violation of
			// assumptions for this secondary index's descriptor format version.
			desc: descpb.TableDescriptor{
				FormatVersion:    descpb.BaseFormatVersion,
				ID:               51,
				Name:             "tbl",
				ParentID:         52,
				NextColumnID:     3,
				NextIndexID:      3,
				NextConstraintID: 2,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo"},
					{ID: 2, Name: "bar"},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  descpb.IndexID(1),
					Name:                "primary",
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"foo", "bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					ConstraintID:        1,
				},
				Indexes: []descpb.IndexDescriptor{
					{
						ID:                  descpb.IndexID(2),
						Name:                "other",
						KeyColumnIDs:        []descpb.ColumnID{1},
						KeyColumnNames:      []string{"foo"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						KeySuffixColumnIDs:  []descpb.ColumnID{1},
						Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
					},
				},
			},
			expValidErr: "relation \"tbl\" (51): index \"other\" has column ID 1 present in: [KeyColumnIDs KeySuffixColumnIDs]",
		},
		{ // 4
			// This test case is much like the first but more complex and with more
			// indexes. All three should be upgraded to the latest format version.
			desc: descpb.TableDescriptor{
				FormatVersion:    descpb.BaseFormatVersion,
				ID:               51,
				Name:             "tbl",
				ParentID:         52,
				NextColumnID:     3,
				NextIndexID:      4,
				NextConstraintID: 2,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo"},
					{ID: 2, Name: "bar"},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  descpb.IndexID(1),
					Name:                "primary",
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"foo", "bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					ConstraintID:        1,
				},
				Indexes: []descpb.IndexDescriptor{
					{
						ID:                  descpb.IndexID(2),
						Name:                "other",
						KeyColumnIDs:        []descpb.ColumnID{1},
						KeyColumnNames:      []string{"foo"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						Version:             descpb.EmptyArraysInInvertedIndexesVersion,
					},
					{
						ID:                  descpb.IndexID(3),
						Name:                "another",
						KeyColumnIDs:        []descpb.ColumnID{2},
						KeyColumnNames:      []string{"bar"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						Version:             descpb.EmptyArraysInInvertedIndexesVersion,
					},
				},
			},
			upgraded: &descpb.TableDescriptor{
				FormatVersion:    descpb.InterleavedFormatVersion,
				ID:               51,
				Name:             "tbl",
				ParentID:         52,
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      4,
				NextConstraintID: 2,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo"},
					{ID: 2, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{
						ID:          0,
						Name:        "primary",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"foo", "bar"},
					},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  descpb.IndexID(1),
					Name:                "primary",
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"foo", "bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					ConstraintID:        1,
				},
				Indexes: []descpb.IndexDescriptor{
					{
						ID:                  descpb.IndexID(2),
						Name:                "other",
						KeyColumnIDs:        []descpb.ColumnID{1},
						KeyColumnNames:      []string{"foo"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						EncodingType:        descpb.SecondaryIndexEncoding,
						Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					},
					{
						ID:                  descpb.IndexID(3),
						Name:                "another",
						KeyColumnIDs:        []descpb.ColumnID{2},
						KeyColumnNames:      []string{"bar"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						EncodingType:        descpb.SecondaryIndexEncoding,
						Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					},
				},
				Privileges: catpb.NewBasePrivilegeDescriptor(security.RootUserName()),
			},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("#%d", i+1), func(t *testing.T) {
			b := NewBuilder(&test.desc)
			b.RunPostDeserializationChanges()
			desc := b.BuildImmutableTable()
			changes, err := GetPostDeserializationChanges(desc)
			require.NoError(t, err)
			err = validate.Self(clusterversion.TestingClusterVersion, desc)
			if test.expValidErr != "" {
				require.EqualError(t, err, test.expValidErr)
				return
			}

			require.NoError(t, err)
			if test.upgraded == nil {
				require.Zero(t, changes)
				return
			}

			if e, a := test.upgraded, desc.TableDesc(); !reflect.DeepEqual(e, a) {
				for _, diff := range pretty.Diff(e, a) {
					t.Error(diff)
				}
			}

			// Run post-deserialization changes again, descriptor should not change.
			b2 := NewBuilder(desc.TableDesc())
			b2.RunPostDeserializationChanges()
			desc2 := b2.BuildImmutableTable()
			changes2, err := GetPostDeserializationChanges(desc2)
			require.NoError(t, err)
			require.Zero(t, changes2)
			require.Equal(t, desc.TableDesc(), desc2.TableDesc())
		})
	}
}

func TestUnvalidateConstraints(t *testing.T) {
	ctx := context.Background()

	desc := NewBuilder(&descpb.TableDescriptor{
		Name:             "test",
		ParentID:         descpb.ID(1),
		NextConstraintID: 2,
		Columns: []descpb.ColumnDescriptor{
			{Name: "a", Type: types.Int},
			{Name: "b", Type: types.Int},
			{Name: "c", Type: types.Int}},
		FormatVersion: descpb.InterleavedFormatVersion,
		Indexes:       []descpb.IndexDescriptor{makeIndexDescriptor("d", []string{"b", "a"})},
		Privileges:    catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()),
		OutboundFKs: []descpb.ForeignKeyConstraint{
			{
				Name:              "fk",
				ReferencedTableID: descpb.ID(1),
				Validity:          descpb.ConstraintValidity_Validated,
				ConstraintID:      1,
			},
		},
	}).BuildCreatedMutableTable()
	if err := desc.AllocateIDs(ctx, clusterversion.TestingClusterVersion); err != nil {
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
		{
			"(a INT PRIMARY KEY, b INT, INDEX (b))",
			1, // Primary index
			1,
		},
		{
			"(a INT PRIMARY KEY, b INT, INDEX (b))",
			2, // 'b' index
			1,
		},
		{
			"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))",
			1, // Primary index
			2,
		},
		{
			"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (b))",
			2, // 'b' index
			1,
		},
		{
			"(a INT PRIMARY KEY, b INT, FAMILY (a), FAMILY (b), INDEX (a) STORING (b))",
			2, // 'a' index
			2,
		},
		{
			"(a INT, b INT, c INT, d INT, e INT, FAMILY f0 (a, b), " +
				"FAMILY f1 (c), FAMILY f2(d, e), INDEX (d) STORING (b))",
			2, // 'd' index
			1, // Only f0 is needed.
		},
		{
			"(a INT, b INT, c INT, d INT, e INT, FAMILY f0 (a, b), " +
				"FAMILY f1 (c), FAMILY f2(d, e), INDEX (d) STORING (c, e))",
			2, // 'd' index
			3, // f1 and f2 are needed, but f0 is always present.
		},
		{
			"(a INT, b INT, c INT, d INT, e INT, FAMILY f0 (a, b), " +
				"FAMILY f1 (c), FAMILY f2(d, e), INDEX (a, c) STORING (d))",
			2, // 'a, c' index
			2, // Only f2 is needed, but f0 is always present.
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%s - %d", test.createTable, test.indexID), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(conn)
			tableName := fmt.Sprintf("t%d", i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE d.%s %s`, tableName, test.createTable))

			desc := desctestutils.TestingGetPublicTableDescriptor(db, keys.SystemSQLCodec, "d", tableName)
			require.NotNil(t, desc)
			idx, err := desc.FindIndexWithID(test.indexID)
			require.NoError(t, err)
			keys := desc.IndexKeysPerRow(idx)
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
	testCases := []struct {
		info string
		desc descpb.ColumnDescriptor
		// add is true of we expect backfill when adding this column.
		add bool
		// drop is true of we expect backfill when adding this column.
		drop bool
	}{
		{
			info: "implicit SET DEFAULT NULL",
			desc: descpb.ColumnDescriptor{
				Name: "am", ID: 2, Type: types.Int, DefaultExpr: nil, Nullable: true, ComputeExpr: nil,
			},
			add:  false,
			drop: true,
		}, {
			info: "explicit SET DEFAULT NULL",
			desc: descpb.ColumnDescriptor{
				Name: "ex", ID: 3, Type: types.Int, DefaultExpr: &null, Nullable: true, ComputeExpr: nil,
			},
			add:  false,
			drop: true,
		},
		{
			info: "explicit SET DEFAULT non-NULL",
			desc: descpb.ColumnDescriptor{
				Name: "four", ID: 4, Type: types.Int, DefaultExpr: &four, Nullable: true, ComputeExpr: nil,
			},
			add:  true,
			drop: true,
		},
		{
			info: "computed stored",
			desc: descpb.ColumnDescriptor{
				Name: "stored", ID: 5, Type: types.Int, DefaultExpr: nil, ComputeExpr: &four,
			},
			add:  true,
			drop: true,
		},
		{
			info: "computed virtual",
			desc: descpb.ColumnDescriptor{
				Name: "virtual", ID: 6, Type: types.Int, DefaultExpr: nil, ComputeExpr: &four, Virtual: true,
			},
			add:  false,
			drop: false,
		},
	}

	for _, tc := range testCases {
		if catalog.ColumnNeedsBackfill(TestingMakeColumn(descpb.DescriptorMutation_ADD, &tc.desc)) != tc.add {
			t.Errorf("expected ColumnNeedsBackfill to be %v for adding %s", tc.add, tc.info)
		}
		if catalog.ColumnNeedsBackfill(TestingMakeColumn(descpb.DescriptorMutation_DROP, &tc.desc)) != tc.drop {
			t.Errorf("expected ColumnNeedsBackfill to be %v for dropping %s", tc.drop, tc.info)
		}
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
		//nolint:descriptormarshal
		for _, col := range desc.GetTable().Columns {
			if col.DefaultExpr != nil {
				t.Errorf("expected Column Default Expression to be 'nil', got %s instead.", *col.DefaultExpr)
			}
		}
	})
}

// TestRemoveDefaultExprFromComputedColumn tests that default expressions are
// correctly removed from descriptors of computed columns as part of the
// RunPostDeserializationChanges suite.
func TestRemoveDefaultExprFromComputedColumn(t *testing.T) {
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	const expectedErrRE = `.*: computed column \"b\" cannot also have a DEFAULT expression`
	// Create a table with a computed column.
	tdb.Exec(t, `CREATE DATABASE t`)
	tdb.Exec(t, `CREATE TABLE t.tbl (a INT PRIMARY KEY, b INT AS (1) STORED)`)

	// Get the descriptor for the table.
	tbl := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "tbl")

	// Setting a default value on the computed column should fail.
	tdb.ExpectErr(t, expectedErrRE, `ALTER TABLE t.tbl ALTER COLUMN b SET DEFAULT 2`)

	// Copy the descriptor proto for the table and modify it by setting a default
	// expression.
	var desc *descpb.TableDescriptor
	{
		desc = NewBuilder(tbl.TableDesc()).BuildImmutableTable().TableDesc()
		defaultExpr := "2"
		desc.Columns[1].DefaultExpr = &defaultExpr
	}

	// This modified table descriptor should fail validation.
	{
		broken := NewBuilder(desc).BuildImmutableTable()
		require.Error(t, validate.Self(clusterversion.TestingClusterVersion, broken))
	}

	// This modified table descriptor should be fixed by removing the default
	// expression.
	{
		b := NewBuilder(desc)
		b.RunPostDeserializationChanges()
		fixed := b.BuildImmutableTable()
		require.NoError(t, validate.Self(clusterversion.TestingClusterVersion, fixed))
		changes, err := GetPostDeserializationChanges(fixed)
		require.NoError(t, err)
		require.True(t, changes.Contains(catalog.RemovedDefaultExprFromComputedColumn))
		require.False(t, fixed.PublicColumns()[1].HasDefault())
	}
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
