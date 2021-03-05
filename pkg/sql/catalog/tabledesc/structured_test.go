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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

	desc := NewBuilder(&descpb.TableDescriptor{
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
	}).BuildCreatedMutableTable()
	if err := desc.AllocateIDs(ctx); err != nil {
		t.Fatal(err)
	}

	expected := NewBuilder(&descpb.TableDescriptor{
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
	}).BuildCreatedMutableTable()
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
				Privileges: descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
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
				Privileges: descpb.NewDefaultPrivilegeDescriptor(security.RootUserName()),
			},
			expUpgrade: false,
			verify:     nil,
		},
	}
	for i, test := range tests {
		b := NewBuilder(&test.desc)
		err := b.RunPostDeserializationChanges(context.Background(), nil)
		desc := b.BuildImmutableTable()
		require.NoError(t, err)
		changes, err := GetPostDeserializationChanges(desc)
		require.NoError(t, err)
		upgraded := changes.UpgradedFormatVersion
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

	desc := NewBuilder(&descpb.TableDescriptor{
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
	}).BuildCreatedMutableTable()
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
		if ColumnNeedsBackfill(descpb.DescriptorMutation_ADD, &tc.desc) != tc.add {
			t.Errorf("expected ColumnNeedsBackfill to be %v for adding %s", tc.add, tc.info)
		}
		if ColumnNeedsBackfill(descpb.DescriptorMutation_DROP, &tc.desc) != tc.drop {
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
