// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func TestMakeTableDescColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testData := []struct {
		sqlType  string
		colType  *types.T
		nullable bool
	}{
		{
			"BIT",
			types.MakeBit(1),
			true,
		},
		{
			"BIT(3)",
			types.MakeBit(3),
			true,
		},
		{
			"VARBIT",
			types.VarBit,
			true,
		},
		{
			"VARBIT(3)",
			types.MakeVarBit(3),
			true,
		},
		{
			"BOOLEAN",
			types.Bool,
			true,
		},
		{
			"INT",
			types.Int,
			true,
		},
		{
			"INT2",
			types.Int2,
			true,
		},
		{
			"INT4",
			types.Int4,
			true,
		},
		{
			"INT8",
			types.Int,
			true,
		},
		{
			"INT64",
			types.Int,
			true,
		},
		{
			"BIGINT",
			types.Int,
			true,
		},
		{
			"FLOAT(3)",
			types.Float4,
			true,
		},
		{
			"DOUBLE PRECISION",
			types.Float,
			true,
		},
		{
			"DECIMAL(6,5)",
			types.MakeDecimal(6, 5),
			true,
		},
		{
			"DATE",
			types.Date,
			true,
		},
		{
			"TIME",
			types.Time,
			true,
		},
		{
			"TIMESTAMP",
			types.Timestamp,
			true,
		},
		{
			"INTERVAL",
			types.Interval,
			true,
		},
		{
			"CHAR",
			types.MakeChar(1),
			true,
		},
		{
			"CHAR(3)",
			types.MakeChar(3),
			true,
		},
		{
			"VARCHAR",
			types.VarChar,
			true,
		},
		{
			"VARCHAR(3)",
			types.MakeVarChar(3),
			true,
		},
		{
			"TEXT",
			types.String,
			true,
		},
		{
			`"char"`,
			types.QChar,
			true,
		},
		{
			"BLOB",
			types.Bytes,
			true,
		},
		{
			"INT NOT NULL",
			types.Int,
			false,
		},
		{
			"INT NULL",
			types.Int,
			true,
		},
	}
	for i, d := range testData {
		s := "CREATE TABLE foo.test (a " + d.sqlType + " PRIMARY KEY, b " + d.sqlType + ")"
		schema, err := CreateTestTableDescriptor(context.Background(), 1, 100, s,
			catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()))
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if schema.Columns[0].Nullable {
			t.Fatalf("%d: expected non-nullable primary key, but got %+v", i, schema.Columns[0].Nullable)
		}
		if !d.colType.Identical(schema.Columns[0].Type) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.colType.DebugString(), schema.Columns[0].Type.DebugString())
		}
		if d.nullable != schema.Columns[1].Nullable {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.nullable, schema.Columns[1].Nullable)
		}
	}
}

func TestMakeTableDescIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testData := []struct {
		sql     string
		primary descpb.IndexDescriptor
		indexes []descpb.IndexDescriptor
	}{
		{
			"a INT PRIMARY KEY",
			descpb.IndexDescriptor{
				Name:                tabledesc.PrimaryKeyIndexName("test"),
				ID:                  1,
				Unique:              true,
				KeyColumnNames:      []string{"a"},
				KeyColumnIDs:        []descpb.ColumnID{1},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				EncodingType:        descpb.PrimaryIndexEncoding,
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				ConstraintID:        1,
			},
			[]descpb.IndexDescriptor{},
		},
		{
			"a INT UNIQUE, b INT PRIMARY KEY",
			descpb.IndexDescriptor{
				Name:                "test_pkey",
				ID:                  1,
				Unique:              true,
				KeyColumnNames:      []string{"b"},
				KeyColumnIDs:        []descpb.ColumnID{2},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				StoreColumnNames:    []string{"a"},
				StoreColumnIDs:      []descpb.ColumnID{1},
				EncodingType:        descpb.PrimaryIndexEncoding,
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				ConstraintID:        2,
			},
			[]descpb.IndexDescriptor{
				{
					Name:                "test_a_key",
					ID:                  2,
					Unique:              true,
					KeyColumnNames:      []string{"a"},
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeySuffixColumnIDs:  []descpb.ColumnID{2},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					ConstraintID:        1,
				},
			},
		},
		{
			"a INT, b INT, CONSTRAINT c PRIMARY KEY (a, b)",
			descpb.IndexDescriptor{
				Name:                "c",
				ID:                  1,
				Unique:              true,
				KeyColumnNames:      []string{"a", "b"},
				KeyColumnIDs:        []descpb.ColumnID{1, 2},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				EncodingType:        descpb.PrimaryIndexEncoding,
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				ConstraintID:        1,
			},
			[]descpb.IndexDescriptor{},
		},
		{
			"a INT, b INT, CONSTRAINT c UNIQUE (b), PRIMARY KEY (a, b)",
			descpb.IndexDescriptor{
				Name:                tabledesc.PrimaryKeyIndexName("test"),
				ID:                  1,
				Unique:              true,
				KeyColumnNames:      []string{"a", "b"},
				KeyColumnIDs:        []descpb.ColumnID{1, 2},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				EncodingType:        descpb.PrimaryIndexEncoding,
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				ConstraintID:        2,
			},
			[]descpb.IndexDescriptor{
				{
					Name:                "c",
					ID:                  2,
					Unique:              true,
					KeyColumnNames:      []string{"b"},
					KeyColumnIDs:        []descpb.ColumnID{2},
					KeySuffixColumnIDs:  []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					ConstraintID:        1,
				},
			},
		},
		{
			"a INT, b INT, PRIMARY KEY (a, b)",
			descpb.IndexDescriptor{
				Name:                tabledesc.PrimaryKeyIndexName("test"),
				ID:                  1,
				Unique:              true,
				KeyColumnNames:      []string{"a", "b"},
				KeyColumnIDs:        []descpb.ColumnID{1, 2},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				EncodingType:        descpb.PrimaryIndexEncoding,
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				ConstraintID:        1,
			},
			[]descpb.IndexDescriptor{},
		},
	}
	for i, d := range testData {
		s := "CREATE TABLE foo.test (" + d.sql + ")"
		schema, err := CreateTestTableDescriptor(context.Background(), 1, 100, s,
			catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()))
		if err != nil {
			t.Fatalf("%d (%s): %v", i, d.sql, err)
		}
		activeIndexDescs := make([]descpb.IndexDescriptor, len(schema.ActiveIndexes()))
		for i, index := range schema.ActiveIndexes() {
			activeIndexDescs[i] = *index.IndexDesc()
		}

		if !reflect.DeepEqual(d.primary, activeIndexDescs[0]) {
			t.Fatalf("%d (%s): primary mismatch: expected %+v, but got %+v", i, d.sql, d.primary, activeIndexDescs[0])
		}
		if !reflect.DeepEqual(d.indexes, activeIndexDescs[1:]) {
			t.Fatalf("%d (%s): index mismatch: expected %+v, but got %+v", i, d.sql, d.indexes, activeIndexDescs[1:])
		}

	}
}

func TestMakeTableDescUniqueConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testData := []struct {
		sql         string
		constraints []descpb.UniqueWithoutIndexConstraint
	}{
		{
			"a INT UNIQUE",
			nil,
		},
		{
			"a INT UNIQUE WITHOUT INDEX, b INT PRIMARY KEY",
			[]descpb.UniqueWithoutIndexConstraint{
				{
					TableID:      100,
					ColumnIDs:    []descpb.ColumnID{1},
					Name:         "unique_a",
					ConstraintID: 2,
				},
			},
		},
		{
			"a INT, b INT, CONSTRAINT c UNIQUE WITHOUT INDEX (b), UNIQUE (a, b)",
			[]descpb.UniqueWithoutIndexConstraint{
				{
					TableID:      100,
					ColumnIDs:    []descpb.ColumnID{2},
					Name:         "c",
					ConstraintID: 3,
				},
			},
		},
		{
			"a INT, b INT, c INT, UNIQUE WITHOUT INDEX (a, b), UNIQUE WITHOUT INDEX (c)",
			[]descpb.UniqueWithoutIndexConstraint{
				{
					TableID:      100,
					ColumnIDs:    []descpb.ColumnID{1, 2},
					Name:         "unique_a_b",
					ConstraintID: 2,
				},
				{
					TableID:      100,
					ColumnIDs:    []descpb.ColumnID{3},
					Name:         "unique_c",
					ConstraintID: 3,
				},
			},
		},
	}
	for i, d := range testData {
		s := "CREATE TABLE foo.test (" + d.sql + ")"
		schema, err := CreateTestTableDescriptor(context.Background(), 1, 100, s,
			catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()))
		if err != nil {
			t.Fatalf("%d (%s): %v", i, d.sql, err)
		}
		if !reflect.DeepEqual(d.constraints, schema.UniqueWithoutIndexConstraints) {
			t.Fatalf(
				"%d (%s): constraints mismatch: expected %+v, but got %+v",
				i, d.sql, d.constraints, schema.UniqueWithoutIndexConstraints,
			)
		}
	}
}

func TestPrimaryKeyUnspecified(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := "CREATE TABLE foo.test (a INT, b INT, CONSTRAINT c UNIQUE (b))"
	ctx := context.Background()
	desc, err := CreateTestTableDescriptor(ctx, 1, 100, s,
		catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()))
	if err != nil {
		t.Fatal(err)
	}
	desc.SetPrimaryIndex(descpb.IndexDescriptor{})

	err = descbuilder.ValidateSelf(desc, clusterversion.TestingClusterVersion)
	if !testutils.IsError(err, tabledesc.ErrMissingPrimaryKey.Error()) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCanCloneTableWithUDT(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	if _, err := sqlDB.Exec(`
CREATE DATABASE test;
CREATE TYPE test.t AS ENUM ('hello');
CREATE TABLE test.tt (x test.t);
`); err != nil {
		t.Fatal(err)
	}
	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "tt")
	typLookup := func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error) {
		var typeDesc catalog.TypeDescriptor
		if err := TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
			typeDesc, err = col.Direct().MustGetTypeDescByID(ctx, txn, id)
			return err
		}); err != nil {
			return tree.TypeName{}, nil, err
		}
		return tree.TypeName{}, typeDesc, nil
	}
	if err := typedesc.HydrateTypesInTableDescriptor(ctx, desc.TableDesc(), typedesc.TypeLookupFunc(typLookup)); err != nil {
		t.Fatal(err)
	}
	// Ensure that we can clone this table.
	_ = protoutil.Clone(desc.TableDesc()).(*descpb.TableDescriptor)
}

// TestSerializedUDTsInTableDescriptor tests that expressions containing
// explicit type references and members of user defined types are serialized
// in a way that is stable across changes to the type itself. For example,
// we want to ensure that enum members are serialized in a way that is stable
// across renames to the member itself.
func TestSerializedUDTsInTableDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	getDefault := func(desc catalog.TableDescriptor) string {
		return desc.PublicColumns()[0].GetDefaultExpr()
	}
	getComputed := func(desc catalog.TableDescriptor) string {
		return desc.PublicColumns()[0].GetComputeExpr()
	}
	getCheck := func(desc catalog.TableDescriptor) string {
		return desc.GetChecks()[0].Expr
	}
	testdata := []struct {
		colSQL       string
		expectedExpr string
		getExpr      func(desc catalog.TableDescriptor) string
	}{
		// Test a simple UDT as the default value.
		{
			"x greeting DEFAULT ('hello')",
			`x'80':::@$OID`,
			getDefault,
		},
		{
			"x greeting DEFAULT ('hello':::greeting)",
			`x'80':::@$OID`,
			getDefault,
		},
		// Test when a UDT is used in a default value, but isn't the
		// final type of the column.
		{
			"x INT DEFAULT (CASE WHEN 'hello'::greeting = 'hello'::greeting THEN 0 ELSE 1 END)",
			`CASE WHEN x'80':::@$OID = x'80':::@$OID THEN 0:::INT8 ELSE 1:::INT8 END`,
			getDefault,
		},
		{
			"x BOOL DEFAULT ('hello'::greeting IS OF (greeting, greeting))",
			`x'80':::@$OID IS OF (@$OID, @$OID)`,
			getDefault,
		},
		// Test check constraints.
		{
			"x greeting, CHECK (x = 'hello')",
			`x = x'80':::@$OID`,
			getCheck,
		},
		{
			"x greeting, y STRING, CHECK (y::greeting = x)",
			`y::@$OID = x`,
			getCheck,
		},
		// Test a computed column in the same cases as above.
		{
			"x greeting AS ('hello') STORED",
			`x'80':::@$OID`,
			getComputed,
		},
		{
			"x INT AS (CASE WHEN 'hello'::greeting = 'hello'::greeting THEN 0 ELSE 1 END) STORED",
			`CASE WHEN x'80':::@$OID = x'80':::@$OID THEN 0:::INT8 ELSE 1:::INT8 END`,
			getComputed,
		},
	}

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	if _, err := sqlDB.Exec(`
	CREATE DATABASE test;
	USE test;
	CREATE TYPE greeting AS ENUM ('hello');
`); err != nil {
		t.Fatal(err)
	}
	typDesc := desctestutils.TestingGetTypeDescriptor(
		kvDB, keys.SystemSQLCodec, "test", "public", "greeting",
	)
	oid := fmt.Sprintf("%d", typedesc.TypeIDToOID(typDesc.GetID()))
	for _, tc := range testdata {
		create := "CREATE TABLE t (" + tc.colSQL + ")"
		if _, err := sqlDB.Exec(create); err != nil {
			t.Fatal(err)
		}
		desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
		found := tc.getExpr(desc)
		expected := os.Expand(tc.expectedExpr, expander{"OID": oid}.mapping)
		if expected != found {
			t.Errorf("for column %s, found %s, expected %s", tc.colSQL, found, expected)
		}
		if _, err := sqlDB.Exec("DROP TABLE t"); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSerializedUDTsInView tests that view queries containing
// explicit type references and members of user defined types are serialized
// in a way that is stable across changes to the type itself. For example,
// we want to ensure that enum members are serialized in a way that is stable
// across renames to the member itself.
func TestSerializedUDTsInView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testdata := []struct {
		viewQuery    string
		expectedExpr string
	}{
		// Test simple UDT in the view query.
		{
			"SELECT 'hello':::greeting",
			`(SELECT b'\x80':::@$OID)`,
		},
		// Test when a UDT is used in a view query, but isn't the
		// final type of the column.
		{
			"SELECT 'hello'::greeting < 'hello'::greeting",
			`(SELECT b'\x80':::@$OID < b'\x80':::@$OID)`,
		},
		// Test when a UDT is used in various parts of a view (subquery, CTE, etc.).
		{
			"SELECT k FROM (SELECT 'hello'::greeting AS k)",
			`(SELECT k FROM (SELECT b'\x80':::@$OID AS k))`,
		},
		{
			"WITH w AS (SELECT 'hello':::greeting AS k) SELECT k FROM w",
			`(WITH w AS (SELECT b'\x80':::@$OID AS k) SELECT k FROM w)`,
		},
	}

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	if _, err := sqlDB.Exec(`
	CREATE DATABASE test;
	USE test;
	CREATE TYPE greeting AS ENUM ('hello');
`); err != nil {
		t.Fatal(err)
	}
	typDesc := desctestutils.TestingGetTypeDescriptor(
		kvDB, keys.SystemSQLCodec, "test", "public", "greeting",
	)
	oid := fmt.Sprintf("%d", typedesc.TypeIDToOID(typDesc.GetID()))
	for _, tc := range testdata {
		create := "CREATE VIEW v AS (" + tc.viewQuery + ")"
		if _, err := sqlDB.Exec(create); err != nil {
			t.Fatal(err)
		}
		desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "v")
		foundViewQuery := desc.GetViewQuery()
		expected := os.Expand(tc.expectedExpr, expander{"OID": oid}.mapping)
		if expected != foundViewQuery {
			t.Errorf("for view %s, found %s, expected %s", tc.viewQuery, foundViewQuery, expected)
		}
		if _, err := sqlDB.Exec("DROP VIEW v"); err != nil {
			t.Fatal(err)
		}
	}
}

// expander is useful for expanding strings using os.Expand
type expander map[string]string

func (e expander) mapping(from string) (to string) {
	if to, ok := e[from]; ok {
		return to
	}
	return ""
}

// TestJobsCache verifies that a job for a given table gets cached and reused
// for following schema changes in the same transaction.
func TestJobsCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	foundInCache := false
	runAfterSCJobsCacheLookup := func(record *jobs.Record) {
		if record != nil {
			foundInCache = true
		}
	}

	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLExecutor = &ExecutorTestingKnobs{
		RunAfterSCJobsCacheLookup: runAfterSCJobsCacheLookup,
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// ALTER TABLE t1 ADD COLUMN x INT should have created a job for the table
	// we're altering.
	// Further schema changes to the table should have an existing cache
	// entry for the job.
	if _, err := sqlDB.Exec(`
CREATE TABLE t1();
BEGIN;
ALTER TABLE t1 ADD COLUMN x INT;
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
ALTER TABLE t1 ADD COLUMN y INT;
`); err != nil {
		t.Fatal(err)
	}

	if !foundInCache {
		t.Fatal("expected a job to be found in cache for table t1, " +
			"but a job was not found")
	}

	// Verify that the cache is cleared once the transaction ends.
	// Commit the old transaction.
	if _, err := sqlDB.Exec(`
COMMIT;
`); err != nil {
		t.Fatal(err)
	}

	foundInCache = false

	if _, err := sqlDB.Exec(`
BEGIN;
ALTER TABLE t1 ADD COLUMN z INT;
`); err != nil {
		t.Fatal(err)
	}

	if foundInCache {
		t.Fatal("expected a job to not be found in cache for table t1, " +
			"but a job was found")
	}
}
