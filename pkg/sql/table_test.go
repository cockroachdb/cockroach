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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestMakeTableDescColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
			types.MakeQChar(0),
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
		schema, err := CreateTestTableDescriptor(context.TODO(), 1, 100, s, sqlbase.NewDefaultPrivilegeDescriptor())
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if schema.Columns[0].Nullable {
			t.Fatalf("%d: expected non-nullable primary key, but got %+v", i, schema.Columns[0].Nullable)
		}
		if !reflect.DeepEqual(*d.colType, schema.Columns[0].Type) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.colType.DebugString(), schema.Columns[0].Type.DebugString())
		}
		if d.nullable != schema.Columns[1].Nullable {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.nullable, schema.Columns[1].Nullable)
		}
	}
}

func TestMakeTableDescIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		sql     string
		primary sqlbase.IndexDescriptor
		indexes []sqlbase.IndexDescriptor
	}{
		{
			"a INT PRIMARY KEY",
			sqlbase.IndexDescriptor{
				Name:             sqlbase.PrimaryKeyIndexName,
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"a"},
				ColumnIDs:        []sqlbase.ColumnID{1},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
				Version:          sqlbase.SecondaryIndexFamilyFormatVersion,
			},
			[]sqlbase.IndexDescriptor{},
		},
		{
			"a INT UNIQUE, b INT PRIMARY KEY",
			sqlbase.IndexDescriptor{
				Name:             "primary",
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"b"},
				ColumnIDs:        []sqlbase.ColumnID{2},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
				Version:          sqlbase.SecondaryIndexFamilyFormatVersion,
			},
			[]sqlbase.IndexDescriptor{
				{
					Name:             "test_a_key",
					ID:               2,
					Unique:           true,
					ColumnNames:      []string{"a"},
					ColumnIDs:        []sqlbase.ColumnID{1},
					ExtraColumnIDs:   []sqlbase.ColumnID{2},
					ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
					Version:          sqlbase.SecondaryIndexFamilyFormatVersion,
				},
			},
		},
		{
			"a INT, b INT, CONSTRAINT c PRIMARY KEY (a, b)",
			sqlbase.IndexDescriptor{
				Name:             "c",
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"a", "b"},
				ColumnIDs:        []sqlbase.ColumnID{1, 2},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_ASC},
				Version:          sqlbase.SecondaryIndexFamilyFormatVersion,
			},
			[]sqlbase.IndexDescriptor{},
		},
		{
			"a INT, b INT, CONSTRAINT c UNIQUE (b), PRIMARY KEY (a, b)",
			sqlbase.IndexDescriptor{
				Name:             "primary",
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"a", "b"},
				ColumnIDs:        []sqlbase.ColumnID{1, 2},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_ASC},
				Version:          sqlbase.SecondaryIndexFamilyFormatVersion,
			},
			[]sqlbase.IndexDescriptor{
				{
					Name:             "c",
					ID:               2,
					Unique:           true,
					ColumnNames:      []string{"b"},
					ColumnIDs:        []sqlbase.ColumnID{2},
					ExtraColumnIDs:   []sqlbase.ColumnID{1},
					ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
					Version:          sqlbase.SecondaryIndexFamilyFormatVersion,
				},
			},
		},
		{
			"a INT, b INT, PRIMARY KEY (a, b)",
			sqlbase.IndexDescriptor{
				Name:             sqlbase.PrimaryKeyIndexName,
				ID:               1,
				Unique:           true,
				ColumnNames:      []string{"a", "b"},
				ColumnIDs:        []sqlbase.ColumnID{1, 2},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_ASC},
				Version:          sqlbase.SecondaryIndexFamilyFormatVersion,
			},
			[]sqlbase.IndexDescriptor{},
		},
	}
	for i, d := range testData {
		s := "CREATE TABLE foo.test (" + d.sql + ")"
		schema, err := CreateTestTableDescriptor(context.TODO(), 1, 100, s, sqlbase.NewDefaultPrivilegeDescriptor())
		if err != nil {
			t.Fatalf("%d (%s): %v", i, d.sql, err)
		}
		if !reflect.DeepEqual(d.primary, schema.PrimaryIndex) {
			t.Fatalf("%d (%s): primary mismatch: expected %+v, but got %+v", i, d.sql, d.primary, schema.PrimaryIndex)
		}
		if !reflect.DeepEqual(d.indexes, append([]sqlbase.IndexDescriptor{}, schema.Indexes...)) {
			t.Fatalf("%d (%s): index mismatch: expected %+v, but got %+v", i, d.sql, d.indexes, schema.Indexes)
		}

	}
}

func TestPrimaryKeyUnspecified(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := "CREATE TABLE foo.test (a INT, b INT, CONSTRAINT c UNIQUE (b))"
	desc, err := CreateTestTableDescriptor(context.TODO(), 1, 100, s, sqlbase.NewDefaultPrivilegeDescriptor())
	if err != nil {
		t.Fatal(err)
	}
	desc.PrimaryIndex = sqlbase.IndexDescriptor{}

	err = desc.ValidateTable()
	if !testutils.IsError(err, sqlbase.ErrMissingPrimaryKey.Error()) {
		t.Fatalf("unexpected error: %v", err)
	}
}
