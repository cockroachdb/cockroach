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

package sql

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/lib/pq/oid"
)

func TestMakeTableDescColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		sqlType  string
		colType  types.T
		nullable bool
	}{
		{
			"BIT",
			types.T{SemanticType: types.BIT, Width: 1},
			true,
		},
		{
			"BIT(3)",
			types.T{SemanticType: types.BIT, Width: 3},
			true,
		},
		{
			"VARBIT",
			types.T{SemanticType: types.BIT, Width: 0, ZZZ_Oid: oid.T_varbit},
			true,
		},
		{
			"VARBIT(3)",
			types.T{SemanticType: types.BIT, Width: 3, ZZZ_Oid: oid.T_varbit},
			true,
		},
		{
			"BOOLEAN",
			types.T{SemanticType: types.BOOL},
			true,
		},
		{
			"INT",
			types.T{SemanticType: types.INT, Width: 64},
			true,
		},
		{
			"INT2",
			types.T{SemanticType: types.INT, Width: 16},
			true,
		},
		{
			"INT4",
			types.T{SemanticType: types.INT, Width: 32},
			true,
		},
		{
			"INT8",
			types.T{SemanticType: types.INT, Width: 64},
			true,
		},
		{
			"INT64",
			types.T{SemanticType: types.INT, Width: 64},
			true,
		},
		{
			"BIGINT",
			types.T{SemanticType: types.INT, Width: 64},
			true,
		},
		{
			"FLOAT(3)",
			types.T{SemanticType: types.FLOAT, Width: 32},
			true,
		},
		{
			"DOUBLE PRECISION",
			types.T{SemanticType: types.FLOAT, Width: 64},
			true,
		},
		{
			"DECIMAL(6,5)",
			types.T{SemanticType: types.DECIMAL, Precision: 6, Width: 5},
			true,
		},
		{
			"DATE",
			types.T{SemanticType: types.DATE},
			true,
		},
		{
			"TIME",
			types.T{SemanticType: types.TIME},
			true,
		},
		{
			"TIMESTAMP",
			types.T{SemanticType: types.TIMESTAMP},
			true,
		},
		{
			"INTERVAL",
			types.ColumnType{SemanticType: types.INTERVAL},
			true,
		},
		{
			"CHAR",
			types.ColumnType{SemanticType: types.STRING, Width: 1, ZZZ_Oid: oid.T_bpchar},
			true,
		},
		{
			"CHAR(3)",
			types.ColumnType{SemanticType: types.STRING, Width: 3, ZZZ_Oid: oid.T_bpchar},
			true,
		},
		{
			"VARCHAR",
			types.ColumnType{SemanticType: types.STRING, Width: 0, ZZZ_Oid: oid.T_varchar},
			true,
		},
		{
			"VARCHAR(3)",
			types.ColumnType{SemanticType: types.STRING, Width: 3, ZZZ_Oid: oid.T_varchar},
			true,
		},
		{
			"TEXT",
			types.ColumnType{SemanticType: types.STRING},
			true,
		},
		{
			`"char"`,
			types.ColumnType{SemanticType: types.STRING, ZZZ_Oid: oid.T_char},
			true,
		},
		{
			"BLOB",
			types.ColumnType{SemanticType: types.BYTES},
			true,
		},
		{
			"INT NOT NULL",
			types.ColumnType{SemanticType: types.INT, Width: 64},
			false,
		},
		{
			"INT NULL",
			types.ColumnType{SemanticType: types.INT, Width: 64},
			true,
		},
	}
	for i, d := range testData {
		s := "CREATE TABLE foo.test (a " + d.sqlType + " PRIMARY KEY, b " + d.sqlType + ")"
		schema, err := CreateTestTableDescriptor(context.TODO(), 1, 100, s, sqlbase.NewDefaultPrivilegeDescriptor())
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if !reflect.DeepEqual(d.colType, schema.Columns[0].Type) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.colType, schema.Columns[0])
		}
		if schema.Columns[0].Nullable {
			t.Fatalf("%d: expected non-nullable primary key, but got %+v", i, schema.Columns[0].Nullable)
		}
		if !reflect.DeepEqual(d.colType, schema.Columns[1].Type) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.colType, schema.Columns[1])
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

	err = desc.ValidateTable(cluster.MakeTestingClusterSettings())
	if !testutils.IsError(err, sqlbase.ErrMissingPrimaryKey.Error()) {
		t.Fatalf("unexpected error: %v", err)
	}
}
