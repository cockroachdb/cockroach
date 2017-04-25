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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// CreateTestTableDescriptor converts a SQL string to a table for test purposes.
// Will fail on complex tables where that operation requires e.g. looking up
// other tables or otherwise utilizing a planner, since the planner used here is
// just a zero value placeholder.
func CreateTestTableDescriptor(
	ctx context.Context,
	parentID, id sqlbase.ID,
	schema string,
	privileges *sqlbase.PrivilegeDescriptor,
) (sqlbase.TableDescriptor, error) {
	stmt, err := parser.ParseOne(schema)
	if err != nil {
		return sqlbase.TableDescriptor{}, err
	}
	p := planner{session: new(Session)}
	p.evalCtx = parser.MakeTestingEvalContext()
	return p.makeTableDesc(ctx, stmt.(*parser.CreateTable), parentID, id, privileges, nil)
}

func TestMakeTableDescColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		sqlType  string
		colType  sqlbase.ColumnType
		nullable bool
	}{
		{
			"BIT",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT, Width: 1},
			true,
		},
		{
			"BIT(3)",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT, Width: 3},
			true,
		},
		{
			"BOOLEAN",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_BOOL},
			true,
		},
		{
			"INT",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
			true,
		},
		{
			"FLOAT(3)",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_FLOAT, Precision: 3},
			true,
		},
		{
			"DECIMAL(6,5)",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_DECIMAL, Precision: 6, Width: 5},
			true,
		},
		{
			"DATE",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_DATE},
			true,
		},
		{
			"TIMESTAMP",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_TIMESTAMP},
			true,
		},
		{
			"INTERVAL",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_INTERVAL},
			true,
		},
		{
			"CHAR",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_STRING},
			true,
		},
		{
			"TEXT",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_STRING},
			true,
		},
		{
			"BLOB",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_BYTES},
			true,
		},
		{
			"INT NOT NULL",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
			false,
		},
		{
			"INT NULL",
			sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
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

	err = desc.ValidateTable()
	if !testutils.IsError(err, sqlbase.ErrMissingPrimaryKey.Error()) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRemoveLeaseIfExpiring(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mc := hlc.NewManualClock(123)
	lc := &LeaseCollection{
		leaseMgr: &LeaseManager{
			LeaseStore: LeaseStore{clock: hlc.NewClock(mc.UnixNano, time.Nanosecond)},
		},
	}

	var txn client.Txn

	if lc.removeLeaseIfExpiring(context.TODO(), &txn, nil) {
		t.Error("expected false with nil input")
	}

	// Add a lease to the planner.
	d := int64(LeaseDuration)
	l1 := &LeaseState{expiration: parser.DTimestamp{Time: time.Unix(0, mc.UnixNano()+d+1)}}
	lc.leases = append(lc.leases, l1)
	et := hlc.Timestamp{WallTime: l1.Expiration().UnixNano()}
	txn.UpdateDeadlineMaybe(et)

	if lc.removeLeaseIfExpiring(context.TODO(), &txn, l1) {
		t.Error("expected false with a non-expiring lease")
	}
	if d := *txn.GetDeadline(); d != et {
		t.Errorf("expected deadline %s but got %s", et, d)
	}

	// Advance the clock so that l1 will be expired.
	mc.Increment(d + 1)

	// Add another lease.
	l2 := &LeaseState{expiration: parser.DTimestamp{Time: time.Unix(0, mc.UnixNano()+d+1)}}
	lc.leases = append(lc.leases, l2)
	if !lc.removeLeaseIfExpiring(context.TODO(), &txn, l1) {
		t.Error("expected true with an expiring lease")
	}
	et = hlc.Timestamp{WallTime: l2.Expiration().UnixNano()}
	txn.UpdateDeadlineMaybe(et)

	if !(len(lc.leases) == 1 && lc.leases[0] == l2) {
		t.Errorf("expected leases to contain %s but has %s", l2, lc.leases)
	}

	if d := *txn.GetDeadline(); d != et {
		t.Errorf("expected deadline %s, but got %s", et, d)
	}
}
