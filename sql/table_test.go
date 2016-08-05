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

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

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
		stmt, err := parser.ParseOneTraditional(
			"CREATE TABLE foo.test (a " + d.sqlType + " PRIMARY KEY, b " + d.sqlType + ")")
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		create := stmt.(*parser.CreateTable)
		if err := create.Table.NormalizeTableName(""); err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		schema, err := MakeTableDesc(create, 1)
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
				Unique:           true,
				ColumnNames:      []string{"a"},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{},
		},
		{
			"a INT UNIQUE, b INT PRIMARY KEY",
			sqlbase.IndexDescriptor{
				Name:             "primary",
				Unique:           true,
				ColumnNames:      []string{"b"},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{
				{
					Name:             "",
					Unique:           true,
					ColumnNames:      []string{"a"},
					ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
				},
			},
		},
		{
			"a INT, b INT, CONSTRAINT c PRIMARY KEY (a, b)",
			sqlbase.IndexDescriptor{
				Name:             "c",
				Unique:           true,
				ColumnNames:      []string{"a", "b"},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{},
		},
		{
			"a INT, b INT, CONSTRAINT c UNIQUE (b), PRIMARY KEY (a, b)",
			sqlbase.IndexDescriptor{
				Name:             "primary",
				Unique:           true,
				ColumnNames:      []string{"a", "b"},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{
				{
					Name:             "c",
					Unique:           true,
					ColumnNames:      []string{"b"},
					ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
				},
			},
		},
		{
			"a INT, b INT, PRIMARY KEY (a, b)",
			sqlbase.IndexDescriptor{
				Name:             sqlbase.PrimaryKeyIndexName,
				Unique:           true,
				ColumnNames:      []string{"a", "b"},
				ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC, sqlbase.IndexDescriptor_ASC},
			},
			[]sqlbase.IndexDescriptor{},
		},
	}
	for i, d := range testData {
		stmt, err := parser.ParseOneTraditional("CREATE TABLE foo.test (" + d.sql + ")")
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		create := stmt.(*parser.CreateTable)
		if err := create.Table.NormalizeTableName(""); err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		schema, err := MakeTableDesc(create, 1)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if !reflect.DeepEqual(d.primary, schema.PrimaryIndex) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.primary, schema.PrimaryIndex)
		}
		if !reflect.DeepEqual(d.indexes, append([]sqlbase.IndexDescriptor{}, schema.Indexes...)) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.indexes, schema.Indexes)
		}

	}
}

func TestPrimaryKeyUnspecified(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stmt, err := parser.ParseOneTraditional(
		"CREATE TABLE foo.test (a INT, b INT, CONSTRAINT c UNIQUE (b))")
	if err != nil {
		t.Fatal(err)
	}
	create := stmt.(*parser.CreateTable)
	if err := create.Table.NormalizeTableName(""); err != nil {
		t.Fatal(err)
	}
	desc, err := MakeTableDesc(create, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = desc.AllocateIDs()
	if !testutils.IsError(err, sqlbase.ErrMissingPrimaryKey.Error()) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestRemoveLeaseIfExpiring(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := planner{session: &Session{context: context.Background()}}
	mc := hlc.NewManualClock(0)
	p.leaseMgr = &LeaseManager{LeaseStore: LeaseStore{clock: hlc.NewClock(mc.UnixNano)}}
	p.leases = make([]*LeaseState, 0)
	txn := client.Txn{Context: context.Background()}
	p.setTxn(&txn)

	if p.removeLeaseIfExpiring(nil) {
		t.Error("expected false with nil input")
	}

	// Add a lease to the planner.
	d := int64(LeaseDuration)
	l1 := &LeaseState{expiration: parser.DTimestamp{Time: time.Unix(0, mc.UnixNano()+d+1)}}
	p.leases = append(p.leases, l1)
	et := hlc.Timestamp{WallTime: l1.Expiration().UnixNano()}
	txn.UpdateDeadlineMaybe(et)

	if p.removeLeaseIfExpiring(l1) {
		t.Error("expected false with a non-expiring lease")
	}
	if !p.txn.GetDeadline().Equal(et) {
		t.Errorf("expected deadline %s but got %s", et, p.txn.GetDeadline())
	}

	// Advance the clock so that l1 will be expired.
	mc.Increment(d + 1)

	// Add another lease.
	l2 := &LeaseState{expiration: parser.DTimestamp{Time: time.Unix(0, mc.UnixNano()+d+1)}}
	p.leases = append(p.leases, l2)
	if !p.removeLeaseIfExpiring(l1) {
		t.Error("expected true with an expiring lease")
	}
	et = hlc.Timestamp{WallTime: l2.Expiration().UnixNano()}
	txn.UpdateDeadlineMaybe(et)

	if !(len(p.leases) == 1 && p.leases[0] == l2) {
		t.Errorf("expected leases to contain %s but has %s", l2, p.leases)
	}

	if !p.txn.GetDeadline().Equal(et) {
		t.Errorf("expected deadline %s, but got %s", et, p.txn.GetDeadline())
	}
}
