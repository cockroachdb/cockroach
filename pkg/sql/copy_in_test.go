// Copyright 2016 The Cockroach Authors.
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
// Author: Matt Jibson

package sql_test

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/lib/pq"
)

func TestCopyNullInfNaN(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (
			i INT NULL,
			f FLOAT NULL,
			s STRING NULL,
			b BYTES NULL,
			d DATE NULL,
			t TIMESTAMP NULL,
			n INTERVAL NULL,
			o BOOL NULL,
			e DECIMAL NULL,
			tz TIMESTAMP WITH TIME ZONE NULL
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("t"))
	if err != nil {
		t.Fatal(err)
	}

	input := [][]interface{}{
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.Inf(1), nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.Inf(-1), nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.NaN(), nil, nil, nil, nil, nil, nil, nil, nil},
	}

	for _, in := range input {
		_, err = stmt.Exec(in...)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for row, in := range input {
		if !rows.Next() {
			t.Fatal("expected more results")
		}
		data := make([]interface{}, len(in))
		for i := range data {
			data[i] = new(interface{})
		}
		if err := rows.Scan(data...); err != nil {
			t.Fatal(err)
		}
		for i, d := range data {
			v := d.(*interface{})
			d = *v
			if a, b := fmt.Sprintf("%#v", d), fmt.Sprintf("%#v", in[i]); a != b {
				t.Fatalf("row %v, col %v: got %#v (%T), expected %#v (%T)", row, i, d, d, in[i], in[i])
			}
		}
	}
}

// TestCopyRandom inserts 100 random rows using COPY and ensures the SELECT'd
// data is the same.
func TestCopyRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		CREATE TABLE IF NOT EXISTS d.t (
			id INT PRIMARY KEY,
			n INTERVAL,
			o BOOL,
			i INT,
			f FLOAT,
			e DECIMAL,
			t TIMESTAMP,
			s STRING,
			b BYTES,
			tz TIMESTAMP WITH TIME ZONE
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("d", "t", "id", "n", "o", "i", "f", "e", "t", "s", "b", "tz"))
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(0))
	types := []sqlbase.ColumnType_Kind{
		sqlbase.ColumnType_BOOL,
		sqlbase.ColumnType_INT,
		sqlbase.ColumnType_FLOAT,
		sqlbase.ColumnType_DECIMAL,
		sqlbase.ColumnType_TIMESTAMP,
		sqlbase.ColumnType_STRING,
		sqlbase.ColumnType_BYTES,
		sqlbase.ColumnType_TIMESTAMPTZ,
	}

	var inputs [][]interface{}

	for i := 0; i < 100; i++ {
		row := make([]interface{}, len(types)+2)
		row[0] = strconv.Itoa(i)

		sign := 1 - rng.Int63n(2)*2
		d := duration.Duration{
			Months: sign * rng.Int63n(1000),
			Days:   sign * rng.Int63n(1000),
			Nanos:  sign * rng.Int63(),
		}

		row[1] = d.String()
		for j, t := range types {
			d := sqlbase.RandDatum(rng, sqlbase.ColumnType{Kind: t}, false)
			ds := parser.AsStringWithFlags(d, parser.FmtBareStrings)
			switch t {
			case sqlbase.ColumnType_DECIMAL:
				// Trailing 0s aren't represented below, so truncate here.
				ds = strings.TrimRight(ds, "0")
			}
			row[j+2] = ds
		}
		_, err = stmt.Exec(row...)
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, row)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM d.t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for row, in := range inputs {
		if !rows.Next() {
			t.Fatal("expected more results")
		}
		data := make([]interface{}, len(in))
		for i := range data {
			data[i] = new(interface{})
		}
		if err := rows.Scan(data...); err != nil {
			t.Fatal(err)
		}
		for i, d := range data {
			v := d.(*interface{})
			d := *v
			ds := fmt.Sprint(d)
			switch d := d.(type) {
			case []byte:
				ds = string(d)
			case time.Time:
				dt := parser.MakeDTimestamp(d, time.Microsecond)
				ds = parser.AsStringWithFlags(dt, parser.FmtBareStrings)
			}
			if !reflect.DeepEqual(in[i], ds) {
				t.Fatalf("row %v, col %v: got %#v (%T), expected %#v", row, i, ds, d, in[i])
			}
		}
	}
}

func TestCopyError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (
			i INT PRIMARY KEY
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("t", "i"))
	if err != nil {
		t.Fatal(err)
	}

	// Insert conflicting primary keys.
	for i := 0; i < 2; i++ {
		_, err = stmt.Exec(1)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = stmt.Close()
	if err == nil {
		t.Fatal("expected error")
	}

	// Make sure we can query after an error.
	var i int
	if err := db.QueryRow("SELECT 1").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != 1 {
		t.Fatalf("expected 1, got %d", i)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestCopyOne verifies that only one COPY can run at once.
func TestCopyOne(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("https://github.com/lib/pq/issues/558")

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (
			i INT PRIMARY KEY
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Prepare(pq.CopyIn("t", "i")); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Prepare(pq.CopyIn("t", "i")); err == nil {
		t.Fatal("expected error")
	}
}

// TestCopyInProgress verifies that after a COPY has started another statement
// cannot run.
func TestCopyInProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("https://github.com/lib/pq/issues/558")

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (
			i INT PRIMARY KEY
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Prepare(pq.CopyIn("t", "i")); err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Query("SELECT 1"); err == nil {
		t.Fatal("expected error")
	}
}

// TestCopyTransaction verifies that COPY data can be used after it is done
// within a transaction.
func TestCopyTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (
			i INT PRIMARY KEY
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("t", "i"))
	if err != nil {
		t.Fatal(err)
	}

	const val = 2

	_, err = stmt.Exec(val)
	if err != nil {
		t.Fatal(err)
	}

	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}

	var i int
	if err := txn.QueryRow("SELECT i FROM d.t").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val {
		t.Fatalf("expected 1, got %d", i)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
}
