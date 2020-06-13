// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/lib/pq"
)

func TestCopyNullInfNaN(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (
			i INT NULL,
			f FLOAT NULL,
			s STRING NULL,
			b BYTES NULL,
			d DATE NULL,
			t TIME NULL,
			ttz TIME NULL,
			ts TIMESTAMP NULL,
			n INTERVAL NULL,
			o BOOL NULL,
			e DECIMAL NULL,
			u UUID NULL,
			ip INET NULL,
			tz TIMESTAMPTZ NULL,
			geography GEOGRAPHY NULL,
			geometry GEOMETRY NULL
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn(
		"t", "i", "f", "s", "b", "d", "t", "ttz",
		"ts", "n", "o", "e", "u", "ip", "tz", "geography", "geometry"))
	if err != nil {
		t.Fatal(err)
	}

	input := [][]interface{}{
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.Inf(1), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.Inf(-1), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.NaN(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
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

// TestCopyRandom inserts random rows using COPY and ensures the SELECT'd
// data is the same.
func TestCopyRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		CREATE TABLE IF NOT EXISTS d.t (
			id INT PRIMARY KEY,
			n INTERVAL,
			o BOOL,
			i INT,
			f FLOAT,
			e DECIMAL,
			t TIME,
			ttz TIMETZ,
			ts TIMESTAMP,
			s STRING,
			b BYTES,
			u UUID,
			ip INET,
			tz TIMESTAMPTZ,
			geography GEOGRAPHY NULL,
			geometry GEOMETRY NULL
		);
		SET extra_float_digits = 3; -- to preserve floats entirely
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("d", "t", "id", "n", "o", "i", "f", "e", "t", "ttz", "ts", "s", "b", "u", "ip", "tz", "geography", "geometry"))
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(0))
	typs := []*types.T{
		types.Int,
		types.Interval,
		types.Bool,
		types.Int,
		types.Float,
		types.Decimal,
		types.Time,
		types.TimeTZ,
		types.Timestamp,
		types.String,
		types.Bytes,
		types.Uuid,
		types.INet,
		types.TimestampTZ,
		types.Geography,
		types.Geometry,
	}

	var inputs [][]interface{}

	for i := 0; i < 1000; i++ {
		row := make([]interface{}, len(typs))
		for j, t := range typs {
			var ds string
			if j == 0 {
				// Special handling for ID field
				ds = strconv.Itoa(i)
			} else {
				d := sqlbase.RandDatum(rng, t, false)
				ds = tree.AsStringWithFlags(d, tree.FmtBareStrings)
				switch t {
				case types.Float:
					ds = strings.TrimSuffix(ds, ".0")
				}
			}
			row[j] = ds
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
				var dt tree.NodeFormatter
				if typs[i].Family() == types.TimeFamily {
					dt = tree.MakeDTime(timeofday.FromTimeAllow2400(d))
				} else if typs[i].Family() == types.TimeTZFamily {
					dt = tree.NewDTimeTZ(timetz.MakeTimeTZFromTimeAllow2400(d))
				} else {
					dt = tree.MustMakeDTimestamp(d, time.Microsecond)
				}
				ds = tree.AsStringWithFlags(dt, tree.FmtBareStrings)
			}
			if !reflect.DeepEqual(in[i], ds) {
				t.Fatalf("row %v, col %v: got %#v (%T), expected %#v", row, i, ds, d, in[i])
			}
		}
	}
}

func TestCopyError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

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

	t.Skip("#18352")

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

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

	t.Skip("#18352")

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

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

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

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

	// Note that, at least with lib/pq, this doesn't actually send a Parse msg
	// (which we wouldn't support, as we don't support Copy-in in extended
	// protocol mode). lib/pq has magic for recognizing a Copy.
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

// TestCopyFKCheck verifies that foreign keys are checked during COPY.
func TestCopyFKCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE p (p INT PRIMARY KEY);
		CREATE TABLE t (
		  a INT PRIMARY KEY,
		  p INT REFERENCES p(p)
		);
	`)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txn.Rollback() }()

	stmt, err := txn.Prepare(pq.CopyIn("t", "a", "p"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	err = stmt.Close()
	if !testutils.IsError(err, "foreign key violation|violates foreign key constraint") {
		t.Fatalf("expected FK error, got: %v", err)
	}
}
