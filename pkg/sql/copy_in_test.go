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
	"net/url"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestCopyNullInfNaN(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
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
			t TIME NULL,
			ttz TIME NULL,
			ts TIMESTAMP NULL,
			n INTERVAL NULL,
			o BOOL NULL,
			e DECIMAL NULL,
			u UUID NULL,
			ip INET NULL,
			tz TIMESTAMPTZ NULL
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
		"ts", "n", "o", "e", "u", "ip", "tz"))
	if err != nil {
		t.Fatal(err)
	}

	input := [][]interface{}{
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.Inf(1), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.Inf(-1), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.NaN(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
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
			t TIME,
			ttz TIMETZ,
			ts TIMESTAMP,
			s STRING,
			b BYTES,
			u UUID,
			ip INET,
			tz TIMESTAMPTZ
		);
		SET extra_float_digits = 3; -- to preserve floats entirely
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("d", "t", "id", "n", "o", "i", "f", "e", "t", "ttz", "ts", "s", "b", "u", "ip", "tz"))
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
					dt = tree.MakeDTime(timeofday.FromTime(d))
				} else if typs[i].Family() == types.TimeTZFamily {
					dt = tree.NewDTimeTZFromTime(d)
				} else {
					dt = tree.MakeDTimestamp(d, time.Microsecond)
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

	t.Skip("#18352")

	params, _ := tests.CreateTestServerParams()
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

	t.Skip("#18352")

	params, _ := tests.CreateTestServerParams()
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

	params, _ := tests.CreateTestServerParams()
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
	defer s.Stopper().Stop(context.TODO())

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
		SET optimizer_foreign_keys = true;
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

// TestCopyInReleasesLeases is a regression test to ensure that the execution
// of CopyIn does not retain table descriptor leases after completing by
// attempting to run a schema change after performing a copy.
func TestCopyInReleasesLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	tdb := sqlutils.MakeSQLRunner(db)
	defer s.Stopper().Stop(context.Background())
	tdb.Exec(t, `CREATE TABLE t (k INT8 PRIMARY KEY)`)
	tdb.Exec(t, `CREATE USER foo WITH PASSWORD 'testabc'`)
	tdb.Exec(t, `GRANT admin TO foo`)

	userURL, cleanupFn := sqlutils.PGUrlWithOptionalClientCerts(t,
		s.ServingSQLAddr(), t.Name(), url.UserPassword("foo", "testabc"),
		false /* withClientCerts */)
	defer cleanupFn()
	conn, err := pgxConn(t, userURL)
	require.NoError(t, err)

	tag, err := conn.CopyFromReader(strings.NewReader("1\n2\n"),
		"copy t(k) from stdin")
	require.NoError(t, err)
	require.Equal(t, int64(2), tag.RowsAffected())

	// Prior to the bug fix which prompted this test, the below schema change
	// would hang until the leases expire. Let's make sure it finishes "soon".
	alterErr := make(chan error, 1)
	go func() {
		_, err := db.Exec(`ALTER TABLE t ADD COLUMN v INT NOT NULL DEFAULT 0`)
		alterErr <- err
	}()
	select {
	case err := <-alterErr:
		require.NoError(t, err)
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatal("alter did not complete")
	}
	require.NoError(t, conn.Close())
}
