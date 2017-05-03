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
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package cli

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/spf13/pflag"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestDumpRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
	CREATE DATABASE d;
	CREATE TABLE d.t (
		i int,
		f float,
		s string,
		b bytes,
		d date,
		t timestamp,
		n interval,
		o bool,
		e decimal,
		tz timestamptz,
		e1 decimal(2),
		e2 decimal(2, 1),
		s1 string(1),
		FAMILY "primary" (i, f, d, t, n, o, tz, e1, e2, s1, rowid),
		FAMILY fam_1_s (s),
		FAMILY fam_2_b (b),
		FAMILY fam_3_e (e)
	);
	INSERT INTO d.t VALUES (
		1,
		2.3,
		'striiing',
		b'\141\061\142\062\143\063', '2016-03-26', '2016-01-25 10:10:10' ,
		'2h30m30s',
		true,
		1.2345,
		'2016-01-25 10:10:10',
		3.4,
		4.5,
		's'
	);
	INSERT INTO d.t VALUES (DEFAULT);
	INSERT INTO d.t (f) VALUES (
		CAST('+Inf' AS FLOAT)
	), (
		CAST('-Inf' AS FLOAT)
	), (
		CAST('NaN' AS FLOAT)
	);
`

	c.RunWithArgs([]string{"sql", "-e", create})

	out, err := c.RunWithCapture("dump d t")
	if err != nil {
		t.Fatal(err)
	}

	const expect = `dump d t
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
	tz TIMESTAMP WITH TIME ZONE NULL,
	e1 DECIMAL(2) NULL,
	e2 DECIMAL(2,1) NULL,
	s1 STRING(1) NULL,
	FAMILY "primary" (i, f, d, t, n, o, tz, e1, e2, s1, rowid),
	FAMILY fam_1_s (s),
	FAMILY fam_2_b (b),
	FAMILY fam_3_e (e)
);

INSERT INTO t (i, f, s, b, d, t, n, o, e, tz, e1, e2, s1) VALUES
	(1, 2.3, 'striiing', b'a1b2c3', '2016-03-26', '2016-01-25 10:10:10+00:00', '2h30m30s', true, 1.2345, '2016-01-25 10:10:10+00:00', 3, 4.5, 's'),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, +Inf, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, -Inf, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, NaN, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
`

	if string(out) != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}

}

func TestDumpFlags(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.f (x int, y int); insert into t.f values (42, 69)"})

	out, err := c.RunWithCapture("dump t f --dump-mode=both")
	if err != nil {
		t.Fatal(err)
	}
	expected := `dump t f --dump-mode=both
CREATE TABLE f (
	x INT NULL,
	y INT NULL,
	FAMILY "primary" (x, y, rowid)
);

INSERT INTO f (x, y) VALUES
	(42, 69);
`
	if string(out) != expected {
		t.Fatalf("expected %s\ngot: %s", expected, out)
	}

	out, err = c.RunWithCapture("dump t f --dump-mode=schema")
	if err != nil {
		t.Fatal(err)
	}
	expected = `dump t f --dump-mode=schema
CREATE TABLE f (
	x INT NULL,
	y INT NULL,
	FAMILY "primary" (x, y, rowid)
);
`
	if string(out) != expected {
		t.Fatalf("expected %s\ngot: %s", expected, out)
	}

	out, err = c.RunWithCapture("dump t f --dump-mode=data")
	if err != nil {
		t.Fatal(err)
	}
	expected = `dump t f --dump-mode=data

INSERT INTO f (x, y) VALUES
	(42, 69);
`
	if string(out) != expected {
		t.Fatalf("expected %s\ngot: %s", expected, out)
	}
}

func TestDumpMultipleTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.f (x int, y int); insert into t.f values (42, 69)"})
	c.RunWithArgs([]string{"sql", "-e", "create table t.g (x int, y int); insert into t.g values (3, 4)"})

	out, err := c.RunWithCapture("dump t f g")
	if err != nil {
		t.Fatal(err)
	}

	expected := `dump t f g
CREATE TABLE f (
	x INT NULL,
	y INT NULL,
	FAMILY "primary" (x, y, rowid)
);

CREATE TABLE g (
	x INT NULL,
	y INT NULL,
	FAMILY "primary" (x, y, rowid)
);

INSERT INTO f (x, y) VALUES
	(42, 69);

INSERT INTO g (x, y) VALUES
	(3, 4);
`
	if string(out) != expected {
		t.Fatalf("expected %s\ngot: %s", expected, out)
	}

	out, err = c.RunWithCapture("dump t")
	if err != nil {
		t.Fatal(err)
	}

	expected = `dump t
CREATE TABLE f (
	x INT NULL,
	y INT NULL,
	FAMILY "primary" (x, y, rowid)
);

CREATE TABLE g (
	x INT NULL,
	y INT NULL,
	FAMILY "primary" (x, y, rowid)
);

INSERT INTO f (x, y) VALUES
	(42, 69);

INSERT INTO g (x, y) VALUES
	(3, 4);
`
	if string(out) != expected {
		t.Fatalf("expected %s\ngot: %s", expected, out)
	}
}

func dumpSingleTable(w io.Writer, conn *sqlConn, dbName string, tName string) error {
	mds, ts, err := getDumpMetadata(conn, dbName, []string{tName}, "")
	if err != nil {
		return err
	}
	if err := dumpCreateTable(w, mds[0]); err != nil {
		return err
	}
	return dumpTableData(w, conn, ts, mds[0])
}

func TestDumpBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingAddr(), "TestDumpBytes", url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	if err := conn.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (b BYTES PRIMARY KEY);
	`, nil); err != nil {
		t.Fatal(err)
	}

	for i := int64(0); i < 256; i++ {
		if err := conn.Exec("INSERT INTO t VALUES ($1)", []driver.Value{[]byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	var b bytes.Buffer
	if err := dumpSingleTable(&b, conn, "d", "t"); err != nil {
		t.Fatal(err)
	}
	dump := b.String()
	b.Reset()

	if err := conn.Exec(`
		CREATE DATABASE o;
		SET DATABASE = o;
	`, nil); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(dump, nil); err != nil {
		t.Fatal(err)
	}
	if err := dumpSingleTable(&b, conn, "o", "t"); err != nil {
		t.Fatal(err)
	}
	dump2 := b.String()
	if dump != dump2 {
		t.Fatalf("unmatching dumps:\n%s\n%s", dump, dump2)
	}
}

const durationRandom = "duration-random"

var randomTestTime = pflag.Duration(durationRandom, time.Second, "duration for randomized dump test to run")

func init() {
	pflag.Lookup(durationRandom).Hidden = true
}

// TestDumpRandom generates a random number of random rows with all data
// types. This data is dumped, inserted, and dumped again. The two dumps
// are compared for exactness. The data from the inserted dump is then
// SELECT'd and compared to the original generated data to ensure it is
// round-trippable.
func TestDumpRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingAddr(), "TestDumpRandom", url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	if err := conn.Exec(`
		CREATE DATABASE d;
		CREATE DATABASE o;
		CREATE TABLE d.t (
			rowid int,
			i int,
			f float,
			d date,
			m timestamp,
			n interval,
			o bool,
			e decimal,
			s string,
			b bytes,
			PRIMARY KEY (rowid, i, f, d, m, n, o, e, s, b)
		);
	`, nil); err != nil {
		t.Fatal(err)
	}

	rnd, seed := randutil.NewPseudoRand()
	t.Logf("random seed: %v", seed)

	start := timeutil.Now()

	for iteration := 0; timeutil.Since(start) < *randomTestTime; iteration++ {
		if err := conn.Exec(`DELETE FROM d.t`, nil); err != nil {
			t.Fatal(err)
		}
		var generatedRows [][]driver.Value
		count := rnd.Int63n(500)
		t.Logf("random iteration %v: %v rows", iteration, count)
		for _i := int64(0); _i < count; _i++ {
			// Generate a random number of random inserts.
			i := rnd.Int63()
			f := rnd.Float64()
			d := time.Unix(0, rnd.Int63()).Round(time.Hour * 24).UTC()
			m := time.Unix(0, rnd.Int63()).Round(time.Microsecond).UTC()
			sign := 1 - rnd.Int63n(2)*2
			dur := duration.Duration{
				Months: sign * rnd.Int63n(1000),
				Days:   sign * rnd.Int63n(1000),
				Nanos:  sign * rnd.Int63(),
			}
			n := dur.String()
			o := rnd.Intn(2) == 1
			e := strings.TrimRight(apd.New(rnd.Int63(), int32(rnd.Int31n(20)-10)).ToStandard(), ".0")
			sr := make([]byte, rnd.Intn(500))
			if _, err := rnd.Read(sr); err != nil {
				t.Fatal(err)
			}
			s := make([]byte, 0, len(sr))
			for _, b := range sr {
				r := rune(b)
				if !utf8.ValidRune(r) {
					continue
				}
				s = append(s, []byte(string(r))...)
			}
			b := make([]byte, rnd.Intn(500))
			if _, err := rnd.Read(b); err != nil {
				t.Fatal(err)
			}

			vals := []driver.Value{
				_i,
				i,
				f,
				d,
				m,
				[]byte(n), // intervals come out as `[]byte`s
				o,
				[]byte(e), // decimals come out as `[]byte`s
				string(s),
				b,
			}
			if err := conn.Exec("INSERT INTO d.t VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", vals); err != nil {
				t.Fatal(err)
			}
			generatedRows = append(generatedRows, vals[1:])
		}

		check := func(table string) {
			q := fmt.Sprintf("SELECT i, f, d, m, n, o, e, s, b FROM %s ORDER BY rowid", table)
			nrows, err := conn.Query(q, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := nrows.Close(); err != nil {
					t.Fatal(err)
				}
			}()
			for gi, generatedRow := range generatedRows {
				fetched := make([]driver.Value, len(nrows.Columns()))
				if err := nrows.Next(fetched); err != nil {
					t.Fatal(err)
				}

				for i, fetchedVal := range fetched {
					generatedVal := generatedRow[i]
					if t, ok := fetchedVal.(time.Time); ok {
						// dates and timestamps come out with offset zero (but
						// not UTC specifically).
						fetchedVal = t.UTC()
					}
					if !reflect.DeepEqual(fetchedVal, generatedVal) {
						t.Errorf("NOT EQUAL: table %s, row %d, col %d\ngenerated (%T): %v (%s)\nselected (%T): %v (%s)\n", table, gi, i, generatedVal, generatedVal, generatedVal, fetchedVal, fetchedVal, fetchedVal)
					}
				}
				if t.Failed() {
					t.FailNow()
				}
			}
		}

		check("d.t")

		var buf bytes.Buffer
		if err := dumpSingleTable(&buf, conn, "d", "t"); err != nil {
			t.Fatal(err)
		}
		dump := buf.String()
		buf.Reset()

		if err := conn.Exec(`
			SET DATABASE = o;
			DROP TABLE IF EXISTS t;
		`, nil); err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(dump, nil); err != nil {
			t.Fatal(err)
		}

		check("o.t")

		if err := dumpSingleTable(&buf, conn, "o", "t"); err != nil {
			t.Fatal(err)
		}
		dump2 := buf.String()
		if dump != dump2 {
			t.Fatalf("unmatching dumps:\nFIRST:\n%s\n\nSECOND:\n%s", dump, dump2)
		}
	}
}

func TestDumpAsOf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
	CREATE DATABASE d;
	CREATE TABLE d.t (i int);
	INSERT INTO d.t VALUES (1);
	SELECT NOW();
`

	out, err := c.RunWithCaptureArgs([]string{"sql", "-e", create})
	if err != nil {
		t.Fatal(err)
	}

	// Last line is the timestamp.
	fs := strings.Split(strings.TrimSpace(out), "\n")
	ts := fs[len(fs)-1]

	dump1, err := c.RunWithCaptureArgs([]string{"dump", "d", "t"})
	if err != nil {
		t.Fatal(err)
	}

	const want1 = `dump d t
CREATE TABLE t (
	i INT NULL,
	FAMILY "primary" (i, rowid)
);

INSERT INTO t (i) VALUES
	(1);
`
	if dump1 != want1 {
		t.Fatalf("expected: %s\ngot: %s", want1, dump1)
	}

	c.RunWithArgs([]string{"sql", "-e", `
		ALTER TABLE d.t ADD COLUMN j int DEFAULT 2;
		INSERT INTO d.t VALUES (3, 4);
	`})

	dump2, err := c.RunWithCaptureArgs([]string{"dump", "d", "t"})
	if err != nil {
		t.Fatal(err)
	}
	const want2 = `dump d t
CREATE TABLE t (
	i INT NULL,
	j INT NULL DEFAULT 2:::INT,
	FAMILY "primary" (i, rowid, j)
);

INSERT INTO t (i, j) VALUES
	(1, 2),
	(3, 4);
`
	if dump2 != want2 {
		t.Fatalf("expected: %s\ngot: %s", want2, dump2)
	}

	dumpAsOf, err := c.RunWithCaptureArgs([]string{"dump", "d", "t", "--as-of", ts})
	if err != nil {
		t.Fatal(err)
	}
	// Remove the timestamp from the first line.
	dumpAsOf = fmt.Sprintf("dump d t\n%s", strings.SplitN(dumpAsOf, "\n", 2)[1])
	if dumpAsOf != want1 {
		t.Fatalf("expected: %s\ngot: %s", want1, dumpAsOf)
	}

	if out, err := c.RunWithCaptureArgs([]string{"dump", "d", "t", "--as-of", "2000-01-01 00:00:00"}); err != nil {
		t.Fatal(err)
	} else if !strings.Contains(string(out), "table d.t does not exist") {
		t.Fatalf("unexpected output: %s", out)
	}
}
