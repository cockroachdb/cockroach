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
	"flag"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

func TestDumpRow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest()
	defer c.stop()

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
		s1 string(1)
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

INSERT INTO t VALUES
	(1, 2.3, 'striiing', b'a1b2c3', '2016-03-26', '2016-01-25 10:10:10+00:00', '2h30m30s', true, 1.2345, '2016-01-25 10:10:10+00:00', 3.4, 4.5, 's'),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, +Inf, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, -Inf, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, NaN, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
`

	if string(out) != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}
}

func TestDumpBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	url, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestDumpBytes")
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
	if err := dumpTable(&b, conn, "d", "t"); err != nil {
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
	if err := dumpTable(&b, conn, "o", "t"); err != nil {
		t.Fatal(err)
	}
	dump2 := b.String()
	if dump != dump2 {
		t.Fatalf("unmatching dumps:\n%s\n%s", dump, dump2)
	}
}

var randomTestTime = flag.Duration("duration-random", time.Second, "duration for randomized dump test to run")

// TestDumpRandom generates a random number of random rows with all data
// types. This data is dumped, inserted, and dumped again. The two dumps
// are compared for exactness. The data from the inserted dump is then
// SELECT'd and compared to the original generated data to ensure it is
// round-trippable.
func TestDumpRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	url, cleanup := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser, "TestDumpRandom")
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
			n := time.Duration(rnd.Int63()).String()
			o := rnd.Intn(2) == 1
			e := strings.TrimRight(inf.NewDec(rnd.Int63(), inf.Scale(rnd.Int31n(20)-10)).String(), ".0")
			s := make([]byte, rnd.Intn(500))
			if _, err := rnd.Read(s); err != nil {
				t.Fatal(err)
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
						t.Errorf("NOT EQUAL: table %s, row %d, col %d\ngenerated (%T): %v\nselected (%T): %v\n", table, gi, i, generatedVal, generatedVal, fetchedVal, fetchedVal)
					}
				}
				if t.Failed() {
					t.FailNow()
				}
			}
		}

		check("d.t")

		var buf bytes.Buffer
		if err := dumpTable(&buf, conn, "d", "t"); err != nil {
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

		if err := dumpTable(&buf, conn, "o", "t"); err != nil {
			t.Fatal(err)
		}
		dump2 := buf.String()
		if dump != dump2 {
			t.Fatalf("unmatching dumps:\nFIRST:\n%s\n\nSECOND:\n%s", dump, dump2)
		}
	}
}
