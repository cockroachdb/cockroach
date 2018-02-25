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
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
		t time,
		ts timestamp,
		n interval,
		o bool,
		e decimal,
		u uuid,
		ip inet,
		j JSON,
		ary string[],
		tz timestamptz,
		e1 decimal(2),
		e2 decimal(2, 1),
		s1 string(1),
		FAMILY "primary" (i, f, d, t, ts, n, o, u, ip, j, ary, tz, e1, e2, s1, rowid),
		FAMILY fam_1_s (s),
		FAMILY fam_2_b (b),
		FAMILY fam_3_e (e)
	);
	INSERT INTO d.t VALUES (
		1,
		2.3,
		'striiing',
		'\x613162326333',
		'2016-03-26',
		'01:02:03.456',
		'2016-01-25 10:10:10',
		'2h30m30s',
		true,
		1.2345,
		'e9716c74-2638-443d-90ed-ffde7bea7d1d',
		'192.168.0.1',
		'{"a":"b"}',
		ARRAY['hello','world'],
		'2016-01-25 10:10:10',
		3.4,
		4.5,
		's'
	);
	INSERT INTO d.t VALUES (DEFAULT);
	INSERT INTO d.t (f, e) VALUES (
		CAST('+Inf' AS FLOAT), CAST('+Inf' AS DECIMAL)
	), (
		CAST('-Inf' AS FLOAT), CAST('-Inf' AS DECIMAL)
	), (
		CAST('NaN' AS FLOAT), CAST('NaN' AS DECIMAL)
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
	t TIME NULL,
	ts TIMESTAMP NULL,
	n INTERVAL NULL,
	o BOOL NULL,
	e DECIMAL NULL,
	u UUID NULL,
	ip INET NULL,
	j JSON NULL,
	ary STRING[] NULL,
	tz TIMESTAMP WITH TIME ZONE NULL,
	e1 DECIMAL(2) NULL,
	e2 DECIMAL(2,1) NULL,
	s1 STRING(1) NULL,
	FAMILY "primary" (i, f, d, t, ts, n, o, u, ip, j, ary, tz, e1, e2, s1, rowid),
	FAMILY fam_1_s (s),
	FAMILY fam_2_b (b),
	FAMILY fam_3_e (e)
);

INSERT INTO t (i, f, s, b, d, t, ts, n, o, e, u, ip, j, ary, tz, e1, e2, s1) VALUES
	(1, 2.3, 'striiing', '\x613162326333', '2016-03-26', '01:02:03.456', '2016-01-25 10:10:10+00:00', '2h30m30s', true, 1.2345, 'e9716c74-2638-443d-90ed-ffde7bea7d1d', '192.168.0.1', '{"a": "b"}', ARRAY['hello':::STRING,'world':::STRING], '2016-01-25 10:10:10+00:00', 3, 4.5, 's'),
	(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, '+Inf', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Infinity', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, '-Inf', NULL, NULL, NULL, NULL, NULL, NULL, NULL, '-Infinity', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
	(NULL, 'NaN', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'NaN', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
`

	if out != expect {
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
	if out != expected {
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
	if out != expected {
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
	if out != expected {
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
	if out != expected {
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
	if out != expected {
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

	url, cleanup := sqlutils.PGUrl(t, c.ServingAddr(), t.Name(), url.User(security.RootUser))
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

	url, cleanup := sqlutils.PGUrl(t, c.ServingAddr(), t.Name(), url.User(security.RootUser))
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
			u uuid,
			ip inet,
			j json,
			PRIMARY KEY (rowid, i, f, d, m, n, o, e, s, b, u, ip)
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
			d := timeutil.Unix(0, rnd.Int63()).Round(time.Hour * 24)
			m := timeutil.Unix(0, rnd.Int63()).Round(time.Microsecond)
			sign := 1 - rnd.Int63n(2)*2
			dur := duration.Duration{
				Months: sign * rnd.Int63n(1000),
				Days:   sign * rnd.Int63n(1000),
				Nanos:  sign * rnd.Int63(),
			}
			n := dur.String()
			o := rnd.Intn(2) == 1
			e := apd.New(rnd.Int63(), rnd.Int31n(20)-10).String()
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

			uuidBytes := make([]byte, 16)
			if _, err := rnd.Read(b); err != nil {
				t.Fatal(err)
			}
			u, err := uuid.FromBytes(uuidBytes)
			if err != nil {
				t.Fatal(err)
			}

			ip := ipaddr.RandIPAddr(rnd)
			j, err := json.Random(20, rnd)
			if err != nil {
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
				[]byte(u.String()),
				[]byte(ip.String()),
				[]byte(j.String()),
			}
			if err := conn.Exec("INSERT INTO d.t VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", vals); err != nil {
				t.Fatal(err)
			}
			generatedRows = append(generatedRows, vals[1:])
		}

		check := func(table string) {
			q := fmt.Sprintf("SELECT i, f, d, m, n, o, e, s, b, u, ip, j FROM %s ORDER BY rowid", table)
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
	} else if !strings.Contains(out, `relation d.public.t does not exist`) {
		t.Fatalf("unexpected output: %s", out)
	}
}

// TestDumpIdentifiers tests dumping a table with a semicolon in the table,
// index, and column names properly escapes.
func TestDumpIdentifiers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
	CREATE DATABASE d;
	CREATE TABLE d.";" (";" int, index (";"));
	INSERT INTO d.";" VALUES (1);
`

	if out, err := c.RunWithCaptureArgs([]string{"sql", "-e", create}); err != nil {
		t.Fatal(err)
	} else {
		t.Log(out)
	}

	out, err := c.RunWithCaptureArgs([]string{"dump", "d"})
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(out)
	}

	const expect = `dump d
CREATE TABLE ";" (
	";" INT NULL,
	INDEX ";_;_idx" (";" ASC),
	FAMILY "primary" (";", rowid)
);

INSERT INTO ";" (";") VALUES
	(1);
`

	if out != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}
}

// TestDumpReferenceOrder tests dumping a database with foreign keys does
// so in correct order.
func TestDumpReferenceOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	// Create tables so they would be in incorrect order if sorted alphabetically.
	const create = `
CREATE DATABASE d1;
CREATE DATABASE d2;
USE d1;

-- B -> A
CREATE TABLE b (i int PRIMARY KEY);
CREATE TABLE a (i int REFERENCES b);
INSERT INTO b VALUES (1);
INSERT INTO a VALUES (1);

-- Test multiple tables to make sure transitive deps are sorted correctly.
-- E -> D -> C
-- G -> F -> D -> C
CREATE TABLE g (i int PRIMARY KEY);
CREATE TABLE f (i int PRIMARY KEY, g int REFERENCES g);
CREATE TABLE e (i int PRIMARY KEY);
CREATE TABLE d (i int PRIMARY KEY, e int REFERENCES e, f int REFERENCES f);
CREATE TABLE c (i int REFERENCES d);
INSERT INTO g VALUES (1);
INSERT INTO f VALUES (1, 1);
INSERT INTO e VALUES (1);
INSERT INTO d VALUES (1, 1, 1);
INSERT INTO c VALUES (1);

-- Test a table that uses a sequence to make sure the sequence is dumped first.
CREATE SEQUENCE s;
CREATE TABLE s_tbl (id INT PRIMARY KEY DEFAULT nextval('s'), v INT);
INSERT INTO s_tbl (v) VALUES (10), (11);
`
	if out, err := c.RunWithCaptureArgs([]string{"sql", "-e", create}); err != nil {
		t.Fatal(err)
	} else {
		t.Log(out)
	}

	out, err := c.RunWithCapture("dump d1")
	if err != nil {
		t.Fatal(err)
	}

	const expectDump = `dump d1
CREATE TABLE b (
	i INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	FAMILY "primary" (i)
);

CREATE TABLE a (
	i INT NULL,
	CONSTRAINT fk_i_ref_b FOREIGN KEY (i) REFERENCES b (i),
	INDEX a_auto_index_fk_i_ref_b (i ASC),
	FAMILY "primary" (i, rowid)
);

CREATE TABLE e (
	i INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	FAMILY "primary" (i)
);

CREATE TABLE g (
	i INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	FAMILY "primary" (i)
);

CREATE TABLE f (
	i INT NOT NULL,
	g INT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	CONSTRAINT fk_g_ref_g FOREIGN KEY (g) REFERENCES g (i),
	INDEX f_auto_index_fk_g_ref_g (g ASC),
	FAMILY "primary" (i, g)
);

CREATE TABLE d (
	i INT NOT NULL,
	e INT NULL,
	f INT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	CONSTRAINT fk_e_ref_e FOREIGN KEY (e) REFERENCES e (i),
	INDEX d_auto_index_fk_e_ref_e (e ASC),
	CONSTRAINT fk_f_ref_f FOREIGN KEY (f) REFERENCES f (i),
	INDEX d_auto_index_fk_f_ref_f (f ASC),
	FAMILY "primary" (i, e, f)
);

CREATE TABLE c (
	i INT NULL,
	CONSTRAINT fk_i_ref_d FOREIGN KEY (i) REFERENCES d (i),
	INDEX c_auto_index_fk_i_ref_d (i ASC),
	FAMILY "primary" (i, rowid)
);

CREATE SEQUENCE s MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1;

CREATE TABLE s_tbl (
	id INT NOT NULL DEFAULT nextval('s':::STRING),
	v INT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	FAMILY "primary" (id, v)
);

INSERT INTO b (i) VALUES
	(1);

INSERT INTO a (i) VALUES
	(1);

INSERT INTO e (i) VALUES
	(1);

INSERT INTO g (i) VALUES
	(1);

INSERT INTO f (i, g) VALUES
	(1, 1);

INSERT INTO d (i, e, f) VALUES
	(1, 1, 1);

INSERT INTO c (i) VALUES
	(1);

SELECT setval('s', 3, false);

INSERT INTO s_tbl (id, v) VALUES
	(1, 10),
	(2, 11);
`

	if out != expectDump {
		t.Fatalf("expected: %s\ngot: %s", expectDump, out)
	}

	// Remove first line of output ("dump a").
	dump := strings.SplitN(out, "\n", 2)[1]
	out, err = c.RunWithCaptureArgs([]string{"sql", "-d", "d2", "-e", dump})
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(out)
	}

	// Verify import of dump was successful.
	const SELECT = `
SELECT * FROM a;
SELECT * FROM c;
`
	out, err = c.RunWithCaptureArgs([]string{"sql", "-d", "d2", "-e", SELECT})
	if err != nil {
		t.Fatal(err)
	}

	const expect = `sql -d d2 -e 
SELECT * FROM a;
SELECT * FROM c;

i
1
i
1
`

	if out != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}

	// Ensure dump specifying only some tables works if those tables reference
	// tables not in the dump.

	out, err = c.RunWithCapture("dump d1 d e")
	if err != nil {
		t.Fatal(err)
	}

	const expectDump2 = `dump d1 d e
CREATE TABLE e (
	i INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	FAMILY "primary" (i)
);

CREATE TABLE d (
	i INT NOT NULL,
	e INT NULL,
	f INT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	CONSTRAINT fk_e_ref_e FOREIGN KEY (e) REFERENCES e (i),
	INDEX d_auto_index_fk_e_ref_e (e ASC),
	CONSTRAINT fk_f_ref_f FOREIGN KEY (f) REFERENCES f (i),
	INDEX d_auto_index_fk_f_ref_f (f ASC),
	FAMILY "primary" (i, e, f)
);

INSERT INTO e (i) VALUES
	(1);

INSERT INTO d (i, e, f) VALUES
	(1, 1, 1);
`

	if out != expectDump2 {
		t.Fatalf("expected: %s\ngot: %s", expectDump2, out)
	}
}

// TestDumpView verifies dump doesn't attempt to dump data of views.
func TestDumpView(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
	CREATE DATABASE d;
	CREATE VIEW d.bar AS SELECT 1;
`
	if out, err := c.RunWithCaptureArgs([]string{"sql", "-e", create}); err != nil {
		t.Fatal(err)
	} else {
		t.Log(out)
	}

	out, err := c.RunWithCaptureArgs([]string{"dump", "d"})
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(out)
	}

	const expect = `dump d
CREATE VIEW bar ("1") AS SELECT 1;
`

	if out != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}
}

func TestDumpSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	// Create database and sequence.

	const create = `
	CREATE DATABASE d;
	CREATE SEQUENCE d.s1 INCREMENT 123; -- test one sequence right at its minval
	CREATE SEQUENCE d.s2 INCREMENT 456; -- test another that's been incremented
	SELECT nextval('d.s2'); -- 1
	SELECT nextval('d.s2'); -- 457
`
	if createOut, err := c.RunWithCaptureArgs([]string{"sql", "-e", create}); err != nil {
		t.Fatal(err)
	} else {
		t.Log(createOut)
	}

	// Dump the database.

	const expectSQL = `CREATE SEQUENCE s1 MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 123 START 1;

CREATE SEQUENCE s2 MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 456 START 1;

SELECT setval('s1', 1, false);

SELECT setval('s2', 913, false);
`

	dumpOut, err := c.RunWithCaptureArgs([]string{"dump", "d"})
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(dumpOut)
	}

	outSQL := removeFirstLine(dumpOut)
	if outSQL != expectSQL {
		t.Fatalf("expected: %s\ngot: %s", expectSQL, outSQL)
	}

	// Round-trip: load this dump and then export it again.
	const recreateDatabase = `
	DROP DATABASE d CASCADE;
	CREATE DATABASE d;
`
	if recreateOut, err := c.RunWithCaptureArgs([]string{"sql", "-e", recreateDatabase}); err != nil {
		t.Fatal(err)
	} else {
		t.Log(recreateOut)
	}

	// Use conn.Exec here because it returns errors cleanly, as opposed to mixing them with
	// the command line input.
	if err := conn.Exec("USE d", nil); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(outSQL, nil); err != nil {
		t.Fatal(err)
	}

	// Now, dump again.
	dumpOut2, err := c.RunWithCaptureArgs([]string{"dump", "d"})
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(dumpOut2)
	}
	outSQL2 := removeFirstLine(dumpOut2)
	if outSQL2 != expectSQL {
		t.Fatalf("expected: %s\ngot: %s", expectSQL, outSQL2)
	}

	// Verify the next value the sequences give out.
	incOutS1, err := c.RunWithCaptureArgs([]string{"sql", "-d", "d", "-e", "SELECT nextval('s1')"})
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(incOutS1)
	}
	incOutValueS1 := removeFirstLine(incOutS1)
	const expectedOutValueS1 = `nextval
1
`
	if incOutValueS1 != expectedOutValueS1 {
		t.Fatalf("expected: %s\ngot: %s", expectedOutValueS1, incOutValueS1)
	}

	incOutS2, err := c.RunWithCaptureArgs([]string{"sql", "-d", "d", "-e", "SELECT nextval('s2')"})
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(incOutS2)
	}
	incOutValueS2 := removeFirstLine(incOutS2)
	const expectedOutValueS2 = `nextval
913
`
	if incOutValueS2 != expectedOutValueS2 {
		t.Fatalf("expected: %s\ngot: %s", expectedOutValueS2, incOutValueS2)
	}
}

func removeFirstLine(s string) string {
	lines := strings.Split(s, "\n")
	linesWithoutFirst := lines[1:]
	return strings.Join(linesWithoutFirst, "\n")
}

func TestDumpSequenceEscaping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
	CREATE DATABASE "'";
	CREATE SEQUENCE "'"."'";
`
	if out, err := c.RunWithCaptureArgs([]string{"sql", "-e", create}); err != nil {
		t.Fatal(err)
	} else {
		t.Log(out)
	}

	out, err := c.RunWithCaptureArgs([]string{"dump", "'"})
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(out)
	}

	const expect = `dump '
CREATE SEQUENCE "'" MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1;

SELECT setval(e'"\'"', 1, false);
`

	if out != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}
}

// TestDumpPrimaryKeyConstraint tests that a primary key with a non-default
// name works.
func TestDumpPrimaryKeyConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
	CREATE DATABASE d;
	CREATE TABLE d.t (
		i int,
		CONSTRAINT pk_name PRIMARY KEY (i)
	);
	INSERT INTO d.t VALUES (1);
`

	c.RunWithArgs([]string{"sql", "-e", create})

	out, err := c.RunWithCapture("dump d t")
	if err != nil {
		t.Fatal(err)
	}

	const expect = `dump d t
CREATE TABLE t (
	i INT NOT NULL,
	CONSTRAINT pk_name PRIMARY KEY (i ASC),
	FAMILY "primary" (i)
);

INSERT INTO t (i) VALUES
	(1);
`

	if out != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}
}

// TestDumpReferenceCycle tests dumping in the presence of cycles.
// This used to crash before with stack overflow due to an infinite loop before:
// https://github.com/cockroachdb/cockroach/pull/20255
func TestDumpReferenceCycle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
	CREATE DATABASE d;
	CREATE TABLE d.t (
		PRIMARY KEY (id),
		FOREIGN KEY (next_id) REFERENCES d.t(id),
		id INT,
		next_id INT
	);
	INSERT INTO d.t VALUES (
		1,
		NULL
	);
`

	c.RunWithArgs([]string{"sql", "-e", create})

	out, err := c.RunWithCapture("dump d t")
	if err != nil {
		t.Fatal(err)
	}

	const expect = `dump d t
CREATE TABLE t (
	id INT NOT NULL,
	next_id INT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	CONSTRAINT fk_next_id_ref_t FOREIGN KEY (next_id) REFERENCES t (id),
	INDEX t_auto_index_fk_next_id_ref_t (next_id ASC),
	FAMILY "primary" (id, next_id)
);

INSERT INTO t (id, next_id) VALUES
	(1, NULL);
`

	if out != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}
}

func TestDumpWithInvertedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
	CREATE DATABASE d;
	CREATE TABLE d.t (
		a JSON,
		b JSON,
		INVERTED INDEX idx (a)
	);

	CREATE INVERTED INDEX idx2 ON d.t (b);

	INSERT INTO d.t VALUES ('{"a": "b"}', '{"c": "d"}');
`

	c.RunWithArgs([]string{"sql", "-e", create})

	out, err := c.RunWithCapture("dump d t")
	if err != nil {
		t.Fatal(err)
	}

	const expect = `dump d t
CREATE TABLE t (
	a JSON NULL,
	b JSON NULL,
	INVERTED INDEX idx (a),
	INVERTED INDEX idx2 (b),
	FAMILY "primary" (a, b, rowid)
);

INSERT INTO t (a, b) VALUES
	('{"a": "b"}', '{"c": "d"}');
`

	if out != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}
}

func TestDumpWithComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
	CREATE DATABASE d;
	CREATE TABLE d.t (
		a INT PRIMARY KEY,
		b INT AS (a + 1) STORED
	);

	INSERT INTO d.t VALUES (1);
`

	c.RunWithArgs([]string{"sql", "-e", create})

	out, err := c.RunWithCapture("dump d t")
	if err != nil {
		t.Fatal(err)
	}

	const expect = `dump d t
CREATE TABLE t (
	a INT NOT NULL,
	b INT NULL AS (a + 1) STORED,
	CONSTRAINT "primary" PRIMARY KEY (a ASC),
	FAMILY "primary" (a, b)
);

INSERT INTO t (a) VALUES
	(1);
`

	if out != expect {
		t.Fatalf("expected: %s\ngot: %s", expect, out)
	}
}
