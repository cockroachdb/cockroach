// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/spf13/pflag"
)

// TestDumpData uses the testdata/dump directory to execute SQL statements
// and compare dump output with expected output. File format is from the
// datadriven package.
//
// The commands supported in the data files are:
//
// sql: execute the commands in the input section; no arguments supported.
//
// dump: runs the CLI dump command with the given arguments, using its
// output as the expected result. Then loads the data back into an empty
// server and dumps it again to ensure the dump is roundtrippable. If the
// input section is equal to `noroundtrip` the roundtrip step is skipped
// (i.e., only the first dump is done). After a roundtripped dump, the tmp
// database may be examined to verify correctness.
func TestDumpData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, filepath.Join("testdata", "dump"), func(t *testing.T, path string) {
		c := newCLITest(cliTestParams{t: t})
		c.omitArgs = true
		defer c.cleanup()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			args := []string{d.Cmd}
			switch d.Cmd {
			case "sql":
				args = append(args, "-e", d.Input)
			case "dump":
				for _, a := range d.CmdArgs {
					args = append(args, a.String())
				}
			default:
				d.Fatalf(t, "unknown command: %s", d.Cmd)
			}
			s, err := c.RunWithCaptureArgs(args)
			if err != nil {
				d.Fatalf(t, "%v", err)
			}
			if d.Cmd == "dump" && d.Input != "noroundtrip" {
				if s != d.Expected {
					return s
				}

				c.RunWithArgs([]string{"sql", "-e", "drop database if exists tmp; create database tmp"})
				if out, err := c.RunWithCaptureArgs([]string{"sql", "-d", "tmp", "-e", s}); err != nil {
					d.Fatalf(t, "%v", err)
				} else {
					t.Logf("executed SQL: %s\nresult: %s", s, out)
				}
				args[1] = "tmp"
				roundtrip, err := c.RunWithCaptureArgs(args)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}
				if roundtrip != s {
					d.Fatalf(t, "roundtrip results unexpected: %s, expected: %s", roundtrip, s)
				}
			}
			return s
		})
	})
}

func dumpSingleTable(w io.Writer, conn *sqlConn, dbName string, tName string) error {
	clusterTS, err := getAsOf(conn, "" /* asOf */)
	if err != nil {
		return err
	}
	mds, err := getDumpMetadata(conn, dbName, []string{tName}, clusterTS)
	if err != nil {
		return err
	}
	if err := dumpCreateTable(w, mds[0]); err != nil {
		return err
	}
	return dumpTableData(w, conn, nil /* typContext */, mds[0])
}

func TestDumpBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
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

	url, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(url.String())
	defer conn.Close()

	if err := conn.Exec(`
		CREATE DATABASE d;
		CREATE DATABASE o;
		CREATE TABLE d.t (
			rowid int,
			i int,
			si smallint,
			bi bigint,
			f float,
			fr real,
			d date,
			m timestamp,
			mtz timestamptz,
			n interval,
			o bool,
			e decimal,
			s string,
			b bytes,
			u uuid,
			ip inet,
			j json,
			PRIMARY KEY (rowid, i, si, bi, f, fr, d, m, mtz, n, o, e, s, b, u, ip)
		);
		SET extra_float_digits = 3;
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
			d, _ := pgdate.MakeCompatibleDateFromDisk(rnd.Int63n(10000)).ToTime()
			m := timeutil.Unix(0, rnd.Int63()).Round(time.Microsecond)
			sign := 1 - rnd.Int63n(2)*2
			dur := duration.MakeDuration(sign*rnd.Int63(), sign*rnd.Int63n(1000), sign*rnd.Int63n(1000))
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
				i & 0x7fff, // si
				i,          // bi
				f,
				f, // fr
				d,
				m,
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
			if err := conn.Exec("INSERT INTO d.t VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)", vals); err != nil {
				t.Fatal(err)
			}
			generatedRows = append(generatedRows, vals[1:])
		}

		check := func(table string) {
			q := fmt.Sprintf("SELECT i, si, bi, f, fr, d, m, mtz, n, o, e, s, b, u, ip, j FROM %s ORDER BY rowid", table)
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
	CREATE TABLE d.t (i int8);
	INSERT INTO d.t VALUES (1);
	SELECT now();
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
	i INT8 NULL,
	FAMILY "primary" (i, rowid)
);

INSERT INTO t (i) VALUES
	(1);
`
	if dump1 != want1 {
		t.Fatalf("expected: %s\ngot: %s", want1, dump1)
	}

	c.RunWithArgs([]string{"sql", "-e", `
		ALTER TABLE d.t ADD COLUMN j int8 DEFAULT 2;
		INSERT INTO d.t VALUES (3, 4);
	`})

	dump2, err := c.RunWithCaptureArgs([]string{"dump", "d", "t"})
	if err != nil {
		t.Fatal(err)
	}
	const want2 = `dump d t
CREATE TABLE t (
	i INT8 NULL,
	j INT8 NULL DEFAULT 2:::INT8,
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

func TestDumpInterleavedTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	const create = `
CREATE DATABASE d;
CREATE TABLE d.customers (id INT PRIMARY KEY, name STRING(50));
CREATE TABLE d.orders (
	customer INT,
	id INT,
	total DECIMAL(20, 5),
	PRIMARY KEY (customer, id),
	CONSTRAINT fk_customer FOREIGN KEY (customer) REFERENCES d.customers
) INTERLEAVE IN PARENT d.customers (customer);
CREATE INDEX i ON d.orders (customer, total) INTERLEAVE IN PARENT d.customers (customer);
`

	_, err := c.RunWithCaptureArgs([]string{"sql", "-e", create})
	if err != nil {
		t.Fatal(err)
	}

	dump1, err := c.RunWithCaptureArgs([]string{"dump", "d", "orders"})
	if err != nil {
		t.Fatal(err)
	}

	const want1 = `dump d orders
CREATE TABLE orders (
	customer INT8 NOT NULL,
	id INT8 NOT NULL,
	total DECIMAL(20,5) NULL,
	CONSTRAINT "primary" PRIMARY KEY (customer ASC, id ASC),
	FAMILY "primary" (customer, id, total)
) INTERLEAVE IN PARENT customers (customer);

ALTER TABLE orders ADD CONSTRAINT fk_customer FOREIGN KEY (customer) REFERENCES customers(id);
CREATE INDEX i ON orders (customer ASC, total ASC) INTERLEAVE IN PARENT customers (customer);

-- Validate foreign key constraints. These can fail if there was unvalidated data during the dump.
ALTER TABLE orders VALIDATE CONSTRAINT fk_customer;
`

	if dump1 != want1 {
		t.Fatalf("expected: %s\ngot: %s", want1, dump1)
	}
}

func TestDatabaseDumpCommand(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		create   string
		expected string
	}{
		{
			name: "columnsless_table",
			create: `
CREATE DATABASE bar;
USE bar;
CREATE TABLE foo ();
`,
			expected: `CREATE TABLE foo (FAMILY "primary" (rowid)
);
`,
		},
		{
			name: "table_with_columns",
			create: `
CREATE DATABASE bar;
USE bar;
CREATE TABLE foo (id int primary key, text string not null);
`,
			expected: `CREATE TABLE foo (
	id INT8 NOT NULL,
	text STRING NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	FAMILY "primary" (id, text)
);
`,
		},
		{
			name: "autogenerate_hidden_colum",
			create: `
CREATE DATABASE bar;
USE bar;
CREATE TABLE foo(id int);
`,
			expected: `CREATE TABLE foo (
	id INT8 NULL,
	FAMILY "primary" (id, rowid)
);
`,
		},
		{
			name: "columns_less_table_with_data",
			create: `
CREATE DATABASE bar;
USE bar;
CREATE TABLE foo(id int);

INSERT INTO foo(id) VALUES(1);
INSERT INTO foo(id) VALUES(2);
INSERT INTO foo(id) VALUES(3);

ALTER TABLE foo DROP COLUMN id; 
`,
			expected: `CREATE TABLE foo (FAMILY "primary" (rowid)
);
`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newCLITest(cliTestParams{t: t})
			c.omitArgs = true
			defer c.cleanup()

			_, err := c.RunWithCaptureArgs([]string{"sql", "-e", test.create})
			if err != nil {
				t.Fatal(err)
			}

			dump, err := c.RunWithCaptureArgs([]string{"dump", "bar", "--dump-mode=schema"})
			if err != nil {
				t.Fatal(err)
			}

			if dump != test.expected {
				t.Fatalf("expected: %s\ngot: %s", test.expected, dump)
			}

			dumpWithData, err := c.RunWithCaptureArgs([]string{"dump", "bar", "--dump-mode=data"})
			if err != nil {
				t.Fatal(err)
			}

			// check we can actually reuse dump output
			_, err = c.RunWithCaptureArgs([]string{"sql", "-e", fmt.Sprintf(`CREATE DATABASE TEST;
USE TEST;
%s
%s`, dump, dumpWithData)})
			if err != nil {
				t.Fatal(err)
			}

			result1, err := c.RunWithCaptureArgs([]string{"sql", "-e", "select * from bar.foo"})
			if err != nil {
				t.Fatal(err)
			}

			result2, err := c.RunWithCaptureArgs([]string{"sql", "-e", "select * from test.foo"})
			if err != nil {
				t.Fatal(err)
			}

			if result1 != result2 {
				t.Fatalf("expected: %s\ngot: %s", test.expected, dump)
			}
		})
	}
}

func TestDumpAllTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		args     []string
		recreate bool
		create   string
		expected string
		clean    string
	}{
		{
			name: " dump_all",
			create: `
CREATE DATABASE db1;
USE db1;
CREATE TABLE t1(id INT NOT NULL, pkey STRING PRIMARY KEY);

INSERT INTO t1(id, pkey) VALUES(1, 'db1-aaaa');
INSERT INTO t1(id, pkey) VALUES(2, 'db1-bbbb');

CREATE DATABASE db2;
USE db2;
CREATE TABLE t2(id INT NOT NULL, pkey STRING PRIMARY KEY);

INSERT INTO t2(id, pkey) VALUES(1, 'db2-aaaa');
INSERT INTO t2(id, pkey) VALUES(2, 'db2-bbbb');
`,
			expected: `
CREATE DATABASE IF NOT EXISTS db1;
USE db1;

CREATE TABLE t1 (
	id INT8 NOT NULL,
	pkey STRING NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (pkey ASC),
	FAMILY "primary" (id, pkey)
);

INSERT INTO t1 (id, pkey) VALUES
	(1, 'db1-aaaa'),
	(2, 'db1-bbbb');

CREATE DATABASE IF NOT EXISTS db2;
USE db2;

CREATE TABLE t2 (
	id INT8 NOT NULL,
	pkey STRING NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (pkey ASC),
	FAMILY "primary" (id, pkey)
);

INSERT INTO t2 (id, pkey) VALUES
	(1, 'db2-aaaa'),
	(2, 'db2-bbbb');
`,
		},
		{
			name: " dump_all_only_data",
			args: []string{"--dump-mode=data"},
			create: `
CREATE DATABASE db1;
USE db1;
CREATE TABLE t1(id INT NOT NULL, pkey STRING PRIMARY KEY);

INSERT INTO t1(id, pkey) VALUES(1, 'db1-aaaa');
INSERT INTO t1(id, pkey) VALUES(2, 'db1-bbbb');

CREATE DATABASE db2;
USE db2;
CREATE TABLE t2(id INT NOT NULL, pkey STRING PRIMARY KEY);

INSERT INTO t2(id, pkey) VALUES(1, 'db2-aaaa');
INSERT INTO t2(id, pkey) VALUES(2, 'db2-bbbb');
`,
			expected: `
INSERT INTO t1 (id, pkey) VALUES
	(1, 'db1-aaaa'),
	(2, 'db1-bbbb');

INSERT INTO t2 (id, pkey) VALUES
	(1, 'db2-aaaa'),
	(2, 'db2-bbbb');
`,
		},
		{
			name:     "dump_cross_references",
			recreate: true,
			create: `
CREATE DATABASE dbB;
USE dbB;

CREATE TABLE person(
	id int PRIMARY KEY,
	name string NOT NULL);

INSERT INTO person(id, name) VALUES(1, 'John Smith');
INSERT INTO person(id, name) VALUES(2, 'Joe Dow');

CREATE DATABASE dbA;
USE dbA;

CREATE TABLE account(
	id int PRIMARY KEY,
	person_id int REFERENCES dbB.person(id),
	accountNo int NOT NULL);

INSERT INTO account(id, person_id, accountNo) VALUES(1, 1, 1111); 
INSERT INTO account(id, person_id, accountNo) VALUES(2, 2, 2222); 
`,
			expected: `
CREATE DATABASE IF NOT EXISTS dba;
USE dba;

CREATE TABLE account (
	id INT8 NOT NULL,
	person_id INT8 NULL,
	accountno INT8 NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	INDEX account_auto_index_fk_person_id_ref_person (person_id ASC),
	FAMILY "primary" (id, person_id, accountno)
);

INSERT INTO account (id, person_id, accountno) VALUES
	(1, 1, 1111),
	(2, 2, 2222);

CREATE DATABASE IF NOT EXISTS dbb;
USE dbb;

CREATE TABLE person (
	id INT8 NOT NULL,
	name STRING NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (id ASC),
	FAMILY "primary" (id, name)
);

INSERT INTO person (id, name) VALUES
	(1, 'John Smith'),
	(2, 'Joe Dow');

ALTER TABLE account ADD CONSTRAINT fk_person_id_ref_person FOREIGN KEY (person_id) REFERENCES dbb.public.person(id);

-- Validate foreign key constraints. These can fail if there was unvalidated data during the dump.
ALTER TABLE account VALIDATE CONSTRAINT fk_person_id_ref_person;
`,
			clean: `
DROP DATABASE dba;
DROP DATABASE dbb;
`,
		},
		{
			name: "verify_defaultdb_dump",
			create: `
CREATE TABLE foo(id INT NOT NULL);

INSERT INTO foo(id) VALUES(1);
INSERT INTO foo(id) VALUES(2);
INSERT INTO foo(id) VALUES(3);

CREATE DATABASE dba;
USE dba;

CREATE TABLE bar(id INT NOT NULL);

INSERT INTO bar(id) VALUES(1);
INSERT INTO bar(id) VALUES(2);
`,
			clean: `
	USE defaultdb;
	DROP TABLE foo;
	DROP DATABASE dba;
`,
			recreate: true,
			expected: `
CREATE DATABASE IF NOT EXISTS dba;
USE dba;

CREATE TABLE bar (
	id INT8 NOT NULL,
	FAMILY "primary" (id, rowid)
);

INSERT INTO bar (id) VALUES
	(1),
	(2);

CREATE DATABASE IF NOT EXISTS defaultdb;
USE defaultdb;

CREATE TABLE foo (
	id INT8 NOT NULL,
	FAMILY "primary" (id, rowid)
);

INSERT INTO foo (id) VALUES
	(1),
	(2),
	(3);
`,
		},
	}
	for _, test := range tests {
		tt := test
		t.Run(tt.name, func(t *testing.T) {

			c := newCLITest(cliTestParams{t: t})
			c.omitArgs = true
			defer c.cleanup()

			_, err := c.RunWithCaptureArgs([]string{"sql", "-e", tt.create})
			if err != nil {
				t.Fatal(err)
			}

			args := []string{"dump", "--dump-all"}
			args = append(args, tt.args...)
			dump, err := c.RunWithCaptureArgs(args)
			if err != nil {
				t.Fatal(err)
			}

			if dump != tt.expected {
				t.Fatalf("expected: %s\ngot: %s", tt.expected, dump)
			}

			// attempt to recreate from dump if test case defines
			//clean up procedure
			if tt.recreate {
				_, err := c.RunWithCaptureArgs([]string{"sql", "-e", tt.clean})
				if err != nil {
					t.Fatal(err)
				}

				_, err = c.RunWithCaptureArgs([]string{"sql", "-e", dump})
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
