// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"bufio"
	gosql "database/sql"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/go-sql-driver/mysql"
)

// This can be toggled to re-write the `testdata`. Requires local mysql
// configured with no password access for `root`, a db called `test` and
// `OUTFILE` enabled. On OSX, this can be setup using `brew install mysql` and
// the following ~/.my.cnf:
// [mysqld_safe]
// [mysqld]
// secure_file_priv=""
var rewriteMysqlTestData = envutil.EnvOrDefaultBool("COCKROACH_REWRITE_MYSQL_TESTDATA", false)

const mysqlTestDB = "cockroachtestdata"

const injectNull = "inject-null"

func randStr(r *rand.Rand, from []rune, length int) string {
	s := make([]rune, length)
	for i := 0; i < length; i++ {
		s[i] = from[r.Intn(len(from))]
	}
	return string(s)
}

type simpleTestRow struct {
	i int
	s string
	b []byte
}

var simpleTestRows = func() []simpleTestRow {
	badChars := []rune{'a', ';', '\n', ',', '"', '\\', '\r', '<', '\t', '✅', 'π', rune(0), rune(10), rune(2425), rune(5183), utf8.RuneError}
	r := rand.New(rand.NewSource(1))
	testRows := []simpleTestRow{
		{i: 1, s: `str`},
		{i: 2, s: ``},
		{i: 3, s: ` `},
		{i: 4, s: `,`},
		{i: 5, s: "\n"},
		{i: 6, s: `\n`},
		{i: 7, s: "\r\n"},
		{i: 8, s: "\r"},
		{i: 9, s: `"`},

		{i: 10, s: injectNull},
		{i: 11, s: `\N`},
		{i: 12, s: `NULL`},

		// Unicode
		{i: 13, s: `¢`},
		{i: 14, s: ` ¢ `},
		{i: 15, s: `✅`},
		{i: 16, s: `","\n,™¢`},
		{i: 17, s: string([]rune{rune(0)})},
		{i: 18, s: `✅¢©ƒƒƒƒåß∂√œ∫∑∆πœ∑˚¬≤µµç∫ø∆œ∑∆¬œ∫œ∑´´†¥¨ˆˆπ‘“æ…¬…¬˚ß∆å˚˙ƒ∆©˙©∂˙≥≤Ω˜˜µ√∫∫Ω¥∑`},
		{i: 19, s: `a quote " or two quotes "" and a quote-comma ", , and then a quote and newline "` + "\n"},
		{i: 20, s: `"a slash \, a double slash \\, a slash+quote \",  \` + "\n"},
	}

	for i := 0; i < 10; i++ {
		buf := make([]byte, 200)
		r.Seed(int64(i))
		r.Read(buf)
		testRows = append(testRows, simpleTestRow{i: len(testRows) + 1, s: randStr(r, badChars, 1000), b: buf})
	}
	return testRows
}()

type everythingTestRow struct {
	i   int
	e   string
	c   string
	bin []byte
	dt  time.Time
	iz  int
	iw  int
	fl  float64
	d53 string
	j   string
}

var everythingTestRows = func() []everythingTestRow {
	return []everythingTestRow{
		{
			1, "Small", string([]byte{99, 32, 32, 32, 32, 32, 32, 32, 32, 32}), []byte("bin"),
			timeutil.Unix(946684800, 0), 1, -2, -1.5, "-12.345",
			`{"a": "b", "c": {"d": ["e", 11, null]}}`,
		},
		{
			2, "Large", string([]byte{99, 50, 32, 32, 32, 32, 32, 32, 32, 32}), []byte("bin2"),
			timeutil.Unix(946684800, 0), 3525343334, 3,
			1.2, "12.345", `{}`,
		},
	}
}()

type testFiles struct {
	simple, second, everything, wholeDB string
}

func getMysqldumpTestdata(t *testing.T) testFiles {
	var files testFiles

	files.simple = testutils.TestDataPath(t, "mysqldump", "simple.sql")
	files.second = testutils.TestDataPath(t, "mysqldump", "second.sql")
	files.everything = testutils.TestDataPath(t, "mysqldump", "everything.sql")
	files.wholeDB = testutils.TestDataPath(t, "mysqldump", "db.sql")

	if rewriteMysqlTestData {
		genMysqlTestdata(t, func() {
			mysqldump(t, files.simple, "simple")
			mysqldump(t, files.second, "second")
			mysqldump(t, files.everything, "everything")
			mysqldump(t, files.wholeDB, "")
		})

		_ = os.Remove(files.wholeDB + ".bz2")
		out, err := exec.Command("bzip2", "-k", files.wholeDB).CombinedOutput()
		if err != nil {
			t.Fatal(err, string(out))
		}
		gzipFile(t, files.wholeDB)
	}
	return files
}

type outfileDumpCfg struct {
	name     string
	filename string
	opts     roachpb.MySQLOutfileOptions
	null     string
}

func getMysqlOutfileTestdata(t *testing.T) ([]simpleTestRow, []outfileDumpCfg) {
	configs := []outfileDumpCfg{
		{
			name: "escape-and-enclose",
			opts: roachpb.MySQLOutfileOptions{
				FieldSeparator: '\t',
				RowSeparator:   '\n',
				HasEscape:      true,
				Escape:         '\\',
				Enclose:        roachpb.MySQLOutfileOptions_Always,
				Encloser:       '"',
			},
			null: `\N`,
		},
		{
			name: "csv-ish",
			opts: roachpb.MySQLOutfileOptions{
				FieldSeparator: ',',
				RowSeparator:   '\n',
				HasEscape:      true,
				Escape:         '\\',
				Enclose:        roachpb.MySQLOutfileOptions_Always,
				Encloser:       '"',
			},
			null: `\N`,
		},
		{
			name: "escape-quote-csv-no-enclose",
			opts: roachpb.MySQLOutfileOptions{
				FieldSeparator: ',',
				RowSeparator:   '\n',
				HasEscape:      true,
				Escape:         '"',
			},
			null: `"N`,
		},
	}

	for i := range configs {
		configs[i].filename = testutils.TestDataPath(t, `mysqlout`, configs[i].name, `simple.txt`)
	}

	if rewriteMysqlTestData {
		genMysqlTestdata(t, func() {
			if err := os.RemoveAll(testutils.TestDataPath(t, `mysqlout`)); err != nil {
				t.Fatal(err)
			}
			for _, cfg := range configs {
				dest := filepath.Dir(cfg.filename)
				if err := os.MkdirAll(dest, 0777); err != nil {
					t.Fatal(err)
				}

				flags := []string{`-u`, `root`, mysqlTestDB, `simple`, `--tab`, `./` + dest}
				if cfg.opts.Enclose == roachpb.MySQLOutfileOptions_Always {
					flags = append(flags, "--fields-enclosed-by", string(cfg.opts.Encloser))
				}
				if cfg.opts.HasEscape {
					flags = append(flags, "--fields-escaped-by", string(cfg.opts.Escape))
				}
				if cfg.opts.FieldSeparator != '\t' {
					flags = append(flags, "--fields-terminated-by", string(cfg.opts.FieldSeparator))
				}
				if cfg.opts.RowSeparator != '\n' {
					flags = append(flags, "--lines-terminated-by", string(cfg.opts.RowSeparator))
				}
				if res, err := exec.Command(
					`mysqldump`, flags...,
				).CombinedOutput(); err != nil {
					t.Fatal(err, string(res))
				}

				if err := os.Remove(filepath.Join(dest, "simple.sql")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}

	return simpleTestRows, configs
}

const secondTableRows = 7

// genMysqlTestdata connects to the a local mysql, creates tables and testdata,
// calls the dump() func and then cleans up.
func genMysqlTestdata(t *testing.T, dump func()) {
	db, err := gosql.Open("mysql", "root@/"+mysqlTestDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	dropTables := `DROP TABLE IF EXISTS everything, third, second, simple CASCADE`
	if _, err := db.Exec(dropTables); err != nil {
		t.Fatal(err)
	}

	for _, schema := range []string{
		`CREATE TABLE simple (i INT PRIMARY KEY AUTO_INCREMENT, s text, b binary(200))`,
		`CREATE TABLE SECOND (
			i INT PRIMARY KEY,
			k INT,
			FOREIGN KEY (k) REFERENCES simple (i) ON UPDATE CASCADE,
			UNIQUE KEY ik (i, k),
			KEY ki (k, i)
		)`,
		`CREATE TABLE third (
			i INT PRIMARY KEY AUTO_INCREMENT,
			a INT, b INT, C INT,
			FOREIGN KEY (a, b) REFERENCES second (i, k) ON DELETE RESTRICT ON UPDATE RESTRICT,
			FOREIGN KEY (c) REFERENCES third (i) ON UPDATE CASCADE
		)`,
		`CREATE TABLE everything (
				i INT PRIMARY KEY,

				c       CHAR(10) NOT NULL,
				s       VARCHAR(100) DEFAULT 'this is s\'s default value',
				tx      TEXT,
				e       ENUM('Small', 'Medium', 'Large'),

				bin     BINARY(100) NOT NULL,
				vbin    VARBINARY(100),
				bl      BLOB,

				dt      DATETIME NOT NULL DEFAULT '2000-01-01 00:00:00',
				d       DATE,
				ts      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				t       TIME,
				-- TODO(dt): fix parser: for YEAR's length option
				-- y       YEAR,

				de      DECIMAL,
				nu      NUMERIC,
				d53     DECIMAL(5,3),

				iw      INT(5) NOT NULL,
				iz      INT ZEROFILL,
				ti      TINYINT DEFAULT 5,
				si      SMALLINT,
				mi      MEDIUMINT,
				bi      BIGINT,

				fl      FLOAT NOT NULL,
				rl      REAL,
				db      DOUBLE,

				f17     FLOAT(17),
				f47     FLOAT(47),
				f75     FLOAT(7, 5),
				j       JSON
		)`,
	} {
		if _, err := db.Exec(schema); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range simpleTestRows {
		s := &tc.s
		if *s == injectNull {
			s = nil
		}
		if _, err := db.Exec(
			`INSERT INTO simple (s, b) VALUES (?, ?)`, s, tc.b,
		); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= secondTableRows; i++ {
		if _, err := db.Exec(`INSERT INTO second VALUES (?, ?)`, -i, i); err != nil {
			t.Fatal(err)
		}
	}

	for _, r := range everythingTestRows {
		if _, err := db.Exec(
			`INSERT INTO everything (
			i, e, c, bin, dt, iz, iw, fl, d53, j
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)`, r.i, r.e, r.c, r.bin, r.dt, r.iz, r.iw, r.fl, r.d53, r.j); err != nil {
			t.Fatal(err)
		}
	}

	dump()

	if _, err := db.Exec(dropTables); err != nil {
		t.Fatal(err)
	}
}

func mysqldump(t *testing.T, dest string, table string) {
	if err := os.MkdirAll(filepath.Dir(dest), 0777); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	writer := bufio.NewWriter(out)

	args := []string{`-u`, `root`, `cockroachtestdata`}
	if table != "" {
		args = append(args, table)
	}
	cmd := exec.Command(`mysqldump`, args...)
	cmd.Stdout = writer
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := out.Sync(); err != nil {
		t.Fatal(err)
	}
}
