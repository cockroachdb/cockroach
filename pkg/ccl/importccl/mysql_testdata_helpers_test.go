// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bufio"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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
		{i: 0, s: `str`},
		{i: 1, s: ``},
		{i: 2, s: ` `},
		{i: 3, s: `,`},
		{i: 4, s: "\n"},
		{i: 5, s: `\n`},
		{i: 6, s: "\r\n"},
		{i: 7, s: "\r"},
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
		{i: 19, s: `✅¢©ƒƒƒƒåß∂√œ∫∑∆πœ∑˚¬≤µµç∫ø∆œ∑∆¬œ∫œ∑´´†¥¨ˆˆπ‘“æ…¬…¬˚ß∆å˚˙ƒ∆©˙©∂˙≥≤Ω˜˜µ√∫∫Ω¥∑`},
		{i: 20, s: `a quote " or two quotes "" and a quote-comma ", , and then a quote and newline "` + "\n"},
		{i: 21, s: `"a slash \, a double slash \\, a slash+quote \",  \` + "\n"},
	}

	for i := 0; i < 10; i++ {
		buf := make([]byte, 200)
		r.Seed(int64(i))
		r.Read(buf)
		testRows = append(testRows, simpleTestRow{i: i + 100, s: randStr(r, badChars, 1000), b: buf})
	}
	return testRows
}()

func getSimpleMysqlDumpTestdata(t *testing.T) ([]simpleTestRow, string) {
	dest := filepath.Join(`testdata`, `mysqldump`, `simple.sql`)
	if rewriteMysqlTestData {
		genSimpleMysqlTestdata(t, func() { mysqldump(t, dest, "simple") })
	}
	return simpleTestRows, dest
}

func getSecondMysqlDumpTestdata(t *testing.T) (int, string) {
	dest := filepath.Join(`testdata`, `mysqldump`, `second.sql`)
	if rewriteMysqlTestData {
		genSecondMysqlTestdata(t, func() { mysqldump(t, dest, "second") })
	}
	return secondTableRows, dest
}

func getEverythingMysqlDumpTestdata(t *testing.T) string {
	dest := filepath.Join(`testdata`, `mysqldump`, `everything.sql`)
	if rewriteMysqlTestData {
		genEverythingMysqlTestdata(t, func() { mysqldump(t, dest, "everything") })
	}
	return dest
}

func getMultiTableMysqlDumpTestdata(t *testing.T) string {
	dest := filepath.Join(`testdata`, `mysqldump`, `db.sql`)
	if rewriteMysqlTestData {
		genEverythingMysqlTestdata(t, func() {
			genSecondMysqlTestdata(t, func() {
				genSimpleMysqlTestdata(t, func() { mysqldump(t, dest, "") })
			})
		})
	}
	return dest
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
		configs[i].filename = filepath.Join(`testdata`, `mysqlout`, configs[i].name, `simple.txt`)
	}

	if rewriteMysqlTestData {
		genSimpleMysqlTestdata(t, func() {
			if err := os.RemoveAll(filepath.Join(`testdata`, `mysqlout`)); err != nil {
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

// genMysqlTestdata connects to the a local mysql, creates the passwd table and
// calls the passed `load` func to populate it and returns a cleanup func.
func genMysqlTestdata(t *testing.T, name, schema string, load func(*gosql.DB)) func() {
	db, err := gosql.Open("mysql", "root@/"+mysqlTestDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		fmt.Sprintf(`CREATE TABLE %s (%s)`, name, schema),
	); err != nil {
		t.Fatal(err)
	}
	load(db)
	return func() {
		db, err := gosql.Open("mysql", "root@/"+mysqlTestDB)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec(
			fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name),
		); err != nil {
			t.Fatal(err)
		}
	}
}

func genSimpleMysqlTestdata(t *testing.T, dump func()) {
	defer genMysqlTestdata(t,
		"simple",
		`i INT PRIMARY KEY, s text, b binary(200)`,
		func(db *gosql.DB) {
			for _, tc := range simpleTestRows {
				s := &tc.s
				if *s == injectNull {
					s = nil
				}
				if _, err := db.Exec(
					`INSERT INTO simple VALUES (?, ?, ?)`, tc.i, s, tc.b,
				); err != nil {
					t.Fatal(err)
				}
			}
		},
	)()
	dump()
}

const secondTableRows = 7

func genSecondMysqlTestdata(t *testing.T, dump func()) {
	defer genMysqlTestdata(t, `second`, `
				i       INT PRIMARY KEY,
				s       VARCHAR(100)
		`,
		func(db *gosql.DB) {
			for i := 0; i < secondTableRows; i++ {
				if _, err := db.Exec(`INSERT INTO second VALUES (?, ?)`, i, strconv.Itoa(i)); err != nil {
					t.Fatal(err)
				}
			}
		},
	)()
	dump()
}

func genEverythingMysqlTestdata(t *testing.T, dump func()) {
	defer genMysqlTestdata(t, `everything`, `
				i       INT PRIMARY KEY,

				c       CHAR(10),
				s       VARCHAR(100),
				tx      TEXT,

				bin     BINARY(100),
				vbin    VARBINARY(100),
				bl      BLOB,

				dt      DATETIME,
				d       DATE,
				ts      TIMESTAMP,
				t       TIME,
				-- TODO(dt): fix parser: for YEAR's length option
				-- y       YEAR,

				de      DECIMAL,
				nu      NUMERIC,
				d53     DECIMAL(5,3),

				iw      INT(5),
				iz      INT ZEROFILL,
				ti      TINYINT,
				si      SMALLINT,
				mi      MEDIUMINT,
				bi      BIGINT,

				fl      FLOAT,
				rl      REAL,
				db      DOUBLE,

				f17     FLOAT(17),
				f47     FLOAT(47),
				f75     FLOAT(7, 5)
		`,
		func(_ *gosql.DB) {},
	)()
	dump()
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
