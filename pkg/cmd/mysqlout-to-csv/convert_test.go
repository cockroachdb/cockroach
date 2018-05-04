package main

import (
	"database/sql"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"
)

type testRow struct {
	i int
	s string
}

type dumpCfg struct {
	name string
	cfg  dumpReader
	null string
}

const injectNull = "inject-null"

func genTestData(t *testing.T, rows []testRow, configs []dumpCfg) {
	db, err := sql.Open("mysql", "root@/test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`DROP TABLE IF EXISTS test`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`CREATE TABLE test (i INT PRIMARY KEY, s text)`,
	); err != nil {
		t.Fatal(err)
	}
	for _, tc := range rows {
		s := &tc.s
		if *s == injectNull {
			s = nil
		}
		if _, err := db.Exec(
			`INSERT INTO test VALUES (?, ?)`, tc.i, s,
		); err != nil {
			t.Fatal(err)
		}
	}

	os.RemoveAll(`testdata`)
	for _, cfg := range configs {
		dest := filepath.Join(`testdata`, cfg.name)
		if err := os.MkdirAll(dest, 0777); err != nil {
			t.Fatal(err)
		}

		flags := []string{`-u`, `root`, `test`, `test`, `--tab`, `./` + dest}
		if cfg.cfg.hasEncloseChar {
			flags = append(flags, "--fields-enclosed-by", string(cfg.cfg.encloseChar))
		}
		if cfg.cfg.hasEscapeChar {
			flags = append(flags, "--fields-escaped-by", string(cfg.cfg.escapeChar))
		}
		if cfg.cfg.fieldSep != '\t' {
			flags = append(flags, "--fields-terminated-by", string(cfg.cfg.fieldSep))
		}
		if cfg.cfg.rowSep != '\n' {
			flags = append(flags, "--lines-terminated-by", string(cfg.cfg.rowSep))
		}
		if res, err := exec.Command(
			`mysqldump`, flags...,
		).CombinedOutput(); err != nil {
			t.Fatal(err, string(res))
		}

		if err := os.Remove(filepath.Join(dest, "test.sql")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(
		`DROP TABLE IF EXISTS test`,
	); err != nil {
		t.Fatal(err)
	}

}

func randStr(r *rand.Rand, from []rune, length int) string {
	s := make([]rune, length)
	for i := 0; i < length; i++ {
		s[i] = from[r.Intn(len(from))]
	}
	return string(s)
}

func TestConvert(t *testing.T) {
	badChars := []rune{'a', ';', '\n', ',', '"', '\\', '\r', '<', '\t', '✅', 'π', rune(0), rune(10), rune(2425), rune(5183), utf8.RuneError}
	r := rand.New(rand.NewSource(1))

	testRows := []testRow{
		{0, `str`},
		{1, ``},
		{2, ` `},
		{3, `,`},
		{4, "\n"},
		{5, `\n`},
		{6, `"`},
		{7, injectNull},
		{8, `\N`},
		{9, `NULL`},

		// Unicode
		{10, `¢`},
		{11, ` ¢ `},
		{12, `✅`},
		{13, `","\n,™¢`},
		{14, `✅¢©ƒƒƒƒåß∂√œ∫∑∆πœ∑˚¬≤µµç∫ø∆œ∑∆¬œ∫œ∑´´†¥¨ˆˆπ‘“æ…¬…¬˚ß∆å˚˙ƒ∆©˙©∂˙≥≤Ω˜˜µ√∫∫Ω¥∑`},
		{15, string([]rune{rune(0)})},

		{20, `a quote " or two quotes "" and a quote-comma ", , and then a quote and newline "` + "\n"},
		{21, `"a slash \, a double slash \\, a slash+quote \",  \` + "\n"},
	}

	for i := 0; i < 10; i++ {
		testRows = append(testRows, testRow{i + 100, randStr(r, badChars, 1000)})
	}

	configs := []dumpCfg{
		{
			name: "escape-and-enclose",
			cfg:  dumpReader{hasEscapeChar: true, escapeChar: '\\', hasEncloseChar: true, encloseChar: '"', fieldSep: '\t', rowSep: '\n'},
			null: `\N`,
		},
		{
			name: "csv-ish",
			cfg:  dumpReader{hasEscapeChar: true, escapeChar: '\\', hasEncloseChar: true, encloseChar: '"', fieldSep: ',', rowSep: '\n'},
			null: `\N`,
		},
		{
			name: "escape-quote-csv-no-enclose",
			cfg:  dumpReader{hasEscapeChar: true, escapeChar: '"', fieldSep: ',', rowSep: '\n'},
			null: `\N`,
		},
	}

	// This can be toggled to re-generate testdata. Requires local mysql
	// configured with no password access for `root`, a db called `test` and
	// `OUTFILE` enabled. On OSX, this can be setup using `brew install mysql` and
	// the following ~/.my.cnf:
	// [mysqld_safe]
	// [mysqld]
	// secure_file_priv=""
	if false {
		genTestData(t, testRows, configs)
	}

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			converter := config.cfg
			var res [][]string
			converter.f = func(r []string) error {
				if len(r) != 2 {
					t.Fatalf("bad row len for %v", r)
				}
				res = append(res, r)
				return nil
			}
			in, err := os.Open(filepath.Join(`testdata`, config.name, `test.txt`))
			if err != nil {
				t.Fatal(err)
			}
			defer in.Close()

			if err := converter.Process(in); err != nil {
				t.Fatal(err)
			}

			if len(res) != len(testRows) {
				t.Fatalf("expected %d rows, got %d: %v", len(testRows), len(res), res)
			}
			for i, row := range testRows {
				expected, actual := row.s, res[i][1]
				if expected == injectNull {
					expected = config.null
				}
				if expected != actual {
					t.Fatalf("row %d (i %d=%s): expected:\n%q\ngot:\n%q\n", i, row.i, res[i][0], expected, actual)
				}
			}
		})
	}
}
