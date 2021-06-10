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
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

func Example_sql() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{`sql`, `-e`, `show application_name`})
	c.RunWithArgs([]string{`sql`, `-e`, `create database t; create table t.f (x int, y int); insert into t.f values (42, 69)`})
	c.RunWithArgs([]string{`sql`, `-e`, `select 3 as "3"`, `-e`, `select * from t.f`})
	c.RunWithArgs([]string{`sql`, `-e`, `begin`, `-e`, `select 3 as "3"`, `-e`, `commit`})
	c.RunWithArgs([]string{`sql`, `-e`, `select * from t.f`})
	c.RunWithArgs([]string{`sql`, `--execute=SELECT database_name, owner FROM [show databases]`})
	c.RunWithArgs([]string{`sql`, `-e`, `select 1 as "1"; select 2 as "2"`})
	c.RunWithArgs([]string{`sql`, `-e`, `select 1 as "1"; select 2 as "@" where false`})
	// CREATE TABLE AS returns a SELECT tag with a row count, check this.
	c.RunWithArgs([]string{`sql`, `-e`, `create table t.g1 (x int)`})
	c.RunWithArgs([]string{`sql`, `-e`, `create table t.g2 as select * from generate_series(1,10)`})
	// It must be possible to access pre-defined/virtual tables even if the current database
	// does not exist yet.
	c.RunWithArgs([]string{`sql`, `-d`, `nonexistent`, `-e`, `select count(*) from "".information_schema.tables limit 0`})
	// It must be possible to create the current database after the
	// connection was established.
	c.RunWithArgs([]string{`sql`, `-d`, `nonexistent`, `-e`, `create database nonexistent; create table foo(x int); select * from foo`})
	// COPY should return an intelligible error message.
	c.RunWithArgs([]string{`sql`, `-e`, `copy t.f from stdin`})

	// Check that partial results + error get reported together. The query will
	// run via the vectorized execution engine which operates on the batches of
	// growing capacity starting at 1 (the batch sizes will be 1, 2, 4, ...),
	// and with the query below the division by zero error will occur after the
	// first batch consisting of 1 row has been returned to the client.
	c.RunWithArgs([]string{`sql`, `-e`, `select 1/(@1-2) from generate_series(1,3)`})
	c.RunWithArgs([]string{`sql`, `-e`, `SELECT '20:01:02+03:04:05'::timetz AS regression_65066`})

	// Output:
	// sql -e show application_name
	// application_name
	// $ cockroach sql
	// sql -e create database t; create table t.f (x int, y int); insert into t.f values (42, 69)
	// INSERT 1
	// sql -e select 3 as "3" -e select * from t.f
	// 3
	// 3
	// x	y
	// 42	69
	// sql -e begin -e select 3 as "3" -e commit
	// BEGIN
	// 3
	// 3
	// COMMIT
	// sql -e select * from t.f
	// x	y
	// 42	69
	// sql --execute=SELECT database_name, owner FROM [show databases]
	// database_name	owner
	// defaultdb	root
	// postgres	root
	// system	node
	// t	root
	// sql -e select 1 as "1"; select 2 as "2"
	// 1
	// 1
	// 2
	// 2
	// sql -e select 1 as "1"; select 2 as "@" where false
	// 1
	// 1
	// @
	// sql -e create table t.g1 (x int)
	// CREATE TABLE
	// sql -e create table t.g2 as select * from generate_series(1,10)
	// CREATE TABLE AS
	// sql -d nonexistent -e select count(*) from "".information_schema.tables limit 0
	// count
	// sql -d nonexistent -e create database nonexistent; create table foo(x int); select * from foo
	// x
	// sql -e copy t.f from stdin
	// ERROR: woops! COPY has confused this client! Suggestion: use 'psql' for COPY
	// sql -e select 1/(@1-2) from generate_series(1,3)
	// ?column?
	// -1
	// (error encountered after some results were delivered)
	// ERROR: division by zero
	// SQLSTATE: 22012
	// sql -e SELECT '20:01:02+03:04:05'::timetz AS regression_65066
	// regression_65066
	// 20:01:02+03:04:05
}

func Example_sql_config() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	// --set changes client-side variables before executing commands.
	c.RunWithArgs([]string{`sql`, `--set=errexit=0`, `-e`, `select nonexistent`, `-e`, `select 123 as "123"`})
	c.RunWithArgs([]string{`sql`, `--set`, `echo=true`, `-e`, `select 123 as "123"`})
	// --set options are processed before -e options.
	c.RunWithArgs([]string{`sql`, `-e`, `select 123`, `--set`, `display_format=raw`})
	// Possible to run client-side commands with -e.
	c.RunWithArgs([]string{`sql`, `-e`, `\set display_format=raw`, `-e`, `select 123 as "123"`})
	// A failure in a client-side command prevents subsequent statements from executing.
	c.RunWithArgs([]string{`sql`, `--set`, `unknownoption`, `-e`, `select 123 as "123"`})
	c.RunWithArgs([]string{`sql`, `--set`, `display_format=invalidvalue`, `-e`, `select 123 as "123"`})
	c.RunWithArgs([]string{`sql`, `-e`, `\set display_format=invalidvalue`, `-e`, `select 123 as "123"`})

	// Output:
	// sql --set=errexit=0 -e select nonexistent -e select 123 as "123"
	// ERROR: column "nonexistent" does not exist
	// SQLSTATE: 42703
	// 123
	// 123
	// sql --set echo=true -e select 123 as "123"
	// > select 123 as "123"
	// 123
	// 123
	// sql -e select 123 --set display_format=raw
	// # 1 column
	// # row 1
	// ## 3
	// 123
	// # 1 row
	// sql -e \set display_format=raw -e select 123 as "123"
	// # 1 column
	// # row 1
	// ## 3
	// 123
	// # 1 row
	// sql --set unknownoption -e select 123 as "123"
	// invalid syntax: \set unknownoption. Try \? for help.
	// ERROR: invalid syntax
	// sql --set display_format=invalidvalue -e select 123 as "123"
	// \set display_format invalidvalue: invalid table display format: invalidvalue (possible values: tsv, csv, table, records, sql, html, raw)
	// ERROR: invalid table display format: invalidvalue (possible values: tsv, csv, table, records, sql, html, raw)
	// sql -e \set display_format=invalidvalue -e select 123 as "123"
	// \set display_format invalidvalue: invalid table display format: invalidvalue (possible values: tsv, csv, table, records, sql, html, raw)
	// ERROR: invalid table display format: invalidvalue (possible values: tsv, csv, table, records, sql, html, raw)

}

func Example_sql_watch() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{`sql`, `-e`, `create table d(x int); insert into d values(3)`})
	c.RunWithArgs([]string{`sql`, `--watch`, `.1s`, `-e`, `update d set x=x-1 returning 1/x as dec`})

	// Output:
	// sql -e create table d(x int); insert into d values(3)
	// INSERT 1
	// sql --watch .1s -e update d set x=x-1 returning 1/x as dec
	// dec
	// 0.5
	// dec
	// 1
	// ERROR: division by zero
	// SQLSTATE: 22012
}

func Example_misc_table() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.t (s string, d string);"})
	c.RunWithArgs([]string{"sql", "--format=table", "-e", "select '  hai' as x"})
	c.RunWithArgs([]string{"sql", "--format=table", "-e", "explain select s, 'foo' from t.t"})

	// Output:
	// sql -e create database t; create table t.t (s string, d string);
	// CREATE TABLE
	// sql --format=table -e select '  hai' as x
	//     x
	// ---------
	//     hai
	// (1 row)
	// sql --format=table -e explain select s, 'foo' from t.t
	//            info
	// --------------------------
	//   distribution: full
	//   vectorized: true
	//
	//   • render
	//   │
	//   └── • scan
	//         missing stats
	//         table: t@primary
	//         spans: FULL SCAN
	// (9 rows)
}

func Example_in_memory() {
	spec, err := base.NewStoreSpec("type=mem,size=1GiB")
	if err != nil {
		panic(err)
	}
	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{spec},
	})
	defer c.Cleanup()

	// Test some sql to ensure that the in memory store is working.
	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.f (x int, y int); insert into t.f values (42, 69)"})
	c.RunWithArgs([]string{"node", "ls"})

	// Output:
	// sql -e create database t; create table t.f (x int, y int); insert into t.f values (42, 69)
	// INSERT 1
	// node ls
	// id
	// 1
	//
}

func Example_pretty_print_numerical_strings() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	// All strings in pretty-print output should be aligned to left regardless of their contents
	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.t (s string, d string);"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'0', 'positive numerical string')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'-1', 'negative numerical string')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'1.0', 'decimal numerical string')"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (e'aaaaa', 'non-numerical string')"})
	c.RunWithArgs([]string{"sql", "--format=table", "-e", "select * from t.t"})

	// Output:
	// sql -e create database t; create table t.t (s string, d string);
	// CREATE TABLE
	// sql -e insert into t.t values (e'0', 'positive numerical string')
	// INSERT 1
	// sql -e insert into t.t values (e'-1', 'negative numerical string')
	// INSERT 1
	// sql -e insert into t.t values (e'1.0', 'decimal numerical string')
	// INSERT 1
	// sql -e insert into t.t values (e'aaaaa', 'non-numerical string')
	// INSERT 1
	// sql --format=table -e select * from t.t
	//     s   |             d
	// --------+----------------------------
	//   0     | positive numerical string
	//   -1    | negative numerical string
	//   1.0   | decimal numerical string
	//   aaaaa | non-numerical string
	// (4 rows)
}

// Example_read_from_file tests the -f parameter.
// The input file contains a mix of client-side and
// server-side commands to ensure that both are supported with -f.
func Example_read_from_file() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{"sql", "-e", "select 1", "-f", "testdata/inputfile.sql"})
	c.RunWithArgs([]string{"sql", "-f", "testdata/inputfile.sql"})

	// Output:
	// sql -e select 1 -f testdata/inputfile.sql
	// ERROR: unsupported combination: --execute and --file
	// sql -f testdata/inputfile.sql
	// SET
	// CREATE TABLE
	// > INSERT INTO test(s) VALUES ('hello'), ('world');
	// INSERT 2
	// > SELECT * FROM test;
	// s
	// hello
	// world
	// > SELECT undefined;
	// ERROR: column "undefined" does not exist
	// SQLSTATE: 42703
	// ERROR: column "undefined" does not exist
	// SQLSTATE: 42703
}

// Example_includes tests the \i command.
func Example_includes() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{"sql", "-f", "testdata/i_twolevels1.sql"})
	c.RunWithArgs([]string{"sql", "-f", "testdata/i_multiline.sql"})
	c.RunWithArgs([]string{"sql", "-f", "testdata/i_stopmiddle.sql"})
	c.RunWithArgs([]string{"sql", "-f", "testdata/i_maxrecursion.sql"})

	// Output:
	// sql -f testdata/i_twolevels1.sql
	// > SELECT 123;
	// ?column?
	// 123
	// > SELECT 789;
	// ?column?
	// 789
	// ?column?
	// 456
	// sql -f testdata/i_multiline.sql
	// ERROR: at or near "\": syntax error
	// SQLSTATE: 42601
	// DETAIL: source SQL:
	// SELECT -- incomplete statement, \i invalid
	// \i testdata/i_twolevels2.sql
	// ^
	// HINT: try \h SELECT
	// ERROR: at or near "\": syntax error
	// SQLSTATE: 42601
	// DETAIL: source SQL:
	// SELECT -- incomplete statement, \i invalid
	// \i testdata/i_twolevels2.sql
	// ^
	// HINT: try \h SELECT
	// sql -f testdata/i_stopmiddle.sql
	// ?column?
	// 123
	// sql -f testdata/i_maxrecursion.sql
	// \i: too many recursion levels (max 10)
	// ERROR: testdata/i_maxrecursion.sql: testdata/i_maxrecursion.sql: testdata/i_maxrecursion.sql: testdata/i_maxrecursion.sql: testdata/i_maxrecursion.sql: testdata/i_maxrecursion.sql: testdata/i_maxrecursion.sql: testdata/i_maxrecursion.sql: testdata/i_maxrecursion.sql: testdata/i_maxrecursion.sql: \i: too many recursion levels (max 10)
}

// Example_sql_lex tests the usage of the lexer in the sql subcommand.
func Example_sql_lex() {
	c := NewCLITest(TestCLIParams{Insecure: true})
	defer c.Cleanup()

	conn := makeSQLConn(fmt.Sprintf("postgres://%s@%s/?sslmode=disable",
		security.RootUser, c.ServingSQLAddr()))
	defer conn.Close()

	tests := []string{`
select '
\?
;
';
`,
		`
select ''''
;

select '''
;
''';
`,
		`select 1 as "1";
-- just a comment without final semicolon`,
	}

	setCLIDefaultsForTests()

	// We need a temporary file with a name guaranteed to be available.
	// So open a dummy file.
	f, err := ioutil.TempFile("", "input")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return
	}
	// Get the name and close it.
	fname := f.Name()
	f.Close()

	// At every point below, when t.Fatal is called we should ensure the
	// file is closed and removed.
	f = nil
	defer func() {
		if f != nil {
			f.Close()
		}
		_ = os.Remove(fname)
	}()

	for _, test := range tests {
		// Populate the test input.
		if f, err = os.OpenFile(fname, os.O_WRONLY, 0644); err != nil {
			fmt.Fprintln(stderr, err)
			return
		}
		if _, err := f.WriteString(test); err != nil {
			fmt.Fprintln(stderr, err)
			return
		}
		f.Close()
		// Make it available for reading.
		if f, err = os.Open(fname); err != nil {
			fmt.Fprintln(stderr, err)
			return
		}
		err := runInteractive(conn, f)
		if err != nil {
			fmt.Fprintln(stderr, err)
		}
	}

	// Output:
	// ?column?
	// ------------
	//
	//   \?
	//   ;
	//
	// (1 row)
	//   ?column?
	// ------------
	//   '
	// (1 row)
	//   ?column?
	// ------------
	//   '
	//   ;
	//   '
	// (1 row)
	//   1
	// -----
	//   1
	// (1 row)
}

func TestIsEndOfStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		in         string
		isEnd      bool
		isNotEmpty bool
	}{
		{
			in:         ";",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in:         "; /* comment */",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in:         "; SELECT",
			isNotEmpty: true,
		},
		{
			in:         "SELECT",
			isNotEmpty: true,
		},
		{
			in:         "SET; SELECT 1;",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in:         "SELECT ''''; SET;",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in: "  -- hello",
		},
		{
			in:         "select 'abc", // invalid token
			isNotEmpty: true,
		},
		{
			in:         "'abc", // invalid token
			isNotEmpty: true,
		},
		{
			in:         `SELECT e'\xaa';`, // invalid token but last token is semicolon
			isEnd:      true,
			isNotEmpty: true,
		},
	}

	for _, test := range tests {
		lastTok, isNotEmpty := parser.LastLexicalToken(test.in)
		if isNotEmpty != test.isNotEmpty {
			t.Errorf("%q: isNotEmpty expected %v, got %v", test.in, test.isNotEmpty, isNotEmpty)
		}
		isEnd := isEndOfStatement(lastTok)
		if isEnd != test.isEnd {
			t.Errorf("%q: isEnd expected %v, got %v", test.in, test.isEnd, isEnd)
		}
	}
}

// Test handleCliCmd cases for client-side commands that are aliases for sql
// statements.
func TestHandleCliCmdSqlAlias(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	initCLIDefaults()

	clientSideCommandTestsTable := []struct {
		commandString string
		wantSQLStmt   string
	}{
		{`\l`, `SHOW DATABASES`},
		{`\dt`, `SHOW TABLES`},
		{`\dT`, `SHOW TYPES`},
		{`\du`, `SHOW USERS`},
		{`\d mytable`, `SHOW COLUMNS FROM mytable`},
		{`\d`, `SHOW TABLES`},
	}

	var c cliState
	for _, tt := range clientSideCommandTestsTable {
		c = setupTestCliState()
		c.lastInputLine = tt.commandString
		gotState := c.doHandleCliCmd(cliStateEnum(0), cliStateEnum(1))

		assert.Equal(t, cliRunStatement, gotState)
		assert.Equal(t, tt.wantSQLStmt, c.concatLines)
	}
}

func TestHandleCliCmdSlashDInvalidSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	initCLIDefaults()

	clientSideCommandTests := []string{`\d goodarg badarg`, `\dz`}

	var c cliState
	for _, tt := range clientSideCommandTests {
		c = setupTestCliState()
		c.lastInputLine = tt
		gotState := c.doHandleCliCmd(cliStateEnum(0), cliStateEnum(1))

		assert.Equal(t, cliStateEnum(0), gotState)
		assert.Equal(t, errInvalidSyntax, c.exitErr)
	}
}

func TestHandleDemoNodeCommandsInvalidNodeName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	initCLIDefaults()

	demoNodeCommandTests := []string{"shutdown", "*"}

	c := setupTestCliState()
	c.handleDemoNodeCommands(demoNodeCommandTests, cliStateEnum(0), cliStateEnum(1))
	assert.Equal(t, errInvalidSyntax, c.exitErr)
}

func setupTestCliState() cliState {
	c := cliState{}
	c.ins = noLineEditor
	return c
}
