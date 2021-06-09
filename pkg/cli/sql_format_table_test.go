// Copyright 2021 The Cockroach Authors.
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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func Example_sql_column_labels() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	testData := []string{
		`f"oo`,
		`f'oo`,
		`f\oo`,
		`short
very very long
not much`,
		`very very long
thenshort`,
		`κόσμε`,
		`a|b`,
		`܈85`,
	}

	tdef := make([]string, len(testData))
	var vals bytes.Buffer
	for i, col := range testData {
		tdef[i] = tree.NameString(col) + " int"
		if i > 0 {
			vals.WriteString(", ")
		}
		vals.WriteByte('0')
	}
	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.u (" + strings.Join(tdef, ", ") + ")"})
	c.RunWithArgs([]string{"sql", "-e", "insert into t.u values (" + vals.String() + ")"})
	c.RunWithArgs([]string{"sql", "-e", "show columns from t.u"})
	c.RunWithArgs([]string{"sql", "-e", "select * from t.u"})
	c.RunWithArgs([]string{"sql", "--format=table", "-e", "show columns from t.u"})
	for i := tableDisplayFormat(0); i < tableDisplayLastFormat; i++ {
		c.RunWithArgs([]string{"sql", "--format=" + i.String(), "-e", "select * from t.u"})
	}

	// Output
	// sql -e create database t; create table t.u ("f""oo" int, "f'oo" int, "f\oo" int, "short
	// very very long
	// not much" int, "very very long
	// thenshort" int, "κόσμε" int, "a|b" int, ܈85 int)
	// CREATE TABLE
	// sql -e insert into t.u values (0, 0, 0, 0, 0, 0, 0, 0)
	// INSERT 1
	// sql -e show columns from t.u
	// column_name	data_type	is_nullable	column_default	generation_expression	indices
	// "f""oo"	INT	true	NULL		{}
	// f'oo	INT	true	NULL		{}
	// f\oo	INT	true	NULL		{}
	// "short
	// very very long
	// not much"	INT	true	NULL		{}
	// "very very long
	// thenshort"	INT	true	NULL		{}
	// κόσμε	INT	true	NULL		{}
	// a|b	INT	true	NULL		{}
	// ܈85	INT	true	NULL		{}
	// sql -e select * from t.u
	// "f""oo"	f'oo	f\oo	"short
	// very very long
	// not much"	"very very long
	// thenshort"	κόσμε	a|b	܈85
	// 0	0	0	0	0	0	0	0
	// sql --format=table -e show columns from t.u
	//    column_name   | data_type | is_nullable | column_default | generation_expression | indices
	// +----------------+-----------+-------------+----------------+-----------------------+---------+
	//   f"oo           | INT       |    true     | NULL           |                       | {}
	//   f'oo           | INT       |    true     | NULL           |                       | {}
	//   f\oo           | INT       |    true     | NULL           |                       | {}
	//   short          | INT       |    true     | NULL           |                       | {}
	//   very very long |           |             |                |                       |
	//   not much       |           |             |                |                       |
	//   very very long | INT       |    true     | NULL           |                       | {}
	//   thenshort      |           |             |                |                       |
	//   κόσμε          | INT       |    true     | NULL           |                       | {}
	//   a|b            | INT       |    true     | NULL           |                       | {}
	//   ܈85            | INT       |    true     | NULL           |                       | {}
	// (8 rows)
	// sql --format=tsv -e select * from t.u
	// "f""oo"	f'oo	f\oo	"short
	// very very long
	// not much"	"very very long
	// thenshort"	κόσμε	a|b	܈85
	// 0	0	0	0	0	0	0	0
	// sql --format=csv -e select * from t.u
	// "f""oo",f'oo,f\oo,"short
	// very very long
	// not much","very very long
	// thenshort",κόσμε,a|b,܈85
	// 0,0,0,0,0,0,0,0
	// sql --format=table -e select * from t.u
	//   f"oo | f'oo | f\oo |     short      | very very long | κόσμε | a|b | ܈85
	//        |      |      | very very long |   thenshort    |       |     |
	//        |      |      |    not much    |                |       |     |
	// +------+------+------+----------------+----------------+-------+-----+-----+
	//      0 |    0 |    0 |              0 |              0 |     0 |   0 |   0
	// (1 row)
	// sql --format=records -e select * from t.u
	// -[ RECORD 1 ]
	// f"oo           | 0
	// f'oo           | 0
	// f\oo           | 0
	// short         +| 0
	// very very long+|
	// not much       |
	// very very long+| 0
	// thenshort      |
	// κόσμε          | 0
	// a|b            | 0
	// ܈85            | 0
	// sql --format=sql -e select * from t.u
	// CREATE TABLE results (
	//   "f""oo" STRING,
	//   "f'oo" STRING,
	//   "f\oo" STRING,
	//   "short
	// very very long
	// not much" STRING,
	//   "very very long
	// thenshort" STRING,
	//   "κόσμε" STRING,
	//   "a|b" STRING,
	//   ܈85 STRING
	// );
	//
	// INSERT INTO results VALUES ('0', '0', '0', '0', '0', '0', '0', '0');
	// -- 1 row
	// sql --format=html -e select * from t.u
	// <table>
	// <thead><tr><th>row</th><th>f&#34;oo</th><th>f&#39;oo</th><th>f\oo</th><th>short<br/>very very long<br/>not much</th><th>very very long<br/>thenshort</th><th>κόσμε</th><th>a|b</th><th>܈85</th></tr></thead>
	// <tbody>
	// <tr><td>1</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td></tr>
	// </tbody>
	// <tfoot><tr><td colspan=9>1 row</td></tr></tfoot></table>
	// sql --format=raw -e select * from t.u
	// # 8 columns
	// # row 1
	// ## 1
	// 0
	// ## 1
	// 0
	// ## 1
	// 0
	// ## 1
	// 0
	// ## 1
	// 0
	// ## 1
	// 0
	// ## 1
	// 0
	// ## 1
	// 0
	// # 1 row
}

func Example_sql_empty_table() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	c.RunWithArgs([]string{"sql", "-e", "create database t;" +
		"create table t.norows(x int);" +
		"create table t.nocolsnorows();" +
		"create table t.nocols(); insert into t.nocols(rowid) values (1),(2),(3);"})
	for _, table := range []string{"norows", "nocols", "nocolsnorows"} {
		for format := tableDisplayFormat(0); format < tableDisplayLastFormat; format++ {
			c.RunWithArgs([]string{"sql", "--format=" + format.String(), "-e", "select * from t." + table})
		}
	}

	// Output:
	// sql -e create database t;create table t.norows(x int);create table t.nocolsnorows();create table t.nocols(); insert into t.nocols(rowid) values (1),(2),(3);
	// INSERT 3
	// sql --format=tsv -e select * from t.norows
	// x
	// sql --format=csv -e select * from t.norows
	// x
	// sql --format=table -e select * from t.norows
	//   x
	// -----
	// (0 rows)
	// sql --format=records -e select * from t.norows
	// sql --format=sql -e select * from t.norows
	// CREATE TABLE results (
	//   x STRING
	// );
	//
	// -- 0 rows
	// sql --format=html -e select * from t.norows
	// <table>
	// <thead><tr><th>row</th><th>x</th></tr></thead>
	// </tbody>
	// <tfoot><tr><td colspan=2>0 rows</td></tr></tfoot></table>
	// sql --format=raw -e select * from t.norows
	// # 1 column
	// # 0 rows
	// sql --format=tsv -e select * from t.nocols
	// # no columns
	// # empty
	// # empty
	// # empty
	// sql --format=csv -e select * from t.nocols
	// # no columns
	// # empty
	// # empty
	// # empty
	// sql --format=table -e select * from t.nocols
	// --
	// (3 rows)
	// sql --format=records -e select * from t.nocols
	// (3 rows)
	// sql --format=sql -e select * from t.nocols
	// CREATE TABLE results (
	// );
	//
	// INSERT INTO results(rowid) VALUES (DEFAULT);
	// INSERT INTO results(rowid) VALUES (DEFAULT);
	// INSERT INTO results(rowid) VALUES (DEFAULT);
	// -- 3 rows
	// sql --format=html -e select * from t.nocols
	// <table>
	// <thead><tr><th>row</th></tr></thead>
	// <tbody>
	// <tr><td>1</td></tr>
	// <tr><td>2</td></tr>
	// <tr><td>3</td></tr>
	// </tbody>
	// <tfoot><tr><td colspan=1>3 rows</td></tr></tfoot></table>
	// sql --format=raw -e select * from t.nocols
	// # 0 columns
	// # row 1
	// # row 2
	// # row 3
	// # 3 rows
	// sql --format=tsv -e select * from t.nocolsnorows
	// # no columns
	// sql --format=csv -e select * from t.nocolsnorows
	// # no columns
	// sql --format=table -e select * from t.nocolsnorows
	// --
	// (0 rows)
	// sql --format=records -e select * from t.nocolsnorows
	// (0 rows)
	// sql --format=sql -e select * from t.nocolsnorows
	// CREATE TABLE results (
	// );
	//
	// -- 0 rows
	// sql --format=html -e select * from t.nocolsnorows
	// <table>
	// <thead><tr><th>row</th></tr></thead>
	// </tbody>
	// <tfoot><tr><td colspan=1>0 rows</td></tr></tfoot></table>
	// sql --format=raw -e select * from t.nocolsnorows
	// # 0 columns
	// # 0 rows
}

func Example_csv_tsv_quoting() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	testData := []string{
		`ab`,
		`a b`,
		`a
bc
def`,
		`a, b`,
		`"a", "b"`,
		`'a', 'b'`,
		`a\,b`,
		`a	b`,
	}

	for _, sqlStr := range testData {
		escaped := lex.EscapeSQLString(sqlStr)
		sql := "select " + escaped + " as s, " + escaped + " as t"
		c.RunWithArgs([]string{"sql", "--format=csv", "-e", sql})
		c.RunWithArgs([]string{"sql", "--format=tsv", "-e", sql})
	}

	for _, identStr := range testData {
		escaped1 := tree.NameString(identStr + "1")
		escaped2 := tree.NameString(identStr + "2")
		sql := "select 1 as " + escaped1 + ", 2 as " + escaped2
		c.RunWithArgs([]string{"sql", "--format=csv", "-e", sql})
		c.RunWithArgs([]string{"sql", "--format=tsv", "-e", sql})
	}

	// Output:
	// sql --format=csv -e select 'ab' as s, 'ab' as t
	// s,t
	// ab,ab
	// sql --format=tsv -e select 'ab' as s, 'ab' as t
	// s	t
	// ab	ab
	// sql --format=csv -e select 'a b' as s, 'a b' as t
	// s,t
	// a b,a b
	// sql --format=tsv -e select 'a b' as s, 'a b' as t
	// s	t
	// a b	a b
	// sql --format=csv -e select e'a\nbc\ndef' as s, e'a\nbc\ndef' as t
	// s,t
	// "a
	// bc
	// def","a
	// bc
	// def"
	// sql --format=tsv -e select e'a\nbc\ndef' as s, e'a\nbc\ndef' as t
	// s	t
	// "a
	// bc
	// def"	"a
	// bc
	// def"
	// sql --format=csv -e select 'a, b' as s, 'a, b' as t
	// s,t
	// "a, b","a, b"
	// sql --format=tsv -e select 'a, b' as s, 'a, b' as t
	// s	t
	// a, b	a, b
	// sql --format=csv -e select '"a", "b"' as s, '"a", "b"' as t
	// s,t
	// """a"", ""b""","""a"", ""b"""
	// sql --format=tsv -e select '"a", "b"' as s, '"a", "b"' as t
	// s	t
	// """a"", ""b"""	"""a"", ""b"""
	// sql --format=csv -e select e'\'a\', \'b\'' as s, e'\'a\', \'b\'' as t
	// s,t
	// "'a', 'b'","'a', 'b'"
	// sql --format=tsv -e select e'\'a\', \'b\'' as s, e'\'a\', \'b\'' as t
	// s	t
	// 'a', 'b'	'a', 'b'
	// sql --format=csv -e select e'a\\,b' as s, e'a\\,b' as t
	// s,t
	// "a\,b","a\,b"
	// sql --format=tsv -e select e'a\\,b' as s, e'a\\,b' as t
	// s	t
	// a\,b	a\,b
	// sql --format=csv -e select e'a\tb' as s, e'a\tb' as t
	// s,t
	// a	b,a	b
	// sql --format=tsv -e select e'a\tb' as s, e'a\tb' as t
	// s	t
	// "a	b"	"a	b"
	// sql --format=csv -e select 1 as ab1, 2 as ab2
	// ab1,ab2
	// 1,2
	// sql --format=tsv -e select 1 as ab1, 2 as ab2
	// ab1	ab2
	// 1	2
	// sql --format=csv -e select 1 as "a b1", 2 as "a b2"
	// a b1,a b2
	// 1,2
	// sql --format=tsv -e select 1 as "a b1", 2 as "a b2"
	// a b1	a b2
	// 1	2
	// sql --format=csv -e select 1 as "a
	// bc
	// def1", 2 as "a
	// bc
	// def2"
	// "a
	// bc
	// def1","a
	// bc
	// def2"
	// 1,2
	// sql --format=tsv -e select 1 as "a
	// bc
	// def1", 2 as "a
	// bc
	// def2"
	// "a
	// bc
	// def1"	"a
	// bc
	// def2"
	// 1	2
	// sql --format=csv -e select 1 as "a, b1", 2 as "a, b2"
	// "a, b1","a, b2"
	// 1,2
	// sql --format=tsv -e select 1 as "a, b1", 2 as "a, b2"
	// a, b1	a, b2
	// 1	2
	// sql --format=csv -e select 1 as """a"", ""b""1", 2 as """a"", ""b""2"
	// """a"", ""b""1","""a"", ""b""2"
	// 1,2
	// sql --format=tsv -e select 1 as """a"", ""b""1", 2 as """a"", ""b""2"
	// """a"", ""b""1"	"""a"", ""b""2"
	// 1	2
	// sql --format=csv -e select 1 as "'a', 'b'1", 2 as "'a', 'b'2"
	// "'a', 'b'1","'a', 'b'2"
	// 1,2
	// sql --format=tsv -e select 1 as "'a', 'b'1", 2 as "'a', 'b'2"
	// 'a', 'b'1	'a', 'b'2
	// 1	2
	// sql --format=csv -e select 1 as "a\,b1", 2 as "a\,b2"
	// "a\,b1","a\,b2"
	// 1,2
	// sql --format=tsv -e select 1 as "a\,b1", 2 as "a\,b2"
	// a\,b1	a\,b2
	// 1	2
	// sql --format=csv -e select 1 as "a	b1", 2 as "a	b2"
	// a	b1,a	b2
	// 1,2
	// sql --format=tsv -e select 1 as "a	b1", 2 as "a	b2"
	// "a	b1"	"a	b2"
	// 1	2
}

func Example_sql_table() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	testData := []struct {
		str, desc string
	}{
		{"e'foo'", "printable ASCII"},
		{"e'\"foo'", "printable ASCII with quotes"},
		{"e'\\\\foo'", "printable ASCII with backslash"},
		{"e'foo\\x0abar'", "non-printable ASCII"},
		{"'κόσμε'", "printable UTF8"},
		{"e'\\xc3\\xb1'", "printable UTF8 using escapes"},
		{"e'\\x01'", "non-printable UTF8 string"},
		{"e'\\xdc\\x88\\x38\\x35'", "UTF8 string with RTL char"},
		{"e'a\\tb\\tc\\n12\\t123123213\\t12313'", "tabs"},
		{"e'\\xc3\\x28'", "non-UTF8 string"}, // This expects an insert error.
	}

	c.RunWithArgs([]string{"sql", "-e", "create database t; create table t.t (s string, d string);"})
	for _, t := range testData {
		c.RunWithArgs([]string{"sql", "-e", "insert into t.t values (" + t.str + ", '" + t.desc + "')"})
	}
	c.RunWithArgs([]string{"sql", "-e", "select * from t.t"})
	for format := tableDisplayFormat(0); format < tableDisplayLastFormat; format++ {
		c.RunWithArgs([]string{"sql", "--format=" + format.String(), "-e", "select * from t.t"})
	}

	// Output:
	// sql -e create database t; create table t.t (s string, d string);
	// CREATE TABLE
	// sql -e insert into t.t values (e'foo', 'printable ASCII')
	// INSERT 1
	// sql -e insert into t.t values (e'"foo', 'printable ASCII with quotes')
	// INSERT 1
	// sql -e insert into t.t values (e'\\foo', 'printable ASCII with backslash')
	// INSERT 1
	// sql -e insert into t.t values (e'foo\x0abar', 'non-printable ASCII')
	// INSERT 1
	// sql -e insert into t.t values ('κόσμε', 'printable UTF8')
	// INSERT 1
	// sql -e insert into t.t values (e'\xc3\xb1', 'printable UTF8 using escapes')
	// INSERT 1
	// sql -e insert into t.t values (e'\x01', 'non-printable UTF8 string')
	// INSERT 1
	// sql -e insert into t.t values (e'\xdc\x88\x38\x35', 'UTF8 string with RTL char')
	// INSERT 1
	// sql -e insert into t.t values (e'a\tb\tc\n12\t123123213\t12313', 'tabs')
	// INSERT 1
	// sql -e insert into t.t values (e'\xc3\x28', 'non-UTF8 string')
	// ERROR: lexical error: invalid UTF-8 byte sequence
	// SQLSTATE: 42601
	// DETAIL: source SQL:
	// insert into t.t values (e'\xc3\x28', 'non-UTF8 string')
	//                         ^
	// HINT: try \h VALUES
	// sql -e select * from t.t
	// s	d
	// foo	printable ASCII
	// """foo"	printable ASCII with quotes
	// \foo	printable ASCII with backslash
	// "foo
	// bar"	non-printable ASCII
	// κόσμε	printable UTF8
	// ñ	printable UTF8 using escapes
	// \x01	non-printable UTF8 string
	// ܈85	UTF8 string with RTL char
	// "a	b	c
	// 12	123123213	12313"	tabs
	// sql --format=tsv -e select * from t.t
	// s	d
	// foo	printable ASCII
	// """foo"	printable ASCII with quotes
	// \foo	printable ASCII with backslash
	// "foo
	// bar"	non-printable ASCII
	// κόσμε	printable UTF8
	// ñ	printable UTF8 using escapes
	// \x01	non-printable UTF8 string
	// ܈85	UTF8 string with RTL char
	// "a	b	c
	// 12	123123213	12313"	tabs
	// sql --format=csv -e select * from t.t
	// s,d
	// foo,printable ASCII
	// """foo",printable ASCII with quotes
	// \foo,printable ASCII with backslash
	// "foo
	// bar",non-printable ASCII
	// κόσμε,printable UTF8
	// ñ,printable UTF8 using escapes
	// \x01,non-printable UTF8 string
	// ܈85,UTF8 string with RTL char
	// "a	b	c
	// 12	123123213	12313",tabs
	// sql --format=table -e select * from t.t
	//            s          |               d
	// ----------------------+---------------------------------
	//   foo                 | printable ASCII
	//   "foo                | printable ASCII with quotes
	//   \foo                | printable ASCII with backslash
	//   foo                 | non-printable ASCII
	//   bar                 |
	//   κόσμε               | printable UTF8
	//   ñ                   | printable UTF8 using escapes
	//   \x01                | non-printable UTF8 string
	//   ܈85                 | UTF8 string with RTL char
	//   a   b         c     | tabs
	//   12  123123213 12313 |
	// (9 rows)
	// sql --format=records -e select * from t.t
	// -[ RECORD 1 ]
	// s | foo
	// d | printable ASCII
	// -[ RECORD 2 ]
	// s | "foo
	// d | printable ASCII with quotes
	// -[ RECORD 3 ]
	// s | \foo
	// d | printable ASCII with backslash
	// -[ RECORD 4 ]
	// s | foo+
	//   | bar
	// d | non-printable ASCII
	// -[ RECORD 5 ]
	// s | κόσμε
	// d | printable UTF8
	// -[ RECORD 6 ]
	// s | ñ
	// d | printable UTF8 using escapes
	// -[ RECORD 7 ]
	// s | \x01
	// d | non-printable UTF8 string
	// -[ RECORD 8 ]
	// s | ܈85
	// d | UTF8 string with RTL char
	// -[ RECORD 9 ]
	// s | a	b	c+
	//   | 12	123123213	12313
	// d | tabs
	// sql --format=sql -e select * from t.t
	// CREATE TABLE results (
	//   s STRING,
	//   d STRING
	// );
	//
	// INSERT INTO results VALUES ('foo', 'printable ASCII');
	// INSERT INTO results VALUES ('"foo', 'printable ASCII with quotes');
	// INSERT INTO results VALUES (e'\\foo', 'printable ASCII with backslash');
	// INSERT INTO results VALUES (e'foo\nbar', 'non-printable ASCII');
	// INSERT INTO results VALUES (e'\u03BA\U00001F79\u03C3\u03BC\u03B5', 'printable UTF8');
	// INSERT INTO results VALUES (e'\u00F1', 'printable UTF8 using escapes');
	// INSERT INTO results VALUES (e'\\x01', 'non-printable UTF8 string');
	// INSERT INTO results VALUES (e'\u070885', 'UTF8 string with RTL char');
	// INSERT INTO results VALUES (e'a\tb\tc\n12\t123123213\t12313', 'tabs');
	// -- 9 rows
	// sql --format=html -e select * from t.t
	// <table>
	// <thead><tr><th>row</th><th>s</th><th>d</th></tr></thead>
	// <tbody>
	// <tr><td>1</td><td>foo</td><td>printable ASCII</td></tr>
	// <tr><td>2</td><td>&#34;foo</td><td>printable ASCII with quotes</td></tr>
	// <tr><td>3</td><td>\foo</td><td>printable ASCII with backslash</td></tr>
	// <tr><td>4</td><td>foo<br/>bar</td><td>non-printable ASCII</td></tr>
	// <tr><td>5</td><td>κόσμε</td><td>printable UTF8</td></tr>
	// <tr><td>6</td><td>ñ</td><td>printable UTF8 using escapes</td></tr>
	// <tr><td>7</td><td>\x01</td><td>non-printable UTF8 string</td></tr>
	// <tr><td>8</td><td>܈85</td><td>UTF8 string with RTL char</td></tr>
	// <tr><td>9</td><td>a	b	c<br/>12	123123213	12313</td><td>tabs</td></tr>
	// </tbody>
	// <tfoot><tr><td colspan=3>9 rows</td></tr></tfoot></table>
	// sql --format=raw -e select * from t.t
	// # 2 columns
	// # row 1
	// ## 3
	// foo
	// ## 15
	// printable ASCII
	// # row 2
	// ## 4
	// "foo
	// ## 27
	// printable ASCII with quotes
	// # row 3
	// ## 4
	// \foo
	// ## 30
	// printable ASCII with backslash
	// # row 4
	// ## 7
	// foo
	// bar
	// ## 19
	// non-printable ASCII
	// # row 5
	// ## 11
	// κόσμε
	// ## 14
	// printable UTF8
	// # row 6
	// ## 2
	// ñ
	// ## 28
	// printable UTF8 using escapes
	// # row 7
	// ## 4
	// \x01
	// ## 25
	// non-printable UTF8 string
	// # row 8
	// ## 4
	// ܈85
	// ## 25
	// UTF8 string with RTL char
	// # row 9
	// ## 24
	// a	b	c
	// 12	123123213	12313
	// ## 4
	// tabs
	// # 9 rows
}

func Example_sql_table_border() {
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

	for i := 0; i < 4; i++ {
		c.RunWithArgs([]string{`sql`, `--format=table`, `--set`, `border=` + strconv.Itoa(i),
			`-e`, `values (123, '123'), (456, e'456\nfoobar')`})
	}

	// Output:
	// sql --format=table --set border=0 -e values (123, '123'), (456, e'456\nfoobar')
	//   column1 | column2
	// ----------+----------
	//       123 | 123
	//       456 | 456
	//           | foobar
	// (2 rows)
	// sql --format=table --set border=1 -e values (123, '123'), (456, e'456\nfoobar')
	//   column1 | column2
	// ----------+----------
	//       123 | 123
	// ----------+----------
	//       456 | 456
	//           | foobar
	// ----------+----------
	// (2 rows)
	// sql --format=table --set border=2 -e values (123, '123'), (456, e'456\nfoobar')
	// +---------+---------+
	// | column1 | column2 |
	// +---------+---------+
	// |     123 | 123     |
	// |     456 | 456     |
	// |         | foobar  |
	// +---------+---------+
	// (2 rows)
	// sql --format=table --set border=3 -e values (123, '123'), (456, e'456\nfoobar')
	// +---------+---------+
	// | column1 | column2 |
	// +---------+---------+
	// |     123 | 123     |
	// +---------+---------+
	// |     456 | 456     |
	// |         | foobar  |
	// +---------+---------+
	// (2 rows)
}

func TestRenderHTML(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cols := []string{"colname"}
	align := "d"
	rows := [][]string{
		{"<b>foo</b>"},
		{"bar"},
	}

	type testCase struct {
		reporter htmlReporter
		out      string
	}

	testCases := []testCase{
		{
			reporter: htmlReporter{},
			out: `<table>
<thead><tr><th>colname</th></tr></thead>
<tbody>
<tr><td><b>foo</b></td></tr>
<tr><td>bar</td></tr>
</tbody>
</table>
`,
		},
		{
			reporter: htmlReporter{escape: true},
			out: `<table>
<thead><tr><th>colname</th></tr></thead>
<tbody>
<tr><td>&lt;b&gt;foo&lt;/b&gt;</td></tr>
<tr><td>bar</td></tr>
</tbody>
</table>
`,
		},
		{
			reporter: htmlReporter{rowStats: true},
			out: `<table>
<thead><tr><th>row</th><th>colname</th></tr></thead>
<tbody>
<tr><td>1</td><td><b>foo</b></td></tr>
<tr><td>2</td><td>bar</td></tr>
</tbody>
<tfoot><tr><td colspan=2>2 rows</td></tr></tfoot></table>
`,
		},
		{
			reporter: htmlReporter{escape: true, rowStats: true},
			out: `<table>
<thead><tr><th>row</th><th>colname</th></tr></thead>
<tbody>
<tr><td>1</td><td>&lt;b&gt;foo&lt;/b&gt;</td></tr>
<tr><td>2</td><td>bar</td></tr>
</tbody>
<tfoot><tr><td colspan=2>2 rows</td></tr></tfoot></table>
`,
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("escape=%v/rowStats=%v", tc.reporter.escape, tc.reporter.rowStats)
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := render(&tc.reporter, &buf,
				cols, NewRowSliceIter(rows, align),
				nil /* completedHook */, nil /* noRowsHook */)
			if err != nil {
				t.Fatal(err)
			}
			if tc.out != buf.String() {
				t.Errorf("expected:\n%s\ngot:\n%s", tc.out, buf.String())
			}
		})
	}
}
