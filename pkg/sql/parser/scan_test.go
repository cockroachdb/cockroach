// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

func TestScanner(t *testing.T) {
	testData := []struct {
		sql      string
		expected []int
	}{
		{``, nil},
		{` `, nil},
		{` /* hello */`, nil},
		{`.`, []int{'.'}},
		{`..`, []int{lexbase.DOT_DOT}},
		{`!`, []int{'!'}},
		{`!=`, []int{lexbase.NOT_EQUALS}},
		{`<`, []int{'<'}},
		{`<>`, []int{lexbase.NOT_EQUALS}},
		{`<=`, []int{lexbase.LESS_EQUALS}},
		{`<<`, []int{lexbase.LSHIFT}},
		{`<<=`, []int{lexbase.INET_CONTAINED_BY_OR_EQUALS}},
		{`>`, []int{'>'}},
		{`>=`, []int{lexbase.GREATER_EQUALS}},
		{`>>`, []int{lexbase.RSHIFT}},
		{`>>=`, []int{lexbase.INET_CONTAINS_OR_EQUALS}},
		{`=`, []int{'='}},
		{`:`, []int{':'}},
		{`::`, []int{lexbase.TYPECAST}},
		{`:: :`, []int{lexbase.TYPECAST, ':'}},
		{`(`, []int{'('}},
		{`)`, []int{')'}},
		{`[`, []int{'['}},
		{`]`, []int{']'}},
		{`,`, []int{','}},
		{`;`, []int{';'}},
		{`+`, []int{'+'}},
		{`-`, []int{'-'}},
		{`*`, []int{'*'}},
		{`/`, []int{'/'}},
		{`//`, []int{lexbase.FLOORDIV}},
		{`%`, []int{'%'}},
		{`^`, []int{'^'}},
		{`$`, []int{'$'}},
		{`&`, []int{'&'}},
		{`&&`, []int{lexbase.AND_AND}},
		{`|`, []int{'|'}},
		{`||`, []int{lexbase.CONCAT}},
		{`|/`, []int{lexbase.SQRT}},
		{`||/`, []int{lexbase.CBRT}},
		{`#`, []int{'#'}},
		{`~`, []int{'~'}},
		{`!~`, []int{lexbase.NOT_REGMATCH}},
		{`~*`, []int{lexbase.REGIMATCH}},
		{`!~*`, []int{lexbase.NOT_REGIMATCH}},
		{`$1`, []int{lexbase.PLACEHOLDER}},
		{`$a`, []int{'$', IDENT}},
		{`a`, []int{lexbase.IDENT}},
		{`foo + bar`, []int{lexbase.IDENT, '+', lexbase.IDENT}},
		{`select a from b`, []int{lexbase.SELECT, lexbase.IDENT, lexbase.FROM, lexbase.IDENT}},
		{`"a" "b"`, []int{lexbase.IDENT, lexbase.IDENT}},
		{`'a'`, []int{lexbase.SCONST}},
		{`$$a$$`, []int{lexbase.SCONST}},
		{`$a$b$a$`, []int{lexbase.SCONST}},
		{`$a$b b$a$`, []int{lexbase.SCONST}},
		{`$a$ $a$`, []int{lexbase.SCONST}},
		{`$a$1$b$2$b$3$a$`, []int{lexbase.SCONST}},
		{`$a$1$b$2$b3$a$`, []int{lexbase.SCONST}},
		{`$a$1$$3$a$`, []int{lexbase.SCONST}},
		{`$a$1$$3$a$`, []int{lexbase.SCONST}},
		{`$a$1$3$a$`, []int{lexbase.SCONST}},
		{`$ab$1$a$ab$`, []int{lexbase.SCONST}},
		{`$ab1$ab$ab1$`, []int{lexbase.SCONST}},
		{`$ab1$ab12$ab1$`, []int{lexbase.SCONST}},
		{`$$~!@#$%^&*()_+:",./<>?;'$$`, []int{lexbase.SCONST}},
		{`$$hello
world$$`, []int{lexbase.SCONST}},
		{`b'a'`, []int{lexbase.BCONST}},
		{`b'\xff'`, []int{lexbase.BCONST}},
		{`B'10101'`, []int{lexbase.BITCONST}},
		{`e'a'`, []int{lexbase.SCONST}},
		{`E'a'`, []int{lexbase.SCONST}},
		{`NOT`, []int{lexbase.NOT}},
		{`NOT BETWEEN`, []int{lexbase.NOT, lexbase.BETWEEN}},
		{`NOT IN`, []int{lexbase.NOT, IN}},
		{`NOT SIMILAR`, []int{lexbase.NOT, lexbase.SIMILAR}},
		{`WITH`, []int{lexbase.WITH}},
		{`WITH TIME`, []int{lexbase.WITH, lexbase.TIME}},
		{`WITH ORDINALITY`, []int{lexbase.WITH, lexbase.ORDINALITY}},
		{`1`, []int{lexbase.ICONST}},
		{`0xa`, []int{lexbase.ICONST}},
		{`x'2F'`, []int{lexbase.BCONST}},
		{`X'2F'`, []int{lexbase.BCONST}},
		{`1.0`, []int{lexbase.FCONST}},
		{`1.0e1`, []int{lexbase.FCONST}},
		{`1e+1`, []int{lexbase.FCONST}},
		{`1e-1`, []int{lexbase.FCONST}},
	}
	for i, d := range testData {
		s := makeScanner(d.sql)
		var tokens []int
		for {
			var lval ScanSymType
			s.scan(lval)
			if lval.Id() == 0 {
				break
			}
			tokens = append(tokens, int(lval.Id()))
		}

		if !reflect.DeepEqual(d.expected, tokens) {
			t.Errorf("%d: %q: expected %d, but found %d", i, d.sql, d.expected, tokens)
		}
	}
}

func TestScanComment(t *testing.T) {
	testData := []struct {
		sql       string
		err       string
		remainder string
	}{
		{`/* hello */world`, "", "world"},
		{`/* hello */*`, "", "*"},
		{`/* /* deeply /* nested */ comment */ */`, "", ""},
		{`/* /* */* */`, "", ""},
		{`/* /* /*/ */ */ */`, "", ""},
		{`/* multi line
comment */`, "", ""},
		{`-- hello world
foo`, "", "foo"},
		{`/*`, "unterminated comment", ""},
		{`/*/`, "unterminated comment", ""},
		{`/* /* */`, "unterminated comment", ""},
	}
	for i, d := range testData {
		s := makeScanner(d.sql)
		var lval ScanSymType
		present, ok := s.scanComment(lval)
		if d.err == "" && (!present || !ok) {
			t.Fatalf("%d: expected success, but found %s", i, lval.Str())
		} else if d.err != "" && (present || ok || d.err != lval.Str()) {
			t.Fatalf("%d: expected %s, but found %s", i, d.err, lval.Str())
		}
		if r := s.in[s.pos:]; d.remainder != r {
			t.Fatalf("%d: expected '%s', but found '%s'", i, d.remainder, r)
		}
	}
}

func TestScanKeyword(t *testing.T) {
	for _, kwName := range lexbase.KeywordNames {
		s := makeScanner(kwName)
		var lval ScanSymType
		s.scan(lval)
		if id := lexbase.GetKeywordID(kwName); id != lval.Id() {
			t.Errorf("%s: expected %d, but found %d", kwName, id, lval.Id())
		}
	}
}

func TestScanNumber(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
		id       int
	}{
		{`0`, `0`, lexbase.ICONST},
		{`000`, `0`, lexbase.ICONST},
		{`1`, `1`, lexbase.ICONST},
		{`0x1`, `0x1`, lexbase.ICONST},
		{`0X2`, `0X2`, lexbase.ICONST},
		{`0xff`, `0xff`, lexbase.ICONST},
		{`0xff.`, `0xff`, lexbase.ICONST},
		{`12345`, `12345`, lexbase.ICONST},
		{`08`, `8`, lexbase.ICONST},
		{`0011`, `11`, lexbase.ICONST},
		{`1.`, `1.`, lexbase.FCONST},
		{`.1`, `.1`, lexbase.FCONST},
		{`1..2`, `1`, lexbase.ICONST},
		{`1.2`, `1.2`, lexbase.FCONST},
		{`1.2e3`, `1.2e3`, lexbase.FCONST},
		{`1e3`, `1e3`, lexbase.FCONST},
		{`1e3.4`, `1e3`, lexbase.FCONST},
		{`.1e3.4`, `.1e3`, lexbase.FCONST},
		{`1e-3`, `1e-3`, lexbase.FCONST},
		{`1e-3-`, `1e-3`, lexbase.FCONST},
		{`1e+3`, `1e+3`, lexbase.FCONST},
		{`1e+3+`, `1e+3`, lexbase.FCONST},
		{`9223372036854775809`, `9223372036854775809`, lexbase.ICONST},
	}
	for _, d := range testData {
		s := makeScanner(d.sql)
		var lval ScanSymType
		s.scan(lval)
		if d.id != int(lval.Id()) {
			t.Errorf("%s: expected %d, but found %d", d.sql, d.id, lval.Id())
		}
		if d.expected != lval.Str() {
			t.Errorf("%s: expected %s, but found %s", d.sql, d.expected, lval.Str())
		}
	}
}

func TestScanPlaceholder(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
	}{
		{`$1`, "1"},
		{`$1a`, "1"},
		{`$123`, "123"},
	}
	for _, d := range testData {
		s := makeScanner(d.sql)
		var lval ScanSymType
		s.scan(lval)
		if lval.Id() != lexbase.PLACEHOLDER {
			t.Errorf("%s: expected %d, but found %d", d.sql, lexbase.PLACEHOLDER, lval.Id())
		}
		if d.expected != lval.Str() {
			t.Errorf("%s: expected %s, but found %s", d.sql, d.expected, lval.Str())
		}
	}
}

func TestScanString(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
	}{
		{`"a"`, `a`},
		{`'a'`, `a`},
		{`"a""b"`, `a"b`},
		{`"a''b"`, `a''b`},
		{`'a""b'`, `a""b`},
		{`'a''b'`, `a'b`},
		{`"a" "b"`, `a`},
		{`'a' 'b'`, `a`},
		{`'\n'`, `\n`},
		{`e'\n'`, "\n"},
		{`'\\n'`, `\\n`},
		{`'\'''`, `\'`},
		{`'\0\'`, `\0\`},
		{`"a"
	"b"`, `ab`},
		{`"a"
	'b'`, `a`},
		{`'a'
	'b'`, `ab`},
		{`'a'
	"b"`, `a`},
		{`e'\"'`, `"`}, // redundant escape
		{"'\n\\'", "\n\\"},
		{`e'foo\"\'\\\a\b\f\n\r\t\vbar'`,
			strings.Join([]string{`foo"'\`, "\a\b\f\n\r\t\v", `bar`}, "")},
		{`e'\\0'`, `\0`},
		{`'\0'`, `\0`},
		{`e'\x'`, `invalid syntax`},
		{`e'\x4'`, `invalid syntax`},
		{`e'\xg'`, `invalid syntax`},
		{`e'\X4'`, `invalid syntax`},
		{`e'\x41'`, `A`},
		{`e'\X41B'`, `AB`},
		{`e'\0'`, `invalid syntax`},
		{`e'\00'`, `invalid syntax`},
		{`e'\009'`, `invalid syntax`},
		{`e'\101'`, `A`},
		{`e'\101B'`, `AB`},
		{`e'\xff'`, `invalid UTF-8 byte sequence`},
		{`e'\u1'`, `invalid syntax`},
		{`e'\U123'`, `invalid syntax`},
		{`e'\u0041'`, `A`},
		{`e'\u0041B'`, `AB`},
		{`e'\U00000041'`, `A`},
		{`e'\U00000041B'`, `AB`},
		{`"''"`, `''`},
		{`'""'''`, `""'`},
		{`""""`, `"`},
		{`''''`, `'`},
		{`''''''`, `''`},
		{`'hello
world'`, `hello
world`},
		{`x'666f6f'`, `foo`},
		{`X'626172'`, `bar`},
		{`X'FF'`, "\xff"},
		{`B'100101'`, "100101"},
		{`$$a$$`, "a"},
		{`$a$b$a$`, "b"},
		{`$a$b b$a$`, "b b"},
		{`$a$ $a$`, " "},
		{`$a$1$b$2$b$3$a$`, "1$b$2$b$3"},
		{`$a$1$b$2$b3$a$`, "1$b$2$b3"},
		{`$a$1$$3$a$`, "1$$3"},
		{`$a$1$3$a$`, "1$3"},
		{`$ab$1$a$ab$`, "1$a"},
		{`$ab1$ab$ab1$`, "ab"},
		{`$ab1$ab12$ab1$`, "ab12"},
		{`$$~!@#$%^&*()_+:",./<>?;'$$`, "~!@#$%^&*()_+:\",./<>?;'"},
		{`$$hello
world$$`, `hello
world`},
		{`$$a`, `unterminated string`},
		{`$a$a$$`, `unterminated string`},
	}
	for _, d := range testData {
		s := makeScanner(d.sql)
		var lval ScanSymType
		s.scan(lval)
		if d.expected != lval.Str() {
			t.Errorf("%s: expected %q, but found %q", d.sql, d.expected, lval.Str())
		}
	}
}

func TestScanError(t *testing.T) {
	testData := []struct {
		sql string
		err string
	}{
		{`1e`, "invalid floating point literal"},
		{`1e-`, "invalid floating point literal"},
		{`1e+`, "invalid floating point literal"},
		{`0x`, "invalid hexadecimal numeric literal"},
		{`1x`, "invalid hexadecimal numeric literal"},
		{`1.x`, "invalid hexadecimal numeric literal"},
		{`1.0x`, "invalid hexadecimal numeric literal"},
		{`0x0x`, "invalid hexadecimal numeric literal"},
		{`00x0x`, "invalid hexadecimal numeric literal"},
		{`x'zzz'`, "invalid hexadecimal bytes literal"},
		{`X'zzz'`, "invalid hexadecimal bytes literal"},
		{`x'beef\x41'`, "invalid hexadecimal bytes literal"},
		{`X'beef\x41\x41'`, "invalid hexadecimal bytes literal"},
		{`x'a'`, "invalid hexadecimal bytes literal"},
		{`$0`, "placeholder index must be between 1 and 65536"},
		{`$9223372036854775809`, "placeholder index must be between 1 and 65536"},
		{`B'123'`, `"2" is not a valid binary digit`},
	}
	for _, d := range testData {
		s := makeScanner(d.sql)
		var lval ScanSymType
		s.scan(lval)
		if lval.Id() != ERROR {
			t.Errorf("%s: expected ERROR, but found %d", d.sql, lval.Id())
		}
		if !testutils.IsError(errors.Newf("%s", lval.Str()), d.err) {
			t.Errorf("%s: expected %s, but found %v", d.sql, d.err, lval.Str())
		}
	}
}

func TestSplitFirstStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		s, res string
	}{
		{
			s:   "SELECT 1",
			res: "",
		},
		{
			s:   "SELECT 1;",
			res: "SELECT 1;",
		},
		{
			s:   "SELECT 1  /* comment */ ;",
			res: "SELECT 1  /* comment */ ;",
		},
		{
			s:   "SELECT 1;SELECT 2",
			res: "SELECT 1;",
		},
		{
			s:   "SELECT 1  /* comment */ ;SELECT 2",
			res: "SELECT 1  /* comment */ ;",
		},
		{
			s:   "SELECT 1  /* comment */ ; /* comment */ SELECT 2",
			res: "SELECT 1  /* comment */ ;",
		},
		{
			s:   ";",
			res: ";",
		},
		{
			s:   "SELECT ';'",
			res: "",
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			pos, ok := SplitFirstStatement(tc.s)
			if !ok && pos != 0 {
				t.Fatalf("!ok but nonzero pos")
			}
			if tc.res != tc.s[:pos] {
				t.Errorf("expected `%s` but got `%s`", tc.res, tc.s[:pos])
			}
		})
	}
}

func TestLastLexicalToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		s   string
		res int
	}{
		{
			s:   "",
			res: 0,
		},
		{
			s:   " /* comment */ ",
			res: 0,
		},
		{
			s:   "SELECT",
			res: SELECT,
		},
		{
			s:   "SELECT 1",
			res: ICONST,
		},
		{
			s:   "SELECT 1;",
			res: ';',
		},
		{
			s:   "SELECT 1; /* comment */",
			res: ';',
		},
		{
			s: `SELECT 1;
			    -- comment`,
			res: ';',
		},
		{
			s: `
				--SELECT 1, 2, 3;
				SELECT 4, 5
				--blah`,
			res: ICONST,
		},
		{
			s: `
				--SELECT 1, 2, 3;
				SELECT 4, 5;
				--blah`,
			res: ';',
		},
		{
			s:   `SELECT 'unfinished`,
			res: ERROR,
		},
		{
			s:   `SELECT e'\xaa';`, // invalid token but last token is semicolon
			res: ';',
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			tok, ok := LastLexicalToken(tc.s)
			if !ok && tok != 0 {
				t.Fatalf("!ok but nonzero tok")
			}
			if tc.res != tok {
				t.Errorf("expected %d but got %d", tc.res, tok)
			}
		})
	}
}
