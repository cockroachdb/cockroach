// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lexbase_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func TestEncodeRestrictedSQLIdent(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{`foo`, `foo`},
		{``, `""`},
		{`3`, `"3"`},
		{`foo3`, `foo3`},
		{`foo"`, `"foo"""`},
		{`fo"o"`, `"fo""o"""`},
		{`fOo`, `"fOo"`},
		{`_foo`, `_foo`},
		{`-foo`, `"-foo"`},
		{`select`, `"select"`},
		{`integer`, `"integer"`},
		// N.B. These type names are examples of type names that *should* be
		// unrestricted (left out of the reserved keyword list) because they're not
		// part of the sql standard type name list. This is important for Postgres
		// compatibility. If you find yourself about to change this, don't - you can
		// convince yourself of such by looking at the output of `quote_ident`
		// against a Postgres instance.
		{`int8`, `int8`},
		{`date`, `date`},
		{`inet`, `inet`},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		lexbase.EncodeRestrictedSQLIdent(&buf, tc.input, lexbase.EncBareStrings)
		out := buf.String()

		if out != tc.output {
			t.Errorf("`%s`: expected `%s`, got `%s`", tc.input, tc.output, out)
		}
	}
}

func TestEncodeSQLBytes(t *testing.T) {
	testEncodeSQL(t, lexbase.EncodeSQLBytes, false)
}

func TestEncodeSQLString(t *testing.T) {
	testEncodeSQL(t, lexbase.EncodeSQLString, true)
}

func testEncodeSQL(t *testing.T, encode func(*bytes.Buffer, string), forceUTF8 bool) {
	type entry struct{ i, j int }
	seen := make(map[string]entry)
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			bytepair := []byte{byte(i), byte(j)}
			if forceUTF8 && !utf8.Valid(bytepair) {
				continue
			}
			stmt := testEncodeString(t, bytepair, encode)
			if e, ok := seen[stmt]; ok {
				t.Fatalf("duplicate entry: %s, from %v, currently at %v, %v", stmt, e, i, j)
			}
			seen[stmt] = entry{i, j}
		}
	}
}

func TestEncodeSQLStringSpecial(t *testing.T) {
	tests := [][]byte{
		// UTF8 replacement character
		{0xEF, 0xBF, 0xBD},
	}
	for _, tc := range tests {
		testEncodeString(t, tc, lexbase.EncodeSQLString)
	}
}

func testEncodeString(t *testing.T, input []byte, encode func(*bytes.Buffer, string)) string {
	s := string(input)
	var buf bytes.Buffer
	encode(&buf, s)
	sql := fmt.Sprintf("SELECT %s", buf.String())
	for n := 0; n < len(sql); n++ {
		ch := sql[n]
		if ch < 0x20 || ch >= 0x7F {
			t.Fatalf("unprintable character: %v (%v): %s %v", ch, input, sql, []byte(sql))
		}
	}
	stmts, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("%s: expected success, but found %s", sql, err)
	}
	stmt := stmts.String()
	if sql != stmt {
		t.Fatalf("expected %s, but found %s", sql, stmt)
	}
	return stmt
}

func BenchmarkEncodeSQLString(b *testing.B) {
	str := strings.Repeat("foo", 10000)
	for i := 0; i < b.N; i++ {
		lexbase.EncodeSQLStringWithFlags(bytes.NewBuffer(nil), str, lexbase.EncBareStrings)
	}
}

func BenchmarkEncodeNonASCIISQLString(b *testing.B) {
	builder := strings.Builder{}
	for r := rune(0); r < 10000; r++ {
		builder.WriteRune(r)
	}
	str := builder.String()
	for i := 0; i < b.N; i++ {
		lexbase.EncodeSQLStringWithFlags(bytes.NewBuffer(nil), str, lexbase.EncBareStrings)
	}
}
