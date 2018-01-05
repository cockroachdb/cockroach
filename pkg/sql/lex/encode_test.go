// Copyright 2017 The Cockroach Authors.
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

package lex_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func TestEncodeSQLBytes(t *testing.T) {
	testEncodeSQL(t, lex.EncodeSQLBytes, false)
}

func TestEncodeSQLString(t *testing.T) {
	testEncodeSQL(t, lex.EncodeSQLString, true)
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
		testEncodeString(t, tc, lex.EncodeSQLString)
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
	b.Run("old version", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			lex.EncodeSQLStringWithFlags(bytes.NewBuffer(nil), str, lex.EncBareStrings)
		}
	})
	b.Run("new version", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			lex.EncodeSQLStringInsideArray(bytes.NewBuffer(nil), str)
		}
	})
}
