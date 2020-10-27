// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lex_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
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
	for i := 0; i < b.N; i++ {
		lex.EncodeSQLStringWithFlags(bytes.NewBuffer(nil), str, lexbase.EncBareStrings)
	}
}

func TestByteArrayDecoding(t *testing.T) {
	const (
		fmtHex = sessiondatapb.BytesEncodeHex
		fmtEsc = sessiondatapb.BytesEncodeEscape
		fmtB64 = sessiondatapb.BytesEncodeBase64
	)
	testData := []struct {
		in    string
		auto  bool
		inFmt sessiondatapb.BytesEncodeFormat
		out   string
		err   string
	}{
		{`a`, false, fmtHex, "", "encoding/hex: odd length hex string"},
		{`aa`, false, fmtHex, "\xaa", ""},
		{`aA`, false, fmtHex, "\xaa", ""},
		{`AA`, false, fmtHex, "\xaa", ""},
		{`x0`, false, fmtHex, "", "encoding/hex: invalid byte: U+0078 'x'"},
		{`a\nbcd`, false, fmtEsc, "", "invalid bytea escape sequence"},
		{`a\'bcd`, false, fmtEsc, "", "invalid bytea escape sequence"},
		{`a\00`, false, fmtEsc, "", "bytea encoded value ends with incomplete escape sequence"},
		{`a\099`, false, fmtEsc, "", "invalid bytea escape sequence"},
		{`a\400`, false, fmtEsc, "", "invalid bytea escape sequence"},
		{`a\777`, false, fmtEsc, "", "invalid bytea escape sequence"},
		{`a'b`, false, fmtEsc, "a'b", ""},
		{`a''b`, false, fmtEsc, "a''b", ""},
		{`a\\b`, false, fmtEsc, "a\\b", ""},
		{`a\000b`, false, fmtEsc, "a\x00b", ""},
		{"a\nb", false, fmtEsc, "a\nb", ""},
		{`a`, false, fmtB64, "", "illegal base64 data at input byte 0"},
		{`aa=`, false, fmtB64, "", "illegal base64 data at input byte 3"},
		{`AA==`, false, fmtB64, "\x00", ""},
		{`/w==`, false, fmtB64, "\xff", ""},
		{`AAAA`, false, fmtB64, "\x00\x00\x00", ""},
		{`a`, true, 0, "a", ""},
		{`\x`, true, 0, "", ""},
		{`\xx`, true, 0, "", "encoding/hex: invalid byte: U+0078 'x'"},
		{`\x6162`, true, 0, "ab", ""},
		{`\\x6162`, true, 0, "\\x6162", ""},
	}
	for _, s := range testData {
		t.Run(fmt.Sprintf("%s:%s", s.in, s.inFmt), func(t *testing.T) {
			var dec []byte
			var err error
			if s.auto {
				dec, err = lex.DecodeRawBytesToByteArrayAuto([]byte(s.in))
			} else {
				dec, err = lex.DecodeRawBytesToByteArray(s.in, s.inFmt)
			}
			if s.err != "" {
				if err == nil {
					t.Fatalf("expected err %q, got no error", s.err)
				}
				if s.err != err.Error() {
					t.Fatalf("expected err %q, got %q", s.err, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(dec) != s.out {
				t.Fatalf("expected %q, got %q", s.out, dec)
			}
		})
	}
}

func TestByteArrayEncoding(t *testing.T) {
	testData := []struct {
		in  string
		out []string
	}{
		// The reference values were gathered from PostgreSQL.
		{"", []string{`\x`, ``, ``}},
		{"abc", []string{`\x616263`, `abc`, `YWJj`}},
		{"a\nb", []string{`\x610a62`, `a\012b`, `YQpi`}},
		{`a\nb`, []string{`\x615c6e62`, `a\\nb`, `YVxuYg==`}},
		{"a'b", []string{`\x612762`, `a'b`, `YSdi`}},
		{"a\"b", []string{`\x612262`, `a"b`, `YSJi`}},
		{"a\x00b", []string{`\x610062`, `a\000b`, `YQBi`}},
	}

	for _, s := range testData {
		t.Run(s.in, func(t *testing.T) {
			for _, format := range []sessiondatapb.BytesEncodeFormat{
				sessiondatapb.BytesEncodeHex,
				sessiondatapb.BytesEncodeEscape,
				sessiondatapb.BytesEncodeBase64,
			} {
				t.Run(format.String(), func(t *testing.T) {
					enc := lex.EncodeByteArrayToRawBytes(s.in, format, false)

					expEnc := s.out[int(format)]
					if enc != expEnc {
						t.Fatalf("encoded %q, expected %q", enc, expEnc)
					}

					if format == sessiondatapb.BytesEncodeHex {
						// Check that the \x also can be skipped.
						enc2 := lex.EncodeByteArrayToRawBytes(s.in, format, true)
						if enc[2:] != enc2 {
							t.Fatal("can't skip prefix")
						}
						enc = enc[2:]
					}

					dec, err := lex.DecodeRawBytesToByteArray(enc, format)
					if err != nil {
						t.Fatal(err)
					}
					if string(dec) != s.in {
						t.Fatalf("decoded %q, expected %q", string(dec), s.in)
					}
				})
			}
		})
	}
}
