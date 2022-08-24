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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
)

func TestByteArrayDecoding(t *testing.T) {
	const (
		fmtHex = lex.BytesEncodeHex
		fmtEsc = lex.BytesEncodeEscape
		fmtB64 = lex.BytesEncodeBase64
	)
	testData := []struct {
		in    string
		auto  bool
		inFmt lex.BytesEncodeFormat
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
			for _, format := range []lex.BytesEncodeFormat{
				lex.BytesEncodeHex,
				lex.BytesEncodeEscape,
				lex.BytesEncodeBase64,
			} {
				t.Run(format.String(), func(t *testing.T) {
					enc := lex.EncodeByteArrayToRawBytes(s.in, format, false)

					expEnc := s.out[int(format)]
					if enc != expEnc {
						t.Fatalf("encoded %q, expected %q", enc, expEnc)
					}

					if format == lex.BytesEncodeHex {
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
