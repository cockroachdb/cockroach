// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This is a fork of pkg/json package.

// Copyright (c) 2020, Dave Cheney <dave@cheney.net>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//   - Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//   - Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package tokenizer

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

var inputs = []struct {
	path       string
	tokens     int // decoded tokens
	alltokens  int // raw tokens, includes : and ,
	whitespace int // number of whitespace chars
}{
	// from https://github.com/miloyip/nativejson-benchmark
	{"canada", 223236, 334373, 33},
	{"citm_catalog", 85035, 135990, 1227563},
	{"twitter", 29573, 55263, 167931},
	{"code", 217707, 396293, 3},

	// from https://raw.githubusercontent.com/mailru/easyjson/master/benchmark/example.json
	{"example", 710, 1297, 4246},

	// from https://github.com/ultrajson/ultrajson/blob/master/tests/sample.json
	{"sample", 5276, 8677, 518549},
}

func BenchmarkScanner(b *testing.B) {
	for _, tc := range inputs {
		r := fixture(b, tc.path)
		data, err := io.ReadAll(r)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(tc.path, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(r.Size())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.Seek(0, 0); err != nil {
					b.Fatal(err)
				}
				sc := &Scanner{data: data}
				n := 0
				for len(sc.Next()) > 0 {
					n++
				}
				if n != tc.alltokens {
					b.Fatalf("expected %v tokens, got %v", tc.alltokens, n)
				}

			}
		})
	}
}

func BenchmarkDecoderNextToken(b *testing.B) {
	for _, tc := range inputs {
		r := fixture(b, tc.path)
		data, err := io.ReadAll(r)
		if err != nil {
			b.Fatal(err)
		}
		b.Run("pkgjson/"+tc.path, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(r.Size())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.Seek(0, 0); err != nil {
					b.Fatal(err)
				}
				dec := MakeDecoder(data)
				n := 0
				for {
					_, err := dec.NextToken()
					if err == io.EOF {
						break
					}
					check(b, err)
					n++
				}
				if n != tc.tokens {
					b.Fatalf("expected %v tokens, got %v", tc.tokens, n)
				}
			}
		})
		b.Run("encodingjson/"+tc.path, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(r.Size())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.Seek(0, 0); err != nil {
					b.Fatal(err)
				}
				dec := json.NewDecoder(r)
				n := 0
				for {
					_, err := dec.Token()
					if err == io.EOF {
						break
					}
					check(b, err)
					n++
				}
				if n != tc.tokens {
					b.Fatalf("expected %v tokens, got %v", tc.tokens, n)
				}
			}
		})
	}
}

// fuxture returns a *bytes.Reader for the contents of path.
func fixture(tb testing.TB, path string) *bytes.Reader {
	f, err := os.Open(filepath.Join("testdata", path+".json.gz"))
	check(tb, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	check(tb, err)
	buf, err := io.ReadAll(gz)
	check(tb, err)
	return bytes.NewReader(buf)
}

func check(tb testing.TB, err error) {
	if err != nil {
		tb.Helper()
		tb.Fatal(err)
	}
}
