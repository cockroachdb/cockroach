// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

import "testing"

func BenchmarkCountWhitespace(b *testing.B) {
	var buf [8 << 10]byte
	for _, tc := range inputs {
		r := fixture(b, tc.path)
		b.Run(tc.path, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(r.Size())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.Seek(0, 0); err != nil {
					b.Fatal(err)
				}
				br := byteReader{
					data: buf[:0],
					r:    r,
				}
				got := countWhitespace(&br)
				if got != tc.whitespace {
					b.Fatalf("expected: %v, got: %v", tc.whitespace, got)
				}
			}
		})
	}
}

func countWhitespace(br *byteReader) int {
	n := 0
	w := br.window(0)
	for {
		for _, c := range w {
			if whitespace[c] {
				n++
			}
		}
		br.release(len(w))
		if br.extend() == 0 {
			return n
		}
		w = br.window(0)
	}
}
