// Copyright 2016 The Cockroach Authors.
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
//
// Author: Matt Jibson

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDecodeCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		in     string
		expect string
		err    bool
	}{
		{
			in:     `new\nline`,
			expect: "new\nline",
		},
		{
			in:     `\b\f\n\r\t\v\\`,
			expect: "\b\f\n\r\t\v\\",
		},
		{
			in:     `\0\12\123`,
			expect: "\000\012\123",
		},
		{
			in:     `\x1\xaf`,
			expect: "\x01\xaf",
		},
		{
			in:     `T\n\07\xEV\x0fA\xb2C\1`,
			expect: "T\n\007\x0eV\x0fA\xb2C\001",
		},

		// Error cases.

		{
			in:  `\x`,
			err: true,
		},
		{
			in:  `\xg`,
			err: true,
		},
		{
			in:  `\`,
			err: true,
		},
		{
			in:  `\8`,
			err: true,
		},
		{
			in:  `\a`,
			err: true,
		},
	}

	for _, test := range tests {
		out, err := decodeCopy(test.in)
		if gotErr := err != nil; gotErr != test.err {
			if gotErr {
				t.Errorf("%q: unexpected error: %v", test.in, err)
				continue
			}
			t.Errorf("%q: expected error", test.in)
			continue
		}
		if out != test.expect {
			t.Errorf("%q: got %q, expected %q", test.in, out, test.expect)
		}
	}
}
