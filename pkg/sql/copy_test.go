// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
