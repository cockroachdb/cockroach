// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package parser

import "testing"

// TestQualifiedName tests the string representation of QualifiedName.
func TestQualifiedNameString(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{"*", "*"},
		// Keyword.
		{"DATABASE", `"DATABASE"`},
		{"dAtAbAse", `"dAtAbAse"`},
		// Ident format: starts with [a-zA-Z_] or extended ascii,
		// and is then followed by [a-zA-Z0-9$_] or extended ascii.
		{"foo$09", "foo$09"},
		{"_Ab10", "_Ab10"},
		// Everything else quotes the string and escapes '"' and '\\'.
		{".foobar", `".foobar"`},
		{`".foobar"`, `"\".foobar\""`},
		{`\".foobar\"`, `"\\\".foobar\\\""`},
	}

	for _, tc := range testCases {
		q := QualifiedName{tc.in}
		if q.String() != tc.out {
			t.Errorf("expected q.String() == %q, got %q", tc.out, q.String())
		}
	}
}
