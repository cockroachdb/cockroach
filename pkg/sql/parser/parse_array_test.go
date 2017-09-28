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

package parser

import "testing"

func TestParseArray(t *testing.T) {
	testData := []struct {
		str      string
		typ      ColumnType
		expected Datums
	}{
		{`{}`, intColTypeInt, Datums{}},
		{`{1}`, intColTypeInt, Datums{NewDInt(1)}},
		{`{1,2}`, intColTypeInt, Datums{NewDInt(1), NewDInt(2)}},
		{`   { 1    ,  2  }  `, intColTypeInt, Datums{NewDInt(1), NewDInt(2)}},
		{`   { 1    ,
			"2"  }  `, intColTypeInt, Datums{NewDInt(1), NewDInt(2)}},
		{`{1,2,3}`, intColTypeInt, Datums{NewDInt(1), NewDInt(2), NewDInt(3)}},
		{`{"1"}`, intColTypeInt, Datums{NewDInt(1)}},
		{` { "1" , "2"}`, intColTypeInt, Datums{NewDInt(1), NewDInt(2)}},
		{` { "1" , 2}`, intColTypeInt, Datums{NewDInt(1), NewDInt(2)}},
		{`{1,NULL}`, intColTypeInt, Datums{NewDInt(1), DNull}},

		{`{hello}`, stringColTypeString, Datums{NewDString("hello")}},
		{`{hel
lo}`, stringColTypeString, Datums{NewDString("hel\nlo")}},
		{`{hel,
lo}`, stringColTypeString, Datums{NewDString("hel"), NewDString("lo")}},
		{`{hel,lo}`, stringColTypeString, Datums{NewDString("hel"), NewDString("lo")}},
		{`{  he llo  }`, stringColTypeString, Datums{NewDString("he llo")}},
		{"{hello,\u1680world}", stringColTypeString, Datums{NewDString("hello"), NewDString("world")}},
		{`{hell\\o}`, stringColTypeString, Datums{NewDString("hell\\o")}},
		{`{"hell\\o"}`, stringColTypeString, Datums{NewDString("hell\\o")}},
		{`{NULL,"NULL"}`, stringColTypeString, Datums{DNull, NewDString("NULL")}},
		{`{"hello"}`, stringColTypeString, Datums{NewDString("hello")}},
		{`{" hello "}`, stringColTypeString, Datums{NewDString(" hello ")}},
		{`{"hel,lo"}`, stringColTypeString, Datums{NewDString("hel,lo")}},
		{`{"hel\"lo"}`, stringColTypeString, Datums{NewDString("hel\"lo")}},
		{`{"h\"el\"lo"}`, stringColTypeString, Datums{NewDString("h\"el\"lo")}},
		{`{"hel\nlo"}`, stringColTypeString, Datums{NewDString("helnlo")}},
		{`{"hel\\lo"}`, stringColTypeString, Datums{NewDString("hel\\lo")}},
		{`{"he\,l\}l\{o"}`, stringColTypeString, Datums{NewDString("he,l}l{o")}},

		{`{日本語}`, stringColTypeString, Datums{NewDString("日本語")}},

		// This can generate some strings with invalid UTF-8, but this isn't a
		// problem, since the input would have had to be invalid UTF-8 for that to
		// occur.
		{string([]byte{'{', 'a', 200, '}'}), stringColTypeString, Datums{NewDString("a\xc8")}},
		{string([]byte{'{', 'a', 200, 'a', '}'}), stringColTypeString, Datums{NewDString("a\xc8a")}},
	}
	for _, td := range testData {
		t.Run(td.str, func(t *testing.T) {
			expected := NewDArray(CastTargetToDatumType(td.typ))
			for _, d := range td.expected {
				if err := expected.Append(d); err != nil {
					t.Fatal(err)
				}
			}
			actual, err := ParseDArrayFromString(NewTestingEvalContext(), td.str, td.typ)
			if err != nil {
				t.Fatalf("ARRAY %s: got error %s, expected %s", td.str, err.Error(), expected)
			}
			if actual.Compare(NewTestingEvalContext(), expected) != 0 {
				t.Fatalf("ARRAY %s: got %s, expected %s", td.str, actual, expected)
			}
		})
	}
}

func TestParseArrayError(t *testing.T) {
	testData := []struct {
		str           string
		typ           ColumnType
		expectedError string
	}{
		{"", intColTypeInt, "array must be enclosed in { and }"},
		{"1", intColTypeInt, "array must be enclosed in { and }"},
		{"1,2", intColTypeInt, "array must be enclosed in { and }"},
		{"{1,2", intColTypeInt, "malformed array"},
		{"{1,2,", intColTypeInt, "malformed array"},
		{"{", intColTypeInt, "malformed array"},
		{"{,}", intColTypeInt, "malformed array"},
		{"{}{}", intColTypeInt, "extra text after closing right brace"},
		{"{} {}", intColTypeInt, "extra text after closing right brace"},
		{"{{}}", intColTypeInt, "nested arrays not supported"},
		{"{1, {1}}", intColTypeInt, "nested arrays not supported"},
		{"{hello}", intColTypeInt, `could not parse "hello" as type int: strconv.ParseInt: parsing "hello": invalid syntax`},
		{"{\"hello}", stringColTypeString, `malformed array`},
		// It might be unnecessary to disallow this, but Postgres does.
		{"{he\"lo}", stringColTypeString, "malformed array"},

		{string([]byte{200}), stringColTypeString, "array must be enclosed in { and }"},
		{string([]byte{'{', 'a', 200}), stringColTypeString, "malformed array"},
	}
	for _, td := range testData {
		t.Run(td.str, func(t *testing.T) {
			_, err := ParseDArrayFromString(NewTestingEvalContext(), td.str, td.typ)
			if err == nil {
				t.Fatalf("expected %#v to error with message %#v", td.str, td.expectedError)
			}
			if err.Error() != td.expectedError {
				t.Fatalf("ARRAY %s: got error %s, expected error %s", td.str, err.Error(), td.expectedError)
			}
		})
	}
}
