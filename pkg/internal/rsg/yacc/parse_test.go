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
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package yacc

import (
	"io/ioutil"
	"testing"

	// Needed for the -verbosity flag on circleci tests.
	_ "github.com/cockroachdb/cockroach/pkg/util/log"
)

// TODO(mjibson): unskip these tests when we move to TeamCity (they fail on CircleCI)

const sqlYPath = "../../../sql/parser/sql.y"

func TestLex(t *testing.T) {
	t.Skip("broken on CircleCI")

	b, err := ioutil.ReadFile(sqlYPath)
	if err != nil {
		t.Fatal(err)
	}
	l := lex(sqlYPath, string(b))
Loop:
	for {
		item := l.nextItem()
		switch item.typ {
		case itemEOF:
			break Loop
		case itemError:
			t.Fatalf("%s:%d: %s", sqlYPath, l.lineNumber(), item)
		}
	}
}

func TestParse(t *testing.T) {
	t.Skip("broken on CircleCI")

	b, err := ioutil.ReadFile(sqlYPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Parse(sqlYPath, string(b))
	if err != nil {
		t.Fatal(err)
	}
}
