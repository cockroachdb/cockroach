// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package yacc

import (
	"io/ioutil"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	// Needed for the -verbosity flag on circleci tests.
	_ "github.com/cockroachdb/cockroach/pkg/util/log"
)

var sqlYPath string

func init() {
	if bazel.BuiltWithBazel() {
		runfile, err := bazel.Runfile("pkg/sql/parser/sql.y")
		if err != nil {
			panic(err)
		}
		sqlYPath = runfile
	} else {
		sqlYPath = "../../../sql/parser/sql.y"
	}
}

func TestLex(t *testing.T) {
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
	b, err := ioutil.ReadFile(sqlYPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Parse(sqlYPath, string(b))
	if err != nil {
		t.Fatal(err)
	}
}
