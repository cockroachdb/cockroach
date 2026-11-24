// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build gofuzz

// The parser fuzzer needs to live in its own package because it must import
// the builtins package so functions that are hard coded in sql.y (like
// "current_user") can be resolved. However importing that in parser creates a
// cyclic dependency.

package fuzz

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	// See above comment about why this is imported.
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
)

func FuzzParse(data []byte) int {
	_, err := parser.Parse(string(data))
	if err != nil {
		return 0
	}
	return 1
}
