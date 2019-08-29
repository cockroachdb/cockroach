// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build gofuzz

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
