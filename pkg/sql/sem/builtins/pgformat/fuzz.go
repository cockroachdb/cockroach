// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build gofuzz
// +build gofuzz

package pgformat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// FuzzFormat passes the input to pgformat.Format()
// as both the format string and format arguments.
func FuzzFormat(input []byte) int {
	evalCtx := eval.MakeTestingEvalContext(nil)
	str := string(input)
	args := make(tree.Datums, 16)
	for i := range args {
		args[i] = tree.NewDString(string(input))
	}
	_, err := Format(context.Background(), &evalCtx, str, args...)

	if err == nil {
		return 0
	}
	return 1
}
