// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build gofuzz

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
