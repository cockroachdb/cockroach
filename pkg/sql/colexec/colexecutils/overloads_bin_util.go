// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// BinaryOverloadHelper is a utility struct used for templates of the binary
// overloads that fall back to the row-based tree.Datum computation.
//
// In order for the templates to see it correctly, a local variable named
// `_overloadHelper` of this type must be declared before the inlined
// overloaded code.
type BinaryOverloadHelper struct {
	BinOp   tree.BinaryEvalOp
	EvalCtx *eval.Context
}
