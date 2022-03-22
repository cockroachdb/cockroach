// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecbase

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// BinaryOverloadHelper is a utility struct used for templates of the binary
// overloads that fall back to the row-based tree.Datum computation.
//
// In order for the templates to see it correctly, a local variable named
// `_overloadHelper` of this type must be declared before the inlined
// overloaded code.
type BinaryOverloadHelper struct {
	BinFn   tree.TwoArgFn
	EvalCtx *tree.EvalContext
}
