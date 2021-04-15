// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import (
	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// OverloadHelper is a utility struct used for templates that helps us avoid
// allocations of temporary decimals on every overloaded operation with them as
// well as plumbs through other useful information.
//
// In order for the templates to see it correctly, a local variable named
// `_overloadHelper` of this type must be declared before the inlined
// overloaded code.
type OverloadHelper struct {
	TmpDec1, TmpDec2 apd.Decimal
	BinFn            tree.TwoArgFn
	EvalCtx          *tree.EvalContext
	ByteScratch      []byte
}
