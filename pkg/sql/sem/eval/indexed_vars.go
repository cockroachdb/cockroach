// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// IndexedVarContainer extends tree.IndexedVarContainer with the ability to
// evaluate the underlying expression.
type IndexedVarContainer interface {
	tree.IndexedVarContainer

	IndexedVarEval(ctx context.Context, idx int, e tree.ExprEvaluator) (tree.Datum, error)
}
