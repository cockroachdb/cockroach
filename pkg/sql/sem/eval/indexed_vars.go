// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// IndexedVarContainer extends tree.IndexedVarContainer with the ability to
// evaluate the underlying expression.
type IndexedVarContainer interface {
	tree.IndexedVarContainer

	IndexedVarEval(idx int) (tree.Datum, error)
}
