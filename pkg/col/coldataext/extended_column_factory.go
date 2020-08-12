// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldataext

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// extendedColumnFactory stores an evalCtx which can be used to construct
// datumVec later. This is to prevent plumbing evalCtx to all vectorized
// operators as well as avoiding introducing dependency from coldata on tree
// package.
type extendedColumnFactory struct {
	evalCtx *tree.EvalContext
}

var _ coldata.ColumnFactory = &extendedColumnFactory{}

// NewExtendedColumnFactory returns an extendedColumnFactory instance.
func NewExtendedColumnFactory(evalCtx *tree.EvalContext) coldata.ColumnFactory {
	return &extendedColumnFactory{evalCtx: evalCtx}
}

func (cf *extendedColumnFactory) MakeColumn(t *types.T, n int) coldata.Column {
	if typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) == typeconv.DatumVecCanonicalTypeFamily {
		return newDatumVec(t, n, cf.evalCtx)
	}
	return coldata.StandardColumnFactory.MakeColumn(t, n)
}
