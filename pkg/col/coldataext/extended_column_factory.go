// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package coldataext

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// extendedColumnFactory stores an evalCtx which can be used to construct
// datumVec later. This is to prevent plumbing evalCtx to all vectorized
// operators as well as avoiding introducing dependency from coldata on tree
// package.
type extendedColumnFactory struct {
	evalCtx *eval.Context
}

var _ coldata.ColumnFactory = &extendedColumnFactory{}

// NewExtendedColumnFactory returns an extendedColumnFactory instance.
func NewExtendedColumnFactory(evalCtx *eval.Context) coldata.ColumnFactory {
	return &extendedColumnFactory{evalCtx: evalCtx}
}

// NewExtendedColumnFactoryNoEvalCtx returns an extendedColumnFactory that will
// be producing coldata.DatumVecs that aren't fully initialized - the eval
// context is not set on those vectors. This can be acceptable if the caller
// cannot provide the eval.Context but also doesn't intend to compare datums.
func NewExtendedColumnFactoryNoEvalCtx() coldata.ColumnFactory {
	return &extendedColumnFactory{}
}

func (cf *extendedColumnFactory) MakeColumn(t *types.T, n int) coldata.Column {
	if typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) == typeconv.DatumVecCanonicalTypeFamily {
		return newDatumVec(t, n, cf.evalCtx)
	}
	return coldata.StandardColumnFactory.MakeColumn(t, n)
}
