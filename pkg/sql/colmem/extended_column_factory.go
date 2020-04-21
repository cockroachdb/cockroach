package colmem

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type extendedColumnFactory struct {
	evalCtx *tree.EvalContext
}

var _ coldata.ColumnFactory = &extendedColumnFactory{}

func NewExtendedColumnFactory(evalCtx *tree.EvalContext) coldata.ColumnFactory {
	return &extendedColumnFactory{evalCtx: evalCtx}
}

func (cf *extendedColumnFactory) ConstructColumn(t coltypes.T, n int) coldata.Column {
	if t == coltypes.Datum {
		return NewDatumVec(n, cf.evalCtx)
	}
	return coldata.StandardVectorizedColumnFactory.ConstructColumn(t, n)
}
