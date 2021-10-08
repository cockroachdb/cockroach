// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestSQLTypesIntegration tests that all SQL types are supported by the
// vectorized engine. For each type, it creates a bunch of rows consisting of a
// single datum (possibly null), converts them into column batches, serializes
// and then deserializes these batches, and finally converts the deserialized
// batches back to rows which are compared with the original rows.
func TestSQLTypesIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: diskMonitor,
	}

	var da rowenc.DatumAlloc
	rng, _ := randutil.NewPseudoRand()
	typesToTest := 20

	for i := 0; i < typesToTest; i++ {
		typ := randgen.RandType(rng)
		for _, numRows := range []int{
			// A few interesting sizes.
			1,
			coldata.BatchSize() - 1,
			coldata.BatchSize(),
			coldata.BatchSize() + 1,
		} {
			rows := make(rowenc.EncDatumRows, numRows)
			for i := 0; i < numRows; i++ {
				rows[i] = make(rowenc.EncDatumRow, 1)
				rows[i][0] = rowenc.DatumToEncDatum(typ, randgen.RandDatum(rng, typ, true /* nullOk */))
			}
			typs := []*types.T{typ}
			source := execinfra.NewRepeatableRowSource(typs, rows)

			columnarizer := NewBufferingColumnarizer(testAllocator, flowCtx, 0 /* processorID */, source)

			c, err := colserde.NewArrowBatchConverter(typs)
			require.NoError(t, err)
			r, err := colserde.NewRecordBatchSerializer(typs)
			require.NoError(t, err)
			arrowOp := newArrowTestOperator(columnarizer, c, r, typs)

			materializer := NewMaterializer(
				flowCtx,
				1, /* processorID */
				colexecargs.OpWithMetaInfo{Root: arrowOp},
				typs,
			)

			materializer.Start(ctx)
			numActualRows := 0
			for _, expectedRow := range rows {
				actualRow, meta := materializer.Next()
				require.Nil(t, meta)
				numActualRows++
				require.Equal(t, len(expectedRow), len(actualRow))
				cmp, err := expectedRow[0].Compare(typ, &da, &evalCtx, &actualRow[0])
				require.NoError(t, err)
				require.Equal(t, 0, cmp)
			}
			require.Equal(t, len(rows), numActualRows)
		}
	}
}

// arrowTestOperator is an Operator that takes in a coldata.Batch from its
// input, passes it through a chain of
// - converting to Arrow format
// - serializing
// - deserializing
// - converting from Arrow format
// and returns the resulting batch.
type arrowTestOperator struct {
	colexecop.OneInputHelper

	c *colserde.ArrowBatchConverter
	r *colserde.RecordBatchSerializer

	typs []*types.T
}

var _ colexecop.Operator = &arrowTestOperator{}

func newArrowTestOperator(
	input colexecop.Operator,
	c *colserde.ArrowBatchConverter,
	r *colserde.RecordBatchSerializer,
	typs []*types.T,
) colexecop.Operator {
	return &arrowTestOperator{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		c:              c,
		r:              r,
		typs:           typs,
	}
}

func (a *arrowTestOperator) Next() coldata.Batch {
	batchIn := a.Input.Next()
	// Note that we don't need to handle zero-length batches in a special way.
	var buf bytes.Buffer
	arrowDataIn, err := a.c.BatchToArrow(batchIn)
	if err != nil {
		colexecerror.InternalError(err)
	}
	_, _, err = a.r.Serialize(&buf, arrowDataIn, batchIn.Length())
	if err != nil {
		colexecerror.InternalError(err)
	}
	var arrowDataOut []*array.Data
	batchLength, err := a.r.Deserialize(&arrowDataOut, buf.Bytes())
	if err != nil {
		colexecerror.InternalError(err)
	}
	batchOut := testAllocator.NewMemBatchWithFixedCapacity(a.typs, batchLength)
	if err := a.c.ArrowToBatch(arrowDataOut, batchLength, batchOut); err != nil {
		colexecerror.InternalError(err)
	}
	return batchOut
}
