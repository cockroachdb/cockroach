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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestSupportedSQLTypesIntegration tests that all SQL types supported by the
// vectorized engine are "actually supported." For each type, it creates a bunch
// of rows consisting of a single datum (possibly null), converts them into
// column batches, serializes and then deserializes these batches, and finally
// converts the deserialized batches back to rows which are compared with the
// original rows.
func TestSupportedSQLTypesIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: diskMonitor,
		},
	}

	var da sqlbase.DatumAlloc
	rng, _ := randutil.NewPseudoRand()

	for _, typ := range allSupportedSQLTypes {
		if typ.Equal(*types.Decimal) {
			// Serialization of Decimals is currently not supported.
			// TODO(yuzefovich): remove this once it is supported.
			continue
		}
		for _, numRows := range []uint16{
			// A few interesting sizes.
			1,
			coldata.BatchSize() - 1,
			coldata.BatchSize(),
			coldata.BatchSize() + 1,
		} {
			rows := make(sqlbase.EncDatumRows, numRows)
			for i := uint16(0); i < numRows; i++ {
				rows[i] = make(sqlbase.EncDatumRow, 1)
				rows[i][0] = sqlbase.DatumToEncDatum(&typ, sqlbase.RandDatum(rng, &typ, true /* nullOk */))
			}
			typs := []types.T{typ}
			source := execinfra.NewRepeatableRowSource(typs, rows)

			columnarizer, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0 /* processorID */, source)
			require.NoError(t, err)

			coltyps, err := typeconv.FromColumnTypes(typs)
			require.NoError(t, err)
			c, err := colserde.NewArrowBatchConverter(coltyps)
			require.NoError(t, err)
			r, err := colserde.NewRecordBatchSerializer(coltyps)
			require.NoError(t, err)
			arrowOp := newArrowTestOperator(columnarizer, c, r)

			output := distsqlutils.NewRowBuffer(typs, nil /* rows */, distsqlutils.RowBufferArgs{})
			materializer, err := NewMaterializer(
				flowCtx,
				1, /* processorID */
				arrowOp,
				typs,
				&execinfrapb.PostProcessSpec{},
				output,
				nil, /* metadataSourcesQueue */
				nil, /* outputStatsToTrace */
				nil, /* cancelFlow */
			)
			require.NoError(t, err)

			materializer.Start(ctx)
			materializer.Run(ctx)
			actualRows := output.GetRowsNoMeta(t)
			require.Equal(t, len(rows), len(actualRows))
			for rowIdx, expectedRow := range rows {
				require.Equal(t, len(expectedRow), len(actualRows[rowIdx]))
				cmp, err := expectedRow[0].Compare(&typ, &da, &evalCtx, &actualRows[rowIdx][0])
				require.NoError(t, err)
				require.Equal(t, 0, cmp)
			}
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
	OneInputNode

	c *colserde.ArrowBatchConverter
	r *colserde.RecordBatchSerializer
}

var _ Operator = &arrowTestOperator{}

func newArrowTestOperator(
	input Operator, c *colserde.ArrowBatchConverter, r *colserde.RecordBatchSerializer,
) Operator {
	return &arrowTestOperator{
		OneInputNode: NewOneInputNode(input),
		c:            c,
		r:            r,
	}
}

func (a *arrowTestOperator) Init() {
	a.input.Init()
}

func (a *arrowTestOperator) Next(ctx context.Context) coldata.Batch {
	batchIn := a.input.Next(ctx)
	// Note that we don't need to handle zero-length batches in a special way.
	var buf bytes.Buffer
	arrowDataIn, err := a.c.BatchToArrow(batchIn)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	_, _, err = a.r.Serialize(&buf, arrowDataIn)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	var arrowDataOut []*array.Data
	if err := a.r.Deserialize(&arrowDataOut, buf.Bytes()); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	batchOut := testAllocator.NewMemBatchWithSize(nil, 0)
	if err := a.c.ArrowToBatch(arrowDataOut, batchOut); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return batchOut
}
