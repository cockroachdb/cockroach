// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

// runProcessorTest instantiates a processor with the provided spec, runs it
// with the given inputs, and asserts that the outputted rows are as expected.
func runProcessorTest(
	t *testing.T,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	inputTypes []*types.T,
	inputRows sqlbase.EncDatumRows,
	outputTypes []*types.T,
	expected sqlbase.EncDatumRows,
	txn *kv.Txn,
) {
	in := distsqlutils.NewRowBuffer(inputTypes, inputRows, distsqlutils.RowBufferArgs{})
	out := &distsqlutils.RowBuffer{}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
		Txn:     txn,
	}

	p, err := NewProcessor(
		context.Background(), &flowCtx, 0 /* processorID */, &core, &post,
		[]execinfra.RowSource{in}, []execinfra.RowReceiver{out}, []execinfra.LocalProcessor{})
	if err != nil {
		t.Fatal(err)
	}

	switch pt := p.(type) {
	case *joinReader:
		// Reduce batch size to exercise batching logic.
		pt.SetBatchSizeBytes(2 * int64(inputRows[0].Size()))
	case *indexJoiner:
		//	Reduce batch size to exercise batching logic.
		pt.SetBatchSize(2 /* batchSize */)
	}

	p.Run(context.Background())
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}
	var res sqlbase.EncDatumRows
	for {
		row := out.NextNoMeta(t).Copy()
		if row == nil {
			break
		}
		res = append(res, row)
	}

	if result := res.String(outputTypes); result != expected.String(outputTypes) {
		t.Errorf(
			"invalid results: %s, expected %s'", result, expected.String(outputTypes))
	}
}

type rowsAccessor interface {
	getRows() *rowcontainer.DiskBackedRowContainer
}

func (s *sorterBase) getRows() *rowcontainer.DiskBackedRowContainer {
	return s.rows.(*rowcontainer.DiskBackedRowContainer)
}

type rowGeneratingSource struct {
	types              []*types.T
	fn                 sqlutils.GenRowFn
	scratchEncDatumRow sqlbase.EncDatumRow

	rowIdx  int
	maxRows int
}

// newRowGeneratingSource creates a new rowGeneratingSource with the given fn
// and a maximum number of rows to generate. Can be reset using Reset.
func newRowGeneratingSource(
	types []*types.T, fn sqlutils.GenRowFn, maxRows int,
) *rowGeneratingSource {
	return &rowGeneratingSource{types: types, fn: fn, rowIdx: 1, maxRows: maxRows}
}

func (r *rowGeneratingSource) OutputTypes() []*types.T { return r.types }

func (r *rowGeneratingSource) Start(ctx context.Context) context.Context { return ctx }

func (r *rowGeneratingSource) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if r.rowIdx > r.maxRows {
		// Done.
		return nil, nil
	}

	datumRow := r.fn(r.rowIdx)
	if cap(r.scratchEncDatumRow) < len(datumRow) {
		r.scratchEncDatumRow = make(sqlbase.EncDatumRow, len(datumRow))
	} else {
		r.scratchEncDatumRow = r.scratchEncDatumRow[:len(datumRow)]
	}

	for i := range r.scratchEncDatumRow {
		r.scratchEncDatumRow[i] = sqlbase.DatumToEncDatum(r.types[i], datumRow[i])
	}
	r.rowIdx++
	return r.scratchEncDatumRow, nil
}

// Reset resets this rowGeneratingSource so that the next rowIdx passed to the
// generating function is 1.
func (r *rowGeneratingSource) Reset() {
	r.rowIdx = 1
}

func (r *rowGeneratingSource) ConsumerDone() {}

func (r *rowGeneratingSource) ConsumerClosed() {}

// rowDisposer is a RowReceiver that discards any rows Push()ed.
type rowDisposer struct {
	bufferedMeta    []execinfrapb.ProducerMetadata
	numRowsDisposed int
}

var _ execinfra.RowReceiver = &rowDisposer{}

// Push is part of the distsql.RowReceiver interface.
func (r *rowDisposer) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if row != nil {
		r.numRowsDisposed++
	} else if meta != nil {
		r.bufferedMeta = append(r.bufferedMeta, *meta)
	}
	return execinfra.NeedMoreRows
}

// ProducerDone is part of the RowReceiver interface.
func (r *rowDisposer) ProducerDone() {}

// Types is part of the RowReceiver interface.
func (r *rowDisposer) Types() []*types.T {
	return nil
}

func (r *rowDisposer) ResetNumRowsDisposed() {
	r.numRowsDisposed = 0
}

func (r *rowDisposer) NumRowsDisposed() int {
	return r.numRowsDisposed
}

func (r *rowDisposer) DrainMeta(context.Context) []execinfrapb.ProducerMetadata {
	return r.bufferedMeta
}
