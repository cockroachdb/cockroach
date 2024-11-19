// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// runProcessorTest instantiates a processor with the provided spec, runs it
// with the given inputs, and asserts that the outputted rows are as expected.
func runProcessorTest(
	t *testing.T,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	inputTypes []*types.T,
	inputRows rowenc.EncDatumRows,
	outputTypes []*types.T,
	expected rowenc.EncDatumRows,
	txn *kv.Txn,
	stopper *stop.Stopper,
	distSender *kvcoord.DistSender,
) {
	in := distsqlutils.NewRowBuffer(inputTypes, inputRows, distsqlutils.RowBufferArgs{})
	out := &distsqlutils.RowBuffer{}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st, Stopper: stopper, DistSender: distSender},
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Txn:     txn,
	}

	p, err := NewProcessor(
		context.Background(), &flowCtx, 0 /* processorID */, &core, &post,
		[]execinfra.RowSource{in}, []execinfra.LocalProcessor{})
	if err != nil {
		t.Fatal(err)
	}

	switch pt := p.(type) {
	case *joinReader:
		// Reduce batch size to exercise batching logic.
		pt.SetBatchSizeBytes(2 * int64(inputRows[0].Size()))
	}

	p.Run(context.Background(), out)
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}
	var res rowenc.EncDatumRows
	for {
		row, meta := out.Next()
		if meta != nil && meta.Metrics == nil {
			t.Fatalf("unexpected metadata %+v", meta)
		}
		row = row.Copy()
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
	scratchEncDatumRow rowenc.EncDatumRow

	rowIdx  int
	maxRows int
}

var _ execinfra.RowSource = &rowGeneratingSource{}

// newRowGeneratingSource creates a new rowGeneratingSource with the given fn
// and a maximum number of rows to generate. Can be reset using Reset.
func newRowGeneratingSource(
	types []*types.T, fn sqlutils.GenRowFn, maxRows int,
) *rowGeneratingSource {
	return &rowGeneratingSource{types: types, fn: fn, rowIdx: 1, maxRows: maxRows}
}

func (r *rowGeneratingSource) OutputTypes() []*types.T { return r.types }

func (r *rowGeneratingSource) Start(context.Context) {}

func (r *rowGeneratingSource) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if r.rowIdx > r.maxRows {
		// Done.
		return nil, nil
	}

	datumRow := r.fn(r.rowIdx)
	if cap(r.scratchEncDatumRow) < len(datumRow) {
		r.scratchEncDatumRow = make(rowenc.EncDatumRow, len(datumRow))
	} else {
		r.scratchEncDatumRow = r.scratchEncDatumRow[:len(datumRow)]
	}

	for i := range r.scratchEncDatumRow {
		r.scratchEncDatumRow[i] = rowenc.DatumToEncDatum(r.types[i], datumRow[i])
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
	logRows         bool
}

var _ execinfra.RowReceiver = &rowDisposer{}

// Push is part of the execinfra.RowReceiver interface.
func (r *rowDisposer) Push(
	row rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	if row != nil {
		if r.logRows {
			fmt.Printf("row #%d : %v\n", r.numRowsDisposed, row)
		}
		r.numRowsDisposed++
	} else if meta != nil {
		r.bufferedMeta = append(r.bufferedMeta, *meta)
	}
	return execinfra.NeedMoreRows
}

// ProducerDone is part of the execinfra.RowReceiver interface.
func (r *rowDisposer) ProducerDone() {}

func (r *rowDisposer) ResetNumRowsDisposed() {
	r.numRowsDisposed = 0
}

func (r *rowDisposer) NumRowsDisposed() int {
	return r.numRowsDisposed
}

// makeFetchSpec creates an IndexFetchSpec for the given index, with the columns
// specified by name, separated by a comma. For example:
//
//	makeFetchSpec(t, table, "idx_c", "a,b,c")
func makeFetchSpec(
	t testing.TB, table catalog.TableDescriptor, indexName string, colNames string,
) fetchpb.IndexFetchSpec {
	index, err := catalog.MustFindIndexByName(table, indexName)
	if err != nil {
		t.Fatal(err)
	}
	var colIDs []descpb.ColumnID
	if colNames != "" {
		for _, col := range strings.Split(colNames, ",") {
			col, err := catalog.MustFindColumnByName(table, col)
			if err != nil {
				t.Fatal(err)
			}
			colIDs = append(colIDs, col.GetID())
		}
	}
	var fetchSpec fetchpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(&fetchSpec, keys.SystemSQLCodec, table, index, colIDs); err != nil {
		t.Fatal(err)
	}
	return fetchSpec
}
