// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
	"github.com/pkg/errors"
)

type distinct struct {
	processorBase

	evalCtx      *tree.EvalContext
	input        RowSource
	types        []sqlbase.ColumnType
	lastGroupKey sqlbase.EncDatumRow
	arena        stringarena.Arena
	seen         map[string]struct{}
	orderedCols  []uint32
	distinctCols util.FastIntSet
	memAcc       mon.BoundAccount
	datumAlloc   sqlbase.DatumAlloc
	scratch      []byte
}

// sortedDistinct is a specialized distinct that can be used when all of the
// distinct columns are also ordered.
type sortedDistinct struct {
	distinct
}

var _ Processor = &distinct{}
var _ RowSource = &distinct{}

var _ Processor = &sortedDistinct{}
var _ RowSource = &sortedDistinct{}

func newDistinct(
	flowCtx *FlowCtx, spec *DistinctSpec, input RowSource, post *PostProcessSpec, output RowReceiver,
) (Processor, error) {
	if len(spec.DistinctColumns) == 0 {
		return nil, errors.New("programming error: 0 distinct columns specified for distinct processor")
	}

	var distinctCols, orderedCols util.FastIntSet
	allSorted := true

	for _, col := range spec.OrderedColumns {
		orderedCols.Add(int(col))
	}
	for _, col := range spec.DistinctColumns {
		if !orderedCols.Contains(int(col)) {
			allSorted = false
		}
		distinctCols.Add(int(col))
	}

	d := &distinct{
		input:        input,
		orderedCols:  spec.OrderedColumns,
		distinctCols: distinctCols,
		memAcc:       flowCtx.EvalCtx.Mon.MakeBoundAccount(),
		types:        input.OutputTypes(),
	}

	if err := d.init(post, d.types, flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}

	if allSorted {
		// We can use the faster sortedDistinct processor.
		return &sortedDistinct{
			distinct: *d,
		}, nil
	}

	return d, nil
}

// Run is part of the processor interface.
func (d *distinct) Run(wg *sync.WaitGroup) {
	if d.out.output == nil {
		panic("distinct output not initialized for emitting rows")
	}
	Run(d.flowCtx.Ctx, d, d.out.output)
	if wg != nil {
		wg.Done()
	}
}

// Run is part of the processor interface.
func (d *sortedDistinct) Run(wg *sync.WaitGroup) {
	if d.out.output == nil {
		panic("distinct output not initialized for emitting rows")
	}
	Run(d.flowCtx.Ctx, d, d.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (d *distinct) matchLastGroupKey(row sqlbase.EncDatumRow) (bool, error) {
	if d.lastGroupKey == nil {
		return false, nil
	}
	for _, colIdx := range d.orderedCols {
		res, err := d.lastGroupKey[colIdx].Compare(
			&d.types[colIdx], &d.datumAlloc, d.evalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

// encode appends the encoding of non-ordered columns, which we use as a key in
// our 'seen' set.
func (d *distinct) encode(appendTo []byte, row sqlbase.EncDatumRow) ([]byte, error) {
	var err error
	for i, datum := range row {
		// Ignore columns that are not in the distinctCols, as if we are
		// post-processing to strip out column Y, we cannot include it as
		// (X1, Y1) and (X1, Y2) will appear as distinct rows, but if we are
		// stripping out Y, we do not want (X1) and (X1) to be in the results.
		if !d.distinctCols.Contains(i) {
			continue
		}

		// TODO(irfansharif): Different rows may come with different encodings,
		// e.g. if they come from different streams that were merged, in which
		// case the encodings don't match (despite having the same underlying
		// datums). We instead opt to always choose sqlbase.DatumEncoding_ASCENDING_KEY
		// but we may want to check the first row for what encodings are already
		// available.
		appendTo, err = datum.Encode(&d.types[i], &d.datumAlloc, sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
}

func (d *distinct) close() {
	if !d.closed {
		// Need to close the mem accounting while the context is still valid.
		d.memAcc.Close(d.ctx)
	}
	if d.internalClose() {
		d.input.ConsumerClosed()
	}
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (d *distinct) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !d.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(d.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		d.close()
	}
	return meta
}

// Next is part of the RowSource interface.
func (d *distinct) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if d.maybeStart("distinct", "Distinct") {
		d.evalCtx = d.flowCtx.NewEvalCtx()
	}

	if d.closed {
		return nil, d.producerMeta(nil /* err */)
	}

	for {
		row, meta := d.input.Next()
		if meta != nil {
			return nil, meta
		}
		if row == nil {
			return nil, d.producerMeta(nil /* err */)
		}

		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define x to be our group key. Our seen set at any given time
		// is only the set of all rows with the same group key. The encoding of
		// the row is the key we use in our 'seen' set.
		encoding, err := d.encode(d.scratch, row)
		if err != nil {
			return nil, d.producerMeta(err)
		}
		d.scratch = encoding[:0]

		// The 'seen' set is reset whenever we find consecutive rows differing on the
		// group key thus avoiding the need to store encodings of all rows.
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			return nil, d.producerMeta(err)
		}

		if !matched {
			// Since the sorted distinct columns have changed, we know that all the
			// distinct keys in the 'seen' set will never be seen again. This allows
			// us to keep the current arena block and overwrite strings previously
			// allocated on it, which implies that UnsafeReset() is safe to call here.
			d.lastGroupKey = row
			if err := d.arena.UnsafeReset(d.ctx); err != nil {
				return nil, d.producerMeta(err)
			}
			d.seen = make(map[string]struct{})
		}

		if len(encoding) > 0 {
			if _, ok := d.seen[string(encoding)]; ok {
				continue
			}
			s, err := d.arena.AllocBytes(d.ctx, encoding)
			if err != nil {
				return nil, d.producerMeta(err)
			}
			d.seen[s] = struct{}{}
		}

		outRow, status, err := d.out.ProcessRow(d.ctx, row)
		if err != nil {
			return nil, d.producerMeta(err)
		}
		switch status {
		case NeedMoreRows:
			if outRow == nil && err == nil {
				continue
			}
		case DrainRequested:
			d.input.ConsumerDone()
			continue
		}

		return outRow, nil
	}
}

// Next is part of the RowSource interface.
func (d *sortedDistinct) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if d.maybeStart("sortedDistinct", "SortedDistinct") {
		d.evalCtx = d.flowCtx.NewEvalCtx()
	}

	if d.closed {
		return nil, d.producerMeta(nil /* err */)
	}
	for {
		// sortedDistinct is simpler than distinct. All it has to do is keep track
		// of the last row it saw, emitting if the new row is different.
		row, meta := d.input.Next()
		if d.closed || meta != nil {
			return nil, meta
		}
		if row == nil {
			return nil, d.producerMeta(nil /* err */)
		}
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			return nil, d.producerMeta(err)
		}
		if matched {
			continue
		}

		d.lastGroupKey = row

		outRow, status, err := d.out.ProcessRow(d.ctx, row)
		if err != nil {
			return nil, d.producerMeta(err)
		}
		switch status {
		case NeedMoreRows:
			if outRow == nil && err == nil {
				continue
			}
		case DrainRequested:
			d.input.ConsumerDone()
			continue
		}

		return outRow, nil
	}
}

// ConsumerDone is part of the RowSource interface.
func (d *distinct) ConsumerDone() {
	d.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (d *distinct) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}
