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
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
	"github.com/pkg/errors"
)

// Distinct is the physical processor implementation of the DISTINCT relational operator.
type Distinct struct {
	processorBase

	input            RowSource
	types            []sqlbase.ColumnType
	haveLastGroupKey bool
	lastGroupKey     sqlbase.EncDatumRow
	arena            stringarena.Arena
	seen             map[string]struct{}
	orderedCols      []uint32
	distinctCols     util.FastIntSet
	memAcc           mon.BoundAccount
	datumAlloc       sqlbase.DatumAlloc
	scratch          []byte
}

// SortedDistinct is a specialized distinct that can be used when all of the
// distinct columns are also ordered.
type SortedDistinct struct {
	Distinct
}

var _ Processor = &Distinct{}
var _ RowSource = &Distinct{}

const distinctProcName = "distinct"

var _ Processor = &SortedDistinct{}
var _ RowSource = &SortedDistinct{}

const sortedDistinctProcName = "sorted distinct"

// NewDistinct instantiates a new Distinct processor.
func NewDistinct(
	flowCtx *FlowCtx,
	processorID int32,
	spec *DistinctSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (RowSourcedProcessor, error) {
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

	d := &Distinct{
		input:        input,
		orderedCols:  spec.OrderedColumns,
		distinctCols: distinctCols,
		memAcc:       flowCtx.EvalCtx.Mon.MakeBoundAccount(),
		types:        input.OutputTypes(),
	}

	if err := d.init(
		post, d.types, flowCtx, processorID, output,
		procStateOpts{
			inputsToDrain: []RowSource{d.input},
			trailingMetaCallback: func() []ProducerMetadata {
				d.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	d.lastGroupKey = d.out.rowAlloc.AllocRow(len(d.types))
	d.haveLastGroupKey = false

	if allSorted {
		// We can use the faster sortedDistinct processor.
		return &SortedDistinct{
			Distinct: *d,
		}, nil
	}

	return d, nil
}

// Start is part of the RowSource interface.
func (d *Distinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.startInternal(ctx, distinctProcName)
}

// Run is part of the processor interface.
func (d *Distinct) Run(ctx context.Context, wg *sync.WaitGroup) {
	if d.out.output == nil {
		panic("distinct output not initialized for emitting rows")
	}
	ctx = d.Start(ctx)
	Run(ctx, d, d.out.output)
	if wg != nil {
		wg.Done()
	}
}

// Start is part of the RowSource interface.
func (d *SortedDistinct) Start(ctx context.Context) context.Context {
	d.input.Start(ctx)
	return d.startInternal(ctx, sortedDistinctProcName)
}

// Run is part of the processor interface.
func (d *SortedDistinct) Run(ctx context.Context, wg *sync.WaitGroup) {
	if d.out.output == nil {
		panic("distinct output not initialized for emitting rows")
	}
	ctx = d.Start(ctx)
	Run(ctx, d, d.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (d *Distinct) matchLastGroupKey(row sqlbase.EncDatumRow) (bool, error) {
	if !d.haveLastGroupKey {
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
func (d *Distinct) encode(appendTo []byte, row sqlbase.EncDatumRow) ([]byte, error) {
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

func (d *Distinct) close() {
	// Need to close the mem accounting while the context is still valid.
	d.memAcc.Close(d.ctx)
	d.internalClose()
}

// Next is part of the RowSource interface.
func (d *Distinct) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for d.state == stateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			return nil, meta
		}
		if row == nil {
			d.moveToDraining(nil /* err */)
			break
		}

		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define x to be our group key. Our seen set at any given time
		// is only the set of all rows with the same group key. The encoding of
		// the row is the key we use in our 'seen' set.
		encoding, err := d.encode(d.scratch, row)
		if err != nil {
			d.moveToDraining(err)
			break
		}
		d.scratch = encoding[:0]

		// The 'seen' set is reset whenever we find consecutive rows differing on the
		// group key thus avoiding the need to store encodings of all rows.
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.moveToDraining(err)
			break
		}

		if !matched {
			// Since the sorted distinct columns have changed, we know that all the
			// distinct keys in the 'seen' set will never be seen again. This allows
			// us to keep the current arena block and overwrite strings previously
			// allocated on it, which implies that UnsafeReset() is safe to call here.
			copy(d.lastGroupKey, row)
			d.haveLastGroupKey = true
			if err := d.arena.UnsafeReset(d.ctx); err != nil {
				d.moveToDraining(err)
				break
			}
			d.seen = make(map[string]struct{})
		}

		if len(encoding) > 0 {
			if _, ok := d.seen[string(encoding)]; ok {
				continue
			}
			s, err := d.arena.AllocBytes(d.ctx, encoding)
			if err != nil {
				d.moveToDraining(err)
				break
			}
			d.seen[s] = struct{}{}
		}

		if outRow := d.processRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.drainHelper()
}

// Next is part of the RowSource interface.
//
// sortedDistinct is simpler than distinct. All it has to do is keep track
// of the last row it saw, emitting if the new row is different.
func (d *SortedDistinct) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for d.state == stateRunning {
		row, meta := d.input.Next()
		if meta != nil {
			return nil, meta
		}
		if row == nil {
			d.moveToDraining(nil /* err */)
			break
		}
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.moveToDraining(err)
			break
		}
		if matched {
			continue
		}

		d.haveLastGroupKey = true
		copy(d.lastGroupKey, row)

		if outRow := d.processRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, d.drainHelper()
}

// ConsumerDone is part of the RowSource interface.
func (d *Distinct) ConsumerDone() {
	d.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (d *Distinct) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}
