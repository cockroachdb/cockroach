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

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"golang.org/x/net/context"
)

type distinct struct {
	processorBase

	flowCtx      *FlowCtx
	input        RowSource
	lastGroupKey sqlbase.EncDatumRow
	seen         map[string]struct{}
	orderedCols  map[uint32]struct{}
	distinctCols map[uint32]struct{}
	memAcc       mon.BoundAccount
	datumAlloc   sqlbase.DatumAlloc
}

var _ Processor = &distinct{}

func newDistinct(
	flowCtx *FlowCtx, spec *DistinctSpec, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*distinct, error) {
	d := &distinct{
		flowCtx:      flowCtx,
		input:        input,
		orderedCols:  make(map[uint32]struct{}),
		distinctCols: make(map[uint32]struct{}),
		memAcc:       flowCtx.EvalCtx.Mon.MakeBoundAccount(),
	}
	for _, col := range spec.OrderedColumns {
		d.orderedCols[col] = struct{}{}
	}
	for _, col := range spec.DistinctColumns {
		d.distinctCols[col] = struct{}{}
	}

	if err := d.out.Init(post, input.Types(), &flowCtx.EvalCtx, output); err != nil {
		return nil, err
	}

	return d, nil
}

// Run is part of the processor interface.
func (d *distinct) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	defer d.memAcc.Close(ctx)

	ctx = log.WithLogTag(ctx, "Evaluator", nil)
	ctx, span := processorSpan(ctx, "distinct")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting distinct process")
		defer log.Infof(ctx, "exiting distinct")
	}

	earlyExit, err := d.mainLoop(ctx)
	if err != nil {
		DrainAndClose(ctx, d.out.output, err, d.input)
	} else if !earlyExit {
		sendTraceData(ctx, d.out.output)
		d.input.ConsumerClosed()
		d.out.Close()
	}
}

func (d *distinct) mainLoop(ctx context.Context) (earlyExit bool, _ error) {
	var scratch []byte
	for {
		row, meta := d.input.Next()
		if !meta.Empty() {
			if meta.Err != nil {
				return false, meta.Err
			}
			if !emitHelper(ctx, &d.out, nil /* row */, meta, d.input) {
				// No cleanup required; emitHelper() took care of it.
				return true, nil
			}
			continue
		}
		if row == nil {
			return false, nil
		}

		encoding := scratch
		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define x to be our group key. Our seen set at any given time
		// is only the set of all rows with the same group key. The encoding of
		// the row is the key we use in our 'seen' set.
		var err error
		encoding, err = d.encode(scratch, row)
		if err != nil {
			return false, err
		}
		// The 'seen' set is reset whenever we find consecutive rows differing on the
		// group key thus avoiding the need to store encodings of all rows.
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			return false, err
		}

		if !matched {
			d.lastGroupKey = row
			d.seen = make(map[string]struct{})
			d.memAcc.Clear(ctx)
		}

		if _, ok := d.seen[string(encoding)]; !ok {
			if len(encoding) > 0 {
				if err := d.memAcc.Grow(ctx, int64(len(encoding))); err != nil {
					return false, err
				}
				d.seen[string(encoding)] = struct{}{}
			}
			if !emitHelper(ctx, &d.out, row, ProducerMetadata{}, d.input) {
				// No cleanup required; emitHelper() took care of it.
				return true, nil
			}
			scratch = encoding[:0]
		}
	}
}

func (d *distinct) matchLastGroupKey(row sqlbase.EncDatumRow) (bool, error) {
	if d.lastGroupKey == nil {
		return false, nil
	}
	for colIdx := range d.orderedCols {
		res, err := d.lastGroupKey[colIdx].Compare(&d.datumAlloc, &d.flowCtx.EvalCtx, &row[colIdx])
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
		if _, distinct := d.distinctCols[uint32(i)]; !distinct {
			continue
		}

		// TODO(irfansharif): Different rows may come with different encodings,
		// e.g. if they come from different streams that were merged, in which
		// case the encodings don't match (despite having the same underlying
		// datums). We instead opt to always choose sqlbase.DatumEncoding_ASCENDING_KEY
		// but we may want to check the first row for what encodings are already
		// available.
		appendTo, err = datum.Encode(&d.datumAlloc, sqlbase.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
}
