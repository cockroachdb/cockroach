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
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsql

import (
	"bytes"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"golang.org/x/net/context"
)

// TODO(irfansharif): Optimizations taking advantage of partial orderings
// possible if part of the output.
type distinct struct {
	input        RowSource
	output       RowReceiver
	ctx          context.Context
	seen         map[string]struct{}
	lastGroupKey []byte
	orderedCols  columns
	distinctCols columns

	// Buffer to store intermediate results when extracting decoded datum values
	// to avoid reallocation.
	tuple      parser.DTuple
	datumAlloc sqlbase.DatumAlloc
	rowAlloc   sqlbase.EncDatumRowAlloc
}

func newDistinct(
	flowCtx *FlowCtx, spec *DistinctSpec, input RowSource, output RowReceiver,
) (*distinct, error) {
	d := &distinct{
		input:        input,
		output:       output,
		ctx:          log.WithLogTag(flowCtx.Context, "Evaluator", nil),
		seen:         make(map[string]struct{}),
		tuple:        make(parser.DTuple, len(spec.Cols)),
		orderedCols:  make(columns, len(spec.Ordering.Columns)),
		distinctCols: make(columns, len(spec.Cols)),
	}
	copy(d.distinctCols, spec.Cols)
	for i, ord := range spec.Ordering.Columns {
		d.orderedCols[i] = ord.ColIdx
	}

	return d, nil
}

// Run is part of the processor interface.
func (d *distinct) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx, span := tracing.ChildSpan(d.ctx, "distinct")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting distinct process")
		defer log.Infof(ctx, "exiting distinct")
	}

	var scratch [2][]byte
	for {
		row, err := d.input.NextRow()
		if err != nil || row == nil {
			d.output.Close(err)
			return
		}

		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define the encoding of x to be our group key.  Our seen set
		// at any given time is only the set of all rows with the same group
		// key. The 'seen' set is reset whenever we find consecutive rows
		// differing on the set of ordered columns thus avoiding the need to
		// store encodings of all rows.
		// The encoding of the distinct column values is the key we use in our
		// 'seen' set.
		groupKey, encoding, err := d.encode(scratch[0], scratch[1], row)
		if err != nil {
			d.output.Close(err)
			return
		}
		if !bytes.Equal(groupKey, d.lastGroupKey) {
			// The prefix of the row which is ordered differs from the last row;
			// reset our seen set.
			if len(d.seen) > 0 {
				d.seen = make(map[string]struct{})
			}

			d.lastGroupKey = append(d.lastGroupKey[:0], groupKey...)
			d.lastGroupKey = d.lastGroupKey[:len(groupKey)]
		}

		key := string(encoding)
		if _, ok := d.seen[key]; !ok {
			d.seen[key] = struct{}{}
			outRow := d.rowAlloc.AllocRow(len(d.distinctCols))
			for i, colIdx := range d.distinctCols {
				outRow[i] = row[colIdx]
			}

			if log.V(3) {
				log.Infof(ctx, "pushing %s\n", outRow)
			}
			if !d.output.PushRow(outRow) {
				if log.V(2) {
					log.Infof(ctx, "no more rows required")
				}
				d.output.Close(nil)
				return
			}
		}
		scratch[0] = groupKey[:0]
		scratch[1] = encoding[:0]
	}
}

// encode returns the group key (formed by the encoding of the ordered columns)
// and the encoding of the distinct columns.
func (d *distinct) encode(
	groupKey, encoding []byte, row sqlbase.EncDatumRow,
) ([]byte, []byte, error) {
	var err error
	// TODO(irfansharif): There's a subtle optimization possible here. If we are
	// processing DISTINCT(x, y) and the input stream is ordered by x, we are
	// using x as our group key (our 'seen' set at any given time is the set of
	// all rows with the same group key). This alleviates the need to use x in
	// our encoding when computing the key into our set.
	for _, colIdx := range d.orderedCols {
		groupKey, err = row[colIdx].Encode(&d.datumAlloc, sqlbase.DatumEncoding_VALUE, groupKey)
		if err != nil {
			return nil, nil, err
		}
	}
	for _, colIdx := range d.distinctCols {
		// TODO(irfansharif): Different rows may come with different encodings,
		// e.g. if they come from different streams that were merged, in which
		// case the encodings don't match (despite having the same underlying
		// datums). We instead opt to always choose sqlbase.DatumEncoding_VALUE
		// but we may want to check the first row for what encodings are already
		// available.
		encoding, err = row[colIdx].Encode(&d.datumAlloc, sqlbase.DatumEncoding_VALUE, encoding)
		if err != nil {
			return nil, nil, err
		}
	}
	return groupKey, encoding, nil
}
