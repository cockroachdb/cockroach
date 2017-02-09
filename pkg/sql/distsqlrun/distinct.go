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

package distsqlrun

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"golang.org/x/net/context"
)

type distinct struct {
	input        RowSource
	lastGroupKey sqlbase.EncDatumRow
	seen         map[string]struct{}
	orderedCols  map[uint32]struct{}
	datumAlloc   sqlbase.DatumAlloc
	out          procOutputHelper
}

var _ processor = &distinct{}

func newDistinct(
	flowCtx *FlowCtx, spec *DistinctSpec, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*distinct, error) {
	d := &distinct{
		input:       input,
		orderedCols: make(map[uint32]struct{}),
	}
	for _, col := range spec.OrderedColumns {
		d.orderedCols[col] = struct{}{}
	}

	if err := d.out.init(post, input.Types(), flowCtx.evalCtx, output); err != nil {
		return nil, err
	}

	return d, nil
}

// Run is part of the processor interface.
func (d *distinct) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "Evaluator", nil)
	ctx, span := tracing.ChildSpan(ctx, "distinct")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting distinct process")
		defer log.Infof(ctx, "exiting distinct")
	}

	defer d.input.ConsumerDone()

	var scratch []byte
	for {
		row, err := d.input.NextRow()
		if err != nil || row == nil {
			d.out.close(err)
			return
		}

		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we define x to be our group key. Our seen set at any given time
		// is only the set of all rows with the same group key. The encoding of
		// the row is the key we use in our 'seen' set.
		encoding, err := d.encode(scratch, row)
		if err != nil {
			d.out.close(err)
			return
		}

		// The 'seen' set is reset whenever we find consecutive rows differing on the
		// group key thus avoiding the need to store encodings of all rows.
		matched, err := d.matchLastGroupKey(row)
		if err != nil {
			d.out.close(err)
			return
		}

		if !matched {
			d.lastGroupKey = row
			d.seen = make(map[string]struct{})
		}

		key := string(encoding)
		if _, ok := d.seen[key]; !ok {
			d.seen[key] = struct{}{}
			if !d.out.emitRow(ctx, row) {
				d.out.close(nil)
				return
			}
		}
		scratch = encoding[:0]
	}
}

func (d *distinct) matchLastGroupKey(row sqlbase.EncDatumRow) (bool, error) {
	if d.lastGroupKey == nil {
		return false, nil
	}
	for colIdx := range d.orderedCols {
		res, err := d.lastGroupKey[colIdx].Compare(&d.datumAlloc, &row[colIdx])
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
		// If we are processing DISTINCT(x, y) and the input stream is ordered
		// by x, we are using x as our group key (our 'seen' set at any given
		// time is the set of all rows with the same group key). This alleviates
		// the need to use x in our encoding when computing the key into our
		// set.
		if _, ordered := d.orderedCols[uint32(i)]; ordered {
			continue
		}

		// TODO(irfansharif): Different rows may come with different encodings,
		// e.g. if they come from different streams that were merged, in which
		// case the encodings don't match (despite having the same underlying
		// datums). We instead opt to always choose sqlbase.DatumEncoding_VALUE
		// but we may want to check the first row for what encodings are already
		// available.
		appendTo, err = datum.Encode(&d.datumAlloc, sqlbase.DatumEncoding_VALUE, appendTo)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
}
