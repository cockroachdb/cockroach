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
	input  RowSource
	output RowReceiver
	ctx    context.Context
	cols   []uint32
	seen   map[string]struct{}

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
		input:  input,
		output: output,
		ctx:    log.WithLogTag(flowCtx.Context, "Evaluator", nil),

		cols:  make([]uint32, len(spec.Cols)),
		seen:  make(map[string]struct{}),
		tuple: make(parser.DTuple, len(spec.Cols)),
	}

	for i, col := range spec.Cols {
		d.cols[i] = col
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

	var scratch []byte
	for {
		row, err := d.input.NextRow()
		if err != nil || row == nil {
			d.output.Close(err)
			return
		}

		encoded, err := d.encode(scratch, row)
		if err != nil {
			d.output.Close(err)
			return
		}

		key := string(encoded)
		if _, ok := d.seen[key]; !ok {
			d.seen[key] = struct{}{}
			outRow := d.rowAlloc.AllocRow(len(d.cols))
			for i, col := range d.cols {
				outRow[i] = row[col]
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
		scratch = encoded[:0]
	}
	d.output.Close(nil)
}

func (d *distinct) encode(b []byte, row sqlbase.EncDatumRow) ([]byte, error) {
	for i, col := range d.cols {
		if err := row[col].Decode(&d.datumAlloc); err != nil {
			return nil, err
		}
		d.tuple[i] = row[col].Datum
	}

	return sqlbase.EncodeDTuple(b, d.tuple)
}
