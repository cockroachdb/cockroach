// Copyright 2017 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// valuesProcessor is a processor that has no inputs and generates "pre-canned"
// rows.
type valuesProcessor struct {
	flowCtx *FlowCtx
	columns []DatumInfo
	data    [][]byte
	out     procOutputHelper
}

var _ processor = &valuesProcessor{}

func newValuesProcessor(
	flowCtx *FlowCtx, spec *ValuesCoreSpec, post *PostProcessSpec, output RowReceiver,
) (*valuesProcessor, error) {
	v := &valuesProcessor{
		flowCtx: flowCtx,
		columns: spec.Columns,
		data:    spec.RawBytes,
	}
	types := make([]sqlbase.ColumnType, len(v.columns))
	for i := range v.columns {
		types[i] = v.columns[i].Type
	}
	if err := v.out.init(post, types, &flowCtx.evalCtx, output); err != nil {
		return nil, err
	}
	return v, nil
}

// Run is part of the processor interface.
func (v *valuesProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx, span := tracing.ChildSpan(ctx, "values")
	defer tracing.FinishSpan(span)

	// We reuse the code in StreamDecoder for decoding the raw data. We just need
	// to manufacture ProducerMessages.
	var sd StreamDecoder

	m := ProducerMessage{
		Typing: v.columns,
		// Add a bogus header to apease the StreamDecoder, which wants to receive a
		// header before any data.
		Header: &ProducerHeader{}}
	if err := sd.AddMessage(&m); err != nil {
		v.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
		v.out.close()
		return
	}

	m = ProducerMessage{}
	rowBuf := make(sqlbase.EncDatumRow, len(v.columns))
	for len(v.data) > 0 {
		// Push a chunk of data.
		m.Data.RawBytes = v.data[0]
		if err := sd.AddMessage(&m); err != nil {
			v.out.output.Push(nil /* row */, ProducerMetadata{Err: err})
			v.out.close()
			return
		}
		v.data = v.data[1:]

		for {
			row, meta, err := sd.GetRow(rowBuf)
			if err != nil {
				// If we got a decoding error, pass it along.
				meta.Err = err
			}

			if row == nil && meta.Empty() {
				break
			}

			if !emitHelper(ctx, &v.out, row, meta, nil /* inputs */) {
				return
			}
		}
	}
	v.out.close()
}
