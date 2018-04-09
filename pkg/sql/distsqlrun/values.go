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

package distsqlrun

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// valuesProcessor is a processor that has no inputs and generates "pre-canned"
// rows.
type valuesProcessor struct {
	processorBase

	columns []DatumInfo
	data    [][]byte

	sd     StreamDecoder
	rowBuf sqlbase.EncDatumRow
}

var _ Processor = &valuesProcessor{}
var _ RowSource = &valuesProcessor{}

const valuesProcName = "values"

func newValuesProcessor(
	flowCtx *FlowCtx, spec *ValuesCoreSpec, post *PostProcessSpec, output RowReceiver,
) (*valuesProcessor, error) {
	v := &valuesProcessor{
		columns: spec.Columns,
		data:    spec.RawBytes,
	}
	types := make([]sqlbase.ColumnType, len(v.columns))
	for i := range v.columns {
		types[i] = v.columns[i].Type
	}
	if err := v.init(post, types, flowCtx, output, procStateOpts{}); err != nil {
		return nil, err
	}
	return v, nil
}

// Run is part of the processor interface.
func (v *valuesProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if v.out.output == nil {
		panic("valuesProcessor output not initialized for emitting rows")
	}
	ctx = v.Start(ctx)
	Run(ctx, v, v.out.output)
	if wg != nil {
		wg.Done()
	}
}

// Start is part of the RowSource interface.
func (v *valuesProcessor) Start(ctx context.Context) context.Context {
	ctx = v.startInternal(ctx, valuesProcName)

	// Add a bogus header to apease the StreamDecoder, which wants to receive a
	// header before any data.
	m := &ProducerMessage{
		Typing: v.columns,
		Header: &ProducerHeader{}}
	if err := v.sd.AddMessage(m); err != nil {
		v.moveToDraining(err)
		return ctx
	}

	v.rowBuf = make(sqlbase.EncDatumRow, len(v.columns))
	return ctx
}

// Next is part of the RowSource interface.
func (v *valuesProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for v.state == stateRunning {
		row, meta, err := v.sd.GetRow(v.rowBuf)
		if err != nil {
			v.moveToDraining(err)
			break
		}

		if meta != nil {
			return nil, meta
		}

		if row == nil {
			if len(v.data) == 0 {
				v.moveToDraining(nil /* err */)
				break
			}
			// Push a chunk of data to the stream decoder.
			m := &ProducerMessage{}
			m.Data.RawBytes = v.data[0]
			if err := v.sd.AddMessage(m); err != nil {
				v.moveToDraining(err)
				break
			}
			v.data = v.data[1:]
			continue
		}

		if outRow := v.processRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, v.drainHelper()

}

// ConsumerDone is part of the RowSource interface.
func (v *valuesProcessor) ConsumerDone() {
	v.moveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (v *valuesProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	v.internalClose()
}
