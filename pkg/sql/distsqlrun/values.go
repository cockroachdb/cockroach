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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// valuesProcessor is a processor that has no inputs and generates "pre-canned"
// rows.
type valuesProcessor struct {
	processorBase
	rowSourceBase

	columns []DatumInfo
	data    [][]byte

	sd     StreamDecoder
	rowBuf sqlbase.EncDatumRow
}

var _ Processor = &valuesProcessor{}
var _ RowSource = &valuesProcessor{}

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
	if err := v.init(post, types, flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}
	return v, nil
}

// Run is part of the processor interface.
func (v *valuesProcessor) Run(wg *sync.WaitGroup) {
	if v.out.output == nil {
		panic("valuesProcessor output not initialized for emitting rows")
	}
	Run(v.flowCtx.Ctx, v, v.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (v *valuesProcessor) close() {
	v.internalClose()
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (v *valuesProcessor) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !v.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(v.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		v.close()
	}
	return meta
}

// Next is part of the RowSource interface.
func (v *valuesProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if v.maybeStart("values", "" /* logTag */) {
		// Add a bogus header to apease the StreamDecoder, which wants to receive a
		// header before any data.
		m := &ProducerMessage{
			Typing: v.columns,
			Header: &ProducerHeader{}}
		if err := v.sd.AddMessage(m); err != nil {
			return nil, v.producerMeta(err)
		}

		v.rowBuf = make(sqlbase.EncDatumRow, len(v.columns))
	}
	if v.closed {
		return nil, v.producerMeta(nil /* err */)
	}

	for {
		row, meta, err := v.sd.GetRow(v.rowBuf)
		if err != nil {
			return nil, v.producerMeta(err)
		}

		if row == nil && meta == nil {
			if len(v.data) == 0 {
				return nil, v.producerMeta(nil /* err */)
			}
			// Push a chunk of data to the stream decoder.
			m := &ProducerMessage{}
			m.Data.RawBytes = v.data[0]
			if err := v.sd.AddMessage(m); err != nil {
				return nil, v.producerMeta(err)
			}
			v.data = v.data[1:]
			continue
		}

		if row != nil {
			if v.consumerStatus != NeedMoreRows {
				continue
			}
			outRow, status, err := v.out.ProcessRow(v.ctx, row)
			if err != nil {
				return nil, v.producerMeta(err)
			}
			switch status {
			case NeedMoreRows:
				if outRow == nil && err == nil {
					continue
				}
			case DrainRequested:
				continue
			}
			return outRow, nil
		}

		return nil, meta
	}
}

// ConsumerDone is part of the RowSource interface.
func (v *valuesProcessor) ConsumerDone() {
	v.consumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (v *valuesProcessor) ConsumerClosed() {
	v.consumerClosed("values")
	// The consumer is done, Next() will not be called again.
	v.close()
}
