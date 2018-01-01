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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
)

// valuesProcessor is a processor that has no inputs and generates "pre-canned"
// rows.
type valuesProcessor struct {
	processorBase

	ctx     context.Context
	span    opentracing.Span
	columns []DatumInfo
	data    [][]byte

	started bool
	closed  bool
	sd      StreamDecoder
	rowBuf  sqlbase.EncDatumRow

	// consumerStatus is used by the RowSource interface to signal that the
	// consumer is done accepting rows or is no longer accepting data.
	consumerStatus ConsumerStatus
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
	if !v.closed {
		v.closed = true
		tracing.FinishSpan(v.span)
		v.span = nil
	}
	// This prevents Next() from returning more rows.
	v.out.consumerClosed()
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// valuesProcessor ran out of rows or encountered an error. It is ok for err to
// be nil indicating that we're done producing rows even though no error
// occurred.
func (v *valuesProcessor) producerMeta(err error) ProducerMetadata {
	var meta ProducerMetadata
	if !v.closed {
		meta = ProducerMetadata{Err: err}
		if err == nil {
			meta.TraceData = getTraceData(v.ctx)
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		v.close()
	}
	return meta
}

// Next is part of the RowSource interface.
func (v *valuesProcessor) Next() (sqlbase.EncDatumRow, ProducerMetadata) {
	if !v.started {
		v.started = true
		v.ctx, v.span = processorSpan(v.flowCtx.Ctx, "values")

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

	for {
		row, meta, err := v.sd.GetRow(v.rowBuf)
		if err != nil {
			return nil, v.producerMeta(err)
		}

		if row == nil && meta.Empty() {
			if len(v.data) == 0 {
				return nil, ProducerMetadata{}
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
			return outRow, ProducerMetadata{}
		}

		return nil, meta
	}
}

// ConsumerDone is part of the RowSource interface.
func (v *valuesProcessor) ConsumerDone() {
	if v.consumerStatus != NeedMoreRows {
		log.Fatalf(context.Background(), "values already done or closed: %d",
			v.consumerStatus)
	}
	v.consumerStatus = DrainRequested
}

// ConsumerClosed is part of the RowSource interface.
func (v *valuesProcessor) ConsumerClosed() {
	if v.consumerStatus == ConsumerClosed {
		log.Fatalf(context.Background(), "values already closed")
	}
	v.consumerStatus = ConsumerClosed
	// The consumer is done, Next() will not be called again.
	v.close()

}
