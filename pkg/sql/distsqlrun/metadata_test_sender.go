// Copyright 2018 The Cockroach Authors.
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

type metadataTestSender struct {
	processorBase
	input RowSource
	id    string

	// sendRowNumMeta is set to true when the next call to Next() must return
	// RowNum metadata, and false otherwise.
	sendRowNumMeta bool
	// sentLastMsg, if set, indicates that the "terminal" RowNum with LastMsg set
	// has been sent.
	sentLastMsg bool
	rowNumCnt   int32
}

var _ Processor = &metadataTestSender{}
var _ RowSource = &metadataTestSender{}

func newMetadataTestSender(
	flowCtx *FlowCtx, input RowSource, post *PostProcessSpec, output RowReceiver, id string,
) (*metadataTestSender, error) {
	mts := &metadataTestSender{input: input, id: id}
	if err := mts.init(post, input.OutputTypes(), flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}
	return mts, nil
}

// Run is part of the Processor interface.
func (mts *metadataTestSender) Run(wg *sync.WaitGroup) {
	if mts.out.output == nil {
		panic("metadataTestSender output not initialized for emitting rows")
	}
	Run(mts.flowCtx.Ctx, mts, mts.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (mts *metadataTestSender) close() {
	if mts.internalClose() {
		mts.input.ConsumerClosed()
	}
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (mts *metadataTestSender) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !mts.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(mts.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		mts.close()
	}
	if meta == nil && !mts.sentLastMsg {
		mts.sentLastMsg = true
		meta = &ProducerMetadata{
			RowNum: &RemoteProducerMetadata_RowNum{
				RowNum:   mts.rowNumCnt,
				SenderID: mts.id,
				LastMsg:  true,
			},
		}
	}
	return meta
}

// Next is part of the RowSource interface.
func (mts *metadataTestSender) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	mts.maybeStart("metadataTestSender", "" /* logTag */)

	if mts.sendRowNumMeta {
		mts.sendRowNumMeta = false
		mts.rowNumCnt++
		return nil, &ProducerMetadata{
			RowNum: &RemoteProducerMetadata_RowNum{
				RowNum:   mts.rowNumCnt,
				SenderID: mts.id,
				LastMsg:  false,
			},
		}
	}

	for {
		row, meta := mts.input.Next()
		if meta != nil {
			return nil, meta
		}
		if mts.closed || row == nil {
			return nil, mts.producerMeta(nil /* err */)
		}

		outRow, status, err := mts.out.ProcessRow(mts.ctx, row)
		if err != nil {
			return nil, mts.producerMeta(err)
		}
		switch status {
		case NeedMoreRows:
			if outRow == nil && err == nil {
				continue
			}
		case DrainRequested:
			mts.input.ConsumerDone()
			continue
		}
		mts.sendRowNumMeta = true
		return outRow, nil
	}
}

// ConsumerDone is part of the RowSource interface.
func (mts *metadataTestSender) ConsumerDone() {
	mts.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (mts *metadataTestSender) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	mts.close()
}
