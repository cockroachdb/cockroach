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
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// metadataTestSender intersperses a metadata record after every row.
type metadataTestSender struct {
	processorBase
	input RowSource
	id    string

	// sendRowNumMeta is set to true when the next call to Next() must return
	// RowNum metadata, and false otherwise.
	sendRowNumMeta bool
	rowNumCnt      int32
}

var _ Processor = &metadataTestSender{}
var _ RowSource = &metadataTestSender{}

const metadataTestSenderProcName = "meta sender"

func newMetadataTestSender(
	flowCtx *FlowCtx,
	processorID int32,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
	id string,
) (*metadataTestSender, error) {
	mts := &metadataTestSender{input: input, id: id}
	if err := mts.init(
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		procStateOpts{
			inputsToDrain: []RowSource{mts.input},
			trailingMetaCallback: func() []ProducerMetadata {
				mts.internalClose()
				// Send a final record with LastMsg set.
				meta := ProducerMetadata{
					RowNum: &RemoteProducerMetadata_RowNum{
						RowNum:   mts.rowNumCnt,
						SenderID: mts.id,
						LastMsg:  true,
					},
				}
				return []ProducerMetadata{meta}
			},
		},
	); err != nil {
		return nil, err
	}
	return mts, nil
}

// Run is part of the Processor interface.
func (mts *metadataTestSender) Run(ctx context.Context, wg *sync.WaitGroup) {
	if mts.out.output == nil {
		panic("metadataTestSender output not initialized for emitting rows")
	}

	ctx = mts.Start(ctx)
	Run(ctx, mts, mts.out.output)
	if wg != nil {
		wg.Done()
	}
}

// Start is part of the RowSource interface.
func (mts *metadataTestSender) Start(ctx context.Context) context.Context {
	mts.input.Start(ctx)
	return mts.startInternal(ctx, metadataTestSenderProcName)
}

// Next is part of the RowSource interface.
func (mts *metadataTestSender) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	// Every call after a row has been returned returns a metadata record.
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

	for mts.state == stateRunning {
		row, meta := mts.input.Next()
		if meta != nil {
			// Other processors will start draining when they get an error meta from
			// their input. We don't do that here, as the mts should be an unintrusive
			// as possible.
			return nil, meta
		}
		if row == nil {
			mts.moveToDraining(nil /* err */)
			break
		}

		if outRow := mts.processRowHelper(row); outRow != nil {
			mts.sendRowNumMeta = true
			return outRow, nil
		}
	}
	return nil, mts.drainHelper()
}

// ConsumerDone is part of the RowSource interface.
func (mts *metadataTestSender) ConsumerDone() {
	mts.moveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (mts *metadataTestSender) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	mts.internalClose()
}
