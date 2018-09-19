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

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// metadataTestSender intersperses a metadata record after every row.
type metadataTestSender struct {
	ProcessorBase
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
	if err := mts.Init(
		mts,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		ProcStateOpts{
			InputsToDrain: []RowSource{mts.input},
			TrailingMetaCallback: func(context.Context) []ProducerMetadata {
				mts.InternalClose()
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

// Start is part of the RowSource interface.
func (mts *metadataTestSender) Start(ctx context.Context) context.Context {
	mts.input.Start(ctx)
	return mts.StartInternal(ctx, metadataTestSenderProcName)
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

	for mts.State == StateRunning {
		row, meta := mts.input.Next()
		if meta != nil {
			// Other processors will start draining when they get an error meta from
			// their input. We don't do that here, as the mts should be an unintrusive
			// as possible.
			return nil, meta
		}
		if row == nil {
			mts.MoveToDraining(nil /* err */)
			break
		}

		if outRow := mts.ProcessRowHelper(row); outRow != nil {
			mts.sendRowNumMeta = true
			return outRow, nil
		}
	}
	return nil, mts.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (mts *metadataTestSender) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	mts.InternalClose()
}
