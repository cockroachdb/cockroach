// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
)

// MetadataTestSender intersperses a metadata record after every row.
type MetadataTestSender struct {
	ProcessorBase
	input RowSource
	id    string

	// sendRowNumMeta is set to true when the next call to Next() must return
	// RowNum metadata, and false otherwise.
	sendRowNumMeta bool
	rowNumCnt      int32
}

var _ Processor = &MetadataTestSender{}
var _ RowSource = &MetadataTestSender{}

const metadataTestSenderProcName = "meta sender"

// NewMetadataTestSender creates a new MetadataTestSender.
func NewMetadataTestSender(
	flowCtx *FlowCtx,
	processorID int32,
	input RowSource,
	post *execinfrapb.PostProcessSpec,
	output RowReceiver,
	id string,
) (*MetadataTestSender, error) {
	mts := &MetadataTestSender{input: input, id: id}
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
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				mts.InternalClose()
				// Send a final record with LastMsg set.
				meta := execinfrapb.ProducerMetadata{
					RowNum: &execinfrapb.RemoteProducerMetadata_RowNum{
						RowNum:   mts.rowNumCnt,
						SenderID: mts.id,
						LastMsg:  true,
					},
				}
				return []execinfrapb.ProducerMetadata{meta}
			},
		},
	); err != nil {
		return nil, err
	}
	return mts, nil
}

// Start is part of the RowSource interface.
func (mts *MetadataTestSender) Start(ctx context.Context) {
	ctx = mts.StartInternal(ctx, metadataTestSenderProcName)
	mts.input.Start(ctx)
}

// Next is part of the RowSource interface.
func (mts *MetadataTestSender) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// Every call after a row has been returned returns a metadata record.
	if mts.sendRowNumMeta {
		mts.sendRowNumMeta = false
		mts.rowNumCnt++
		return nil, &execinfrapb.ProducerMetadata{
			RowNum: &execinfrapb.RemoteProducerMetadata_RowNum{
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
