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
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

type metadataTestReceiver struct {
	processorBase
	input RowSource

	// trailingErrMeta stores the error metadata received from the input. We
	// do not return this metadata immediately because metadata propagation errors
	// are prioritized over query errors, which ensures that tests which expect a
	// query error can still fail if they do not properly propagate metadata.
	trailingErrMeta []ProducerMetadata

	senders   []string
	rowCounts map[string]rowNumCounter
}

type rowNumCounter struct {
	expected, actual int32
	seen             util.FastIntSet
	err              error
}

var _ Processor = &metadataTestReceiver{}
var _ RowSource = &metadataTestReceiver{}

const metadataTestReceiverProcName = "meta receiver"

func newMetadataTestReceiver(
	flowCtx *FlowCtx, input RowSource, post *PostProcessSpec, output RowReceiver, senders []string,
) (*metadataTestReceiver, error) {
	mtr := &metadataTestReceiver{
		input:     input,
		senders:   senders,
		rowCounts: make(map[string]rowNumCounter),
	}
	if err := mtr.init(
		post,
		input.OutputTypes(),
		flowCtx,
		output,
		procStateOpts{
			inputsToDrain: []RowSource{input},
			trailingMetaCallback: func() []ProducerMetadata {
				var trailingMeta []ProducerMetadata
				if mtr.rowCounts != nil {
					if meta := mtr.checkRowNumMetadata(); meta != nil {
						trailingMeta = append(trailingMeta, *meta)
					}
				}
				mtr.internalClose()
				return trailingMeta
			},
		},
	); err != nil {
		return nil, err
	}
	return mtr, nil
}

// Run is part of the Processor interface.
func (mtr *metadataTestReceiver) Run(ctx context.Context, wg *sync.WaitGroup) {
	if mtr.out.output == nil {
		panic("metadataTestReceiver output not initialized for emitting rows")
	}

	ctx = mtr.Start(ctx)
	Run(ctx, mtr, mtr.out.output)
	if wg != nil {
		wg.Done()
	}
}

// checkRowNumMetadata examines all of the received RowNum metadata to ensure
// that it has received exactly one of each expected RowNum. If the check
// detects dropped or repeated metadata, it returns error metadata. Otherwise,
// it returns nil.
func (mtr *metadataTestReceiver) checkRowNumMetadata() *ProducerMetadata {
	defer func() { mtr.rowCounts = nil }()

	if len(mtr.rowCounts) != len(mtr.senders) {
		var missingSenders string
		for _, sender := range mtr.senders {
			if _, exists := mtr.rowCounts[sender]; !exists {
				if missingSenders == "" {
					missingSenders = sender
				} else {
					missingSenders += fmt.Sprintf(", %s", sender)
				}
			}
		}
		return &ProducerMetadata{
			Err: fmt.Errorf(
				"expected %d metadata senders but found %d; missing %s",
				len(mtr.senders), len(mtr.rowCounts), missingSenders,
			),
		}
	}
	for id, cnt := range mtr.rowCounts {
		if cnt.err != nil {
			return &ProducerMetadata{Err: cnt.err}
		}
		if cnt.expected != cnt.actual {
			return &ProducerMetadata{
				Err: fmt.Errorf(
					"dropped metadata from sender %s: expected %d RowNum messages but got %d",
					id, cnt.expected, cnt.actual),
			}
		}
		for i := 0; i < int(cnt.expected); i++ {
			if !cnt.seen.Contains(i) {
				return &ProducerMetadata{
					Err: fmt.Errorf(
						"dropped and repeated metadata from sender %s: have %d messages but missing RowNum #%d",
						id, cnt.expected, i+1),
				}
			}
		}
	}

	return nil
}

// Start is part of the RowSource interface.
func (mtr *metadataTestReceiver) Start(ctx context.Context) context.Context {
	mtr.input.Start(ctx)
	return mtr.startInternal(ctx, metadataTestReceiverProcName)
}

// Next is part of the RowSource interface.
//
// This implementation doesn't follow the usual patterns of other processors; it
// makes more limited use of the processorBase's facilities because it needs to
// inspect metadata while draining.
func (mtr *metadataTestReceiver) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for {
		if mtr.state == stateTrailingMeta {
			if meta := mtr.popTrailingMeta(); meta != nil {
				return nil, meta
			}
			// If there's no more trailingMeta, we've moved to stateExhausted, and we
			// might return some trailingErrMeta below.
		}
		if mtr.state == stateExhausted {
			if len(mtr.trailingErrMeta) > 0 {
				meta := mtr.trailingErrMeta[0]
				mtr.trailingErrMeta = mtr.trailingErrMeta[1:]
				return nil, &meta
			}
			return nil, nil
		}

		row, meta := mtr.input.Next()

		if meta != nil {
			if meta.RowNum != nil {
				rowNum := meta.RowNum
				rcnt, exists := mtr.rowCounts[rowNum.SenderID]
				if !exists {
					rcnt.expected = -1
				}
				if rcnt.err != nil {
					return nil, meta
				}
				if rowNum.LastMsg {
					if rcnt.expected != -1 {
						rcnt.err = fmt.Errorf(
							"repeated metadata from reader %s: received more than one RowNum with LastMsg set",
							rowNum.SenderID)
						mtr.rowCounts[rowNum.SenderID] = rcnt
						return nil, meta
					}
					rcnt.expected = rowNum.RowNum
				} else {
					rcnt.actual++
					rcnt.seen.Add(int(rowNum.RowNum - 1))
				}
				mtr.rowCounts[rowNum.SenderID] = rcnt
			}
			if meta.Err != nil {
				// Keep track of the err in trailingErrMeta, which will be returned
				// after everything else (including processorBase.trailingMeta).
				mtr.trailingErrMeta = append(mtr.trailingErrMeta, *meta)
				continue
			}

			return nil, meta
		}

		if row == nil {
			mtr.moveToTrailingMeta()
			continue
		}

		// We don't use processorBase.processRowHelper() here because we need
		// special handling for errors: this proc never starts draining in order for
		// it to be as unintrusive as possible.
		outRow, status, err := mtr.out.ProcessRow(mtr.ctx, row)
		if err != nil {
			mtr.trailingMeta = append(mtr.trailingMeta, ProducerMetadata{Err: err})
			continue
		}
		switch status {
		case NeedMoreRows:
			if outRow == nil {
				continue
			}
		case DrainRequested:
			mtr.moveToDraining(nil /* err */)
			continue
		}

		// Swallow rows if we're draining.
		if mtr.state == stateDraining && outRow != nil {
			continue
		}

		return outRow, nil
	}
}

// ConsumerDone is part of the RowSource interface.
func (mtr *metadataTestReceiver) ConsumerDone() {
	mtr.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (mtr *metadataTestReceiver) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	mtr.internalClose()
}
