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
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

type metadataTestReceiver struct {
	processorBase
	input RowSource

	// trailingErrMetadata stores the error metadata received from the input. We
	// do not return this metadata immediately because metadata propagation errors
	// are prioritized over query errors, which ensures that tests which expect a
	// query error can still fail if they do not properly propagate metadata.
	trailingErrMetadata []*ProducerMetadata

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

func newMetadataTestReceiver(
	flowCtx *FlowCtx, input RowSource, post *PostProcessSpec, output RowReceiver, senders []string,
) (*metadataTestReceiver, error) {
	mtr := &metadataTestReceiver{
		input:     input,
		senders:   senders,
		rowCounts: make(map[string]rowNumCounter),
	}
	if err := mtr.init(post, input.OutputTypes(), flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}
	return mtr, nil
}

// Run is part of the Processor interface.
func (mtr *metadataTestReceiver) Run(wg *sync.WaitGroup) {
	if mtr.out.output == nil {
		panic("metadataTestReceiver output not initialized for emitting rows")
	}
	Run(mtr.flowCtx.Ctx, mtr, mtr.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (mtr *metadataTestReceiver) close() {
	if mtr.internalClose() {
		mtr.input.ConsumerClosed()
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

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error.
func (mtr *metadataTestReceiver) producerMeta() *ProducerMetadata {
	if mtr.rowCounts != nil {
		if meta := mtr.checkRowNumMetadata(); meta != nil {
			return meta
		}
	}

	if !mtr.closed {
		if trace := getTraceData(mtr.ctx); trace != nil {
			return &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		mtr.close()
	}

	if len(mtr.trailingErrMetadata) > 0 {
		meta := mtr.trailingErrMetadata[0]
		mtr.trailingErrMetadata = mtr.trailingErrMetadata[1:]
		return meta
	}

	return nil
}

// Next is part of the RowSource interface.
func (mtr *metadataTestReceiver) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	mtr.maybeStart("metadataTestReceiver", "" /* logTag */)

	for {
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
				mtr.trailingErrMetadata = append(mtr.trailingErrMetadata, meta)
				continue
			}

			return nil, meta
		}

		if row == nil {
			// We have exhausted the input. Check that we received all the RowNum
			// metadata we expected to get.
			return nil, mtr.producerMeta()
		}

		outRow, status, err := mtr.out.ProcessRow(mtr.ctx, row)
		if err != nil {
			mtr.trailingErrMetadata = append(mtr.trailingErrMetadata, &ProducerMetadata{Err: err})
			continue
		}
		switch status {
		case NeedMoreRows:
			if outRow == nil && err == nil {
				continue
			}
		case DrainRequested:
			mtr.input.ConsumerDone()
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
	mtr.close()
}
