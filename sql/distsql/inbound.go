// Copyright 2016 The Cockroach Authors.
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

package distsql

import (
	"io"

	"github.com/cockroachdb/cockroach/util/log"
)

// ProcessInboundStream receives rows from a DistSQL_FlowStreamServer and sends
// them to a RowReceiver. Optionally processes an initial StreamMessage that was
// already received (because the first message contains the flow and stream IDs,
// it needs to be received before we can get here).
func ProcessInboundStream(
	flowCtx *FlowCtx, stream DistSQL_FlowStreamServer, firstMsg *StreamMessage, dst RowReceiver,
) error {
	ctx := flowCtx.Context
	// Function which we call when we are done.
	finish := func(err error) error {
		dst.Close(err)
		if err != nil {
			if log.V(1) {
				log.Errorf(ctx, "inbound stream error: %s", err)
			}
			// TODO(radu): populate response and send error instead
			return err
		}
		if log.V(2) {
			log.Infof(ctx, "inbound stream done")
		}
		return stream.SendAndClose(&SimpleResponse{})
	}

	var sd StreamDecoder
	for {
		var msg *StreamMessage
		if firstMsg != nil {
			msg = firstMsg
			firstMsg = nil
		} else {
			var err error
			msg, err = stream.Recv()
			if err != nil {
				if err != io.EOF {
					// Communication error.
					dst.Close(err)
					return err
				}
				// End of the stream.
				return finish(nil)
			}
		}
		err := sd.AddMessage(msg)
		if err != nil {
			return finish(err)
		}
		for {
			row, err := sd.GetRow(nil)
			if err != nil {
				return finish(err)
			}
			if row == nil {
				// No more rows in the last message.
				break
			}
			if log.V(3) {
				log.Infof(ctx, "inbound stream pushing row %s\n", row)
			}
			if !dst.PushRow(row) {
				// Rest of rows not needed.
				return finish(nil)
			}
		}
		done, err := sd.IsDone()
		if done {
			return finish(err)
		}
	}
}
