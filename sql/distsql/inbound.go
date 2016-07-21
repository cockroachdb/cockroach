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
// them to a RowReceiver. Optionally processes an initial StreamMessage that
// was already received.
func ProcessInboundStream(
	ctx *FlowCtx, stream DistSQL_FlowStreamServer, firstMsg *StreamMessage, dst RowReceiver,
) error {
	var sd StreamDecoder
	var retErr error
loop:
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
				break loop
			}
		}
		err := sd.AddMessage(msg)
		if err != nil {
			retErr = err
			break loop
		}
		for {
			row, err := sd.GetRow(nil)
			if err != nil {
				retErr = err
				break loop
			}
			if row == nil {
				// No more rows in the last message.
				break
			}
			if log.V(3) {
				log.Infoc(ctx, "inbound stream pushing row %s\n", row)
			}
			if !dst.PushRow(row) {
				// Rest of rows not needed.
				break loop
			}
		}
		done, err := sd.IsDone()
		if done {
			retErr = err
			break
		}
	}
	dst.Close(retErr)
	if retErr != nil {
		if log.V(1) {
			log.Errorc(ctx, "inbound stream error: %s", retErr)
		}
		// TODO(radu): populate response error instead
		return retErr
	}
	if log.V(2) {
		log.Infoc(ctx, "inbound stream done")
	}
	return stream.SendAndClose(&SimpleResponse{})
}
