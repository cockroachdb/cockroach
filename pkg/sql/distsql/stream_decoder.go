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
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/pkg/errors"
)

// StreamDecoder converts a sequence of StreamMessage to EncDatumRows.
//
// Sample usage:
//   sd := StreamDecoder{}
//   var row sqlbase.EncDatumRow
//   for each message in stream {
//       err := sd.AddMessage(msg)
//       if err != nil { ... }
//       for {
//           row, err := sd.GetRow(row)
//           if err != nil { ... }
//           if row == nil {
//               // No more rows in this message.
//               break
//           }
//           // Use <row>
//           ...
//       }
//   }
//
// AddMessage can be called multiple times before getting the rows, but this
// will cause data to accumulate internally.
type StreamDecoder struct {
	info            []DatumInfo
	data            []byte
	trailerReceived bool
	// trailerErr stores the StreamTrailer.error received in the trailer.
	trailerErr error
	rowAlloc   sqlbase.EncDatumRowAlloc
}

// AddMessage adds the data in a StreamMessage to the decoder.
//
// The StreamDecoder may keep a reference to msg.Data.RawBytes until
// all the rows in the message are retrieved with GetRow.
func (sd *StreamDecoder) AddMessage(msg *StreamMessage) error {
	if sd.trailerReceived {
		return errors.Errorf("message after trailer was received")
	}
	headerReceived := (sd.info != nil)
	if msg.Header != nil {
		if headerReceived {
			return errors.Errorf("received multiple headers")
		}
		sd.info = msg.Header.Info
	} else {
		if !headerReceived {
			if msg.Trailer == nil {
				return errors.Errorf("first message doesn't have header or trailer")
			}
			if len(msg.Data.RawBytes) != 0 {
				return errors.Errorf("first and final message doesn't have header but has data")
			}
		}
	}
	if len(msg.Data.RawBytes) > 0 {
		if len(sd.data) == 0 {
			sd.data = msg.Data.RawBytes
		} else {
			// We make a copy of sd.data to avoid appending to the slice given
			// to us in msg.Data.RawBytes.
			// This can only happen if we don't retrieve all the rows before
			// adding another message, which shouldn't be the normal case.
			// TODO(radu): maybe don't support this case at all?
			sd.data = append([]byte(nil), sd.data...)
			sd.data = append(sd.data, msg.Data.RawBytes...)
		}
	}
	if msg.Trailer != nil {
		sd.trailerReceived = true
		sd.trailerErr = msg.Trailer.Error.GoError()
	}
	return nil
}

// GetRow returns a row of EncDatums received in the stream. A row buffer can be
// provided optionally.
// Returns nil if there are no more rows received so far.
func (sd *StreamDecoder) GetRow(rowBuf sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	if len(sd.data) == 0 {
		return nil, nil
	}
	rowLen := len(sd.info)
	if cap(rowBuf) >= rowLen {
		rowBuf = rowBuf[:rowLen]
	} else {
		rowBuf = sd.rowAlloc.AllocRow(rowLen)
	}
	for i := range rowBuf {
		var err error
		sd.data, err = rowBuf[i].SetFromBuffer(sd.info[i].Type, sd.info[i].Encoding, sd.data)
		if err != nil {
			// Reset sd because it is no longer usable.
			*sd = StreamDecoder{}
			return nil, err
		}
	}
	return rowBuf, nil
}

// IsDone returns true if all the rows were returned and the stream trailer was
// received (in which case any error in the trailer is returned as well).
func (sd *StreamDecoder) IsDone() (bool, error) {
	if len(sd.data) > 0 || !sd.trailerReceived {
		return false, nil
	}
	return true, sd.trailerErr
}
