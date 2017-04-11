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

package distsqlrun

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// StreamDecoder converts a sequence of ProducerMessage to rows and metadata
// records.
//
// Sample usage:
//   sd := StreamDecoder{}
//   var row sqlbase.EncDatumRow
//   for each message in stream {
//       err := sd.AddMessage(msg)
//       if err != nil { ... }
//       for {
//           row, meta, err := sd.GetRow(row)
//           if err != nil { ... }
//           if row == nil && meta.Empty() {
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
	typing   []DatumInfo
	data     []byte
	metadata []ProducerMetadata
	rowAlloc sqlbase.EncDatumRowAlloc

	headerReceived bool
	typingReceived bool
}

// AddMessage adds the data in a ProducerMessage to the decoder.
//
// The StreamDecoder may keep a reference to msg.Data.RawBytes and
// msg.Data.Metadata until all the rows in the message are retrieved with GetRow.
//
// If an error is returned, no records have been buffered in the StreamDecoder.
func (sd *StreamDecoder) AddMessage(msg *ProducerMessage) error {
	if msg.Header != nil {
		if sd.headerReceived {
			return errors.Errorf("received multiple headers")
		}
		sd.headerReceived = true
	}
	if msg.Typing != nil {
		if sd.typingReceived {
			return errors.Errorf("typing information received multiple times")
		}
		sd.typingReceived = true
		sd.typing = msg.Typing
	}

	if len(msg.Data.RawBytes) != 0 {
		if !sd.headerReceived || !sd.typingReceived {
			return errors.Errorf("received data before header and/or typing info")
		}
	}

	if len(msg.Data.RawBytes) > 0 {
		if len(sd.data) == 0 {
			// We limit the capacity of the slice (using "three-index slices") out of
			// paranoia: if the slice is going to need to grow later, we don't want to
			// clobber any memory outside what the protobuf allocated for us
			// initially (in case this memory might be coming from some buffer).
			sd.data = msg.Data.RawBytes[:len(msg.Data.RawBytes):len(msg.Data.RawBytes)]
		} else {
			// This can only happen if we don't retrieve all the rows before
			// adding another message, which shouldn't be the normal case.
			// TODO(radu): maybe don't support this case at all?
			sd.data = append(sd.data, msg.Data.RawBytes...)
		}
	}
	if len(msg.Data.Metadata) > 0 {
		for _, md := range msg.Data.Metadata {
			var meta ProducerMetadata
			if rangeInfo := md.GetRangeInfo(); rangeInfo != nil {
				meta.Ranges = rangeInfo.RangeInfo
			} else if pErr := md.GetError(); pErr != nil {
				meta.Err = pErr.ErrorDetail()
			}
			sd.metadata = append(sd.metadata, meta)
		}
	}
	return nil
}

// GetRow returns a row received in the stream. A row buffer can be provided
// optionally.
//
// Returns an empty row if there are no more rows received so far.
//
// A decoding error may be returned. Note that these are separate from error
// coming from the upstream (through ProducerMetadata.Err).
func (sd *StreamDecoder) GetRow(
	rowBuf sqlbase.EncDatumRow,
) (sqlbase.EncDatumRow, ProducerMetadata, error) {
	if len(sd.metadata) != 0 {
		r := sd.metadata[0]
		sd.metadata = sd.metadata[1:]
		return nil, r, nil
	}

	if len(sd.data) == 0 {
		return nil, ProducerMetadata{}, nil
	}
	rowLen := len(sd.typing)
	if cap(rowBuf) >= rowLen {
		rowBuf = rowBuf[:rowLen]
	} else {
		rowBuf = sd.rowAlloc.AllocRow(rowLen)
	}
	for i := range rowBuf {
		var err error
		rowBuf[i], sd.data, err = sqlbase.EncDatumFromBuffer(sd.typing[i].Type, sd.typing[i].Encoding, sd.data)
		if err != nil {
			// Reset sd because it is no longer usable.
			*sd = StreamDecoder{}
			return nil, ProducerMetadata{}, err
		}
	}
	return rowBuf, ProducerMetadata{}, nil
}
