// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
	typing       []execinfrapb.DatumInfo
	data         []byte
	numEmptyRows int
	metadata     []execinfrapb.ProducerMetadata
	rowAlloc     rowenc.EncDatumRowAlloc

	headerReceived bool
	typingReceived bool
}

// AddMessage adds the data in a ProducerMessage to the decoder.
//
// The StreamDecoder may keep a reference to msg.Data.RawBytes and
// msg.Data.Metadata until all the rows in the message are retrieved with GetRow.
//
// If an error is returned, no records have been buffered in the StreamDecoder.
func (sd *StreamDecoder) AddMessage(ctx context.Context, msg *execinfrapb.ProducerMessage) error {
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

	if len(msg.Data.RawBytes) > 0 {
		if !sd.headerReceived || !sd.typingReceived {
			return errors.Errorf("received data before header and/or typing info")
		}

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
	if msg.Data.NumEmptyRows > 0 {
		if len(msg.Data.RawBytes) > 0 {
			return errors.Errorf("received both data and empty rows")
		}
		sd.numEmptyRows += int(msg.Data.NumEmptyRows)
	}
	if len(msg.Data.Metadata) > 0 {
		for _, md := range msg.Data.Metadata {
			meta, ok := execinfrapb.RemoteProducerMetaToLocalMeta(ctx, md)
			if !ok {
				// Unknown metadata, ignore.
				continue
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
	rowBuf rowenc.EncDatumRow,
) (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata, error) {
	if len(sd.metadata) != 0 {
		r := &sd.metadata[0]
		sd.metadata = sd.metadata[1:]
		return nil, r, nil
	}

	if sd.numEmptyRows > 0 {
		sd.numEmptyRows--
		row := make(rowenc.EncDatumRow, 0) // this doesn't actually allocate.
		return row, nil, nil
	}

	if len(sd.data) == 0 {
		return nil, nil, nil
	}
	rowLen := len(sd.typing)
	if cap(rowBuf) >= rowLen {
		rowBuf = rowBuf[:rowLen]
	} else {
		rowBuf = sd.rowAlloc.AllocRow(rowLen)
	}
	for i := range rowBuf {
		var err error
		rowBuf[i], sd.data, err = rowenc.EncDatumFromBuffer(
			sd.typing[i].Type, sd.typing[i].Encoding, sd.data,
		)
		if err != nil {
			// Reset sd because it is no longer usable.
			*sd = StreamDecoder{}
			return nil, nil, err
		}
	}
	return rowBuf, nil, nil
}

// Types returns the types of the columns; can only be used after we received at
// least one row.
func (sd *StreamDecoder) Types() []*types.T {
	types := make([]*types.T, len(sd.typing))
	for i := range types {
		types[i] = sd.typing[i].Type
	}
	return types
}
