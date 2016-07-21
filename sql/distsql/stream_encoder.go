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
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/pkg/errors"
)

// StreamEncoder converts EncDatum rows into a sequence of StreamMessage.
//
// Sample usage:
//   se := StreamEncoder{}
//
//   for {
//       for ... {
//          err := se.AddRow(...)
//          ...
//       }
//       msg := se.FormMessage(false, nil)
//       // Send out message.
//       ...
//   }
//   msg := se.FormMessage(true, nil)
//   // Send out final message
//   ...
type StreamEncoder struct {
	// infos is initialized when the first row is received.
	infos []DatumInfo

	rowBuf []byte

	firstMessageDone bool
	alloc            sqlbase.DatumAlloc

	// Preallocated structures to avoid allocations.
	msg    StreamMessage
	msgHdr StreamHeader
	msgTrl StreamTrailer
}

func (se *StreamEncoder) setHeaderFields(flowID FlowID, streamID StreamID) {
	se.msgHdr.FlowID = flowID
	se.msgHdr.StreamID = streamID
}

// AddRow encodes a row.
func (se *StreamEncoder) AddRow(row sqlbase.EncDatumRow) error {
	if se.infos == nil {
		// First row. Initialize encodings.
		se.infos = make([]DatumInfo, len(row))
		for i := range row {
			enc, ok := row[i].Encoding()
			if !ok {
				enc = preferredEncoding
			}
			se.infos[i].Encoding = enc
			se.infos[i].Type = row[i].Type
		}
	}
	if len(se.infos) != len(row) {
		return errors.Errorf("inconsistent row length: had %d, now %d",
			len(se.infos), len(row))
	}
	for i := range row {
		var err error
		se.rowBuf, err = row[i].Encode(&se.alloc, se.infos[i].Encoding, se.rowBuf)
		if err != nil {
			return err
		}
	}
	return nil
}

// FormMessage populates a message containing the rows added since the last call
// to FormMessage. The returned StreamMessage should be treated as immutable. If
// final is true, a message trailer is populated with the given error.
func (se *StreamEncoder) FormMessage(final bool, trailerErr error) *StreamMessage {
	msg := &se.msg
	msg.Header = nil
	msg.Data.RawBytes = se.rowBuf
	msg.Trailer = nil
	if !se.firstMessageDone {
		if se.infos != nil {
			msg.Header = &se.msgHdr
			msg.Header.Info = se.infos
		} else {
			if !final {
				panic("trying to form non-final message with no rows")
			}
		}
	}
	if final {
		msg.Trailer = &se.msgTrl
		msg.Trailer.Error = roachpb.NewError(trailerErr)
	}
	se.rowBuf = se.rowBuf[:0]
	se.firstMessageDone = true
	return msg
}
