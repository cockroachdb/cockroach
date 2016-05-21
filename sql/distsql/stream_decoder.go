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
	"github.com/cockroachdb/cockroach/util"
)

// StreamDecoder converts a sequence of StreamMessage to EncDatum rows.
type StreamDecoder struct {
	info            []DatumInfo
	data            []byte
	trailerReceived bool
	trailerErr      error
}

// AddMessage adds the data in a StreamMessage to the decoder.
func (sd *StreamDecoder) AddMessage(msg *StreamMessage) error {
	if sd.trailerReceived {
		return util.Errorf("message after trailer was received")
	}
	if msg.Header != nil {
		if sd.info != nil {
			return util.Errorf("received multiple headers")
		}
		sd.info = msg.Header.Info
	} else {
		if sd.info == nil {
			if msg.Trailer == nil {
				return util.Errorf("first message doesn't have header or trailer")
			}
			if len(msg.Data.RawBytes) != 0 {
				return util.Errorf("first and final message doesn't have header but has data")
			}
		}
	}
	if len(msg.Data.RawBytes) > 0 {
		if len(sd.data) == 0 {
			sd.data = msg.Data.RawBytes
		} else {
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
		rowBuf = make(sqlbase.EncDatumRow, rowLen)
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
// received. Any error in the trailer is returned as well.
func (sd *StreamDecoder) IsDone() (bool, error) {
	if len(sd.data) > 0 || !sd.trailerReceived {
		return false, nil
	}
	return true, sd.trailerErr
}
