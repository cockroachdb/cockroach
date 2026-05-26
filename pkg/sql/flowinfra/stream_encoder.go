// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// PreferredEncoding is the encoding used for EncDatums that don't already have
// an encoding available.
const PreferredEncoding = catenumpb.DatumEncoding_ASCENDING_KEY

// StreamEncoder converts EncDatum rows into a sequence of ProducerMessage.
//
// Sample usage:
//
//	se := StreamEncoder{}
//
//	for {
//	    for ... {
//	       err := se.AddRow(...)
//	       ...
//	    }
//	    msg := se.FormMessage(nil)
//	    // Send out message.
//	    ...
//	}
type StreamEncoder struct {
	types []*types.T
	// encodings is fully initialized when the first row is received.
	encodings            []catenumpb.DatumEncoding
	encodingsInitialized bool

	rowBuf       []byte
	numEmptyRows int
	metadata     []execinfrapb.RemoteProducerMetadata

	// headerSent is set after the first message (which contains the header) has
	// been sent.
	headerSent bool
	// encodingSent is set after the first message that contains any rows has
	// been sent.
	encodingSent bool
	alloc        tree.DatumAlloc

	// Preallocated structures to avoid allocations.
	msg    execinfrapb.ProducerMessage
	msgHdr execinfrapb.ProducerHeader
}

// HasHeaderBeenSent returns whether the header has been sent.
func (se *StreamEncoder) HasHeaderBeenSent() bool {
	return se.headerSent
}

// SetHeaderFields sets the header fields.
func (se *StreamEncoder) SetHeaderFields(flowID execinfrapb.FlowID, streamID execinfrapb.StreamID) {
	se.msgHdr.FlowID = flowID
	se.msgHdr.StreamID = streamID
}

// Init initializes the encoder.
func (se *StreamEncoder) Init(types []*types.T) {
	se.types = types
	se.encodings = make([]catenumpb.DatumEncoding, len(types))
}

// AddMetadata encodes a metadata message. Unlike AddRow(), it cannot fail. This
// is important for the caller because a failure to encode a piece of metadata
// (particularly one that contains an error) would not be recoverable.
//
// Metadata records lose their ordering wrt the data rows. The convention is
// that the StreamDecoder will return them first, before the data rows, thus
// ensuring that rows produced _after_ an error are not received _before_ the
// error.
func (se *StreamEncoder) AddMetadata(ctx context.Context, meta execinfrapb.ProducerMetadata) {
	se.metadata = append(se.metadata, execinfrapb.LocalMetaToRemoteProducerMeta(ctx, meta))
}

// AddRow encodes a message.
func (se *StreamEncoder) AddRow(row rowenc.EncDatumRow) error {
	if se.encodings == nil {
		panic("Init not called")
	}
	if len(se.encodings) != len(row) {
		return errors.Errorf("inconsistent row length: expected %d, got %d", len(se.encodings), len(row))
	}
	if !se.encodingsInitialized {
		// First row. Initialize encodings.
		for i := range row {
			enc, ok := row[i].Encoding()
			if !ok {
				enc = PreferredEncoding
			}
			sType := se.types[i]
			if enc != catenumpb.DatumEncoding_VALUE &&
				(colinfo.CanHaveCompositeKeyEncoding(sType) || colinfo.MustBeValueEncoded(sType)) {
				// Force VALUE encoding for composite types (key encodings may lose data).
				enc = catenumpb.DatumEncoding_VALUE
			}
			se.encodings[i] = enc
		}
		se.encodingsInitialized = true
	}
	if len(row) == 0 {
		se.numEmptyRows++
		return nil
	}
	for i := range row {
		var err error
		se.rowBuf, err = row[i].Encode(se.types[i], &se.alloc, se.encodings[i], se.rowBuf)
		if err != nil {
			return err
		}
	}
	return nil
}

// FormMessage populates a message containing the rows added since the last call
// to FormMessage. The returned ProducerMessage should be treated as immutable.
func (se *StreamEncoder) FormMessage(ctx context.Context) *execinfrapb.ProducerMessage {
	msg := &se.msg
	msg.Header = nil
	msg.Data.RawBytes = se.rowBuf
	msg.Data.NumEmptyRows = int32(se.numEmptyRows)
	msg.Data.Metadata = make([]execinfrapb.RemoteProducerMetadata, len(se.metadata))
	copy(msg.Data.Metadata, se.metadata)
	se.metadata = se.metadata[:0]

	if !se.headerSent {
		msg.Header = &se.msgHdr
		se.headerSent = true
	}
	if !se.encodingSent {
		if se.encodingsInitialized {
			msg.Encoding = se.encodings
			se.encodingSent = true
		}
	} else {
		msg.Encoding = nil
	}

	se.rowBuf = se.rowBuf[:0]
	se.numEmptyRows = 0
	return msg
}
