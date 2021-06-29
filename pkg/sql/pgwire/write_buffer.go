// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// writeBuffer is a wrapper around bytes.Buffer that provides a convenient interface
// for writing PGWire results. The buffer preserves any errors it encounters when writing,
// and will turn all subsequent write attempts into no-ops until finishMsg is called.
type writeBuffer struct {
	_ util.NoCopy

	wrapped bytes.Buffer

	// scratch will be used to store temporary data when serializing array/tuple
	scratch bytes.Buffer

	// usingScratch will be used to keep track of which buffer we are currently using
	// i.e either wrapped or scratch
	usingScratch bool

	err     error

	// Buffer used for temporary storage.
	putbuf [64]byte

	textFormatter   *tree.FmtCtx
	simpleFormatter *tree.FmtCtx

	// bytecount counts the number of bytes written across all pgwire connections, not just this
	// buffer. This is passed in so that finishMsg can track all messages we've sent to a network
	// socket, reducing the onus on the many callers of finishMsg.
	bytecount *metric.Counter
}

func newWriteBuffer(bytecount *metric.Counter) *writeBuffer {
	b := new(writeBuffer)
	b.init(bytecount)
	return b
}

// init exists to avoid the allocation imposed by newWriteBuffer.
func (b *writeBuffer) init(bytecount *metric.Counter) {
	b.bytecount = bytecount
	b.textFormatter = tree.NewFmtCtx(tree.FmtPgwireText)
	b.simpleFormatter = tree.NewFmtCtx(tree.FmtSimple)
	b.usingScratch = false
}

// Write implements the io.Write interface.
func (b *writeBuffer) Write(p []byte) (int, error) {
	b.write(p)
	return len(p), b.err
}

func (b *writeBuffer) writeByte(c byte) {
	if b.err == nil {
		b.err = b.wrapped.WriteByte(c)
	}
}

func (b *writeBuffer) write(p []byte) {
	if b.err == nil {
		_, b.err = b.wrapped.Write(p)
	}
}

func (b *writeBuffer) writeString(s string) {
	if b.err == nil {
		_, b.err = b.wrapped.WriteString(s)
	}
}

func (b *writeBuffer) nullTerminate() {
	if b.err == nil {
		b.err = b.wrapped.WriteByte(0)
	}
}

// WriteFromFmtCtx writes the current contents of
// the given formatter with a length prefix.
// The function resets the contents of the formatter.
func (b *writeBuffer) writeFromFmtCtx(fmtCtx *tree.FmtCtx) {
	if b.err == nil {
		b.putInt32(int32(fmtCtx.Buffer.Len()))

		// bytes.Buffer.WriteTo resets the Buffer.
		_, b.err = fmtCtx.Buffer.WriteTo(&b.wrapped)
	}
}

// writeLengthPrefixedBuffer writes the contents of a bytes.Buffer with a
// length prefix.
func (b *writeBuffer) writeLengthPrefixedBuffer(buf *bytes.Buffer) {
	if b.err == nil {
		b.putInt32(int32(buf.Len()))

		// bytes.Buffer.WriteTo resets the Buffer.
		_, b.err = buf.WriteTo(&b.wrapped)
	}
}

// writeLengthPrefixedString writes a length-prefixed string. The
// length is encoded as an int32.
func (b *writeBuffer) writeLengthPrefixedString(s string) {
	b.putInt32(int32(len(s)))
	b.writeString(s)
}

// writeLengthPrefixedDatum writes a length-prefixed Datum in its
// string representation. The length is encoded as an int32.
func (b *writeBuffer) writeLengthPrefixedDatum(d tree.Datum) {
	b.simpleFormatter.FormatNode(d)
	b.writeFromFmtCtx(b.simpleFormatter)
}

// writeTerminatedString writes a null-terminated string.
func (b *writeBuffer) writeTerminatedString(s string) {
	b.writeString(s)
	b.nullTerminate()
}

func (b *writeBuffer) putInt16(v int16) {
	if b.err == nil {
		binary.BigEndian.PutUint16(b.putbuf[:], uint16(v))
		_, b.err = b.wrapped.Write(b.putbuf[:2])
	}
}

func (b *writeBuffer) putInt32(v int32) {
	if b.err == nil {
		binary.BigEndian.PutUint32(b.putbuf[:], uint32(v))
		_, b.err = b.wrapped.Write(b.putbuf[:4])
	}
}

func (b *writeBuffer) putInt64(v int64) {
	if b.err == nil {
		binary.BigEndian.PutUint64(b.putbuf[:], uint64(v))
		_, b.err = b.wrapped.Write(b.putbuf[:8])
	}
}

func (b *writeBuffer) putErrFieldMsg(field pgwirebase.ServerErrFieldType) {
	if b.err == nil {
		b.err = b.wrapped.WriteByte(byte(field))
	}
}

func (b *writeBuffer) writeTuple(ctx context.Context, v *tree.DTuple, sessionLoc *time.Location, t *types.T){
	// initialState will be used to decide whether to swap the wrapped and the scratch buffer
	initialState := b.usingScratch
	if b.usingScratch == false{
		temp := b.wrapped
		b.wrapped = b.scratch
		b.scratch = temp
		b.usingScratch = true
	}

	// Put the number of datums.
	b.putInt32(int32(len(v.D)))
	tupleTypes := t.TupleContents()
	for i, elem := range v.D {
		oid := tupleTypes[i].Oid()
		b.putInt32(int32(oid))
		b.writeBinaryDatum(ctx, elem, sessionLoc, tupleTypes[i])
	}

	if initialState == false{

		// We are done with recursive calls, copy scratch buffer to wrapped buffer and reset it
		temp := b.wrapped
		b.wrapped = b.scratch
		b.scratch = temp
		b.writeLengthPrefixedBuffer(&b.scratch)
		b.scratch.Reset()
		b.usingScratch = false
	}
}

func (b *writeBuffer) writeArray(ctx context.Context, v *tree.DArray, sessionLoc *time.Location, t *types.T){
	if v.ParamTyp.Family() == types.ArrayFamily {
		b.setError(unimplemented.NewWithIssueDetail(32552,
		"binenc", "unsupported binary serialization of multidimensional arrays"))
		return
	}

	// Look at writeTuple for more info on initialState
	initialState := b.usingScratch
	if b.usingScratch == false{
		temp := b.wrapped
		b.wrapped = b.scratch
		b.scratch = temp
		b.usingScratch = true
	}

	// Put the number of dimensions. We currently support 1d arrays only.
	var ndims int32 = 1
	if v.Len() == 0 {
		ndims = 0
	}
	b.putInt32(ndims)
	hasNulls := 0
	if v.HasNulls {
		hasNulls = 1
	}
	oid := v.ParamTyp.Oid()
	b.putInt32(int32(hasNulls))
	b.putInt32(int32(oid))
	if v.Len() > 0 {
		b.putInt32(int32(v.Len()))
		// Lower bound, we only support a lower bound of 1.
		b.putInt32(1)
		for _, elem := range v.Array {
			b.writeBinaryDatum(ctx, elem, sessionLoc, v.ParamTyp)
		}
	}

	if initialState == false{
		temp := b.wrapped
		b.wrapped = b.scratch
		b.scratch = temp
		b.writeLengthPrefixedBuffer(&b.scratch)
		b.scratch.Reset()
		b.usingScratch = false
	}
}

func (b *writeBuffer) reset() {
	b.wrapped.Reset()
	b.err = nil
}

// initMsg begins writing a message into the writeBuffer with the provided type.
func (b *writeBuffer) initMsg(typ pgwirebase.ServerMessageType) {
	b.reset()
	b.putbuf[0] = byte(typ)
	_, b.err = b.wrapped.Write(b.putbuf[:5]) // message type + message length
}

// finishMsg attempts to write the data it has accumulated to the provided io.Writer.
// If the writeBuffer previously encountered an error since the last call to initMsg,
// or if it encounters an error while writing to w, it will return an error.
func (b *writeBuffer) finishMsg(w io.Writer) error {
	defer b.reset()
	if b.err != nil {
		return b.err
	}
	bytes := b.wrapped.Bytes()
	binary.BigEndian.PutUint32(bytes[1:5], uint32(b.wrapped.Len()-1))
	n, err := w.Write(bytes)
	b.bytecount.Inc(int64(n))
	return err
}

// setError sets the writeBuffer's error, if it does not already have one.
func (b *writeBuffer) setError(err error) {
	if b.err == nil {
		b.err = err
	}
}
