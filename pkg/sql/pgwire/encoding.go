// Copyright 2015 The Cockroach Authors.
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
// Author: Ben Darnell

package pgwire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/pkg/errors"
)

const maxMessageSize = 1 << 24

var _ bufferedReader = &bufio.Reader{}
var _ bufferedReader = &bytes.Buffer{}

type bufferedReader interface {
	io.Reader
	ReadString(delim byte) (string, error)
	ReadByte() (byte, error)
}

type readBuffer struct {
	msg []byte
	tmp [4]byte
}

// reset sets b.msg to exactly size, attempting to use spare capacity
// at the end of the existing slice when possible and allocating a new
// slice when necessary.
func (b *readBuffer) reset(size int) {
	if b.msg != nil {
		b.msg = b.msg[len(b.msg):]
	}

	if cap(b.msg) >= size {
		b.msg = b.msg[:size]
		return
	}

	allocSize := size
	if allocSize < 4096 {
		allocSize = 4096
	}
	b.msg = make([]byte, size, allocSize)
}

// readUntypedMsg reads a length-prefixed message. It is only used directly
// during the authentication phase of the protocol; readTypedMsg is
// used at all other times. This returns the number of bytes read and an error,
// if there was one. The number of bytes returned can be non-zero even with an
// error (e.g. if data was read but didn't validate) so that we can more
// accurately measure network traffic.
func (b *readBuffer) readUntypedMsg(rd io.Reader) (int, error) {
	nread, err := io.ReadFull(rd, b.tmp[:])
	if err != nil {
		return nread, err
	}
	size := int(binary.BigEndian.Uint32(b.tmp[:]))
	// size includes itself.
	size -= 4
	if size > maxMessageSize || size < 0 {
		return nread, errors.Errorf("message size %d out of bounds (0..%d)",
			size, maxMessageSize)
	}

	b.reset(size)
	n, err := io.ReadFull(rd, b.msg)
	return nread + n, err
}

// readTypedMsg reads a message from the provided reader, returning its type code and body.
// It returns the message type, number of bytes read, and an error if there was one.
func (b *readBuffer) readTypedMsg(rd bufferedReader) (clientMessageType, int, error) {
	typ, err := rd.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	n, err := b.readUntypedMsg(rd)
	return clientMessageType(typ), n, err
}

// getString reads a null-terminated string.
func (b *readBuffer) getString() (string, error) {
	pos := bytes.IndexByte(b.msg, 0)
	if pos == -1 {
		return "", errors.Errorf("NUL terminator not found")
	}
	// Note: this is a conversion from a byte slice to a string which avoids
	// allocation and copying. It is safe because we never reuse the bytes in our
	// read buffer. It is effectively the same as: "s := string(b.msg[:pos])"
	s := b.msg[:pos]
	b.msg = b.msg[pos+1:]
	return *((*string)(unsafe.Pointer(&s))), nil
}

func (b *readBuffer) getPrepareType() (prepareType, error) {
	v, err := b.getBytes(1)
	if err != nil {
		return 0, err
	}
	return prepareType(v[0]), nil
}

func (b *readBuffer) getBytes(n int) ([]byte, error) {
	if len(b.msg) < n {
		return nil, errors.Errorf("insufficient data: %d", len(b.msg))
	}
	v := b.msg[:n]
	b.msg = b.msg[n:]
	return v, nil
}

func (b *readBuffer) getUint16() (uint16, error) {
	if len(b.msg) < 2 {
		return 0, errors.Errorf("insufficient data: %d", len(b.msg))
	}
	v := binary.BigEndian.Uint16(b.msg[:2])
	b.msg = b.msg[2:]
	return v, nil
}

func (b *readBuffer) getUint32() (uint32, error) {
	if len(b.msg) < 4 {
		return 0, errors.Errorf("insufficient data: %d", len(b.msg))
	}
	v := binary.BigEndian.Uint32(b.msg[:4])
	b.msg = b.msg[4:]
	return v, nil
}

// writeBuffer is a wrapper around bytes.Buffer that provides a convenient interface
// for writing PGWire results. The buffer preserves any errors it encounters when writing,
// and will turn all subsequent write attempts into no-ops until finishMsg is called.
type writeBuffer struct {
	wrapped bytes.Buffer
	err     error
	putbuf  [64]byte

	// bytecount counts the number of bytes written across all pgwire connections, not just this
	// buffer. This is passed in so that finishMsg can track all messages we've sent to a network
	// socket, reducing the onus on the many callers of finishMsg.
	bytecount *metric.Counter
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

// writeString writes a length-prefixed string. The length is encoded as
// an int32.
func (b *writeBuffer) writeLengthPrefixedString(s string) {
	b.putInt32(int32(len(s)))
	b.writeString(s)
}

// writeString writes a null-terminated string.
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

func (b *writeBuffer) putErrFieldMsg(field serverErrFieldType) {
	if b.err == nil {
		b.err = b.wrapped.WriteByte(byte(field))
	}
}

func (b *writeBuffer) reset() {
	b.wrapped.Reset()
	b.err = nil
}

// initMsg begins writing a message into the writeBuffer with the provided type.
func (b *writeBuffer) initMsg(typ serverMessageType) {
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
