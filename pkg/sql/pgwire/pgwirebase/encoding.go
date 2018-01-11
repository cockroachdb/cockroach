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

package pgwirebase

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/pkg/errors"
)

const maxMessageSize = 1 << 24

var _ BufferedReader = &bufio.Reader{}
var _ BufferedReader = &bytes.Buffer{}

// BufferedReader extended io.Reader with some convenience methods.
type BufferedReader interface {
	io.Reader
	ReadString(delim byte) (string, error)
	ReadByte() (byte, error)
}

// ReadBuffer provides a convenient way to read pgwire protocol messages.
type ReadBuffer struct {
	Msg []byte
	tmp [4]byte
}

// reset sets b.Msg to exactly size, attempting to use spare capacity
// at the end of the existing slice when possible and allocating a new
// slice when necessary.
func (b *ReadBuffer) reset(size int) {
	if b.Msg != nil {
		b.Msg = b.Msg[len(b.Msg):]
	}

	if cap(b.Msg) >= size {
		b.Msg = b.Msg[:size]
		return
	}

	allocSize := size
	if allocSize < 4096 {
		allocSize = 4096
	}
	b.Msg = make([]byte, size, allocSize)
}

// ReadUntypedMsg reads a length-prefixed message. It is only used directly
// during the authentication phase of the protocol; readTypedMsg is used at all
// other times. This returns the number of bytes read and an error, if there
// was one. The number of bytes returned can be non-zero even with an error
// (e.g. if data was read but didn't validate) so that we can more accurately
// measure network traffic.
func (b *ReadBuffer) ReadUntypedMsg(rd io.Reader) (int, error) {
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
	n, err := io.ReadFull(rd, b.Msg)
	return nread + n, err
}

// ReadTypedMsg reads a message from the provided reader, returning its type code and body.
// It returns the message type, number of bytes read, and an error if there was one.
func (b *ReadBuffer) ReadTypedMsg(rd BufferedReader) (ClientMessageType, int, error) {
	typ, err := rd.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	n, err := b.ReadUntypedMsg(rd)
	return ClientMessageType(typ), n, err
}

// GetString reads a null-terminated string.
func (b *ReadBuffer) GetString() (string, error) {
	pos := bytes.IndexByte(b.Msg, 0)
	if pos == -1 {
		return "", errors.Errorf("NUL terminator not found")
	}
	// Note: this is a conversion from a byte slice to a string which avoids
	// allocation and copying. It is safe because we never reuse the bytes in our
	// read buffer. It is effectively the same as: "s := string(b.Msg[:pos])"
	s := b.Msg[:pos]
	b.Msg = b.Msg[pos+1:]
	return *((*string)(unsafe.Pointer(&s))), nil
}

// GetPrepareType returns the buffer's contents as a PrepareType.
func (b *ReadBuffer) GetPrepareType() (PrepareType, error) {
	v, err := b.GetBytes(1)
	if err != nil {
		return 0, err
	}
	return PrepareType(v[0]), nil
}

// GetBytes returns the buffer's contents as a []byte.
func (b *ReadBuffer) GetBytes(n int) ([]byte, error) {
	if len(b.Msg) < n {
		return nil, errors.Errorf("insufficient data: %d", len(b.Msg))
	}
	v := b.Msg[:n]
	b.Msg = b.Msg[n:]
	return v, nil
}

// GetUint16 returns the buffer's contents as a uint16.
func (b *ReadBuffer) GetUint16() (uint16, error) {
	if len(b.Msg) < 2 {
		return 0, errors.Errorf("insufficient data: %d", len(b.Msg))
	}
	v := binary.BigEndian.Uint16(b.Msg[:2])
	b.Msg = b.Msg[2:]
	return v, nil
}

// GetUint32 returns the buffer's contents as a uint32.
func (b *ReadBuffer) GetUint32() (uint32, error) {
	if len(b.Msg) < 4 {
		return 0, errors.Errorf("insufficient data: %d", len(b.Msg))
	}
	v := binary.BigEndian.Uint32(b.Msg[:4])
	b.Msg = b.Msg[4:]
	return v, nil
}

// NewUnrecognizedMsgTypeErr creates an error for an unrecognized pgwire
// message.
func NewUnrecognizedMsgTypeErr(typ ClientMessageType) error {
	return pgerror.NewErrorf(
		pgerror.CodeProtocolViolationError, "unrecognized client message type %v", typ)
}
