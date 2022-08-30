// Copyright 2017 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s2

import (
	"encoding/binary"
	"io"
)

const (
	// encodingVersion is the current version of the encoding
	// format that is compatible with C++ and other S2 libraries.
	encodingVersion = int8(1)

	// encodingCompressedVersion is the current version of the
	// compressed format.
	encodingCompressedVersion = int8(4)
)

// encoder handles the specifics of encoding for S2 types.
type encoder struct {
	w   io.Writer // the real writer passed to Encode
	err error
}

func (e *encoder) writeUvarint(x uint64) {
	if e.err != nil {
		return
	}
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], x)
	_, e.err = e.w.Write(buf[:n])
}

func (e *encoder) writeBool(x bool) {
	if e.err != nil {
		return
	}
	var val int8
	if x {
		val = 1
	}
	e.err = binary.Write(e.w, binary.LittleEndian, val)
}

func (e *encoder) writeInt8(x int8) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.w, binary.LittleEndian, x)
}

func (e *encoder) writeInt16(x int16) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.w, binary.LittleEndian, x)
}

func (e *encoder) writeInt32(x int32) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.w, binary.LittleEndian, x)
}

func (e *encoder) writeInt64(x int64) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.w, binary.LittleEndian, x)
}

func (e *encoder) writeUint8(x uint8) {
	if e.err != nil {
		return
	}
	_, e.err = e.w.Write([]byte{x})
}

func (e *encoder) writeUint32(x uint32) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.w, binary.LittleEndian, x)
}

func (e *encoder) writeUint64(x uint64) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.w, binary.LittleEndian, x)
}

func (e *encoder) writeFloat32(x float32) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.w, binary.LittleEndian, x)
}

func (e *encoder) writeFloat64(x float64) {
	if e.err != nil {
		return
	}
	e.err = binary.Write(e.w, binary.LittleEndian, x)
}

type byteReader interface {
	io.Reader
	io.ByteReader
}

// byteReaderAdapter embellishes an io.Reader with a ReadByte method,
// so that it implements the io.ByteReader interface.
type byteReaderAdapter struct {
	io.Reader
}

func (b byteReaderAdapter) ReadByte() (byte, error) {
	buf := []byte{0}
	_, err := io.ReadFull(b, buf)
	return buf[0], err
}

func asByteReader(r io.Reader) byteReader {
	if br, ok := r.(byteReader); ok {
		return br
	}
	return byteReaderAdapter{r}
}

type decoder struct {
	r   byteReader // the real reader passed to Decode
	err error
}

func (d *decoder) readBool() (x bool) {
	if d.err != nil {
		return
	}
	var val int8
	d.err = binary.Read(d.r, binary.LittleEndian, &val)
	return val == 1
}

func (d *decoder) readInt8() (x int8) {
	if d.err != nil {
		return
	}
	d.err = binary.Read(d.r, binary.LittleEndian, &x)
	return
}

func (d *decoder) readInt16() (x int16) {
	if d.err != nil {
		return
	}
	d.err = binary.Read(d.r, binary.LittleEndian, &x)
	return
}

func (d *decoder) readInt32() (x int32) {
	if d.err != nil {
		return
	}
	d.err = binary.Read(d.r, binary.LittleEndian, &x)
	return
}

func (d *decoder) readInt64() (x int64) {
	if d.err != nil {
		return
	}
	d.err = binary.Read(d.r, binary.LittleEndian, &x)
	return
}

func (d *decoder) readUint8() (x uint8) {
	if d.err != nil {
		return
	}
	x, d.err = d.r.ReadByte()
	return
}

func (d *decoder) readUint32() (x uint32) {
	if d.err != nil {
		return
	}
	d.err = binary.Read(d.r, binary.LittleEndian, &x)
	return
}

func (d *decoder) readUint64() (x uint64) {
	if d.err != nil {
		return
	}
	d.err = binary.Read(d.r, binary.LittleEndian, &x)
	return
}

func (d *decoder) readFloat32() (x float32) {
	if d.err != nil {
		return
	}
	d.err = binary.Read(d.r, binary.LittleEndian, &x)
	return
}

func (d *decoder) readFloat64() (x float64) {
	if d.err != nil {
		return
	}
	d.err = binary.Read(d.r, binary.LittleEndian, &x)
	return
}

func (d *decoder) readUvarint() (x uint64) {
	if d.err != nil {
		return
	}
	x, d.err = binary.ReadUvarint(d.r)
	return
}
