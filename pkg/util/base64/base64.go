// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base64

import (
	"bytes"
	"encoding/base64"
)

// Encoder is a streaming encoder for base64 strings. It must be initialized
// with Init.
//
// The code has been adapted from the standard library encoder that is returned
// by base64.NewEncoder. The main differences are that Init allows for an
// Encoder value to be initialized directly and that it writes to an internal
// strings.Builder instead of an io.Writer. Both allow streaming encoding
// without extra allocations. Writes to a strings.Builder never fail, so error
// handling has been removed for simplification.
type Encoder struct {
	enc  *base64.Encoding
	sb   bytes.Buffer
	buf  [3]byte    // buffered data waiting to be encoded
	nbuf int8       // number of bytes in buf
	out  [1024]byte // output buffer
}

// Init initializes an Encoder with the given base64.Encoding.
func (e *Encoder) Init(enc *base64.Encoding) {
	*e = Encoder{
		enc: enc,
	}
}

// Write encodes p in an internal buffer.
func (e *Encoder) Write(p []byte) {
	// Leading fringe.
	if e.nbuf > 0 {
		var i int
		for i = 0; i < len(p) && e.nbuf < 3; i++ {
			e.buf[e.nbuf] = p[i]
			e.nbuf++
		}
		p = p[i:]
		if e.nbuf < 3 {
			return
		}
		e.enc.Encode(e.out[:], e.buf[:])
		_, _ = e.sb.Write(e.out[:4])
		e.nbuf = 0
	}

	// Large interior chunks.
	for len(p) >= 3 {
		nn := len(e.out) / 4 * 3
		if nn > len(p) {
			nn = len(p)
			nn -= nn % 3
		}
		e.enc.Encode(e.out[:], p[:nn])
		_, _ = e.sb.Write(e.out[0 : nn/3*4])
		p = p[nn:]
	}

	// Trailing fringe.
	copy(e.buf[:], p)
	e.nbuf = int8(len(p))
}

// String flushes any pending output from the encoder and returns the encoded
// string. The Encoder is reset to its initial state so it can be reused.
func (e *Encoder) String() (s string) {
	// If there's anything left in the buffer, flush it out.
	if e.nbuf > 0 {
		e.enc.Encode(e.out[:], e.buf[:e.nbuf])
		_, _ = e.sb.Write(e.out[:e.enc.EncodedLen(int(e.nbuf))])
		e.nbuf = 0
	}
	s = e.sb.String()
	e.Init(e.enc)
	return s
}
