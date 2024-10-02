// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tokenizer

import "unicode/utf8"

// buffer, is very much like bytes.Buffer, but is simpler. In particular,
// because we don't need to support entirety of bytes.Buffer interface, the
// functions (s.a. AppendByte) are simpler, and as a result can be inlined.
type buffer struct {
	buf []byte // We maintain an invariant that len(buf) == cap(buf).
	l   int    // Current length.
}

// Reset resets the buffer for next use.
func (b *buffer) Reset() {
	b.l = 0
}

// Bytes returns byte slice previously written.
// NB: We do not make a copy here.  Returned buffer valid
// until next call to buffer.
func (b *buffer) Bytes() []byte {
	return b.buf[:b.l]
}

// AppendByte appends byte to this buffer
func (b *buffer) AppendByte(c byte) {
	if cap(b.buf)-b.l <= 1 {
		b.buf = grow(b.buf, 1)
	}
	b.buf[b.l] = c
	b.l++
}

// Append appends buf to this buffer.
func (b *buffer) Append(buf []byte) {
	if cap(b.buf)-b.l <= len(buf) {
		b.buf = grow(b.buf, len(buf))
	}
	copy(b.buf[b.l:], buf)
	b.l += len(buf)
}

// AppendRune writes rune to this buffer.
func (b *buffer) AppendRune(r rune) {
	// Compare as uint32 to correctly handle negative runes.
	if uint32(r) < utf8.RuneSelf {
		b.AppendByte(byte(r))
		return
	}

	if cap(b.buf)-b.l <= utf8.UTFMax {
		b.buf = grow(b.buf, utf8.UTFMax)
	}

	n := utf8.EncodeRune(b.buf[b.l:b.l+utf8.UTFMax], r)
	b.l += n
}

// smallBufferSize is an initial allocation minimal capacity.
const smallBufferSize = 64

// grow increases capacity of buf by at least n, and returns new buffer,
// containing a copy of buf.
func grow(buf []byte, n int) []byte {
	if n < smallBufferSize {
		n = smallBufferSize
	}

	if buf == nil {
		return make([]byte, n)
	}

	// Cribbed from bytes.Buffer.
	c := len(buf) + n // ensure enough space for n elements
	if c < 2*cap(buf) {
		c = 2 * cap(buf)
	}

	nb := append([]byte(nil), make([]byte, c)...)
	copy(nb, buf)
	// Make whole cap available since append may produce larger slice.
	return nb[:cap(nb)]
}
